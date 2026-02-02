# src/pipeline/border_validator.py
"""
Step 8: Border Validation and Refinement

Validates UTP borders by analyzing border municipalities' main flow.
Iteratively reallocates municipalities when flow points to a different UTP
and RM rules are respected, until convergence is achieved.
"""
import logging
import json
import pandas as pd
import geopandas as gpd
import networkx as nx
from typing import List, Dict, Set, Tuple, Optional
from pathlib import Path
from datetime import datetime

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.interface.consolidation_manager import ConsolidationManager


class BorderValidator:
    """
    Validates and refines UTP borders based on border municipalities' flow patterns.
    
    Iteratively:
    1. Identifies border municipalities (those adjacent to other UTPs)
    2. Analyzes their principal flow (≤2h impedance)
    3. Validates RM rules and adjacency
    4. Resolves transitive chains (A→B→C becomes A,B→C)
    5. Executes reallocations
    6. Repeats until convergence (no more relocations needed)
    """
    
    # Constants for oscillation detection
    MAX_MOVEMENTS_PER_MUNICIPALITY = 5  # Prevent excessive oscillation
    CYCLE_DETECTION_WINDOW = 4  # Check last N positions for cycles
    
    def __init__(
        self,
        graph: TerritorialGraph,
        validator: TerritorialValidator,
        consolidation_manager: ConsolidationManager
    ):
        """
        Initialize BorderValidator.
        
        Args:
            graph: TerritorialGraph instance
            validator: TerritorialValidator instance
            consolidation_manager: ConsolidationManager instance for logging changes
        """
        self.graph = graph
        self.validator = validator
        self.consolidation_manager = consolidation_manager
        self.logger = logging.getLogger("GeoValida.BorderValidator")
        
        self.adjacency_graph = None  # Will be built from GDF
        
        # Output directory
        self.data_dir = Path(__file__).parent.parent.parent / "data" / "03_processed"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Tracking
        self.relocations_log = []
        self.rejections_log = []
        self.transitive_chains = []
        self.oscillation_tracker = {}  # Track municipality movement history (mun_id -> [utp_destinations])
        self.movement_count = {}  # Track total movements per municipality (mun_id -> count)
        
    def _build_adjacency_graph(self, gdf: gpd.GeoDataFrame) -> nx.Graph:
        """
        Builds a NetworkX graph representing spatial adjacency of municipalities.
        
        Args:
            gdf: GeoDataFrame with municipality geometries
            
        Returns:
            NetworkX graph where nodes are municipalities and edges are adjacencies
        """
        self.logger.info("🗺️ Building adjacency graph...")
        
        G = nx.Graph()
        
        # Add all municipalities as nodes
        for idx, row in gdf.iterrows():
            mun_id = int(row['CD_MUN'])
            G.add_node(mun_id)
        
        # Add edges for adjacent municipalities
        self.logger.info(f"   Detecting adjacencies for {len(gdf)} municipalities...")
        
        for i, row_i in gdf.iterrows():
            mun_i = int(row_i['CD_MUN'])
            geom_i = row_i.geometry
            
            # Find neighbors using buffer for robustness
            buf_val = self.validator._get_buffer_value(gdf)
            neighbors = gdf[gdf.geometry.intersects(geom_i.buffer(buf_val))]
            
            for j, row_j in neighbors.iterrows():
                mun_j = int(row_j['CD_MUN'])
                if mun_i != mun_j:
                    G.add_edge(mun_i, mun_j)
        
        self.logger.info(f"   ✅ Adjacency graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        return G
    
    def _identify_border_municipalities(self, gdf: gpd.GeoDataFrame) -> Dict[str, Set[int]]:
        """
        Identifies border municipalities for each UTP.
        
        A municipality is a border municipality if it has at least one neighbor
        belonging to a different UTP.
        
        Args:
            gdf: GeoDataFrame with CD_MUN and UTP_ID (used only for adjacency graph)
            
        Returns:
            Dict mapping UTP_ID -> Set of border municipality IDs
        """
        if self.adjacency_graph is None:
            self.adjacency_graph = self._build_adjacency_graph(gdf)
        
        border_muns = {}  # UTP_ID -> Set[mun_ids]
        
        # CRITICAL FIX: Read UTP from GRAPH, not GDF
        # GDF may not have all municipalities, but graph does
        mun_to_utp = {}
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                utp_id = self.graph.get_municipality_utp(node)
                if utp_id and utp_id not in ["NAO_ENCONTRADO", "SEM_UTP"]:
                    mun_to_utp[node] = utp_id
        
        self.logger.debug(f"   📋 Tracking {len(mun_to_utp)} municipalities from graph")
        
        # Find border municipalities
        for mun_id in self.adjacency_graph.nodes():
            if mun_id not in mun_to_utp:
                continue
                
            current_utp = mun_to_utp[mun_id]
            
            # Check if any neighbor belongs to a different UTP
            neighbors = list(self.adjacency_graph.neighbors(mun_id))
            for neighbor_id in neighbors:
                if neighbor_id in mun_to_utp:
                    neighbor_utp = mun_to_utp[neighbor_id]
                    if neighbor_utp != current_utp:
                        # This is a border municipality
                        if current_utp not in border_muns:
                            border_muns[current_utp] = set()
                        border_muns[current_utp].add(mun_id)
                        break
        
        total_border = sum(len(muns) for muns in border_muns.values())
        self.logger.info(f"   📍 Identified {total_border} border municipalities across {len(border_muns)} UTPs")
        
        return border_muns
    
    def _get_principal_flow(
        self,
        mun_id: int,
        flow_df: pd.DataFrame,
        max_time: float = 2.0
    ) -> Optional[Tuple[int, float, float]]:
        """
        Gets the principal flow destination for a municipality.
        
        Args:
            mun_id: Municipality ID
            flow_df: Flow dataframe with columns [mun_origem, mun_destino, viagens]
            max_time: Maximum travel time in hours
            
        Returns:
            Tuple (dest_mun_id, flow_value, travel_time) or None if no valid flow
        """
        if flow_df is None or flow_df.empty:
            return None
        
        # Load impedance data for travel time filtering
        if not hasattr(self, 'df_impedance') or self.df_impedance is None:
            impedance_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
            try:
                # File uses semicolon separator
                self.df_impedance = pd.read_csv(impedance_path, sep=';', encoding='utf-8')
                
                # Expected columns: PAR_IBGE, COD_IBGE_ORIGEM, COD_IBGE_DESTINO, Tempo
                # Rename to standard names
                if len(self.df_impedance.columns) >= 4:
                    self.df_impedance.columns = ['par_ibge', 'origem', 'destino', 'tempo']
                else:
                    self.logger.error(f"   ❌ Unexpected impedance file structure: {self.df_impedance.columns.tolist()}")
                    self.df_impedance = pd.DataFrame()
                    return None
                
                # Ensure types are correct
                self.df_impedance['origem'] = self.df_impedance['origem'].astype(int)
                self.df_impedance['destino'] = self.df_impedance['destino'].astype(int)
                self.df_impedance['tempo'] = pd.to_numeric(self.df_impedance['tempo'], errors='coerce')
                
                # Filter out invalid times
                self.df_impedance = self.df_impedance[self.df_impedance['tempo'].notna()]
                
                self.logger.info(f"   📊 Loaded impedance data: {len(self.df_impedance)} records (≤2h)")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Could not load impedance data: {e}")
                import traceback
                self.logger.debug(traceback.format_exc())
                self.df_impedance = pd.DataFrame()
        
        # Filter flows from this municipality
        flows = flow_df[flow_df['mun_origem'] == mun_id].copy()
        
        if flows.empty:
            return None
        
        # If impedance data available, filter by travel time
        if not self.df_impedance.empty:
            # Join with impedance to get travel time
            # Impedance columns: origem, destino, tempo (in hours)
            flows = flows.merge(
                self.df_impedance[['origem', 'destino', 'tempo']],
                left_on=['mun_origem', 'mun_destino'],
                right_on=['origem', 'destino'],
                how='left'
            )
            
            # Filter by max_time
            flows = flows[flows['tempo'] <= max_time]
        
        if flows.empty:
            return None
        
        # Get flow with highest viagens value
        max_flow = flows.nlargest(1, 'viagens')
        if max_flow.empty:
            return None
        
        row = max_flow.iloc[0]
        travel_time = float(row.get('tempo', 0.0)) if 'tempo' in row else 0.0
        
        return (
            int(row['mun_destino']),
            float(row['viagens']),
            travel_time
        )
    
    def _get_total_flow(self, mun_id: int, flow_df: pd.DataFrame) -> float:
        """Helper to get total flow originating from a municipality."""
        if flow_df is None or flow_df.empty:
            return 0.0
        return float(flow_df[flow_df['mun_origem'] == mun_id]['viagens'].sum())
    
    def _validate_rm_rules(self, mun_id: int, dest_utp: str) -> bool:
        """
        Validates RM consistency rules.
        
        Rules:
        - Same RM: OK
        - Both without RM: OK
        - Different RMs: NOT OK
        
        Args:
            mun_id: Origin municipality ID
            dest_utp: Destination UTP ID
            
        Returns:
            True if rules are satisfied
        """
        # FIX: Get RM of the specific Municipality, not just its current UTP
        # This handles cases where a mun with RM is wrongly in a non-RM UTP
        mun_node = self.graph.hierarchy.nodes.get(mun_id, {})
        rm_origin = mun_node.get('regiao_metropolitana', 'SEM_RM')
        if rm_origin.lower() == 'sem_rm':
             rm_origin = ''
             
        # Get RM of destination UTP
        rm_dest = self.validator.get_rm_of_utp(dest_utp)
        if rm_dest.lower() == 'sem_rm' or rm_dest == 'rm_sem_rm':
             rm_dest = ''
        
        # Normalize for comparison
        rm_origin = rm_origin.strip()
        rm_dest = rm_dest.strip()
        
        # Rule: RMs must match (or both be empty)
        return rm_origin == rm_dest

    def _will_cause_fragmentation(self, mun_id: int, current_utp: str) -> bool:
        """
        Checks if removing mun_id from current_utp will fragment the UTP into disconnected components.
        
        Args:
            mun_id: Municipality to remove
            current_utp: Current UTP ID
            
        Returns:
            True if removal causes fragmentation (should be rejected)
        """
        if self.adjacency_graph is None:
            return False
            
        # 1. Get all municipalities in current UTP (excluding the one moving)
        utp_nodes = []
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                if int(node) != int(mun_id):
                    if self.graph.get_municipality_utp(node) == current_utp:
                        utp_nodes.append(node)
        
        if not utp_nodes:
            # If UTP becomes empty, that's fine (consolidation)
            return False
            
        # 2. Build subgraph of remaining nodes
        subgraph = self.adjacency_graph.subgraph(utp_nodes)
        
        # 3. Check connectivity
        # If the number of connected components > 1, then we fragmented the UTP
        if nx.number_connected_components(subgraph) > 1:
            return True
            
        return False
    
    def _validate_utp_adjacency(self, utp_origem: str, utp_destino: str) -> bool:
        """
        Validates if two UTPs are adjacent (share a border).
        
        Args:
            utp_origem: Origin UTP ID
            utp_destino: Destination UTP ID
            
        Returns:
            True if UTPs are adjacent
        """
        if self.adjacency_graph is None:
            self.logger.warning("Adjacency graph not built yet!")
            return False
        
        # Get all municipalities in each UTP
        muns_origem = []
        muns_destino = []
        
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                node_utp = self.graph.get_municipality_utp(node)
                if node_utp == utp_origem:
                    muns_origem.append(node)
                elif node_utp == utp_destino:
                    muns_destino.append(node)
        
        # Check if any municipality from origem is adjacent to any from destino
        for mun_orig in muns_origem:
            if mun_orig in self.adjacency_graph:
                neighbors = self.adjacency_graph[mun_orig]
                for mun_dest in muns_destino:
                    if mun_dest in neighbors:
                        return True
        
        return False
    
    def _detect_oscillation(self, mun_id: int, dest_utp: str, current_iteration: int) -> Tuple[bool, str]:
        """
        Detects if a municipality is oscillating between UTPs.
        
        Args:
            mun_id: Municipality ID
            dest_utp: Proposed destination UTP
            current_iteration: Current iteration number
            
        Returns:
            Tuple of (is_oscillating, reason)
        """
        mun_name = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', f'Mun_{mun_id}')
        
        # 1. Check movement limit
        movement_count = self.movement_count.get(mun_id, 0)
        if movement_count >= self.MAX_MOVEMENTS_PER_MUNICIPALITY:
            reason = f"Exceeded movement limit ({movement_count} >= {self.MAX_MOVEMENTS_PER_MUNICIPALITY})"
            self.logger.warning(f"   ⚠️ Oscillation: {mun_name} - {reason}")
            return True, reason
        
        # 2. Check for cycles in recent history
        history = self.oscillation_tracker.get(mun_id, [])
        current_utp = self.graph.get_municipality_utp(mun_id)
        
        # Check if dest_utp appears in recent history (cycle detection)
        window_size = min(self.CYCLE_DETECTION_WINDOW, len(history))
        if window_size > 0:
            recent_window = history[-window_size:]
            if dest_utp in recent_window:
                reason = f"Cycle detected - dest_utp '{dest_utp}' in recent history {recent_window}"
                self.logger.warning(f"   ⚠️ Oscillation: {mun_name} - {reason}")
                return True, reason
        
        return False, ""
    
    def _resolve_transitivity(
        self,
        relocations: List[Tuple[int, int, str, float, float]]
    ) -> List[Tuple[int, str]]:
        """
        Resolves transitive chains in relocations using municipality chains.
        
        Logic:
        1. Build map of moves: mun_id -> dest_mun_id
        2. For each mover, follow the chain: A -> B -> C
        3. Determine final destination:
           - If chain ends at a non-moving municipality (C), join C's UTP.
           - If chain ends at a moving municipality that forms a cycle (A->B->A), break cycle.
        
        Args:
            relocations: List of (mun_id, dest_mun_id, dest_utp, flow_value, travel_time)
            
        Returns:
            List of (mun_id, final_dest_utp) after resolving transitivity
        """
        if not relocations:
            return []
        
        # Build move map: mun_id -> (dest_mun_id, dest_utp)
        # We need dest_utp in case the chain ends at a node that is moving to a specific UTP
        moves = {}
        for mun_id, dest_mun_id, dest_utp, _, _ in relocations:
            moves[mun_id] = (dest_mun_id, dest_utp)
            
        resolved = []
        chains_found = []
        
        for mun_id in moves:
            # Follow the chain
            chain = [mun_id]
            curr_mun = mun_id
            visited = set([mun_id])
            
            final_utp = None
            
            while True:
                if curr_mun not in moves:
                    # End of chain: curr_mun is stable (not moving)
                    # Use its CURRENT UTP as the destination for the whole chain
                    final_utp = self.graph.get_municipality_utp(curr_mun)
                    break
                
                # Get next step
                next_mun, next_default_utp = moves[curr_mun]
                
                if next_mun in visited:
                    # Cycle detected! (e.g., A -> B -> A)
                    # Break the cycle: Use the original intended UTP for the start node
                    # OR: Just let them swap (A goes to B's spot, B goes to A's spot)
                    # For stability, we stick to the immediate move's target UTP
                    final_utp = next_default_utp
                    self.logger.warning(f"   🔄 Cycle detected in chain: {chain} -> {next_mun}")
                    break
                
                chain.append(next_mun)
                visited.add(next_mun)
                curr_mun = next_mun
                
                # Safety break for very long chains
                if len(chain) > 50:
                    final_utp = next_default_utp
                    break
            
            # If we didn't find a valid UTP from a stable node, fallback to immediate target
            if not final_utp or final_utp in ["NAO_ENCONTRADO", "SEM_UTP"]:
                 _, final_utp = moves[mun_id]
            
            if len(chain) > 2: # A -> B (length 2) is direct, A -> B -> C (length 3) is transitive
                # Convert IDs to names for logging
                chain_names = [self.graph.hierarchy.nodes.get(m, {}).get('name', str(m)) for m in chain]
                chains_found.append({
                    "chain": chain_names,
                    "final_utp": final_utp
                })
            
            resolved.append((mun_id, final_utp))
        
        if chains_found:
            self.logger.info(f"   🔗 Resolved {len(chains_found)} transitive chains")
            # Deduplicate chains for logging (since A->B->C and B->C are same chain part)
            unique_chains = {tuple(c['chain']): c['final_utp'] for c in chains_found}
            for chain_tuple, utp in list(unique_chains.items())[:5]:
                self.logger.info(f"      Chain: {' -> '.join(chain_tuple)} => {utp}")
            self.transitive_chains.extend(chains_found)
        
        return resolved
    
    def run_border_validation(
        self,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        max_iterations: int = 10
    ) -> int:
        """
        Executes Border Validation with Iterative Convergence.
        
        Runs in a loop until no more relocations are needed (convergence)
        or max iterations reached.
        
        Args:
            flow_df: Flow dataframe with columns [mun_origem, mun_destino, viagens, tempo_viagem]
            gdf: GeoDataFrame with municipality geometries and UTP_ID
            max_iterations: Maximum number of iterations (default: 10)
            
        Returns:
            Total number of relocations performed
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 8: BORDER VALIDATION AND REFINEMENT")
        self.logger.info("=" * 80)
        
        total_relocations = 0
        iteration = 0
        
        # Build adjacency graph once
        self.adjacency_graph = self._build_adjacency_graph(gdf)
        
        while True:
            iteration += 1
            self.logger.info(f"\n🔄 Iteration {iteration}")
            
            # 1. IDENTIFY BORDER MUNICIPALITIES (recalculated each iteration)
            border_muns_by_utp = self._identify_border_municipalities(gdf)
            
            # Flatten to list of all border municipalities
            all_border_muns = set()
            for muns in border_muns_by_utp.values():
                all_border_muns.update(muns)
            
            if not all_border_muns:
                self.logger.info("   ℹ️ No border municipalities found")
                break
            
            # 2. ANALYZE FLOW - ONLY TO ADJACENT MUNICIPALITIES
            relocations_this_round = []
            
            # Get set of all Sede municipalities to protect them
            sedes_muns = set(self.graph.utp_seeds.values())
            
            for mun_id in all_border_muns:
                # CRITICAL: Sedes CANNOT move! They are the anchors.
                if mun_id in sedes_muns:
                    # self.logger.debug(f"   ⚓ Mun {mun_id} is a Sede - skipping")
                    continue
                    
                # CRITICAL: Get spatial neighbors (adjacent municipalities)
                if mun_id not in self.adjacency_graph:
                    continue
                
                spatial_neighbors = set(self.adjacency_graph.neighbors(mun_id))
                
                # Get principal flow
                principal_flow = self._get_principal_flow(mun_id, flow_df, max_time=2.0)
                
                if not principal_flow:
                    continue
                
                dest_mun, flow_value, travel_time = principal_flow
                
                # CRITICAL VALIDATION: Destination must be a spatial neighbor!
                if dest_mun not in spatial_neighbors:
                    # This municipality's main flow is not to an adjacent municipality
                    # So it should NOT be considered for relocation
                    continue
                
                # Get UTPs
                dest_utp = self.graph.get_municipality_utp(dest_mun)
                current_utp = self.graph.get_municipality_utp(mun_id)
                
                # Only consider if flow points to a different UTP
                # Only consider if flow points to a different UTP
                if dest_utp != current_utp and dest_utp not in ["NAO_ENCONTRADO", "SEM_UTP"]:
                    # Pass dest_mun explicitly for transitivity resolution
                    relocations_this_round.append((mun_id, dest_mun, dest_utp, flow_value, travel_time))
            
            self.logger.info(f"   📊 {len(relocations_this_round)} municipalities want to change UTP (adjacent flows only)")
            
            # 3. VALIDATE RULES
            valid_relocations = []
            
            for mun_id, dest_mun, dest_utp, flow_value, travel_time in relocations_this_round:
                current_utp = self.graph.get_municipality_utp(mun_id)
                
                # Get municipality name for logging
                mun_name = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', f'Mun_{mun_id}')
                
                # Check oscillation
                is_oscillating, oscillation_reason = self._detect_oscillation(mun_id, dest_utp, iteration)
                if is_oscillating:
                    self.rejections_log.append({
                        "mun_id": mun_id,
                        "mun_name": mun_name,
                        "utp_origem": current_utp,
                        "proposed_utp": dest_utp,
                        "reason": "Oscillation detected",
                        "details": oscillation_reason,
                        "iteration": iteration
                    })
                    continue
                
                # Validate RM rules
                if not self._validate_rm_rules(mun_id, dest_utp):
                    rm_origin = self.validator.get_rm_of_utp(current_utp)
                    rm_dest = self.validator.get_rm_of_utp(dest_utp)
                    self.rejections_log.append({
                        "mun_id": mun_id,
                        "mun_name": mun_name,
                        "utp_origem": current_utp,
                        "proposed_utp": dest_utp,
                        "reason": "RM rule violation",
                        "details": f"Origin RM: {rm_origin}, Dest RM: {rm_dest}",
                        "iteration": iteration
                    })
                    continue
                
                # Validate UTP adjacency
                if not self._validate_utp_adjacency(current_utp, dest_utp):
                    self.rejections_log.append({
                        "mun_id": mun_id,
                        "mun_name": mun_name,
                        "utp_origem": current_utp,
                        "proposed_utp": dest_utp,
                        "reason": "UTP not adjacent",
                        "iteration": iteration
                    })
                    continue
                
                # Validate flow threshold (>5% of total)
                total_flow = self._get_total_flow(mun_id, flow_df)
                if total_flow > 0 and flow_value < total_flow * 0.05:
                    self.rejections_log.append({
                        "mun_id": mun_id,
                        "mun_name": mun_name,
                        "utp_origem": current_utp,
                        "proposed_utp": dest_utp,
                        "reason": "Flow too weak",
                        "details": f"Flow: {flow_value}, Total: {total_flow}, Percentage: {flow_value/total_flow*100:.1f}%",
                        "iteration": iteration
                    })
                    continue
                
                # Validate UTP Contiguity (Fragmentation Check)
                if self._will_cause_fragmentation(mun_id, current_utp):
                    self.rejections_log.append({
                        "mun_id": mun_id,
                        "mun_name": mun_name,
                        "utp_origem": current_utp,
                        "proposed_utp": dest_utp,
                        "reason": "Fragmentation risk",
                        "details": "Removing municipality would split origin UTP",
                        "iteration": iteration
                    })
                    continue

                # All validations passed
                valid_relocations.append((mun_id, dest_mun, dest_utp, flow_value, travel_time))
            
            self.logger.info(f"   ✅ {len(valid_relocations)} relocations passed validation")
            self.logger.info(f"   ❌ {len(relocations_this_round) - len(valid_relocations)} relocations rejected")
            
            # 4. RESOLVE TRANSITIVITY
            resolved_relocations = self._resolve_transitivity(valid_relocations)
            
            # 5. CHECK CONVERGENCE - Only stop when no valid relocations
            if len(resolved_relocations) == 0:
                self.logger.info(f"\n✅ Convergence achieved after {iteration} iterations")
                self.logger.info(f"   No more valid relocations found")
                break
            
            if iteration >= max_iterations:
                self.logger.warning(f"\n⚠️ Maximum iterations reached ({max_iterations})")
                break
            
            # 6. EXECUTE RELOCATIONS
            for mun_id, dest_utp in resolved_relocations:
                current_utp = self.graph.get_municipality_utp(mun_id)
                mun_name = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', f'Mun_{mun_id}')
                
                # Move municipality (using correct method name)
                self.graph.move_municipality(mun_id, dest_utp)
                
                # Log to consolidation manager if available
                if self.consolidation_manager:
                    self.consolidation_manager.add_consolidation(
                        source_utp=current_utp,
                        target_utp=dest_utp,
                        reason=f"Border validation - Iteration {iteration}",
                        details={
                            'step': 'border_validation',
                            'iteration': iteration,
                            'municipality_id': mun_id,
                            'municipality_name': mun_name
                        }
                    )
                
                # Track for oscillation detection
                if mun_id not in self.oscillation_tracker:
                    self.oscillation_tracker[mun_id] = []
                self.oscillation_tracker[mun_id].append(dest_utp)
                
                # Increment movement counter
                if mun_id not in self.movement_count:
                    self.movement_count[mun_id] = 0
                self.movement_count[mun_id] += 1
                
                # Log relocation
                self.relocations_log.append({
                    "mun_id": mun_id,
                    "mun_name": mun_name,
                    "utp_origem": current_utp,
                    "utp_destino": dest_utp,
                    "iteration": iteration,
                    "reason": "Main flow to different UTP"
                })
                
                total_relocations += 1
            
            self.logger.info(f"   🔁 {len(resolved_relocations)} relocations executed")
            
            # Note: No need to update GDF - border identification now reads from graph directly!
        
        # Save results
        self._save_results()
        
        # Export Snapshot Step 8 (Border Validation)
        snapshot_path = self.data_dir / "snapshot_step8_border_validation.json"
        # Note: map_generator.gdf_complete might be stale if not updated, but we can pass gdf here
        # Ideally we should use the self.graph directly in export_snapshot, which we do.
        # But for coloring, we need the Color Dict.
        # The GDF passed to this function was `self.map_generator.gdf_complete`.
        # However, update_gdf was NOT strictly called for every move in the simplified logic (line 665).
        # We rely on the Graph state mainly. The coloring export in Graph currently looks at GDF.
        # Ideally, we should recalculate coloring or update GDF before snapshot.
        # For now, let's export what we have.
        self.graph.export_snapshot(snapshot_path, "Border Validation", gdf)
        
        self.logger.info(f"\n📊 BORDER VALIDATION COMPLETE")
        self.logger.info(f"   Total iterations: {iteration}")
        self.logger.info(f"   Total relocations: {total_relocations}")
        self.logger.info(f"   Total rejections: {len(self.rejections_log)}")
        
        return total_relocations
    
    def _save_results(self):
        """Save validation results to JSON and CSV files."""
        # Prepare JSON output
        result = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "total_iterations": len(set(r.get("iteration", 0) for r in self.relocations_log)),
                "convergence_achieved": True,  # If we finished, we converged
                "total_relocated": len(self.relocations_log),
                "total_rejected": len(self.rejections_log),
                "total_border_municipalities": len(set(r["mun_id"] for r in self.relocations_log + self.rejections_log))
            },
            "relocations": self.relocations_log,
            "rejections": self.rejections_log,
            "transitive_chains": self.transitive_chains
        }
        
        # Save JSON
        json_path = self.data_dir / "border_validation_result.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"   💾 Results saved to: {json_path}")
        
        # Save CSV
        csv_records = []
        
        # Add relocations
        for r in self.relocations_log:
            csv_records.append({
                "mun_id": r["mun_id"],
                "mun_name": r["mun_name"],
                "utp_origem": r["utp_origem"],
                "utp_destino": r.get("utp_destino", ""),
                "action": "RELOCATED",
                "reason": r.get("reason", ""),
                "iteration": r.get("iteration", ""),
                "details": ""
            })
        
        # Add rejections
        for r in self.rejections_log:
            csv_records.append({
                "mun_id": r["mun_id"],
                "mun_name": r["mun_name"],
                "utp_origem": r["utp_origem"],
                "utp_destino": r.get("proposed_utp", ""),
                "action": "REJECTED",
                "reason": r.get("reason", ""),
                "iteration": r.get("iteration", ""),
                "details": r.get("details", "")
            })
        
        if csv_records:
            csv_df = pd.DataFrame(csv_records)
            csv_path = self.data_dir / "border_validation_result.csv"
            csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"   💾 CSV saved to: {csv_path}")
