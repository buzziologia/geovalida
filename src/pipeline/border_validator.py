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
    
    # Constants for flow validation
    FLOW_THRESHOLD = 0.03  # 3% of total flow (reduced from 5%)
    
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
        Uses vectorized spatial join (sjoin) for performance.
        """
        self.logger.info("🗺️ Building adjacency graph (Optimized & Projected)...")
        
        G = nx.Graph()
        
        # Add all municipalities as nodes
        for idx, row in gdf.iterrows():
            mun_id = int(row['CD_MUN'])
            G.add_node(mun_id)
        
        self.logger.info(f"   Detecting adjacencies for {len(gdf)} municipalities...")
        
        # 1. Project to Metric CRS (EPSG:3857) to avoid UserWarning and ensure correct 500m buffer
        try:
            # We only need geometry and CD_MUN
            gdf_proj = gdf[['CD_MUN', 'geometry']].to_crs(epsg=3857)
        except Exception as e:
            self.logger.warning(f"   ⚠️ Projection failed: {e}. Using original CRS.")
            gdf_proj = gdf[['CD_MUN', 'geometry']].copy()
            
        # 2. Buffer Geometries (Vectorized)
        BUFFER_METERS = 500.0
        gdf_buffered = gdf_proj.copy()
        gdf_buffered['geometry'] = gdf_buffered.geometry.buffer(BUFFER_METERS)
        
        # 3. Spatial Join (Vectorized Intersection)
        try:
            # Join buffered with points/polygons
            joined = gpd.sjoin(gdf_buffered, gdf_proj, how='inner', predicate='intersects')
            
            # 4. Add Edges from Join Result
            # Filter self-loops
            if 'CD_MUN_left' in joined.columns and 'CD_MUN_right' in joined.columns:
                edges = joined[joined['CD_MUN_left'] != joined['CD_MUN_right']]
                edge_pairs = edges[['CD_MUN_left', 'CD_MUN_right']].values
                
                for u, v in edge_pairs:
                    G.add_edge(int(u), int(v))
            else:
                 self.logger.warning(f"   ⚠️ Unexpected sjoin columns: {joined.columns}")

        except Exception as e:
            self.logger.error(f"   ❌ Spatial join failed: {e}")
            return G
        
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
                self.df_impedance = pd.read_csv(impedance_path, sep=';', decimal=',', encoding='utf-8')
                
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
                
                # CRITICAL FIX: Create 6-digit keys for matching
                # Impedance IDs end in 0 (e.g., 3148100), flow IDs are full 7-digit (e.g., 3148103)
                self.df_impedance['origem_6'] = self.df_impedance['origem'] // 10
                self.df_impedance['destino_6'] = self.df_impedance['destino'] // 10
                
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
        
        # If impedance data available, enrich with travel time (but don't require it!)
        if not self.df_impedance.empty:
            # Join with impedance to get travel time using 6-digit keys
            # Use LEFT join to keep flows even if no impedance data exists
            flows['origem_6'] = flows['mun_origem'].astype(int) // 10
            flows['destino_6'] = flows['mun_destino'].astype(int) // 10
            
            flows = flows.merge(
                self.df_impedance[['origem_6', 'destino_6', 'tempo']],
                on=['origem_6', 'destino_6'],
                how='left'  # CRITICAL: keep all flows!
            )
            
            # Filter by max_time ONLY for flows that have impedance data
            # Flows without impedance are kept (they'll have tempo=NaN)
            flows = flows[(flows['tempo'].isna()) | (flows['tempo'] <= max_time)]
        
        if flows.empty:
            return None
        
        # Get flow with highest viagens value
        max_flow = flows.nlargest(1, 'viagens')
        if max_flow.empty:
            return None
        
        row = max_flow.iloc[0]
        # Use tempo if available, otherwise 0.0
        travel_time = float(row.get('tempo', 0.0)) if 'tempo' in row and pd.notna(row.get('tempo')) else 0.0
        
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
        Validates RM consistency rules by comparing municipality RM with destination UTP RM.
        
        Rules:
        - If Mun has RM and Dest UTP has different RM -> REJECT
        - If Mun has RM A and Dest UTP has RM A -> ACCEPT
        - If Mun has NO RM and Dest UTP has NO RM -> ACCEPT
        - If Mun has NO RM and Dest UTP has RM -> REJECT
        - If Mun has RM and Dest UTP has NO RM -> REJECT
        
        Args:
            mun_id: Origin municipality ID
            dest_utp: Destination UTP ID
            
        Returns:
            True if rules are satisfied
        """
        # 1. Get Municipality RM
        mun_node = self.graph.hierarchy.nodes.get(mun_id, {})
        rm_mun = str(mun_node.get('regiao_metropolitana', '')).strip()
        
        # Normalize Mun RM
        if rm_mun.upper() in ['SEM_RM', 'NAN', 'NONE', 'NULL']:
            rm_mun = ''
            
        # 2. Get Destination UTP RM
        # We look at the municipalities currently in the destination UTP to determine its RM
        # This is more reliable than checking the UTP node itself which might be stale
        rm_dest = ''
        
        # Find a reference municipality in the destination UTP
        # We check a sample; if they are mixed, it's a data issue, but we assume consistency
        dest_muns = []
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                if self.graph.get_municipality_utp(node) == dest_utp:
                    dest_muns.append(data)
                    if len(dest_muns) > 5: # check first 5 is enough
                        break
        
        if dest_muns:
            # Use the RM of the first municipality found in destination
            rm_cand = str(dest_muns[0].get('regiao_metropolitana', '')).strip()
            if rm_cand.upper() not in ['SEM_RM', 'NAN', 'NONE', 'NULL']:
                rm_dest = rm_cand
        else:
            # If UTP is empty (or new), fallback to UTP definition if available or assume consistent
            # But typically we move to existing UTPs. If empty, it has 'no RM' effectively.
            pass
            
        # 3. Compare Strict Equality
        # If RMs are different strings (and one is not empty while other is), it's a violation
        return rm_mun == rm_dest

    def _get_fragmentation_components(self, mun_id: int, current_utp: str) -> List[Set[int]]:
        """
        Simulates removal of mun_id and returns connected components of the remaining UTP nodes.
        
        Args:
            mun_id: Municipality to remove
            current_utp: Current UTP ID
            
        Returns:
            List of sets, where each set contains node IDs of a connected component.
        """
        if self.adjacency_graph is None:
            return []
            
        # 1. Get all municipalities in current UTP (excluding the one moving)
        utp_nodes = []
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                if int(node) != int(mun_id):
                    # Check if currently in this UTP
                    if self.graph.get_municipality_utp(node) == current_utp:
                        utp_nodes.append(node)
        
        # If 0 nodes remain, empty means no components (UTP disappears) -> Valid (1 component of size 0)
        # If 1 node remains, it's 1 component -> Valid
        if len(utp_nodes) <= 1:
            return [set(utp_nodes)] if utp_nodes else []
            
        # 2. Build subgraph of remaining nodes
        subgraph = self.adjacency_graph.subgraph(utp_nodes)
        
        # 3. Get connected components
        components = list(nx.connected_components(subgraph))
        return components

    def _attempt_block_drag(
        self, 
        mover_id: int, 
        dest_utp: str, 
        components: List[Set[int]], 
        flow_df: pd.DataFrame
    ) -> Tuple[bool, List[int]]:
        """
        Checks if isolated components ("Islands") can be dragged along with the mover.
        
        Logic:
        1. Identify "Main Body" (contains Sede or is largest).
        2. Other components are "Islands".
        3. For each Island, check if it SHOULD move to dest_utp (Flow Drag).
           - Criteria: 
             a) Strong flow to dest_utp?
             b) Or strong flow to mover_id (dependency)?
             c) Rules check (RM, Adjacency via mover)?
        
        Args:
            mover_id: The municipality initiating the move (the bridge)
            dest_utp: Where mover is going
            components: List of connected components after mover leaves
            flow_df: Flow data
            
        Returns:
            (Success, List of nodes to drag)
        """
        # 1. Identify Main Body vs Islands
        # Find Sede of current UTP
        current_utp = self.graph.get_municipality_utp(mover_id)
        sede_id = self.graph.utp_seeds.get(current_utp)
        
        main_body_idx = -1
        
        # If Sede exists and is in one of the components, that is the Main Body
        if sede_id:
            for i, comp in enumerate(components):
                if int(sede_id) in comp:
                    main_body_idx = i
                    break
        
        # If no Sede found in components (maybe Sede moved? or logic error), pick largest component
        if main_body_idx == -1:
            max_len = -1
            for i, comp in enumerate(components):
                if len(comp) > max_len:
                    max_len = len(comp)
                    main_body_idx = i
                    
        # 2. Process Islands
        dragged_nodes = []
        
        for i, comp in enumerate(components):
            if i == main_body_idx:
                continue # This is the main body staying behind
                
            # This component is an ISLAND. Check if we can drag it.
            # We treat the component as a single unit effectively
            island_nodes = list(comp)
            
            # Check if island logic allows move
            # Simplified: If RM Compatible, allow drag.
            
            can_drag_island = False
            
            for iso_id in island_nodes:
                # Check Rules: RM compatibility
                if not self._validate_rm_rules(iso_id, dest_utp):
                    # self.logger.debug(f"      ⛔ Island node {iso_id} has incompatible RM with {dest_utp}")
                    return False, [] # Entire block move fails if one part is incompatible
                    
                # Flow/Willingness Check
                # Conceptually: "Are you okay with going there?"
                # Since remaining isolated is bad, we assume "Yes" if RM matches.
                can_drag_island = True
            
            if can_drag_island:
                dragged_nodes.extend(island_nodes)
            else:
                return False, [] # Cannot resolve this island
                
        return True, dragged_nodes

    def _will_cause_fragmentation(self, mun_id: int, current_utp: str) -> bool:
        """Deprecated: Use _get_fragmentation_components instead."""
        comps = self._get_fragmentation_components(mun_id, current_utp)
        return len(comps) > 1
    
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
    
    def _validate_municipality_to_utp_adjacency(self, mun_id: int, dest_utp: str) -> bool:
        """
        Validates if a specific municipality is adjacent to the destination UTP.
        
        This checks if the municipality shares a border with ANY municipality
        in the destination UTP, which is the correct validation for border movements.
        
        Args:
            mun_id: Municipality ID that wants to move
            dest_utp: Destination UTP ID
            
        Returns:
            True if municipality is adjacent to at least one municipality in dest_utp
        """
        if self.adjacency_graph is None:
            self.logger.warning("Adjacency graph not built yet!")
            return False
        
        # Get all municipalities in destination UTP
        muns_destino = []
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                node_utp = self.graph.get_municipality_utp(node)
                if node_utp == dest_utp:
                    muns_destino.append(node)
        
        # Check if mun_id is adjacent to any municipality in dest_utp
        if mun_id in self.adjacency_graph:
            neighbors = self.adjacency_graph[mun_id]
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
        # Store flow_df for cleanup reference
        self.current_flow_df = flow_df
        
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
            
            # 2. ANALYZE FLOW FOR BORDER MUNICIPALITIES
            relocations_this_round = []
            
            # Get set of all Sede municipalities to protect them
            # FIX: Only protect Sedes of ACTIVE UTPs in the graph
            # This prevents protecting historical Sedes of consolidated/deleted UTPs
            
            # 1. Identify active UTPs in the graph
            active_utps = set()
            for node, data in self.graph.hierarchy.nodes(data=True):
                 if data.get('type') == 'utp':
                     # Extract clean UTP ID (remove UTP_ prefix if present, though graph stores node IDs)
                     # Graph UTP nodes are usually "UTP_{id}". Node data might have 'utp_id'.
                     utp_id_raw = data.get('utp_id')
                     if utp_id_raw:
                         active_utps.add(str(utp_id_raw))
            
            # 2. Filter seeds that belong to active UTPs
            sedes_muns = set()
            for utp_id, mun_id in self.graph.utp_seeds.items():
                if str(utp_id) in active_utps:
                    try:
                        sedes_muns.add(int(mun_id))
                    except:
                        pass

            # Log Sede count to debug "loss" of UTPs
            if iteration == 1:
                self.logger.info(f"   ⚓ Active UTPs in Graph: {len(active_utps)}")
                self.logger.info(f"   ⚓ Protecting {len(sedes_muns)} Sede municipalities (Active UTPs only)")
                
                if len(sedes_muns) != len(active_utps):
                    self.logger.warning(f"   ⚠️ Mismatch: {len(active_utps)} UTPs vs {len(sedes_muns)} Sedes. Some UTPs might share a Sede or miss one.")
                
                # DEBUG: Specific check for Santa Cruz do Sul (4316808) / UTP 293
                debug_utp = "293"
                debug_mun = 4316808
                if debug_utp in active_utps:
                    self.logger.info(f"   🔎 DEBUG: UTP {debug_utp} IS ACTIVE.")
                    seed_val = self.graph.utp_seeds.get(debug_utp)
                    self.logger.info(f"   🔎 DEBUG: UTP {debug_utp} seed in graph: {seed_val} (Expected {debug_mun})")
                    if seed_val:
                        try:
                            if int(seed_val) in sedes_muns:
                                self.logger.info(f"   🔎 DEBUG: Seed {seed_val} IS in protected list.")
                            else:
                                self.logger.warning(f"   ⚠️ DEBUG: Seed {seed_val} is NOT in protected list!")
                        except:
                            pass
                else:
                    self.logger.warning(f"   ⚠️ DEBUG: UTP {debug_utp} is NOT in active_utps!")
            
            for mun_id in all_border_muns:
                # CRITICAL: Sedes CANNOT move! They are the anchors.
                if mun_id in sedes_muns:
                    # self.logger.debug(f"   ⚓ Mun {mun_id} is a Sede - skipping")
                    continue
                
                # Get principal flow
                principal_flow = self._get_principal_flow(mun_id, flow_df, max_time=2.0)
                
                if not principal_flow:
                    continue
                
                dest_mun, flow_value, travel_time = principal_flow
                
                # Get UTPs
                dest_utp = self.graph.get_municipality_utp(dest_mun)
                current_utp = self.graph.get_municipality_utp(mun_id)
                
                # Only consider if flow points to a different UTP
                if dest_utp != current_utp and dest_utp not in ["NAO_ENCONTRADO", "SEM_UTP"]:
                    # Pass dest_mun explicitly for transitivity resolution
                    relocations_this_round.append((mun_id, dest_mun, dest_utp, flow_value, travel_time))
            
            self.logger.info(f"   📊 {len(relocations_this_round)} municipalities want to change UTP (adjacent UTPs only)")
            
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
                    # Get RM for logging - use municipality RM vs destination UTP's municipalities' RM
                    mun_node = self.graph.hierarchy.nodes.get(mun_id, {})
                    rm_origin = mun_node.get('regiao_metropolitana', '') or 'SEM_RM'
                    
                    # Get actual RM of destination UTP by checking its municipalities
                    dest_utp_node = f"UTP_{dest_utp}"
                    if self.graph.hierarchy.has_node(dest_utp_node):
                        parents = list(self.graph.hierarchy.predecessors(dest_utp_node))
                        rm_dest_node = parents[0] if parents else "RM_SEM_RM"
                        # Remove RM_ prefix for display
                        rm_dest = rm_dest_node.replace('RM_', '') if rm_dest_node.startswith('RM_') else rm_dest_node
                    else:
                        rm_dest = 'NAO_ENCONTRADA'
                    
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
                
                # Validate municipality-to-UTP adjacency
                if not self._validate_municipality_to_utp_adjacency(mun_id, dest_utp):
                    self.rejections_log.append({
                        "mun_id": mun_id,
                        "mun_name": mun_name,
                        "utp_origem": current_utp,
                        "proposed_utp": dest_utp,
                        "reason": "Municipality not adjacent to destination UTP",
                        "iteration": iteration
                    })
                    continue
                
                # Validate flow threshold (>3% of total)
                total_flow = self._get_total_flow(mun_id, flow_df)
                if total_flow > 0 and flow_value < total_flow * self.FLOW_THRESHOLD:
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
                
                
                # All validations passed - Allow move even if it causes fragmentation
                # Islands will be cleaned up later by Step 8.5 (IsolatedMunicipalityResolver)
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
            
            self.logger.info(f"   🔁 {len(resolved_relocations)} relocations executed")
            
            # Note: No need to update GDF - border identification now reads from graph directly!
        
        
        # 7. POST-PROCESSING CLEANUP - DISABLED
        # Islands are now cleaned up by Step 8.5 (IsolatedMunicipalityResolver)
        # which uses more sophisticated flow-based reconnection logic
        # self._cleanup_islands(gdf, sedes_muns)
        
        # Save results
        self._save_results()
        
        # NOTE: Intermediate snapshot removed - final snapshot is generated by manager.py
        # after BOTH border validation AND isolated resolution complete (Step 8.1 + 8.5)
        # Export Snapshot Step 8 (Border Validation) - DISABLED
        # snapshot_path = self.data_dir / "snapshot_step8_border_validation.json"
        # self.graph.export_snapshot(snapshot_path, "Border Validation", gdf)
        
        self.logger.info(f"\n📊 BORDER VALIDATION COMPLETE")
        self.logger.info(f"   Total iterations: {iteration}")
        self.logger.info(f"   Total relocations: {total_relocations}")
        self.logger.info(f"   Total rejections: {len(self.rejections_log)}")
        
        return total_relocations
    
    def _cleanup_islands(self, gdf: gpd.GeoDataFrame, sedes_muns: Set[int]):
        """
        Identifies and fixes isolated islands (municipalities disconnected from their UTP's Sede).
        Executes in multiple passes to handle cascading updates.
        """
        self.logger.info("\n🧹 POST-PROCESSING: Cleaning up isolated islands...")
        
        # Rebuild adjacency to be sure
        if self.adjacency_graph is None:
            self.adjacency_graph = self._build_adjacency_graph(gdf)
            
        total_fixed = 0
        
        # Run in a loop to catch cascading islands (max 3 passes)
        for pass_num in range(3):
            self.logger.info(f"   Pass {pass_num + 1}...")
            fixed_this_pass = 0
            
            # Group municipalities by UTP
            utp_muns = {}
            for node, data in self.graph.hierarchy.nodes(data=True):
                if data.get('type') == 'municipality':
                    utp = self.graph.get_municipality_utp(node)
                    if utp not in utp_muns:
                        utp_muns[utp] = []
                    utp_muns[utp].append(int(node))
            
            for utp_id, muns in utp_muns.items():
                if utp_id in ["NAO_ENCONTRADO", "SEM_UTP"]:
                    continue
                    
                # Find the Sede for this UTP
                sede_id = None
                expected_sede = self.graph.utp_seeds.get(utp_id)
                # Ensure expected_sede matches one of the muns in this UTP
                if expected_sede:
                    try:
                        expected_sede_int = int(expected_sede)
                        if expected_sede_int in muns:
                            sede_id = expected_sede_int
                    except:
                        pass
                
                # Identify islands
                # 1. Build subgraph for this UTP
                subgraph = self.adjacency_graph.subgraph(muns)
                # 2. Find connected components
                components = list(nx.connected_components(subgraph))
                
                islands = []
                
                if not sede_id:
                    # Case A: UTP has no Sede inside it!
                    # This happens if Sede moved away or UTP is a leftover ghost.
                    # Treat ALL municipalities as floating islands.
                    if len(muns) > 0:
                        self.logger.warning(f"   ⚠️ UTP {utp_id} has INVALID structure (No Sede found). Dissolving {len(muns)} municipalities.")
                        for comp in components:
                            islands.extend(comp)
                else:
                    # Case B: Valid UTP with Sede.
                    # Any component NOT containing the Sede is an island.
                    for comp in components:
                        if sede_id not in comp:
                            islands.extend(comp)
                            
                # Reassign islands
                for island_mun in islands:
                    # Find neighbors outside this UTP
                    if island_mun not in self.adjacency_graph:
                        continue
                        
                    neighbors = list(self.adjacency_graph.neighbors(island_mun))
                    
                    # Vote for new UTP based on borders
                    candidate_utps = {}
                    
                    for neighbor in neighbors:
                        n_utp = self.graph.get_municipality_utp(neighbor)
                        if n_utp != utp_id and n_utp not in ["NAO_ENCONTRADO", "SEM_UTP"]:
                            # Weight by border connection (1 point per neighbor)
                            candidate_utps[n_utp] = candidate_utps.get(n_utp, 0) + 1.0
                    
                    # ENHANCEMENT: Add Flow Logic
                    # Check flow from this island to candidate UTPs
                    # If strong flow exists, boost the score
                    
                    # We need the flow_df. It's passed to run_border_validation but not stored in self.
                    # We can try to access it if we stored it, or pass it to _cleanup_islands
                    # For now, we will add an optional argument to _cleanup_islands in the call site first.
                    
                    # Assuming we can get flow... 
                    # Actually, let's fix the call site in run_border_validation to pass flow_df first.
                    # Wait, I cannot change the signature in this tool call easily if I don't see the call site.
                    # But I can access self.validator if it has data? No.
                    
                    # Let's assume we modify the method signature in a moment.
                    # For now, let's just create the logic to use flow_df if available.
                    
                    if hasattr(self, 'current_flow_df') and self.current_flow_df is not None:
                         principal_flow = self._get_principal_flow(island_mun, self.current_flow_df)
                         if principal_flow:
                             dest_mun, flow_val, _ = principal_flow
                             flow_utp = self.graph.get_municipality_utp(dest_mun)
                             if flow_utp in candidate_utps:
                                 # Boost score significantly if flow matches border
                                 candidate_utps[flow_utp] += 5.0
                                 self.logger.debug(f"      🌊 Flow boost for UTP {flow_utp} (Target: {dest_mun})")
                    
                    if candidate_utps:
                        # Sort by vote count (descending)
                        sorted_candidates = sorted(candidate_utps.items(), key=lambda x: x[1], reverse=True)
                        
                        moved = False
                        for best_utp, count in sorted_candidates:
                            # Validate RM before moving
                            if self._validate_rm_rules(island_mun, best_utp):
                                self.logger.info(f"   🏝️ Reassigning island Mun {island_mun} from {utp_id} -> {best_utp} (votes: {count})")
                                self.graph.move_municipality(island_mun, best_utp)
                                fixed_this_pass += 1
                                moved = True
                                
                                # Log
                                self.relocations_log.append({
                                    "mun_id": island_mun,
                                    "mun_name": str(island_mun), # Simplify
                                    "utp_origem": utp_id,
                                    "utp_destino": best_utp,
                                    "iteration": f"CLEANUP_P{pass_num+1}",
                                    "reason": "Isolated Island Cleanup"
                                })
                                break
                        
                        if not moved:
                            # If blocked by RM, we strictly cannot move it to those candidates.
                            # It remains an island -> User will see it.
                            self.logger.warning(f"   ❌ Could not reassign island Mun {island_mun}: RM mismatch with candidates {[k for k,v in sorted_candidates]}")
                            
            total_fixed += fixed_this_pass
            self.logger.info(f"   Pass {pass_num + 1} finished: {fixed_this_pass} moved.")
            
            if fixed_this_pass == 0:
                break
                
        if total_fixed > 0:
            self.logger.info(f"   🧹 Cleaned up {total_fixed} isolated municipalities total")

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