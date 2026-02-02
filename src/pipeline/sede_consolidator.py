import logging
import pandas as pd
import geopandas as gpd
import networkx as nx
import json
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.interface.consolidation_manager import ConsolidationManager
from src.pipeline.sede_analyzer import SedeAnalyzer

class SedeConsolidator:
    """
    Step 6: Consolidation of Sedes based on functional dependency (2h distance + flow)
    and infrastructure scoring (Airport + Tourism).
    """

    def __init__(self, graph: TerritorialGraph, validator: TerritorialValidator, sede_analyzer: SedeAnalyzer):
        self.graph = graph
        self.validator = validator
        self.analyzer = sede_analyzer
        self.logger = logging.getLogger("GeoValida.SedeConsolidator")
        self.consolidation_manager = ConsolidationManager()
        self.adjacency_graph = None  # Will be built from GDF
        
        # Set data directory for exports
        from pathlib import Path
        self.data_dir = Path(__file__).parent.parent.parent / "data" / "03_processed"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def _build_adjacency_graph(self, gdf: gpd.GeoDataFrame):
        """Builds a NetworkX graph representing spatial adjacency of municipalities."""
        self.logger.info("Building spatial adjacency graph for pathfinding...")
        self.adjacency_graph = nx.Graph()
        
        if gdf is None or gdf.empty:
            return

        # Ensure we have geometries
        gdf_valid = gdf[gdf.geometry.notna()]
        
        # Use touching/intersection with small buffer to find neighbors
        # Optimized approach: sjoin
        # Project to estimate buffer if needed, but assuming GDF is suitable
        # Using a small buffer helps with imperfect topologies
        
        # Project to metric CRS for buffering (EPSG:3857 is fast and sufficient for adjacency)
        gdf_valid_metric = gdf_valid.to_crs(epsg=3857)
        
        # Use a small buffer (e.g., 100 meters) to handle topology gaps
        buffer_val = 100.0 
        gdf_buff = gdf_valid_metric.copy()
        gdf_buff['geometry'] = gdf_buff.geometry.buffer(buffer_val)
        
        # Self-join to find neighbors
        # Use simple index-based join on metric/buffered geometries
        sjoin = gpd.sjoin(gdf_buff, gdf_buff, how='inner', predicate='intersects')
        
        edges = []
        for idx, row in sjoin.iterrows():
             left = int(row['CD_MUN_left'])
             right = int(row['CD_MUN_right'])
             if left != right:
                 edges.append((left, right))
                 
        self.adjacency_graph.add_edges_from(edges)
        self.logger.info(f"Adjacency graph built: {self.adjacency_graph.number_of_nodes()} nodes, {self.adjacency_graph.number_of_edges()} edges.")


    def _get_regic_rank(self, regic_val: str) -> int:
        """
        Returns numeric rank for REGIC description. Lower is MORE important/relevant.
        Based on standard IBGE hierarchy.
        """
        if not regic_val: return 99
        
        r = str(regic_val).lower().strip()
        
        # Hierarchy Definition (1 = Most Relevant)
        mapping = {
            'grande metrópole nacional': 1,
            'metrópole nacional': 2,
            'metrópole': 3,
            'capital regional a': 4,
            'capital regional b': 5,
            'capital regional c': 6,
            'centro sub-regional a': 7,
            'centro sub-regional b': 8,
            'centro de zona a': 9,
            'centro de zona b': 10,
            'centro local': 11
        }
        
        for k, v in mapping.items():
            if k in r:
                return v
                
        return 99 # Unknown

    def _get_sede_score(self, sede_metrics: Dict) -> int:
        """Calculates score (0-2) based on Airport and Tourism."""
        score = 0
        if sede_metrics.get('tem_aeroporto'):
            score += 1
        
        # Check tourism class.
        # User defined: "1 - Município Turístico"
        turismo = str(sede_metrics.get('turismo', '')).strip()
        if "1 - Município Turístico" in turismo: 
             score += 1
             
        return score

    def _validate_utp_adjacency(self, utp_origem: str, utp_destino: str) -> bool:
        """
        Validates if two UTPs are adjacent (share a border).
        Required to maintain territorial continuity.
        
        IMPORTANT: Adjacency is between UTPs, not between sedes.
        Checks if ANY municipality from origin UTP is adjacent to 
        ANY municipality from destination UTP.
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

    def _filter_candidates(self, df_metrics: pd.DataFrame) -> List[Dict]:
        """
        Filters potential consolidation candidates based on simplified rules:
        1. Sede must have main flow to another sede
        2. Travel time <= 2 hours
        3. RM rule: same RM OR both without RM
        4. UTPs must be adjacent
        5. Destination score >= Origin score
        """
        candidates = []
        rejected = []
        
        # Track filtering statistics
        filter_stats = {
            'total_checked': 0,
            'no_alert': 0,
            'invalid_destination': 0,
            'same_utp': 0,
            'travel_time_exceeded': 0,
            'rm_mismatch': 0,
            'not_adjacent': 0,
            'rejected_by_score': 0,
            'accepted': 0
        }
        
        for _, row in df_metrics.iterrows():
            filter_stats['total_checked'] += 1
            
            # Must have dependency alert
            if not row['tem_alerta_dependencia']:
                filter_stats['no_alert'] += 1
                continue
                
            sede_origem = row['cd_mun_sede']
            utp_origem = row['utp_id']
            sede_destino = row['principal_destino_cd']
            
            # Validate destination exists
            if pd.isna(sede_destino):
                filter_stats['invalid_destination'] += 1
                continue
            
            utp_destino = self.graph.get_municipality_utp(sede_destino)
            
            # Check if same UTP
            if utp_origem == utp_destino:
                filter_stats['same_utp'] += 1
                continue

            # RULE 1: Travel time <= 2 hours
            tempo_viagem = row.get('tempo_ate_destino_h')
            if tempo_viagem is None or tempo_viagem > 2.0:
                filter_stats['travel_time_exceeded'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': f'Tempo de viagem {tempo_viagem:.2f}h > 2h'
                })
                continue

            # RULE 2: RM consistency
            rm_origem = str(row.get('regiao_metropolitana', '')).strip()
            if rm_origem.lower() == 'nan':
                rm_origem = ''
            
            # Get RM of destination sede
            sede_utp_destino = self.graph.utp_seeds.get(utp_destino)
            if not sede_utp_destino:
                filter_stats['invalid_destination'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': 'UTP destino não tem sede ativa'
                })
                continue
            
            dest_row = df_metrics[df_metrics['cd_mun_sede'] == sede_utp_destino]
            if dest_row.empty:
                filter_stats['invalid_destination'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': 'Sede destino não encontrada em métricas'
                })
                continue
            
            dest_row = dest_row.iloc[0]
            rm_destino = str(dest_row.get('regiao_metropolitana', '')).strip()
            if rm_destino.lower() == 'nan':
                rm_destino = ''

            # RM Rule: Both without RM OR same RM
            if rm_origem or rm_destino:  # At least one has RM
                if rm_origem != rm_destino:
                    filter_stats['rm_mismatch'] += 1
                    rejected.append({
                        'sede_origem': sede_origem,
                        'nm_origem': row['nm_sede'],
                        'utp_origem': utp_origem,
                        'sede_destino': sede_destino,
                        'utp_destino': utp_destino,
                        'rm_origem': rm_origem,
                        'rm_destino': rm_destino,
                        'motivo_rejeicao': f"RM incompatível: '{rm_origem}' != '{rm_destino}'"
                    })
                    continue

            # RULE 3: UTP Adjacency
            if not self._validate_utp_adjacency(utp_origem, utp_destino):
                filter_stats['not_adjacent'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': 'UTPs não são adjacentes'
                })
                continue
            
            # RULE 4: Score validation
            score_origem = self._get_sede_score(row)
            score_destino = self._get_sede_score(dest_row)
            
            # Destination must have better or equal infrastructure
            if score_destino < score_origem:
                filter_stats['rejected_by_score'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'nm_destino': dest_row['nm_sede'],
                    'utp_destino': utp_destino,
                    'score_origem': score_origem,
                    'score_destino': score_destino,
                    'tempo_viagem_h': tempo_viagem,
                    'rm_origem': rm_origem,
                    'rm_destino': rm_destino,
                    'motivo_rejeicao': f'Score destino ({score_destino}) < origem ({score_origem})'
                })
                continue
            
            # APPROVED!
            filter_stats['accepted'] += 1
            candidates.append({
                'sede_origem': sede_origem,
                'nm_origem': row['nm_sede'],
                'utp_origem': utp_origem,
                'sede_destino': sede_destino,
                'nm_destino': dest_row['nm_sede'],
                'utp_destino': utp_destino,
                'score_origem': score_origem,
                'score_destino': score_destino,
                'tempo_viagem_h': tempo_viagem,
                'rm_origem': rm_origem,
                'rm_destino': rm_destino,
                'motivo_rejeicao': ''
            })
        
        # Log statistics
        self.logger.info(f"\n📈 Candidate Filtering Statistics:")
        self.logger.info(f"   Total sedes checked: {filter_stats['total_checked']}")
        self.logger.info(f"   Filtered out - No dependency alert: {filter_stats['no_alert']}")
        self.logger.info(f"   Filtered out - Invalid destination: {filter_stats['invalid_destination']}")
        self.logger.info(f"   Filtered out - Same UTP: {filter_stats['same_utp']}")
        self.logger.info(f"   Filtered out - Travel time > 2h: {filter_stats['travel_time_exceeded']}")
        self.logger.info(f"   Filtered out - RM mismatch: {filter_stats['rm_mismatch']}")
        self.logger.info(f"   Filtered out - UTPs not adjacent: {filter_stats['not_adjacent']}")
        self.logger.info(f"   Filtered out - Rejected by score: {filter_stats['rejected_by_score']}")
        self.logger.info(f"   ✅ Accepted candidates: {filter_stats['accepted']}")
        
        # Store rejected for CSV export
        self.rejected_candidates = rejected
        
        return candidates

    def _sync_analyzer_with_graph(self):
        """Syncs the analyzer's DataFrame with the current graph state (UTPs and Sedes)."""
        if self.analyzer.df_municipios is None:
            return

        self.logger.info("Syncing Analyzer with Graph State...")
        
        # Build a set of all sede municipalities for fast lookup
        # graph.utp_seeds is a dict: {utp_id: mun_id_sede}
        sede_municipalities = set(self.graph.utp_seeds.values())
        
        updates = {}
        for node in self.graph.hierarchy.nodes():
             # We assume integer nodes are municipalities
             if isinstance(node, int) or (isinstance(node, str) and node.isdigit()):
                 # Verify it is a municipality node (or just trust ID)
                 mun_id = int(node)
                 
                 # Get UTP
                 utp = self.graph.get_municipality_utp(mun_id)
                 
                 # Get Sede Status - check if this municipality is a sede
                 # A municipality is a sede if it's a value in utp_seeds
                 is_sede = mun_id in sede_municipalities
                 
                 updates[mun_id] = {'utp_id': utp, 'sede_utp': is_sede}
        
        # Bulk Update DataFrame
        # Iterate updates is safer/faster than df iterrows for this
        sede_count = 0
        for idx, row in self.analyzer.df_municipios.iterrows():
            mun_id = int(row['cd_mun'])
            if mun_id in updates:
                self.analyzer.df_municipios.at[idx, 'utp_id'] = updates[mun_id]['utp_id']
                self.analyzer.df_municipios.at[idx, 'sede_utp'] = updates[mun_id]['sede_utp']
                if updates[mun_id]['sede_utp']:
                    sede_count += 1
        
        self.logger.info(f"✅ Synced {len(updates)} municipalities. {sede_count} are sedes.")

        # Force analyzer to clear cached metrics if any
        # (calculate_socioeconomic_metrics recalculates from df_municipios every time, so this is sufficient)

    def run_sede_consolidation(self, flow_df: pd.DataFrame, gdf: gpd.GeoDataFrame, map_gen) -> int:
        """
        Executes Simplified Sede Consolidation Pipeline (Single Pass).
        
        Logic:
        1. Identify sedes with main flow to another sede (travel time <= 2h)
        2. Validate RM rules (same RM or both without RM)
        3. Validate UTP adjacency
        4. Validate infrastructure scores
        5. Move entire UTP (sede + all municipalities) to destination UTP
        """
        self.logger.info("Starting Step 6: Sede Consolidation (Simplified)...")
        
        # Initialize
        self.consolidation_manager = ConsolidationManager()
        self.changes_current_run = []
        self.rejected_candidates = []
        
        # Build Adjacency Graph
        self._build_adjacency_graph(gdf)
        
        # Sync analyzer with current graph state
        self._sync_analyzer_with_graph()
        
        # Calculate metrics for all sedes
        df_metrics = self.analyzer.calculate_socioeconomic_metrics()
        
        total_sedes = len(df_metrics)
        sedes_com_alerta = df_metrics['tem_alerta_dependencia'].sum() if 'tem_alerta_dependencia' in df_metrics.columns else 0
        
        self.logger.info(f"📊 Sede Analysis Stats:")
        self.logger.info(f"   Total sedes: {total_sedes}")
        self.logger.info(f"   Sedes with dependency alerts: {sedes_com_alerta}")
        
        if total_sedes == 0:
            self.logger.warning("⚠️  NO SEDES FOUND!")
            self._save_results_and_csv()
            return 0
        
        # Filter candidates using simplified rules
        candidates = self._filter_candidates(df_metrics)
        
        if not candidates:
            self.logger.info("✅ No consolidation candidates found.")
            self._save_results_and_csv()
            return 0
        
        self.logger.info(f"✅ Found {len(candidates)} consolidation candidates")
        
        # Execute consolidations
        total_changes = 0
        
        for cand in candidates:
            sede_origem = cand['sede_origem']
            utp_origem = cand['utp_origem']
            utp_destino = cand['utp_destino']
            
            self.logger.info(f"\n🔄 Consolidating: {cand['nm_origem']} (UTP {utp_origem}) -> {cand['nm_destino']} (UTP {utp_destino})")
            
            # Get all municipalities in origin UTP
            muns_to_move = []
            for node, data in self.graph.hierarchy.nodes(data=True):
                if data.get('type') == 'municipality':
                    if self.graph.get_municipality_utp(node) == utp_origem:
                        muns_to_move.append(node)
            
            if not muns_to_move:
                self.logger.warning(f"  ⚠️  No municipalities found in UTP {utp_origem}")
                continue
            
            self.logger.info(f"  Moving {len(muns_to_move)} municipalities from UTP {utp_origem} to {utp_destino}")
            
            # Move all municipalities
            for mun in muns_to_move:
                self.graph.move_municipality(mun, utp_destino)
                
                # Update GDF
                if gdf is not None and 'CD_MUN' in gdf.columns:
                    try:
                        mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun)
                        gdf.loc[mask, 'UTP_ID'] = str(utp_destino)
                    except Exception as e:
                        self.logger.error(f"    Failed to update GDF for {mun}: {e}")
                
                # Log consolidation
                cons_entry = self.consolidation_manager.add_consolidation(
                    source_utp=utp_origem,
                    target_utp=utp_destino,
                    reason=f"Sede consolidation: Score {cand['score_origem']}->{cand['score_destino']}, Travel {cand['tempo_viagem_h']:.2f}h",
                    details={
                        "mun_id": mun,
                        "is_sede": (mun == sede_origem),
                        "score_origem": cand['score_origem'],
                        "score_destino": cand['score_destino'],
                        "tempo_viagem_h": cand['tempo_viagem_h'],
                        "rm_origem": cand.get('rm_origem', ''),
                        "rm_destino": cand.get('rm_destino', '')
                    }
                )
                self.changes_current_run.append(cons_entry)
            
            # Revoke sede status from origin
            if self.graph.hierarchy.has_node(sede_origem):
                self.graph.hierarchy.nodes[sede_origem]['sede_utp'] = False
            
            # Remove UTP from seeds
            if utp_origem in self.graph.utp_seeds:
                del self.graph.utp_seeds[utp_origem]
            
            total_changes += 1
            self.logger.info(f"  ✅ Consolidation complete")
        
        # Save results
        self._save_results_and_csv()
        
        # Recolor graph
        self.logger.info("\n🎨 Recalculating graph coloring...")
        try:
            coloring = self.graph.compute_graph_coloring(gdf)
            self.logger.info(f"✅ Coloring updated: {max(coloring.values(), default=0) + 1} colors needed")
            
            if gdf is not None:
                gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring)
            
            # Save coloring
            # Imports moved to top of file
            
            coloring_file = self.data_dir / "post_sede_coloring.json"
            coloring_str_keys = {str(k): v for k, v in coloring.items()}
            
            with open(coloring_file, 'w') as f:
                json.dump(coloring_str_keys, f, indent=2)
            
            self.logger.info(f"💾 Coloring saved to: {coloring_file}")
            
            # CRITICAL: Confirm all active seeds are marked as sede_utp=True in graph nodes
            # This ensures the snapshot reflects the final consolidation state
            count_sedes_marked = 0
            for utp_id, mun_id in self.graph.utp_seeds.items():
                if self.graph.hierarchy.has_node(mun_id):
                    self.graph.hierarchy.nodes[mun_id]['sede_utp'] = True
                    count_sedes_marked += 1
            self.logger.info(f"✅ Marked {count_sedes_marked} municipalities as Active Sedes in Graph.")

            # Export Snapshot Step 6 (Sede Consolidation)
            snapshot_path = self.data_dir / "snapshot_step6_sede_consolidation.json"
            self.graph.export_snapshot(snapshot_path, "Sede Consolidation", gdf)
            
        except Exception as e:
            self.logger.warning(f"⚠️  Error recalculating coloring or saving snapshot: {e}")
        
        self.logger.info(f"\n✅ Sede Consolidation complete: {total_changes} consolidations executed")
        return total_changes

    def _save_results_and_csv(self):
        """Save consolidation results to JSON and CSV files."""
        import pandas as pd
        from pathlib import Path
        
        # Save JSON (existing functionality)
        self.logger.info(f"💾 Saving consolidation results ({len(self.changes_current_run)} changes)...")
        self.consolidation_manager.save_sede_batch(self.changes_current_run if self.changes_current_run else [])
        
        # Generate CSV with all candidates (approved + rejected)
        csv_records = []
        
        # Add approved consolidations
        for change in self.changes_current_run:
            if change.get('details', {}).get('is_sede', False):  # Only record sede movements, not individual municipalities
                csv_records.append({
                    'sede_origem': change['details']['mun_id'],
                    'utp_origem': change['source_utp'],
                    'sede_destino': '',  # We don't track this in current structure
                    'utp_destino': change['target_utp'],
                    'tempo_viagem_h': change['details'].get('tempo_viagem_h', ''),
                    'score_origem': change['details'].get('score_origem', ''),
                    'score_destino': change['details'].get('score_destino', ''),
                    'rm_origem': change['details'].get('rm_origem', ''),
                    'rm_destino': change['details'].get('rm_destino', ''),
                    'status': 'APROVADO',
                    'motivo_rejeicao': ''
                })
        
        # Add rejected candidates
        for rejected in self.rejected_candidates:
            csv_records.append({
                'sede_origem': rejected.get('sede_origem', ''),
                'utp_origem': rejected.get('utp_origem', ''),
                'sede_destino': rejected.get('sede_destino', ''),
                'utp_destino': rejected.get('utp_destino', ''),
                'tempo_viagem_h': rejected.get('tempo_viagem_h', ''),
                'score_origem': rejected.get('score_origem', ''),
                'score_destino': rejected.get('score_destino', ''),
                'rm_origem': rejected.get('rm_origem', ''),
                'rm_destino': rejected.get('rm_destino', ''),
                'status': 'REJEITADO',
                'motivo_rejeicao': rejected.get('motivo_rejeicao', '')
            })
        
        # Save CSV
        if csv_records:
            df_csv = pd.DataFrame(csv_records)
            csv_path = Path(self.data_dir) / 'sede_consolidation_result.csv'
            df_csv.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"💾 CSV saved to: {csv_path} ({len(csv_records)} records)")
        else:
            self.logger.info("No consolidation records to save to CSV")




    def _get_total_flow(self, mun_id: int, flow_df: pd.DataFrame) -> float:
        """Helper to get total flow originating from a municipality."""
        if flow_df is None: return 0
        return flow_df[flow_df['mun_origem'] == mun_id]['viagens'].sum()

    def _export_candidate_analysis_json(self, df_metrics, candidates, pass_num):
        '''Exporta análise detalhada de candidatos para JSON.'''
        import json
        from pathlib import Path
        from datetime import datetime
        
        export_data = {
            'pass_number': pass_num,
            'timestamp': datetime.now().isoformat(),
            'total_sedes_analyzed': len(df_metrics),
            'total_candidates_found': len(candidates),
            'candidates': []
        }
        
        for cand in candidates:
            sede_origem = cand['sede_origem']
            sede_destino = cand['sede_destino']
            origem_row = df_metrics[df_metrics['cd_mun_sede'] == sede_origem]
            dest_row = df_metrics[df_metrics['cd_mun_sede'] == sede_destino]
            if origem_row.empty or dest_row.empty:
                continue
            origem_row = origem_row.iloc[0]
            dest_row = dest_row.iloc[0]
            candidate_data = {
                'approved': True,
                'origem': {'cd_mun': int(sede_origem), 'nm_mun': origem_row['nm_sede'], 'utp_id': cand['utp_origem'], 'uf': origem_row['uf'], 'populacao': int(origem_row['populacao_total_utp']), 'regic': origem_row['regic'], 'tem_aeroporto': bool(origem_row['tem_aeroporto']), 'aeroporto_icao': origem_row.get('aeroporto_icao', ''), 'turismo': origem_row.get('turismo', ''), 'score': cand['score_origem']},
                'destino': {'cd_mun': int(sede_destino), 'nm_mun': dest_row['nm_sede'], 'utp_id': cand['utp_destino'], 'uf': dest_row['uf'], 'populacao': int(dest_row['populacao_total_utp']), 'regic': dest_row['regic'], 'tem_aeroporto': bool(dest_row['tem_aeroporto']), 'aeroporto_icao': dest_row.get('aeroporto_icao', ''), 'turismo': dest_row.get('turismo', ''), 'score': cand['score_dest']},
                'fluxo': {'principal_destino': origem_row.get('principal_destino_nm', ''), 'tempo_h': float(origem_row.get('tempo_ate_destino_h', 0)), 'proporcao': float(origem_row.get('proporcao_fluxo_principal', 0))},
                'reason': cand['reason']
            }
            export_data['candidates'].append(candidate_data)
        output_path = Path(self.data_dir) / f'sede_consolidation_analysis_pass{pass_num}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        self.logger.info(f'✅ Análise exportada: {output_path}')
        return output_path

