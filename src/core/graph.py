import networkx as nx
import pandas as pd
import geopandas as gpd
import logging
from typing import Dict, List, Optional, Union
from pathlib import Path

class TerritorialGraph:
    """
    Classe para gerenciar a hierarquia territorial brasileira e integração funcional.
    Estrutura: Brasil (Raiz) -> RMs (Nós) -> UTPs (Subnós) -> Municípios (Folhas).
    Integração: Municípios possuem arestas com pesos baseados em impedâncias.
    """

    def __init__(self):
        # Grafo Direcionado para Hierarquia (Pai -> Filho)
        self.hierarchy = nx.DiGraph()
        # Grafo Não-Direcionado para Impedâncias Funcionais (Distância/Tempo)
        self.functional = nx.Graph()
        
        # Armazenamento
        self.utp_seeds = {} # Armazena utp_id -> cd_mun (sede)
        self.mun_regic = {} # cd_mun -> regic_code (ex: '2B')
        
        # Inicializa a raiz
        self.root = "BRASIL"
        self.hierarchy.add_node(self.root, type='country', level=0)
        
        logging.info("Grafo Territorial inicializado.")

    def add_rm(self, rm_name: str):
        """Adiciona uma Região Metropolitana ao grafo."""
        node_id = f"RM_{rm_name}"
        if not self.hierarchy.has_node(node_id):
            self.hierarchy.add_node(node_id, type='rm', name=rm_name, level=1)
            self.hierarchy.add_edge(self.root, node_id)
        return node_id

    def add_utp(self, utp_id: Union[str, int], parent_id: str = "BRASIL"):
        """Adiciona uma UTP vinculada a uma RM ou diretamente ao Brasil."""
        node_id = f"UTP_{utp_id}"
        if not self.hierarchy.has_node(node_id):
            self.hierarchy.add_node(node_id, type='utp', utp_id=utp_id, level=2)
            self.hierarchy.add_edge(parent_id, node_id)
        return node_id

    def add_municipality(self, cd_mun: int, nm_mun: str, utp_id: Union[str, int]):
        """Adiciona um município vinculado a uma UTP."""
        mun_id = int(cd_mun)
        utp_node = f"UTP_{utp_id}"
        
        # Garante que a UTP existe
        if not self.hierarchy.has_node(utp_node):
            self.add_utp(utp_id)
            
        self.hierarchy.add_node(mun_id, type='municipality', name=nm_mun, level=3)
        self.hierarchy.add_edge(utp_node, mun_id)
        
        # Mantém nó no grafo funcional para compatibilidade
        if not self.functional.has_node(mun_id):
            self.functional.add_node(mun_id, name=nm_mun)

    def move_municipality(self, cd_mun: int, target_utp_id: Union[str, int]):
        """Troca um município de UTP, removendo o vínculo anterior."""
        mun_id = int(cd_mun)
        target_utp_node = f"UTP_{target_utp_id}"
        
        if not self.hierarchy.has_node(mun_id):
            raise ValueError(f"Município {cd_mun} não existe no grafo.")
        
        # Remove arestas de hierarquia atuais (um mun só tem um pai UTP)
        old_parents = [p for p in self.hierarchy.predecessors(mun_id) 
                       if self.hierarchy.nodes[p].get('type') == 'utp']
        for p in old_parents:
            self.hierarchy.remove_edge(p, mun_id)
            
        # Adiciona à nova UTP
        if not self.hierarchy.has_node(target_utp_node):
            self.add_utp(target_utp_id)
        self.hierarchy.add_edge(target_utp_node, mun_id)
        
        logging.info(f"Município {cd_mun} movido para {target_utp_node}.")

    def add_impedance(self, origin: int, destination: int, weight: float):
        """Adiciona peso funcional (impedância) entre dois municípios."""
        if self.functional.has_node(origin) and self.functional.has_node(destination):
            self.functional.add_edge(origin, destination, weight=weight)

    def load_from_dataframe(self, df_base: pd.DataFrame, df_regic: pd.DataFrame):
        """
        Popula o grafo: df_regic define as Sedes/REGIC e df_base define o Território.
        A ID da UTP é estritamente a coluna 'UTPs_PAN_3'.
        """
        logging.info("Vinculando Sedes e REGIC pela coluna UTPs_PAN_3...")
        
        # 1. Carrega Sedes e REGIC do arquivo externo
        for _, row in df_regic.iterrows():
            cd_mun = int(row['CD_MUN'])
            utp_id = str(row['UTPs_PAN_3'])
            regic_desc = str(row['REGIC'])
            
            # Define o município como SEDE e guarda o seu nível de influência
            self.utp_seeds[utp_id] = cd_mun
            self.mun_regic[cd_mun] = regic_desc
            
        # 2. Constrói a hierarquia baseada no território principal
        for _, row in df_base.iterrows():
            cd_mun = int(row['CD_MUN'])
            nm_mun = row['NM_MUN']
            utp_id = str(row['UTPs_PAN_3'])
            
            # Identifica Região Metropolitana (NM_CONCU)
            rm_name = str(row['NM_CONCU']) if pd.notna(row['NM_CONCU']) else "SEM_RM"
            
            # Criação dos nós no grafo (RM -> UTP -> Município)
            rm_node = f"RM_{rm_name}"
            if not self.hierarchy.has_node(rm_node):
                self.hierarchy.add_node(rm_node, type='rm', name=rm_name)
                self.hierarchy.add_edge(self.root, rm_node)

            utp_node = f"UTP_{utp_id}"
            if not self.hierarchy.has_node(utp_node):
                self.hierarchy.add_node(utp_node, type='utp', utp_id=utp_id)
                self.hierarchy.add_edge(rm_node, utp_node)
            
            self.hierarchy.add_node(cd_mun, type='municipality', name=nm_mun)
            self.hierarchy.add_edge(utp_node, cd_mun)

        logging.info("Grafo Territorial populado com sucesso.")

    def get_municipality_utp(self, cd_mun: int) -> str:
        """Retorna o ID da UTP de um município."""
        if not self.hierarchy.has_node(cd_mun):
            return "NAO_ENCONTRADO"
        
        # O pai do município é sempre o nó da UTP
        parents = list(self.hierarchy.predecessors(cd_mun))
        for p in parents:
            if str(p).startswith("UTP_"):
                return str(p).replace("UTP_", "")
        return "SEM_UTP"

    def export_to_csv(self, path: Path):
        """Exporta a estrutura de hierarquia para um CSV legível."""
        rows = []
        for u, v, data in self.hierarchy.edges(data=True):
            rows.append({
                "parent": u,
                "child": v,
                "parent_type": self.hierarchy.nodes[u]['type'],
                "child_type": self.hierarchy.nodes[v]['type']
            })
        pd.DataFrame(rows).to_csv(path, index=False, sep=';')
        logging.info(f"Hierarquia exportada para {path}")

    def get_unitary_utps(self) -> List[str]:
        """
        Identifica e retorna uma lista com os IDs de todas as UTPs 
        que possuem apenas um único município vinculado.
        """
        unitary_utps = []
        
        # 1. Filtra todos os nós que são do tipo 'utp'
        utp_nodes = [n for n, d in self.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        
        for node in utp_nodes:
            # 2. No NetworkX, os municípios são sucessores (filhos) do nó UTP
            successors = list(self.hierarchy.successors(node))
            
            # 3. Se houver exatamente um sucessor, é uma UTP unitária
            if len(successors) == 1:
                # Remove o prefixo 'UTP_' para retornar apenas o ID limpo
                utp_id = str(node).replace("UTP_", "")
                unitary_utps.append(utp_id)
                
        return unitary_utps

    def compute_graph_coloring(self, gdf: gpd.GeoDataFrame) -> Dict[int, int]:
        """Calcula a coloração mínima usando projeção métrica para precisão."""
        if gdf is None or gdf.empty:
            return {}

        logging.info("Calculando coloração topológica mínima (EPSG:5880)...")
        
        # 1. Limpeza e Dissolve (Cria o mapa de UTPs)
        gdf_clean = gdf.dropna(subset=['UTP_ID', 'geometry']).copy()
        gdf_clean['UTP_ID'] = gdf_clean['UTP_ID'].astype(str)
        
        # DEBUG: Verificar diversidade de UTPs
        unique_utps = gdf_clean['UTP_ID'].unique()
        logging.info(f"   [DEBUG] UTPs únicas no GDF: {len(unique_utps)}")
        if len(unique_utps) <= 5:
             logging.info(f"   [DEBUG] Lista de UTPs: {unique_utps}")

        gdf_utps = gdf_clean[['UTP_ID', 'geometry']].dissolve(by='UTP_ID')
        
        # 2. Projeção métrica para buffer preciso
        gdf_projected = gdf_utps.to_crs(epsg=5880)
        
        G = nx.Graph()
        G.add_nodes_from(gdf_projected.index)

        # 3. Identificação de vizinhos com Buffer de 100m (Mais robusto para gaps)
        logging.info(f"Analisando adjacência para {len(gdf_projected)} UTPs com buffer de 100m...")
        gdf_left = gdf_projected.copy()
        gdf_left['geometry'] = gdf_left.geometry.buffer(100)
        
        gdf_right = gdf_projected.reset_index()[['UTP_ID', 'geometry']].rename(
            columns={'UTP_ID': 'ID_RIGHT'}
        )
        
        joins = gpd.sjoin(gdf_left, gdf_right, predicate='intersects', how='inner')

        for idx_left, row in joins.iterrows():
            idx_right = row['ID_RIGHT']
            if str(idx_left) != str(idx_right):
                G.add_edge(str(idx_left), str(idx_right))

        logging.info(f"Grafo de adjacência construído: {G.number_of_nodes()} nós e {G.number_of_edges()} conexões.")
        
        if G.number_of_edges() == 0 and len(unique_utps) > 1:
             logging.warning("   [DEBUG] ⚠️ Nenhuma adjacência encontrada! O mapa pode ficar monocromático se o algoritmo falhar.")

        # 4. Coloração Mínima (DSATUR garante menos cores em mapas geográficos)
        utp_color_map = nx.coloring.greedy_color(G, strategy='DSATUR')
        
        # 5. Mapeamento Final: cd_mun (int) -> cor_id
        final_coloring = {}
        for _, row in gdf_clean.iterrows():
            cd_mun = int(row['CD_MUN'])
            utp_id = str(row['UTP_ID'])
            final_coloring[cd_mun] = utp_color_map.get(utp_id, 0)
            
        colors_used = max(utp_color_map.values(), default=0) + 1
        logging.info(f"Coloração concluída: {colors_used} cores.")
        return final_coloring

    def export_snapshot(self, path: Path, step_name: str, gdf: gpd.GeoDataFrame = None):
        """
        Exporta um snapshot completo do estado atual do grafo para JSON.
        
        Inclui:
        - Nós e seus atributos (utp_id, sede_utp, regic, etc.)
        - Mapeamento de Sedes (utp_seeds)
        - Coloração atual (se gdf fornecido ou já calculada)
        """
        import json
        from datetime import datetime
        
        # 1. Preparar dados dos nós
        # 1. Preparar dados dos nós (Enrich com dados estruturais)
        nodes_data = {}
        for node in self.hierarchy.nodes():
             data = self.hierarchy.nodes[node].copy()
             
             # Se for município, precisamos injetar o utp_id derivado da estrutura (arestas)
             # pois o SnapshotLoader espera encontrar esse atributo no nó
             if data.get('type') == 'municipality':
                 # Resolver UTP pai
                 utp_id = self.get_municipality_utp(node)
                 data['utp_id'] = utp_id
                 
                 # Garantir que sede_utp está presente se for True
                 # (Às vezes pode estar faltando se foi definido apenas na lista de seeds)
                 if str(data.get('utp_id')) in self.utp_seeds:
                     # FIX: Enforce strict consistency with utp_seeds to prevent 'ghost sedes'
                     # If the UTP has a registered seed, ONLY that seed should be True.
                     # All others must be False.
                     expected_sede = self.utp_seeds[str(data.get('utp_id'))]
                     is_sede = (str(expected_sede) == str(node))
                     data['sede_utp'] = is_sede
             
             # Converter chaves para string para JSON
             nodes_data[str(node)] = data
             
        # 2. Preparar seeds (convertendo chaves/valores para str/int)
        seeds_data = {str(k): int(v) for k, v in self.utp_seeds.items()}
        
        # 3. Preparar coloração (se existir no GDF)
        coloring = {}
        if gdf is not None and 'COLOR_ID' in gdf.columns and 'CD_MUN' in gdf.columns:
            for _, row in gdf[['CD_MUN', 'COLOR_ID']].dropna().iterrows():
                coloring[str(int(row['CD_MUN']))] = int(row['COLOR_ID'])
        
        snapshot = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "step": step_name,
                "version": "1.0"
            },
            "nodes": nodes_data,
            "utp_seeds": seeds_data,
            "coloring": coloring
        }
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
            
        logging.info(f"📸 Snapshot '{step_name}' salvo em: {path}")
