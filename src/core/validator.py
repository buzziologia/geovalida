# -*- coding: utf-8 -*-
import logging
from typing import List

import networkx as nx
import geopandas as gpd
import shapely.geometry as sgeom
import shapely.ops as ops

from .graph import TerritorialGraph


class TerritorialValidator:
    # Ranking atualizado baseado nos nomes presentes em SEDE+regic
    # Menor valor = Maior influência
    REGIC_RANK = {
        'Metrópole Nacional': 1,
        'Metrópole': 2,
        'Capital Regional A': 3,
        'Capital Regional B': 4,
        'Capital Regional C': 5,
        'Centro Sub-Regional A': 6,
        'Centro Sub-Regional B': 7,
        'Centro de Zona A': 8,
        'Centro de Zona B': 9,
        'Centro Local': 10,
        'Sem Dados': 98,
        '6': 99  # Padrão caso não encontrado
    }

    def __init__(self, graph: TerritorialGraph):
        self.graph = graph
        self.logger = logging.getLogger("TerritorialValidator")

    def get_regic_score(self, cd_mun: int) -> int:
        """Retorna o peso hierárquico usando as descrições carregadas no grafo."""
        level_desc = self.graph.mun_regic.get(int(cd_mun), '6')
        return self.REGIC_RANK.get(level_desc, 99)

    def _safe_get_geometry(self, gdf: gpd.GeoDataFrame, filter_col: str, value):
        """Retorna a geometria (shapely) de forma segura ou `None`."""
        if gdf is None or gdf.empty:
            return None
        rows = gdf.loc[gdf[filter_col] == value]
        if rows.empty:
            return None
        try:
            geom = rows.geometry.iloc[0]
            if geom is None or geom.is_empty:
                return None
            return geom
        except Exception:
            return None

    def get_shared_boundary_length(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> float:
        """Calcula o comprimento da fronteira partilhada em metros.

        Reprojeta as geometrias para EPSG:5880 antes de medir comprimento.
        Retorna 0.0 em caso de qualquer ausência de geometria ou erro.
        """
        if gdf is None or gdf.empty:
            return 0.0

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return 0.0

        target_rows = gdf.loc[gdf['UTP_ID'] == str(target_utp_id)]
        if target_rows.empty:
            return 0.0

        try:
            # unary_union retorna shapely geometry (Polygon/MultiPolygon)
            target_utp_geom = target_rows.unary_union
        except Exception:
            return 0.0

        if target_utp_geom is None or target_utp_geom.is_empty:
            return 0.0

        # Reprojetar para EPSG:5880 para medições métricas
        try:
            source_crs = gdf.crs
            if source_crs is None:
                # Sem CRS definido: não é seguro medir em metros
                self.logger.debug("get_shared_boundary_length: GeoDataFrame sem CRS definido.")
                return 0.0

            # Cria GeoSeries temporárias e reprojeta
            mun_series = gpd.GeoSeries([mun_geom], crs=source_crs).to_crs(epsg=5880)
            target_series = gpd.GeoSeries([target_utp_geom], crs=source_crs).to_crs(epsg=5880)

            mun_proj = mun_series.iloc[0]
            target_proj = target_series.unary_union
        except Exception as e:
            self.logger.debug(f"Erro ao reprojetar geometrias: {e}")
            return 0.0

        try:
            shared = mun_proj.boundary.intersection(target_proj)
            # Algumas interseções podem gerar GeometryCollections sem comprimento
            length = getattr(shared, 'length', 0.0)
            return float(length) if length is not None else 0.0
        except Exception as e:
            self.logger.debug(f"Erro ao calcular interseção de fronteira: {e}")
            return 0.0

    def get_rm_of_utp(self, utp_id: str) -> str:
        utp_node = f"UTP_{utp_id}"
        if not self.graph.hierarchy.has_node(utp_node):
            return "NAO_ENCONTRADA"
        parents = list(self.graph.hierarchy.predecessors(utp_node))
        return parents[0] if parents else "SEM_RM"

    def is_change_allowed(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> bool:
        """Verifica RM e Adjacência Geográfica.

        Garante checagens defensivas sobre geometrias e evita exceções.
        """
        current_utp = self.graph.get_municipality_utp(mun_id)
        rm_origin = self.get_rm_of_utp(current_utp)
        rm_dest = self.get_rm_of_utp(target_utp_id)

        if rm_origin != rm_dest:
            return False

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return False

        target_geoms = gdf.loc[gdf['UTP_ID'] == str(target_utp_id)]
        if target_geoms.empty:
            return False

        try:
            # Buffer pequeno em graus é uma heurística; se CRS for métrico, buffer em metros seria preferível
            buf = mun_geom.buffer(0.01)
            return target_geoms.geometry.intersects(buf).any()
        except Exception:
            return False

    def validate_utp_contiguity(self, utp_id: str, gdf_mun_utp: gpd.GeoDataFrame, sede_id: int) -> List[int]:
        """Detecta municípios isolados dentro de uma UTP usando uma construção de grafo otimizada.

        Usa `sjoin(..., predicate='touches')` quando disponível para evitar o loop O(n^2).
        """
        if gdf_mun_utp is None or gdf_mun_utp.empty:
            return []

        # Normaliza colunas esperadas
        if 'CD_MUN' not in gdf_mun_utp.columns or 'geometry' not in gdf_mun_utp.columns:
            self.logger.debug("validate_utp_contiguity: GDF sem colunas esperadas.")
            return []

        # Cria um GeoDataFrame reduzido para a operação
        gdf = gdf_mun_utp[['CD_MUN', 'geometry']].reset_index(drop=True).copy()

        # Tentativa vetorizada com spatial join (muito mais rápida em grandes conjuntos)
        edges = set()
        try:
            sjoin = gpd.sjoin(gdf, gdf, how='left', predicate='touches')
            left_col = 'CD_MUN_left' if 'CD_MUN_left' in sjoin.columns else 'CD_MUN_x' if 'CD_MUN_x' in sjoin.columns else 'CD_MUN'
            right_col = 'CD_MUN_right' if 'CD_MUN_right' in sjoin.columns else 'CD_MUN_y' if 'CD_MUN_y' in sjoin.columns else 'CD_MUN'

            # Ajusta nomes quando necessário
            if left_col == right_col:
                # Caso raro: sjoin pode manter mesmo nome; usar index mapping
                sjoin = sjoin.rename(columns={left_col: 'CD_MUN_left'})
                sjoin['CD_MUN_right'] = sjoin['index_right']
                left_col = 'CD_MUN_left'; right_col = 'CD_MUN_right'

            for _, row in sjoin.iterrows():
                a = row.get(left_col)
                b = row.get(right_col)
                if a is None or b is None:
                    continue
                if a == b:
                    continue
                # Mantém arestas únicas (ordem independente)
                edges.add(tuple(sorted((int(a), int(b)))))
        except Exception:
            # Fallback seguro caso sjoin/predicate não esteja disponível
            self.logger.debug("sjoin falhou; usando fallback quadrático (menor desempenho).")
            for i, r in gdf.iterrows():
                candidates = gdf.loc[gdf.geometry.touches(r.geometry), 'CD_MUN'].tolist()
                for c in candidates:
                    if int(c) != int(r['CD_MUN']):
                        edges.add(tuple(sorted((int(r['CD_MUN']), int(c)))))

        # Monta o grafo de adjacência
        G = nx.Graph()
        municipios_ids = gdf_mun_utp['CD_MUN'].tolist()
        G.add_nodes_from(municipios_ids)
        for a, b in edges:
            G.add_edge(a, b)

        # Verifica SEDE
        if sede_id not in G:
            self.logger.error(f"Erro Contiguidade: SEDE {sede_id} não encontrada na UTP {utp_id}")
            return []

        alcancaveis = nx.node_connected_component(G, sede_id)
        isolados = [m for m in municipios_ids if m not in alcancaveis]

        if isolados:
            self.logger.warning(f"UTP {utp_id} possui {len(isolados)} municípios isolados.")

        return isolados

    def is_adjacent(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> bool:
        """Verifica se o município toca geograficamente qualquer município da UTP destino."""
        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return False

        target_geoms = gdf.loc[gdf['UTP_ID'] == str(target_utp_id)]
        if target_geoms.empty:
            return False

        try:
            return target_geoms.geometry.touches(mun_geom.buffer(0.01)).any()
        except Exception:
            return False

    def is_adjacent_to_any_in_utp(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> bool:
        """Verifica se o município toca QUALQUER município da UTP de destino."""
        if gdf is None or gdf.empty:
            return False

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return False

        target_geoms = gdf.loc[gdf['UTP_ID'] == str(target_utp_id)]
        if target_geoms.empty:
            return False

        try:
            return target_geoms.geometry.intersects(mun_geom.buffer(0.01)).any()
        except Exception:
            return False

    def is_non_rm(self, utp_id: str) -> bool:
        """Verifica se a UTP pertence ao grupo SEM_RM."""
        rm_node = self.get_rm_of_utp(utp_id)
        return rm_node == "RM_SEM_RM"

    def is_non_rm_utp(self, utp_id: str) -> bool:
        """Verifica se a UTP pertence ao grupo genérico SEM_RM."""
        rm_node = self.get_rm_of_utp(utp_id)
        return rm_node == "RM_SEM_RM"

    def get_neighboring_utps(self, mun_id: int, gdf: gpd.GeoDataFrame) -> List[str]:
        """Retorna IDs de UTPs que fazem fronteira com o município."""
        if gdf is None or gdf.empty:
            return []

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return []

        try:
            neighbors = gdf.loc[gdf.geometry.intersects(mun_geom.buffer(0.05))]
            return neighbors['UTP_ID'].dropna().unique().astype(str).tolist()
        except Exception:
            return []
