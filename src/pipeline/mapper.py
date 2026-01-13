# src/pipeline/mapper.py
import logging
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path


class UTPMapGenerator:
    def __init__(self, graph):
        self.graph = graph
        self.gdf_complete = None
        self.logger = logging.getLogger("GeoValida.Mapper")

    def load_shapefiles(self):
        """Carrega e prepara os arquivos geográficos.

        Tenta localizar o shapefile de municípios padrão dentro da configuração `FILES`.
        """
        from src.config import FILES

        shp_candidate = None
        try:
            # Preferimos um arquivo .shp com o nome padrão
            shp_candidate = Path(FILES['shapefiles']) / "BR_Municipios_2024.shp"
            if not shp_candidate.exists():
                # fallback para qualquer .shp dentro da pasta
                shp_dir = Path(FILES['shapefiles'])
                shp_files = list(shp_dir.glob('*.shp'))
                if shp_files:
                    shp_candidate = shp_files[0]
                else:
                    raise FileNotFoundError(f"Nenhum shapefile encontrado em {shp_dir}")

            self.gdf_complete = gpd.read_file(shp_candidate)
        except Exception as e:
            self.logger.error(f"Erro carregando shapefile de municípios: {e}")
            raise

    def sync_with_graph(self, graph):
        """Atualiza o GeoDataFrame com o estado atual do Grafo (quem pertence a qual UTP).

        Implementação vetorizada: constrói um dicionário `cd_mun -> utp_id` e aplica um `map` sobre
        a coluna `CD_MUN` do GeoDataFrame para atualizar `UTP_ID` de forma rápida.
        """
        if self.gdf_complete is None:
            raise RuntimeError("GDF não carregado. Execute `load_shapefiles()` antes.")

        if 'CD_MUN' not in self.gdf_complete.columns:
            raise RuntimeError("GeoDataFrame não contém coluna 'CD_MUN'.")

        # Constrói mapeamento do grafo: município -> UTP
        mapping = {}
        for node, data in graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                mun_id = int(node)
                # encontra o pai UTP
                parents = list(graph.hierarchy.predecessors(node))
                utp_id = None
                for p in parents:
                    ps = str(p)
                    if ps.startswith('UTP_'):
                        utp_id = ps.replace('UTP_', '')
                        break
                if utp_id:
                    mapping[mun_id] = utp_id

        self.logger.info(f"Mapeamento: {len(mapping)} municípios → UTPs")

        # Aplica mapeamento de forma vetorizada
        try:
            # Converte CD_MUN para int para matching correto
            self.gdf_complete['CD_MUN_int'] = self.gdf_complete['CD_MUN'].astype(int)
            self.gdf_complete['UTP_ID'] = self.gdf_complete['CD_MUN_int'].map(mapping)
            
            # Remove coluna temporária
            self.gdf_complete.drop('CD_MUN_int', axis=1, inplace=True)
            
            # Preenche NAs com valor padrão
            self.gdf_complete['UTP_ID'] = self.gdf_complete['UTP_ID'].fillna('SEM_UTP')
            
            self.logger.info(f"Sincronização completa. UTPs únicas: {self.gdf_complete['UTP_ID'].nunique()}")
        except Exception as e:
            self.logger.error(f"Erro ao sincronizar: {e}")
            raise

        return self

    def save_map(self, output_path, title="Mapa UTP"):
        """Gera e salva a imagem do mapa."""
        if self.gdf_complete is None:
            raise RuntimeError("GDF não carregado. Execute `load_shapefiles()` antes.")

        fig, ax = plt.subplots(figsize=(15, 15))
        # Lógica de plotagem (cores por UTP, bordas de UF, etc)
        try:
            self.gdf_complete.plot(ax=ax, column='UTP_ID', cmap='tab20')
        except Exception as e:
            self.logger.error(f"Erro ao plotar o mapa: {e}")
            raise
        plt.title(title)
        plt.savefig(output_path)
        plt.close()