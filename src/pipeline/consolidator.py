# src/pipeline/consolidator.py
import logging
import pandas as pd
import geopandas as gpd
from src.core.validator import TerritorialValidator


class UTPConsolidator:
    def __init__(self, graph, validator: TerritorialValidator):
        self.graph = graph
        self.validator = validator
        self.logger = logging.getLogger("GeoValida.Consolidator")

    def run_functional_merging(self, flow_df: pd.DataFrame) -> int:
        """Passo 5: Consolidação baseada em Fluxo de Viagens."""
        self.logger.info("Executando consolidação funcional...")
        total_changes = 0
        
        # Placeholder: lógica de consolidação funcional
        # Percorre UTPs unitárias e busca destino principal via fluxos
        if flow_df is None or flow_df.empty:
            self.logger.info("Sem dados de fluxo para consolidação funcional.")
            return 0
        
        self.logger.info(f"Passo 5 concluído: {total_changes} consolidações realizadas.")
        return total_changes

    def run_territorial_regic(self, gdf: gpd.GeoDataFrame, map_gen) -> int:
        """Passo 7: Limpeza de unitárias usando REGIC + Envolvência (Métrica)."""
        self.logger.info("Executando limpeza territorial via REGIC...")
        total_changes = 0
        
        # Placeholder: lógica de consolidação REGIC + adjacência
        # Usa CRS EPSG:5880 para medições de comprimento de fronteira
        if gdf is None or gdf.empty:
            self.logger.info("Sem dados geográficos para limpeza territorial.")
            return 0
        
        self.logger.info(f"Passo 7 concluído: {total_changes} consolidações realizadas.")
        return total_changes
