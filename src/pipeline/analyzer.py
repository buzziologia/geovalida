# src/pipeline/analyzer.py
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple


class ODAnalyzer:
    """Analisa Matriz de Origem-Destino para identificar fluxos principais."""

    def __init__(self):
        self.logger = logging.getLogger("GeoValida.ODAnalyzer")
        self.full_flow_df = None

    def run_full_analysis(self) -> pd.DataFrame:
        """Executa análise completa dos fluxos OD."""
        self.logger.info("Iniciando análise de Matriz OD...")
        
        # Placeholder: análise básica de fluxos
        # Em produção, carregaria CSVs da pasta person-matrix-data
        self.full_flow_df = pd.DataFrame({
            'origem': [],
            'destino': [],
            'fluxo': [],
            'proporcao': []
        })
        
        self.logger.info(f"Análise OD concluída: {len(self.full_flow_df)} registros.")
        return self.full_flow_df

    def get_main_destination(self, origin_mun: int, threshold: float = 0.1) -> Tuple[int, float]:
        """Retorna o destino principal para um município origem.
        
        Args:
            origin_mun: Código do município de origem
            threshold: Proporção mínima de fluxo para considerar como principal
            
        Returns:
            (cd_mun_destino, proporcao_fluxo)
        """
        if self.full_flow_df is None or self.full_flow_df.empty:
            return None, 0.0
        
        flows = self.full_flow_df[self.full_flow_df['origem'] == origin_mun]
        if flows.empty:
            return None, 0.0
        
        top = flows.nlargest(1, 'proporcao')
        if top.empty or top['proporcao'].values[0] < threshold:
            return None, 0.0
        
        return int(top['destino'].values[0]), float(top['proporcao'].values[0])

    def filter_significant_flows(self, min_proportion: float = 0.05) -> pd.DataFrame:
        """Filtra fluxos significativos (acima do threshold)."""
        if self.full_flow_df is None or self.full_flow_df.empty:
            return pd.DataFrame()
        
        return self.full_flow_df[self.full_flow_df['proporcao'] >= min_proportion]
