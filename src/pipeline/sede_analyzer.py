# src/pipeline/sede_analyzer.py
"""
Módulo para análise de dependências entre sedes (UTPs).
Identifica hierarquias e relações de dependência baseadas em dados socioeconômicos e fluxos.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json


class SedeAnalyzer:
    """
    Analisa dependências entre sedes de UTPs usando dados socioeconômicos e padrões de fluxo.
    
    O principal critério de alerta é quando o fluxo principal de uma sede vai para 
    outra sede a até 2 horas de distância, indicando possível dependência funcional.
    """
    
    def __init__(self, data_path: Optional[Path] = None, consolidation_loader=None):
        """
        Inicializa o analisador de sedes.
        
        Args:
            data_path: Caminho para o diretório de dados. Se None, usa o padrão do projeto.
            consolidation_loader: Opcional. ConsolidationLoader para aplicar consolidações territoriais
                                 antes da análise. Se fornecido, a análise será feita sobre a
                                 configuração territorial PÓS-consolidação.
        """
        self.logger = logging.getLogger("GeoValida.SedeAnalyzer")
        
        if data_path is None:
            self.data_path = Path(__file__).parent.parent.parent / "data"
        else:
            self.data_path = Path(data_path)
        
        # DataFrames principais
        self.df_municipios = None
        self.df_impedance = None
        self.df_sede_analysis = None
        
        # Consolidation loader (se fornecido)
        self.consolidation_loader = consolidation_loader
        
        # Dados agregados
        self.sede_metrics = {}
        self.dependency_alerts = []
    
    def load_initialization_data(self) -> bool:
        """
        Carrega dados do initialization.json que contém informações consolidadas.
        
        Se consolidation_loader foi fornecido no __init__, aplica as consolidações
        territoriais aos dados antes de prosseguir com a análise.
        
        Returns:
            True se carregou com sucesso, False caso contrário.
        """
        json_path = self.data_path / "initialization.json"
        
        if not json_path.exists():
            self.logger.error(f"Arquivo {json_path} não encontrado")
            return False
        
        try:
            self.logger.info(f"Carregando dados de {json_path}...")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Converter lista de municípios para DataFrame
            municipios = data.get('municipios', [])
            self.df_municipios = pd.DataFrame(municipios)
            
            # Aplicar consolidações territoriais se fornecidas
            if self.consolidation_loader and self.consolidation_loader.is_executed():
                self.logger.info("📍 Aplicando consolidações territoriais aos dados...")
                original_count = len(self.df_municipios)
                
                # Aplicar consolidações
                self.df_municipios = self.consolidation_loader.apply_consolidations_to_dataframe(
                    self.df_municipios
                )
                
                summary = self.consolidation_loader.get_summary()
                self.logger.info(f"  ✓ Consolidações aplicadas: {summary['total_consolidations']} movimentações")
                self.logger.info(f"  ✓ UTPs reduzidas: {summary['unique_sources']} → {summary['unique_targets']}")
                self.logger.info(f"  ⚠️  ANÁLISE SERÁ BASEADA NA CONFIGURAÇÃO PÓS-CONSOLIDAÇÃO")
            else:
                self.logger.info("  ℹ️  Usando configuração inicial (sem consolidações)")
            
            self.logger.info(f"✓ Carregados {len(self.df_municipios)} municípios")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar initialization.json: {e}")
            return False
    
    def load_impedance_data(self) -> bool:
        """
        Carrega matriz de impedância (tempo de viagem) filtrada para até 2h.
        
        Returns:
            True se carregou com sucesso, False caso contrário.
        """
        impedance_path = self.data_path / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
        
        if not impedance_path.exists():
            self.logger.warning(f"Arquivo de impedância não encontrado: {impedance_path}")
            return False
        
        try:
            self.logger.info(f"Carregando matriz de impedância de {impedance_path}...")
            
            self.df_impedance = pd.read_csv(
                impedance_path, 
                sep=';',
                dtype={'COD_IBGE_ORIGEM': str, 'COD_IBGE_DESTINO': str}
            )
            
            # Renomear colunas para consistência
            self.df_impedance.columns = ['par_ibge', 'origem', 'destino', 'tempo_horas']
            
            # Converter coluna tempo_horas de formato brasileiro (vírgula) para float
            self.df_impedance['tempo_horas'] = (
                self.df_impedance['tempo_horas']
                .astype(str)
                .str.replace(',', '.')
                .astype(float)
            )
            
            self.logger.info(f"✓ Carregados {len(self.df_impedance)} pares origem-destino (≤2h)")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar matriz de impedância: {e}")
            return False
    
    def get_main_flow_destination(self, cd_mun_origem: int) -> Tuple[Optional[int], float, int]:
        """
        Identifica o principal destino de fluxo para um município origem.
        
        Args:
            cd_mun_origem: Código IBGE do município de origem
            
        Returns:
            Tupla com (cd_mun_destino, proporcao_fluxo, total_viagens)
        """
        if self.df_municipios is None:
            return None, 0.0, 0
        
        # Buscar município
        mun = self.df_municipios[self.df_municipios['cd_mun'] == cd_mun_origem]
        if mun.empty:
            return None, 0.0, 0
        
        # Obter modal_matriz (fluxos por destino)
        modal_matriz = mun.iloc[0].get('modal_matriz', {})
        
        # Agregar todos os modais
        flows_by_dest = {}
        total_viagens = 0
        
        for modal, destinos in modal_matriz.items():
            if isinstance(destinos, dict):
                for dest, viagens in destinos.items():
                    dest_int = int(dest)
                    flows_by_dest[dest_int] = flows_by_dest.get(dest_int, 0) + viagens
                    total_viagens += viagens
        
        if not flows_by_dest or total_viagens == 0:
            return None, 0.0, 0
        
        # Encontrar destino com maior fluxo
        max_dest = max(flows_by_dest.items(), key=lambda x: x[1])
        cd_mun_destino = max_dest[0]
        viagens = max_dest[1]
        proporcao = viagens / total_viagens
        
        return cd_mun_destino, proporcao, total_viagens
    
    def get_travel_time(self, cd_origem: int, cd_destino: int) -> Optional[float]:
        """
        Obtém o tempo de viagem entre dois municípios.
        
        Args:
            cd_origem: Código IBGE do município de origem
            cd_destino: Código IBGE do município de destino
            
        Returns:
            Tempo em horas, ou None se não houver conexão ≤2h
        """
        if self.df_impedance is None:
            return None
        
        # Converter para string para busca
        origem_str = str(cd_origem)
        destino_str = str(cd_destino)
        
        # Buscar par na matriz
        pair = self.df_impedance[
            (self.df_impedance['origem'] == origem_str) & 
            (self.df_impedance['destino'] == destino_str)
        ]
        
        if pair.empty:
            return None
        
        # Retornar tempo (já convertido para float no carregamento)
        return float(pair.iloc[0]['tempo_horas'])
    
    def is_sede(self, cd_mun: int) -> bool:
        """
        Verifica se um município é sede de UTP.
        
        Args:
            cd_mun: Código IBGE do município
            
        Returns:
            True se é sede, False caso contrário
        """
        if self.df_municipios is None:
            return False
        
        mun = self.df_municipios[self.df_municipios['cd_mun'] == cd_mun]
        if mun.empty:
            return False
        
        return bool(mun.iloc[0].get('sede_utp', False))
    
    def check_dependency_criteria(self, cd_sede_origem: int) -> Optional[Dict]:
        """
        Verifica se uma sede atende ao critério de dependência:
        - O principal fluxo vai para outra sede
        - O tempo de viagem é ≤ 2h
        
        Args:
            cd_sede_origem: Código IBGE da sede de origem
            
        Returns:
            Dict com informações do alerta, ou None se não há dependência
        """
        # 1. Obter principal destino
        cd_destino, proporcao, total_viagens = self.get_main_flow_destination(cd_sede_origem)
        
        if cd_destino is None:
            return None
        
        # 2. Verificar se o destino é uma sede
        if not self.is_sede(cd_destino):
            return None
        
        # 3. Verificar tempo de viagem
        tempo = self.get_travel_time(cd_sede_origem, cd_destino)
        
        if tempo is None or tempo > 2.0:
            return None
        
        # Critério atendido - criar alerta
        mun_origem = self.df_municipios[self.df_municipios['cd_mun'] == cd_sede_origem].iloc[0]
        mun_destino = self.df_municipios[self.df_municipios['cd_mun'] == cd_destino].iloc[0]
        
        return {
            'sede_origem': cd_sede_origem,
            'nm_sede_origem': mun_origem['nm_mun'],
            'utp_origem': mun_origem['utp_id'],
            'sede_destino': cd_destino,
            'nm_sede_destino': mun_destino['nm_mun'],
            'utp_destino': mun_destino['utp_id'],
            'proporcao_fluxo': proporcao,
            'total_viagens': total_viagens,
            'tempo_horas': tempo,
            'alerta': 'DEPENDÊNCIA FUNCIONAL DETECTADA'
        }
    
    def calculate_socioeconomic_metrics(self) -> pd.DataFrame:
        """
        Calcula métricas socioeconômicas agregadas por UTP (sede).
        
        Returns:
            DataFrame com métricas por sede
        """
        if self.df_municipios is None:
            return pd.DataFrame()
        
        # Filtrar apenas sedes
        df_sedes = self.df_municipios[self.df_municipios['sede_utp'] == True].copy()
        
        metrics_list = []
        
        for _, sede in df_sedes.iterrows():
            cd_mun = sede['cd_mun']
            utp_id = sede['utp_id']
            
            # Agregar dados de todos os municípios da UTP
            municipios_utp = self.df_municipios[self.df_municipios['utp_id'] == utp_id]
            
            # Calcular métricas agregadas
            pop_total = municipios_utp['populacao_2022'].sum()
            num_municipios = len(municipios_utp)
            
            # Agregar viagens totais da UTP (soma de todos os modais de todos os municípios)
            total_viagens_utp = 0
            for _, mun in municipios_utp.iterrows():
                modais = mun.get('modais', {})
                if isinstance(modais, dict):
                    total_viagens_utp += sum(modais.values())
            
            # Verificar se a sede possui aeroporto comercial
            # Extrair dados completos do aeroporto quando disponível
            aeroporto_info = sede.get('aeroporto')
            tem_aeroporto = False
            aeroporto_icao = ''
            aeroporto_passageiros = 0
            
            if aeroporto_info is not None and isinstance(aeroporto_info, dict):
                # Nova estrutura: {'icao': 'SBSP', 'cidade': 'São Paulo', 'passageiros_anual': 24252637}
                icao = aeroporto_info.get('icao', '')
                passageiros = aeroporto_info.get('passageiros_anual', 0)
                
                if icao and str(icao).strip() != '' and str(icao).lower() not in ['nan', 'none']:
                    tem_aeroporto = True
                    aeroporto_icao = str(icao)
                    aeroporto_passageiros = int(passageiros) if passageiros else 0

            
            # Obter classificação de turismo da sede
            # Nota: Pode estar vazio se dados não foram carregados
            turismo_classificacao = sede.get('turismo_classificacao', '')
            if turismo_classificacao and str(turismo_classificacao).lower() not in ['nan', 'none', '']:
                turismo_classificacao = str(turismo_classificacao)
            else:
                turismo_classificacao = ''
            
            # Obter principal destino da sede (para onde mais viajam seus habitantes)
            cd_destino, prop_fluxo, viagens_sede = self.get_main_flow_destination(cd_mun)
            
            # Obter nome do destino principal e tempo de viagem
            nm_destino = ''
            tempo_destino = None
            if cd_destino is not None:
                dest_mun = self.df_municipios[self.df_municipios['cd_mun'] == cd_destino]
                if not dest_mun.empty:
                    nm_destino = dest_mun.iloc[0]['nm_mun']
                tempo_destino = self.get_travel_time(cd_mun, cd_destino)
            
            # Verificar se há alerta de dependência funcional
            # (sede depende de outra sede a até 2h de distância)
            alerta = self.check_dependency_criteria(cd_mun)
            
            metrics_list.append({
                'utp_id': utp_id,
                'cd_mun_sede': cd_mun,
                'nm_sede': sede['nm_mun'],
                'uf': sede['uf'],
                'regic': sede.get('regic', ''),
                'regiao_metropolitana': sede.get('regiao_metropolitana', ''),
                'populacao_total_utp': int(pop_total),
                'num_municipios': num_municipios,  # Quantidade de municípios nesta UTP
                'total_viagens': total_viagens_utp,
                'tem_aeroporto': tem_aeroporto,
                'aeroporto_icao': aeroporto_icao,
                'aeroporto_passageiros': aeroporto_passageiros,
                'turismo': turismo_classificacao,
                'principal_destino_cd': cd_destino,
                'principal_destino_nm': nm_destino,
                'proporcao_fluxo_principal': prop_fluxo,
                'tempo_ate_destino_h': tempo_destino,
                'tem_alerta_dependencia': alerta is not None,
                'alerta_detalhes': alerta
            })
        
        self.df_sede_analysis = pd.DataFrame(metrics_list)
        return self.df_sede_analysis
    
    def analyze_sede_dependencies(self) -> Dict:
        """
        Executa análise completa de dependências entre sedes.
        
        Returns:
            Dicionário com resumo da análise
        """
        self.logger.info("Iniciando análise de dependências entre sedes...")
        
        # 1. Carregar dados
        if not self.load_initialization_data():
            self.logger.error("Falha ao carregar dados de inicialização")
            return {'success': False, 'error': 'Falha ao carregar dados'}
        
        # 2. Carregar impedância
        self.load_impedance_data()
        
        # 3. Calcular métricas
        df_metrics = self.calculate_socioeconomic_metrics()
        
        # 4. Identificar alertas
        self.dependency_alerts = df_metrics[df_metrics['tem_alerta_dependencia']].to_dict('records')
        
        summary = {
            'success': True,
            'total_sedes': len(df_metrics),
            'total_alertas': len(self.dependency_alerts),
            'utps_analisadas': df_metrics['utp_id'].nunique(),
            'populacao_total': int(df_metrics['populacao_total_utp'].sum()),
            'sedes_com_aeroporto': int(df_metrics['tem_aeroporto'].sum())
        }
        
        self.logger.info(f"✅ Análise concluída: {summary['total_sedes']} sedes, {summary['total_alertas']} alertas")
        
        return summary
    
    def export_sede_comparison_table(self) -> pd.DataFrame:
        """
        Exporta tabela comparativa entre sedes para visualização.
        
        Returns:
            DataFrame formatado para exibição no dashboard
        """
        if self.df_sede_analysis is None:
            return pd.DataFrame()
        
        # Selecionar e renomear colunas para exibição
        df_display = self.df_sede_analysis[[
            'utp_id',
            'nm_sede',
            'uf',
            'regic',
            'populacao_total_utp',
            'num_municipios',
            'total_viagens',
            'tem_aeroporto',
            'aeroporto_icao',
            'turismo',
            'principal_destino_nm',
            'proporcao_fluxo_principal',
            'tempo_ate_destino_h',
            'tem_alerta_dependencia'
        ]].copy()
        
        # Renomear para nomes amigáveis
        df_display.columns = [
            'UTP',
            'Sede',
            'UF',
            'REGIC',
            'População',
            'Nº Municípios',
            'Viagens',
            'Aeroporto',
            'ICAO',
            'Turismo',
            'Principal Destino',
            'Fluxo (%)',
            'Tempo (h)',
            'Alerta'
        ]
        
        # Formatar percentual
        df_display['Fluxo (%)'] = (df_display['Fluxo (%)'] * 100).round(1)
        
        # Formatar tempo
        df_display['Tempo (h)'] = df_display['Tempo (h)'].round(2)
        
        # Converter booleanos para símbolos
        df_display['Aeroporto'] = df_display['Aeroporto'].map({True: 'Sim', False: ''})
        df_display['Alerta'] = df_display['Alerta'].map({True: 'SIM', False: ''})
        
        return df_display
