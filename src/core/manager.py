import logging
import json
import pandas as pd
from pathlib import Path

# Importações modulares
from src.config import FILES, setup_logging
from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.analyzer import ODAnalyzer
from src.pipeline.consolidator import UTPConsolidator
from src.pipeline.mapper import UTPMapGenerator

class GeoValidaManager:
    """
    Orquestrador Principal do GeoValida.
    Gere o estado do Grafo, Análise de Fluxos e Geração de Mapas.
    """
    def __init__(self):
        # Inicializa o Logger centralizado
        setup_logging()
        self.logger = logging.getLogger("GeoValida")
        
        # Componentes Core
        self.graph = TerritorialGraph()
        self.validator = TerritorialValidator(self.graph)
        
        # Componentes de Pipeline
        self.analyzer = ODAnalyzer()
        self.map_generator = UTPMapGenerator(self.graph)
        self.consolidator = UTPConsolidator(self.graph, self.validator)
    
    @property
    def gdf(self):
        """Propriedade para acessar o GeoDataFrame do map_generator."""
        return self.map_generator.gdf_complete if self.map_generator else None

    def load_from_initialization_json(self):
        """Carrega dados pre-consolidados do initialization.json e popula o grafo territorial."""
        # Using a reliable way to find 'data/initialization.json' relative to this file
        # src/core/manager.py -> src/core -> src -> root -> data/initialization.json
        json_path = Path(__file__).parent.parent.parent / "data" / "initialization.json"
        
        if not json_path.exists():
            self.logger.warning(f"Arquivo {json_path} não encontrado. Usando carregamento tradicional.")
            return False
        
        try:
            self.logger.info(f"Carregando dados pré-consolidados de {json_path}...")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extrair municipios e UTPs
            municipios = data.get('municipios', [])
            utps = data.get('utps', [])
            metadata = data.get('metadata', {})
            
            self.logger.info(f"  ✓ {len(municipios)} municipios carregados")
            self.logger.info(f"  ✓ {len(utps)} UTPs carregadas")
            
            # Converter para dataframes para compatibilidade
            df_municipios = pd.DataFrame(municipios)
            df_utps = pd.DataFrame(utps)
            
            # --- POPULAR O GRAFO TERRITORIAL ---
            self.logger.info("Populando grafo territorial...")
            
            # 1. Criar dicionário de municípios para lookup rápido
            mun_dict = {m['cd_mun']: m for m in municipios}
            
            # 2. Iterar sobre cada município e criar a hierarquia
            for mun in municipios:
                cd_mun = int(mun['cd_mun'])
                nm_mun = mun.get('nm_mun', str(cd_mun))
                utp_id = str(mun.get('utp_id', 'SEM_UTP'))
                rm_name = mun.get('regiao_metropolitana', '')
                
                # Define RM (usa SEM_RM se vazio)
                if not rm_name or rm_name.strip() == '':
                    rm_name = "SEM_RM"
                
                # Cria nó da RM se não existir
                rm_node = f"RM_{rm_name}"
                if not self.graph.hierarchy.has_node(rm_node):
                    self.graph.hierarchy.add_node(rm_node, type='rm', name=rm_name)
                    self.graph.hierarchy.add_edge(self.graph.root, rm_node)
                
                # Cria nó da UTP se não existir
                utp_node = f"UTP_{utp_id}"
                if not self.graph.hierarchy.has_node(utp_node):
                    self.graph.hierarchy.add_node(utp_node, type='utp', utp_id=utp_id)
                    self.graph.hierarchy.add_edge(rm_node, utp_node)
                
                # Cria nó do município
                self.graph.hierarchy.add_node(cd_mun, type='municipality', name=nm_mun)
                self.graph.hierarchy.add_edge(utp_node, cd_mun)
                
                # Registra sede e REGIC se for sede
                if mun.get('sede_utp'):
                    self.graph.utp_seeds[utp_id] = cd_mun
                    regic = mun.get('regic', '')
                    if regic:
                        self.graph.mun_regic[cd_mun] = regic
            
            self.logger.info(f"  ✓ Grafo populado: {len(self.graph.hierarchy.nodes)} nós")
            
            # Carregar nos componentes
            self.analyzer.full_flow_df = df_municipios
            
            # Armazenar dados na memoria
            self.municipios_data = df_municipios
            self.utps_data = df_utps
            self.metadata = metadata
            
            self.logger.info("✓ Dados de initialization.json carregados com sucesso!")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Erro ao fazer parsing do JSON: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Erro ao carregar initialization.json: {type(e).__name__}: {e}")
            return False

    def step_0_initialize_data(self):
        """Carrega as bases de dados e sincroniza o Grafo."""
        self.logger.info("Etapa 0: Carregando Bases de Dados...")
        
        # Carrega dados do JSON (que já população o grafo)
        if not self.load_from_initialization_json():
            self.logger.warning("Falha ao carregar de initialization.json, tentando fallback...")
            # Fallback: carregamento tradicional
            try:
                self.logger.info(f"Carregando UTP base de {FILES['utp_base']}...")
                if str(FILES['utp_base']).endswith('.xlsx'):
                    df_utp = pd.read_excel(FILES['utp_base'], dtype=str)
                else:
                    df_utp = pd.read_csv(FILES['utp_base'], sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
                self.logger.info(f"  ✓ UTP: {len(df_utp)} linhas carregadas")
                
                self.logger.info(f"Carregando SEDE+REGIC de {FILES['sede_regic']}...")
                if str(FILES['sede_regic']).endswith('.xlsx'):
                    df_regic = pd.read_excel(FILES['sede_regic'], dtype=str)
                else:
                    df_regic = pd.read_csv(FILES['sede_regic'], sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
                self.logger.info(f"  ✓ REGIC: {len(df_regic)} linhas carregadas")
                
                # Popula o Grafo
                self.graph.load_from_dataframe(df_utp, df_regic)

            except Exception as e:
                self.logger.error(f"Arquivo não encontrado: {e}")
                self.logger.error(f"Verifique se os arquivos estão em data/01_raw/")
                return False
        
        # --- CARREGAR COMPOSIÇÃO DE RMs (NOVO - MOVIDO PARA FORA DO BLOCO CONDICIONAL) ---
        self.logger.info(f"Carregando Composição de RMs de {FILES['rm_composition']}...")
        try:
            df_rm = pd.read_excel(FILES['rm_composition'], dtype={'COD_MUN': str})
            # Filtrar colunas relevantes
            if 'COD_MUN' in df_rm.columns and 'NOME_RECMETROPOL' in df_rm.columns:
                rm_mapping = df_rm.set_index('COD_MUN')['NOME_RECMETROPOL'].to_dict()
                
                count_updates = 0
                # Atualizar info de RM no grafo (se o nó do município existir)
                for mun_node in self.graph.hierarchy.nodes():
                    if self.graph.hierarchy.nodes[mun_node].get('type') == 'municipality':
                        # tenta obter RM do mapping
                        rm_name = rm_mapping.get(str(mun_node))
                        if rm_name:
                            # Atualiza atributo no nó
                            self.graph.hierarchy.nodes[mun_node]['regiao_metropolitana'] = rm_name
                            count_updates += 1
                
                self.logger.info(f"  ✓ RM Composição: {len(df_rm)} linhas. {count_updates} municípios atualizados no grafo.")
                
            else:
                self.logger.warning("  ⚠️ Arquivo de RM não possui colunas 'COD_MUN' e 'NOME_RECMETROPOL'.")

        except Exception as e:
            self.logger.error(f"  ❌ Erro ao carregar Composição de RM: {e}")
            # Não pára o processo, apenas loga o erro

        # Sempre carregar shapefiles (independente da fonte de dados)
        self.logger.info(f"Carregando shapefiles...")
        try:
            self.map_generator.load_shapefiles()
            self.logger.info(f"  ✓ Shapefiles carregados: {len(self.map_generator.gdf_complete)} geometrias")
        except Exception as e:
            self.logger.error(f"  ❌ Erro ao carregar shapefiles: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
        
        self.logger.info(f"Dados carregados. Grafo: {len(self.graph.hierarchy.nodes)} nós.")
        return True

    def step_1_generate_initial_map(self):
        """Gera o mapa da situação atual das UTPs."""
        self.logger.info("Etapa 1: Gerando Mapa Inicial...")
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_01'], title="Situação Inicial das UTPs"))
        
        # Gera também o mapa de RMs
        self.map_generator.save_rm_map(FILES['mapa_rm'])

    def step_2_analyze_flows(self):
        """Analisa a Matriz OD para encontrar dependências funcionais."""
        self.logger.info("Etapa 2: Analisando Fluxos de Viagens...")
        self.analyzer.run_full_analysis()
        return self.analyzer.full_flow_df

    def step_5_consolidate_functional(self):
        """Une UTPs baseadas estritamente em Fluxos (Passo 5)."""
        self.logger.info("Etapa 5: Consolidação Funcional (Fluxos)...")
        changes = self.consolidator.run_functional_merging(
            self.analyzer.full_flow_df, 
            self.map_generator.gdf_complete,
            self.map_generator
        )
        
        # Gera o mapa intermédio
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_05'], title="Pós-Consolidação Funcional (Fluxos)"))
        
        return changes

    def step_7_territorial_cleanup(self):
        """Resolve UTPs unitárias usando REGIC + Adjacência (Passo 7)."""
        self.logger.info("Etapa 7: Limpeza Territorial Final (REGIC + Geografia)...")
        
        # Sincroniza antes de executar
        self.map_generator.sync_with_graph(self.graph)
        
        # Executa a lógica corrigida (EPSG:5880 + Boundary Length)
        changes = self.consolidator.run_territorial_regic(
            self.map_generator.gdf_complete, 
            self.map_generator
        )
        
        # Gera o mapa final absoluto
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_final'], title="Resultado Final: Mapa Estruturado"))
        
        return changes
