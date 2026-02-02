# src/interface/dashboard.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import json
import logging
from pathlib import Path
from datetime import datetime
from src.utils import DataLoader
from src.interface.consolidation_loader import ConsolidationLoader
from src.interface.snapshot_loader import SnapshotLoader
from src.run_consolidation import run_consolidation
from src.pipeline.sede_analyzer import SedeAnalyzer
from src.interface.components import sede_comparison
from src.core.graph import TerritorialGraph


# ===== CONFIGURAÇÃO DA PÁGINA =====
st.set_page_config(
    page_title="GeoValida - Consolidação Territorial",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    :root {
        --primary-color: #1351B4;
    }
    [data-testid="stMetricValue"] {
        color: #1351B4;
        font-weight: bold;
    }
    .step-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .step-initial { background-color: #e3f2fd; color: #1351B4; }
    .step-final { background-color: #e8f5e9; color: #2e7d32; }
    .status-badge {
        display: inline-block;
        padding: 6px 16px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9rem;
    }
    .status-executed { background-color: #d4edda; color: #155724; }
    .status-pending { background-color: #fff3cd; color: #856404; }
    
    /* Forçar alinhamento à esquerda das tabs */
    .stTabs {
        width: 100% !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        justify-content: flex-start !important;
        gap: 1rem !important; /* Reduzido de 2rem */
        width: 100% !important;
        margin-left: 0 !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: auto !important;
        white-space: pre-wrap !important;
        background-color: transparent !important;
        border: none !important;
        padding-left: 0 !important;
        padding-right: 0 !important;
        margin-right: 1.5rem !important; /* Espaçamento entre tabs */
        flex-grow: 0 !important; /* Impede que estiquem */
    }

    /* Container das tabs - removendo centralização do Streamlit */
    div[data-testid="stHorizontalBlock"] > div:has([data-baseweb="tab-list"]) {
        width: 100% !important;
    }
    
    /* Garantir que o indicador de seleção acompanhe */
    .stTabs [data-baseweb="tab-highlight"] {
        background-color: #1351B4 !important;
    }
</style>
""", unsafe_allow_html=True)

# Paleta Pastel (Cores suaves e agradáveis)
from src.interface.palette import get_palette

# Carregar Paleta Ativa
PASTEL_PALETTE = get_palette()


@st.cache_data(show_spinner="Carregando mapa...", hash_funcs={gpd.GeoDataFrame: id, pd.DataFrame: id})
def get_geodataframe(optimized_geojson_path, df_municipios):
    """
    Carrega o GeoDataFrame pré-processado de municípios.
    
    Se o arquivo otimizado não existir (gerado pelo pipeline main.py),
    exibe um aviso e retorna None.
    """
    if not optimized_geojson_path.exists():
        st.warning("""
        **GeoDataFrame otimizado não encontrado!**
        
        Para melhor performance, execute o pipeline completo:
        ```bash
        python main.py
        ```
        Isso irá pré-processar e salvar os GeoDataFrames otimizados.
        """)
        return None

    try:
        # Carregar GeoJSON pré-processado
        gdf = gpd.read_file(optimized_geojson_path)
        
        # Atualizar com dados mais recentes do df_municipios
        # (caso o initialization.json tenha sido alterado após o pré-processamento)
        df_mun_copy = df_municipios.copy()
        df_mun_copy['cd_mun'] = df_mun_copy['cd_mun'].astype(str)
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        
        # Re-merge para garantir dados atualizados
        gdf = gdf.drop(columns=['uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_sede'], errors='ignore')
        gdf = gdf.merge(
            df_mun_copy[['cd_mun', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_mun']], 
            left_on='CD_MUN', right_on='cd_mun', how='left'
        )
        
        # Recalcular nomes das sedes
        df_sedes = df_mun_copy[df_mun_copy['sede_utp'] == True][['utp_id', 'nm_mun']].set_index('utp_id')
        sede_mapper = df_sedes['nm_mun'].to_dict()
        gdf['nm_sede'] = gdf['utp_id'].map(sede_mapper).fillna('')
        gdf['regiao_metropolitana'] = gdf['regiao_metropolitana'].fillna('')
        
        return gdf
    except Exception as e:
        st.error(f"Erro ao carregar mapa otimizado: {e}")
        return None


@st.cache_data(show_spinner="Carregando RMs...", hash_funcs={gpd.GeoDataFrame: id})
def get_derived_rm_geodataframe(optimized_rm_geojson_path):
    """
    Carrega o GeoDataFrame pré-processado de Regiões Metropolitanas.
    
    Se o arquivo otimizado não existir (gerado pelo pipeline main.py),
    retorna None silenciosamente (RMs são opcionais).
    """
    if not optimized_rm_geojson_path.exists():
        logging.info("GeoDataFrame de RMs otimizado não encontrado (opcional)")
        return None
    
    try:
        # Carregar GeoJSON pré-processado
        gdf_rm = gpd.read_file(optimized_rm_geojson_path)
        return gdf_rm
        
    except Exception as e:
        logging.error(f"Erro ao carregar RMs otimizadas: {e}")
        return None


@st.cache_resource(show_spinner="Construindo Grafo Territorial...")
def get_territorial_graph(df_municipios):
    """
    Cria e cacheia o grafo territorial completo.
    Evita recriação a cada renderização.
    """
    if df_municipios is None or df_municipios.empty:
        return None
        
    try:
        graph = TerritorialGraph()
        # Carregar estrutura do grafo a partir dos dados
        for _, row in df_municipios.iterrows():
            cd_mun = int(row['cd_mun'])
            nm_mun = row.get('nm_mun', str(cd_mun))
            utp_id = str(row.get('utp_id', 'SEM_UTP'))
            rm_name = row.get('regiao_metropolitana', '')
            
            if not rm_name or str(rm_name).strip() == '':
                rm_name = "SEM_RM"
            
            # Criar hierarquia no grafo
            rm_node = f"RM_{rm_name}"
            if not graph.hierarchy.has_node(rm_node):
                graph.hierarchy.add_node(rm_node, type='rm', name=rm_name)
                graph.hierarchy.add_edge(graph.root, rm_node)
            
            utp_node = f"UTP_{utp_id}"
            if not graph.hierarchy.has_node(utp_node):
                graph.hierarchy.add_node(utp_node, type='utp', utp_id=utp_id)
                graph.hierarchy.add_edge(rm_node, utp_node)
            
            graph.hierarchy.add_node(cd_mun, type='municipality', name=nm_mun)
            graph.hierarchy.add_edge(utp_node, cd_mun)
        
        logging.info(f"Grafo territorial criado: {len(graph.hierarchy.nodes)} nós")
        return graph
    except Exception as e:
        logging.error(f"Erro ao criar grafo territorial: {e}")
        return None


@st.cache_data(show_spinner="Carregando coloração pré-calculada...", hash_funcs={gpd.GeoDataFrame: id, pd.DataFrame: id})
def load_or_compute_coloring(gdf, cache_filename="initial_coloring.json"):
    """
    Carrega a coloração pré-calculada do cache.
    
    O cache é gerado pela Etapa 3 do main.py (scripts/s03_precompute_coloring.py).
    Se o cache não existir, retorna um dicionário vazio e exibe um aviso.
    
    Args:
        gdf: GeoDataFrame (não usado, mantido para compatibilidade)
        cache_filename: Nome do arquivo de cache ("initial_coloring.json" ou "consolidated_coloring.json")
    
    Returns:
        Dict: mapeamento cd_mun (int) -> color_index (int)
    """
    cache_path = Path(__file__).parent.parent.parent / "data" / cache_filename
    
    # Check alternate location (03_processed) if not in root data
    if not cache_path.exists():
        alt_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / cache_filename
        if alt_path.exists():
            cache_path = alt_path

    # Tentar carregar do arquivo
    if cache_path.exists():
        try:
            with open(cache_path, "r") as f:
                coloring_str_keys = json.load(f)
                # JSON chaves são sempre strings, converter para int
                coloring = {int(k): v for k, v in coloring_str_keys.items()}
                logging.info(f"✅ Coloração carregada do cache: {len(coloring)} municípios")
                return coloring
        except Exception as e:
            logging.error(f"❌ Erro ao ler cache de coloração: {e}")
            st.error(f"Erro ao carregar cache de coloração: {e}")
            return {}
    
    # Cache não existe - avisar usuário
    logging.warning("⚠️ Cache de coloração não encontrado!")
    st.warning("""
    **Cache de coloração não encontrado!**
    
    Para otimizar o carregamento do dashboard, execute o pipeline completo:
    
    ```bash
    python main.py
    ```
    
    Isso irá pré-calcular a coloração e salvar em cache.
    """)
    
    return {}






def create_enriched_utp_summary(df_municipios):
    """
    Cria resumo enriquecido das UTPs com métricas territoriais relevantes.
    
    Args:
        df_municipios: DataFrame com dados dos municípios
        
    Returns:
        DataFrame com métricas agregadas por UTP
    """
    if df_municipios.empty:
        return pd.DataFrame()
    
    # Preparar dados
    df = df_municipios.copy()
    
    # Garantir types numéricos
    numeric_cols = ['populacao_2022', 'area_km2']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Agregar viagens por município
    df['total_viagens'] = 0
    if 'modais' in df.columns:
        df['total_viagens'] = df['modais'].apply(
            lambda x: sum(x.values()) if isinstance(x, dict) else 0
        )
    
    # Identificar modal dominante
    def get_modal_dominante(modais):
        if not isinstance(modais, dict) or not modais:
            return ''
        max_modal = max(modais.items(), key=lambda x: x[1])
        if max_modal[1] == 0:
            return ''
        # Simplificar nomes
        modal_map = {
            'rodoviaria_coletiva': 'Rod. Coletiva',
            'rodoviaria_particular': 'Rod. Particular',
            'aeroviaria': 'Aérea',
            'ferroviaria': 'Ferroviária',
            'hidroviaria': 'Hidroviária'
        }
        return modal_map.get(max_modal[0], max_modal[0])
    
    df['modal_dominante'] = df['modais'].apply(get_modal_dominante) if 'modais' in df.columns else ''
    
    # Agrupar por UTP
    summary_list = []
    
    for utp_id, group in df.groupby('utp_id'):
        # Identificar sede
        sede_row = group[group['sede_utp'] == True]
        if sede_row.empty:
            continue
        
        sede = sede_row.iloc[0]
        
        # População
        pop_total = group['populacao_2022'].sum()
        
        # Maior município
        maior_mun = group.loc[group['populacao_2022'].idxmax()]
        maior_mun_nome = f"{maior_mun['nm_mun']} ({maior_mun['populacao_2022']:,.0f})"
        
        # Turismo
        turismo_cat = sede.get('turismo_classificacao', '')
        if pd.isna(turismo_cat) or str(turismo_cat).strip() == '':
            turismo_cat = '-'
        else:
            # Simplificar categoria (pegar apenas primeira parte antes do hífen)
            turismo_cat = str(turismo_cat).split('-')[0].strip()
        
        # Aeroportos na UTP
        aeroportos_info = []
        for _, mun in group.iterrows():
            if 'aeroporto' in mun and isinstance(mun['aeroporto'], dict):
                aero = mun['aeroporto']
                icao = aero.get('sigla', '') or aero.get('icao', '')
                passageiros = aero.get('passageiros_anual', 0)
                
                if icao and str(icao).strip() not in ['', 'nan', 'None']:
                    aeroportos_info.append({
                        'icao': str(icao),
                        'passageiros': int(passageiros) if passageiros else 0,
                        'municipio': mun['nm_mun']
                    })
        
        # Determinar principal aeroporto e formatar
        if aeroportos_info:
            # Ordenar por passageiros
            aeroportos_info.sort(key=lambda x: x['passageiros'], reverse=True)
            principal = aeroportos_info[0]
            
            # Formatar passageiros
            if principal['passageiros'] > 1000000:
                pass_fmt = f"{principal['passageiros']/1000000:.1f}M"
            elif principal['passageiros'] > 1000:
                pass_fmt = f"{principal['passageiros']/1000:.0f}k"
            else:
                pass_fmt = str(principal['passageiros'])
            
            if len(aeroportos_info) == 1:
                aeroporto_display = f"{principal['icao']} ({pass_fmt})"
            else:
                aeroporto_display = f"{len(aeroportos_info)} aeros | {principal['icao']} ({pass_fmt})"
        else:
            aeroporto_display = '-'
        
        # Viagens
        viagens_total = group['total_viagens'].sum()
        
        # Modal dominante da UTP (baseado na sede)
        modal_dom = sede.get('modal_dominante', '-')
        
        # REGIC
        regic = sede.get('regic', '-')
        if pd.isna(regic) or str(regic).strip() == '':
            regic = '-'
        
        # Região Metropolitana
        rm = sede.get('regiao_metropolitana', '')
        if pd.isna(rm) or str(rm).strip() == '':
            rm = '-'
        
        summary_list.append({
            'UTP': utp_id,
            'Sede': sede['nm_mun'],
            'UF': sede['uf'],
            'Municípios': len(group),
            'População': int(pop_total),
            'Maior Município': maior_mun_nome,
            'REGIC': regic,
            'RM': rm,
            'Turismo': turismo_cat,
            'Aeroportos': aeroporto_display,
            'Viagens': int(viagens_total),
            'Modal': modal_dom if modal_dom else '-'
        })
    
    # Criar DataFrame
    summary_df = pd.DataFrame(summary_list)
    
    if summary_df.empty:
        return summary_df
    
    # Ordenar por população (decrescente)
    summary_df = summary_df.sort_values('População', ascending=False)
    
    # Formatar colunas numéricas para display
    summary_df_display = summary_df.copy()
    summary_df_display['População'] = summary_df_display['População'].apply(lambda x: f"{x:,}")
    summary_df_display['Viagens'] = summary_df_display['Viagens'].apply(
        lambda x: f"{x:,}" if x > 0 else '-'
    )
    
    return summary_df_display


def analyze_unitary_utps(df_municipios):
    """
    Identifica UTPs que possuem apenas 1 município.
    
    Returns:
        DataFrame com lista de UTPs unitárias e seus detalhes
    """
    utp_counts = df_municipios.groupby('utp_id').size().reset_index(name='num_municipios')
    unitary_utps = utp_counts[utp_counts['num_municipios'] == 1]['utp_id'].tolist()
    
    if not unitary_utps:
        return pd.DataFrame()
    
    # Buscar detalhes dos municípios únicos
    df_unitary = df_municipios[df_municipios['utp_id'].isin(unitary_utps)].copy()
    
    result = df_unitary[['utp_id', 'nm_mun', 'uf', 'populacao_2022', 'regiao_metropolitana']].copy()
    result.columns = ['UTP', 'Município', 'UF', 'População', 'RM']
    result['População'] = result['População'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else '-')
    result['RM'] = result['RM'].fillna('-')
    
    return result.sort_values('UTP')


def analyze_non_contiguous_utps(gdf):
    """
    Identifica UTPs cujos municípios não são geograficamente contíguos.
    
    Usa análise espacial para verificar se todos os municípios de uma UTP
    formam uma região conectada.
    
    Returns:
        Dict com UTP_ID -> lista de componentes desconectados
    """
    if gdf is None or gdf.empty:
        return {}
    
    from shapely.ops import unary_union
    
    non_contiguous = {}
    
    # Agrupar por UTP
    for utp_id, group in gdf.groupby('utp_id'):
        if len(group) <= 1:
            continue  # UTP unitária, não há como ser não-contígua
        
        # Criar união das geometrias
        try:
            # Dissolve para criar geometria única da UTP
            utp_geom = group.geometry.unary_union
            
            # Verificar se é MultiPolygon (indica descontinuidade)
            if utp_geom.geom_type == 'MultiPolygon':
                # Contar componentes desconectados
                num_components = len(utp_geom.geoms)
                
                # Identificar quais municípios estão em cada componente
                components_info = []
                for i, component in enumerate(utp_geom.geoms):
                    # Encontrar municípios que intersectam este componente
                    municipalities_in_component = []
                    for idx, row in group.iterrows():
                        if row.geometry.intersects(component):
                            municipalities_in_component.append(row['NM_MUN'])
                    
                    if municipalities_in_component:
                        components_info.append(municipalities_in_component)
                
                if num_components > 1:
                    non_contiguous[utp_id] = {
                        'num_components': num_components,
                        'components': components_info,
                        'num_municipalities': len(group)
                    }
        except Exception as e:
            logging.warning(f"Erro ao analisar contiguidade da UTP {utp_id}: {e}")
            continue
    
    return non_contiguous


def render_map(gdf_filtered, title="Mapa", global_colors=None, graph=None, gdf_rm=None, show_rm_borders=False):
    """Função auxiliar para renderizar um mapa folium com coloração por grafo."""
    if gdf_filtered is None or gdf_filtered.empty:
        st.info("Nenhum dado para visualizar neste filtro.")
        return
    
    gdf_filtered = gdf_filtered.copy()
    
    # Garantir índice único
    gdf_filtered = gdf_filtered.reset_index(drop=True)
    
    # Inicializar coluna de cor com valor seguro padrão para evitar problemas no Folium
    gdf_filtered['color'] = '#cccccc'
    
    # ESTRATÉGIA DE COLORAÇÃO
    # 1. Se colors globais fornecidas (cacheado, rápido), usar.
    # 2. Se grafo fornecido (dinâmico, lento), calcular.
    # 3. Fallback: cores aleatórias por ID.
    
    coloring_applied = False
    
    if global_colors:
        try:
            # Mapear cores diretamente por município (cd_mun -> cor)
            # A coloração global já considera UTPs vizinhas
            for idx, row in gdf_filtered.iterrows():
                try:
                    # Tenta acessar CD_MUN ou cd_mun
                    cd_mun_val = row.get('CD_MUN') if 'CD_MUN' in row else row.get('cd_mun')
                    if cd_mun_val is None: continue
                    
                    cd_mun = int(cd_mun_val)
                    if not PASTEL_PALETTE:
                         raise ValueError("Paleta vazia")
                         
                    color_idx = global_colors.get(cd_mun, 0) % len(PASTEL_PALETTE)
                    gdf_filtered.at[idx, 'color'] = PASTEL_PALETTE[color_idx]
                except (ValueError, KeyError, IndexError):
                    pass # Mantém cor padrão #cccccc
            
            coloring_applied = True
        except Exception as e:
            logging.warning(f"Erro ao aplicar coloração global: {e}")
    
    if not coloring_applied and graph is not None:
        try:
            # Preparar GeoDataFrame para coloração (precisa de UTP_ID e CD_MUN como int)
            gdf_for_coloring = gdf_filtered.copy()
            
            # Garantir que CD_MUN existe e é inteiro
            if 'CD_MUN' not in gdf_for_coloring.columns:
                logging.warning("Coluna CD_MUN não encontrada, usando coloração simples")
                raise ValueError("Missing CD_MUN")
            
            gdf_for_coloring['CD_MUN'] = gdf_for_coloring['CD_MUN'].astype(str)
            gdf_for_coloring['UTP_ID'] = gdf_for_coloring['utp_id'].astype(str)
            
            # Calcular coloração usando algoritmo de grafo (Stateless)
            coloring = graph.compute_graph_coloring(gdf_for_coloring)
            
            # Mapear cores diretamente por município (cd_mun -> cor)
            for idx, row in gdf_filtered.iterrows():
                try:
                    cd_mun = int(row['CD_MUN'])
                    color_idx = coloring.get(cd_mun, 0) % len(PASTEL_PALETTE)
                    gdf_filtered.at[idx, 'color'] = PASTEL_PALETTE[color_idx]
                except (ValueError, KeyError):
                    gdf_filtered.at[idx, 'color'] = PASTEL_PALETTE[0]
            
            coloring_applied = True
        
        except Exception as e:
            logging.warning(f"Erro ao aplicar coloração por grafo, usando fallback: {e}")
            
    if not coloring_applied:
        # Fallback: coloração simples por UTP
        utps_unique = gdf_filtered['utp_id'].dropna().unique()
        if len(utps_unique) > 0 and len(PASTEL_PALETTE) > 0:
            colors = {utp: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] 
                     for i, utp in enumerate(sorted(utps_unique))}
            gdf_filtered['color'] = gdf_filtered['utp_id'].map(colors).fillna('#cccccc')

    
    # Criar mapa folium
    m = folium.Map(
        location=[-15, -55],
        zoom_start=4,
        tiles="CartoDB positron",
        prefer_canvas=True,
        control_scale=True
    )
    
    # Fit bounds automáticos
    if not gdf_filtered.empty:
        bounds = gdf_filtered.total_bounds
        m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]], padding=(0.05, 0.05))
    
    # Separar municípios regulares e sedes
    gdf_members = gdf_filtered[~gdf_filtered['sede_utp']].copy()
    gdf_seats = gdf_filtered[gdf_filtered['sede_utp']].copy()
    
    # Adicionar primeira camada: Municípios regulares
    if not gdf_members.empty:
        folium.GeoJson(
            gdf_members.to_json(),
            style_function=lambda x: {
                'fillColor': x['properties'].get('color', '#cccccc'),
                'color': '#ffffff',
                'weight': 0.3,
                'fillOpacity': 0.9
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['NM_MUN', 'utp_id', 'regiao_metropolitana', 'uf', 'nm_sede'],
                aliases=['Município:', 'UTP:', 'RM:', 'UF:', 'Sede:'],
                localize=True,
                sticky=False
            ),
            popup=folium.GeoJsonPopup(
                fields=['NM_MUN', 'CD_MUN', 'utp_id', 'regiao_metropolitana', 'uf', 'nm_sede'],
                aliases=['Município', 'Código IBGE', 'UTP', 'RM', 'UF', 'Sede UTP']
            )
        ).add_to(m)
    
    # Adicionar segunda camada: Sedes com destaque
    if not gdf_seats.empty:
        folium.GeoJson(
            gdf_seats.to_json(),
            style_function=lambda x: {
                'fillColor': x['properties'].get('color', '#cccccc'),
                'color': '#000000',
                'weight': 3.0,
                'fillOpacity': 1.0
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['NM_MUN', 'utp_id', 'regiao_metropolitana', 'uf', 'nm_sede'],
                aliases=['Município Sede:', 'UTP:', 'RM:', 'UF:', 'Sede:'],
                localize=True,
                sticky=False
            ),
            popup=folium.GeoJsonPopup(
                fields=['NM_MUN', 'CD_MUN', 'utp_id', 'regiao_metropolitana', 'uf', 'nm_sede'],
                aliases=['Município (SEDE)', 'Código IBGE', 'UTP', 'RM', 'UF', 'Sede UTP']
            )
        ).add_to(m)
    
    # Adicionar camada de contornos de Regiões Metropolitanas (opcional)
    if show_rm_borders and gdf_rm is not None and not gdf_rm.empty:
        logging.info(f"DEBUG RM: show_rm_borders={show_rm_borders}, gdf_rm rows={len(gdf_rm)}")
        
        try:
            # Como gdf_rm já é derivado dos municípios, não precisamos de spatial join
            # Precisamos apenas filtrar para as RMs que estão na área visível (ou intersecção com gdf_filtered)
            
            # Para otimizar, podemos filtrar apenas as RMs presentes nos municípios filtrados
            rms_visible = gdf_filtered['regiao_metropolitana'].unique()
            gdf_rm_filtered = gdf_rm[gdf_rm['regiao_metropolitana'].isin(rms_visible)].copy()
            
            if not gdf_rm_filtered.empty:
                # Criar pane customizado para garantir que as bordas fiquem por cima
                # z-index padrão de overlay é ~400. Usamos 450 para ficar acima.
                folium.map.CustomPane("rm_borders", z_index=450).add_to(m)

                for idx, row in gdf_rm_filtered.iterrows():
                    nome_rm = row['regiao_metropolitana']
                    uf = row['uf']
                    num_municipios = row['count']
                    
                    tooltip_rm = f"RM: {nome_rm} ({uf}) - {num_municipios} municípios"
                    
                    folium.GeoJson(
                        row.geometry,
                        style_function=lambda x: {
                            'fillColor': 'none',
                            'color': '#FF0000',  # Vermelho para destacar
                            'weight': 3,
                            'fillOpacity': 0,
                            'dashArray': '4, 4'  # Linha pontilhada
                        },
                        tooltip=tooltip_rm,
                        name=f"RM: {nome_rm}",
                        pane="rm_borders"
                    ).add_to(m)
                
                logging.info(f"DEBUG RM: {len(gdf_rm_filtered)} contornos de RM adicionados")
            else:
                logging.info("DEBUG RM: Nenhuma RM relevante para os municípios filtrados")
                
        except Exception as e:
            logging.error(f"Erro ao renderizar RMs: {e}")
    
    map_html = m._repr_html_()
    st.components.v1.html(map_html, height=600, scrolling=False)


def render_dashboard(manager):
    """Dashboard com visualização do pipeline de consolidação territorial."""
    
    # === LOAD CONSOLIDATION CACHE ===
    consolidation_loader = ConsolidationLoader()
    snapshot_loader = SnapshotLoader()
    
    # Flags de controle de fluxo (inicializa se não existir).columns([2, 1, 1])
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.title("GeoValida - Consolidação Territorial")
        st.markdown("Visualização da distribuição inicial e pós-consolidação de UTPs")
    
    with col3:
        status_class = "status-executed" if consolidation_loader.is_executed() else "status-pending"
        status_text = "Consolidado" if consolidation_loader.is_executed() else "Pendente"
        st.markdown(f"<div class='status-badge {status_class}'>{status_text}</div>", unsafe_allow_html=True)
    
    # === SIDEBAR - FILTROS & CONTROLES ===
    with st.sidebar:
        st.markdown("### Filtros")
        st.markdown("---")
        
        # Carregar dados
        data_loader = DataLoader()
        df_municipios = data_loader.get_municipios_dataframe()
        metadata = data_loader.get_metadata()
        
        if df_municipios.empty:
            st.error("Falha ao carregar dados.")
            return
        
        # Filtro por UF
        ufs = sorted(df_municipios['uf'].unique().tolist())
        all_ufs = st.checkbox("Brasil Completo", value=True)
        
        if all_ufs:
            selected_ufs = ufs
            st.multiselect("Estados (UF)", ufs, default=ufs, disabled=True)
        else:
            selected_ufs = st.multiselect("Estados (UF)", ufs, default=[])
        
        # Filtro por Município (Novo)
        st.markdown("---")
        # Criar lista formatada "Nome (UF)"
        df_municipios['display_name'] = df_municipios['nm_mun'] + " (" + df_municipios['uf'] + ")"
        mun_options = sorted(df_municipios['display_name'].unique().tolist())
        
        selected_muns_search = st.multiselect(
            "Buscar Município", 
            mun_options,
            help="Selecione um ou mais municípios para visualizar suas UTPs completas."
        )
        
        # Lógica de Filtro Reverso: Município -> UTP
        # Se municípios forem selecionados, eles REDEFINEM a lista de UTPs selecionadas
        forced_utps_from_search = []
        if selected_muns_search:
            # Extrair nomes puros (remove UF) - mas melhor usar ID se possível, aqui vamos pelo display_name
            # Filtrar DF original
            mask_search = df_municipios['display_name'].isin(selected_muns_search)
            forced_utps_from_search = df_municipios[mask_search]['utp_id'].unique().tolist()
            
            # Atualizar/Forçar UTPs selecionadas
            # Nota: Isso vai apenas impactar o filtro visual, não altera o widget de multiselect acima
            # para não quebrar o estado do Streamlit. Apenas usamos na lógica de filtragem.
            if forced_utps_from_search:
                st.info(f"Visualizando {len(forced_utps_from_search)} UTP(s) referente(s) à busca.")
        
        # Filtro por UTP (Mantido, mas com lógica condicional)
        if selected_ufs:
            df_utp_options = df_municipios[df_municipios['uf'].isin(selected_ufs)]
        else:
            df_utp_options = df_municipios
            
        utps_list = sorted(df_utp_options['utp_id'].unique().tolist())
        all_utps = st.checkbox("Todas as UTPs", value=False)
        
        if all_utps:
            selected_utps = utps_list
            st.multiselect("UTPs", utps_list, default=utps_list, disabled=True)
        else:
            # Se houver busca por muni, podemos pré-selecionar ou apenas ignorar este campo na lógica final
            default_utps = []
            selected_utps = st.multiselect("UTPs", utps_list, default=default_utps)
        
        st.markdown("---")
        st.caption(f"Dados de: {metadata.get('timestamp', 'N/A')[:10]}")
        
        # === SEÇÃO DE CONSOLIDAÇÃO ===
        st.markdown("---")
        st.markdown("### Consolidação")
        
        if consolidation_loader.is_executed():

            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Rodar Agora", width='stretch', help="Executa o pipeline completo de consolidação (Fluxos + REGIC)"):
                    with st.spinner("Executando pipeline..."):
                        if run_consolidation():
                            st.success("Sucesso!")
                            st.rerun()
                        else:
                            st.error("Falha na execução.")
            
            with col2:
                if st.button("Limpar Cache", width='stretch'):
                    consolidation_loader.clear()
                    st.rerun()
        else:
            st.warning("Nenhuma consolidação em cache")
            if st.button("Executar Consolidação", width='stretch'):
                with st.spinner("Executando pipeline..."):
                    if run_consolidation():
                        st.success("Sucesso!")
                        st.rerun()
                    else:
                        st.error("Falha na execução.")
    
    # Aplicar filtros
    # Aplicar filtros
    df_filtered = df_municipios[df_municipios['uf'].isin(selected_ufs)].copy()
    
    # Lógica de prioridade: Busca por Município > Filtro de UTP
    if forced_utps_from_search:
        # Se buscou município, ignora o filtro de UTP manual e mostra as UTPs da busca
        # Mas mantemos o filtro de UF? Geralmente user quer ver o resultado da busca independente da UF
        # Vamos priorizar a busca globalmente
        df_filtered = df_municipios[df_municipios['utp_id'].isin(forced_utps_from_search)].copy()
    elif selected_utps:
        df_filtered = df_filtered[df_filtered['utp_id'].isin(selected_utps)]
    
    
    # Carregar GeoDataFrames otimizados (gerados pelo pipeline main.py)
    maps_dir = Path(__file__).parent.parent.parent / "data" / "04_maps"
    optimized_municipalities_path = maps_dir / "municipalities_optimized.geojson"
    optimized_rm_path = maps_dir / "rm_boundaries_optimized.geojson"
    
    gdf = get_geodataframe(optimized_municipalities_path, df_municipios)
    gdf_rm = get_derived_rm_geodataframe(optimized_rm_path)

    
    # 1. Preparar DataFrame limpo para o grafo (sem dicts para evitar erro de hash)
    # Selecionamos apenas as colunas necessárias para a estrutura topológica
    topology_cols = ['cd_mun', 'nm_mun', 'utp_id', 'regiao_metropolitana']
    df_topology = df_municipios[topology_cols].copy()
    
    # Criar e cachear grafo territorial usando o DF limpo
    graph = get_territorial_graph(df_topology)
    
    # 2. Carregar ou calcular coloração GLOBAL (Persistente em arquivo)
    # ATENÇÃO: Carregamos aqui a INITIAL por padrão, mas cada tab pode pedir a sua
    global_colors_initial = load_or_compute_coloring(gdf, "initial_coloring.json") if gdf is not None else {}
    
    # === TABS ===
    # === TABS ===
    tab1, tab2, tab3, tab4, tab_sedes, tab_borders = st.tabs([
        "Distribuição Inicial",
        "Pós-Consolidação",
        "Análise de Dependências",
        "Análise Interestadual",
        "Consolidação Sedes",
        "Validação de Fronteiras"
    ])
    
    # ==== TAB 1: DISTRIBUIÇÃO INICIAL ====
    with tab1:
        st.markdown("### <span class='step-badge step-initial'>INICIAL</span> Situação Atual", unsafe_allow_html=True)
        st.markdown("Mapa da distribuição atual das UTPs antes da consolidação.")
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            # Usar contagem única de CDs para evitar contar shapefiles duplicados
            total_unique = df_municipios['cd_mun'].nunique()
            current_unique = df_filtered['cd_mun'].nunique()
            st.metric("Municípios", current_unique, f"{total_unique} total")
        with col2:
            st.metric("UTPs", len(df_filtered['utp_id'].unique()), f"{len(utps_list)} total")
        with col3:
            st.metric("Estados", len(df_filtered['uf'].unique()), f"{len(ufs)} total")
        
        st.markdown("---")
        st.markdown("#### Mapa Interativo")
        
        # Controle de visualização de contornos de RM
        show_rm_borders = st.checkbox(
            "Mostrar contornos de Regiões Metropolitanas",
            value=False,
            key='show_rm_tab1',
            help="Ativa/desativa a visualização dos contornos das Regiões Metropolitanas sobre o mapa de UTPs"
        )
        
        # Tentar carregar snapshot do estado inicial (Step 1)
        # Se não existir, usa o gdf base (que já é o inicial carregado dos inputs)
        gdf_initial = snapshot_loader.get_geodataframe_for_step('step1', gdf)
        gdf_display = gdf_initial if gdf_initial is not None else gdf

        if gdf_display is not None:
            gdf_filtered = gdf_display[gdf_display['uf'].isin(selected_ufs)].copy()
            if selected_utps:
                gdf_filtered = gdf_filtered[gdf_filtered['utp_id'].isin(selected_utps)]
            
            st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_filtered)} Municípios")
            
            # Renderizar mapa com opção de mostrar contornos de RM
            # Se usou snapshot, as cores ja devem estar no gdf_display (se o snapshot tiver coloring)
            # Mas o render_map usa global_colors_initial se fornecido.
            # O snapshot step1 tem coloring? Sim, export_snapshot salva coloring se existir.
            # Se global_colors_initial falhar/não bater, o render_map usa a coluna 'color' do GDF.
            
            render_map(gdf_filtered, title="Distribuição por UTP (Inicial)", global_colors=global_colors_initial, 
                       gdf_rm=gdf_rm, show_rm_borders=show_rm_borders)
        
        st.markdown("---")
        st.markdown("#### Resumo das UTPs")
        st.caption("Características socioeconômicas e territoriais agregadas por UTP")
        
        # Criar resumo enriquecido
        utp_summary = create_enriched_utp_summary(df_filtered)
        
        if not utp_summary.empty:
            st.markdown(f"**{len(utp_summary)} UTPs (ordenadas por população)**")
            
            # Mostrar todas as UTPs
            st.dataframe(
                utp_summary,
                width='stretch',
                hide_index=True,
                height=600
            )
            
            # Estatísticas rápidas
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            
            # Converter população de volta para número para estatísticas
            pop_values = df_filtered.groupby('utp_id')['populacao_2022'].sum()
            
            with col1:
                st.metric("População Média", f"{pop_values.mean():,.0f}")
            with col2:
                st.metric("População Mediana", f"{pop_values.median():,.0f}")
            with col3:
                utps_com_aero = (utp_summary['Aeroportos'] != '-').sum()
                st.metric("UTPs com Aeroporto", utps_com_aero)
            with col4:
                # Contar total de aeroportos
                total_aeros = 0
                for val in utp_summary['Aeroportos']:
                    if val != '-':
                        if 'aeros' in val:
                            # Extrair número antes de 'aeros'
                            total_aeros += int(val.split(' ')[0])
                        else:
                            total_aeros += 1
                st.metric("Total de Aeroportos", total_aeros)
        else:
            st.info("Nenhuma UTP encontrada com os filtros selecionados.")


    
    # ==== TAB 2: PÓS-CONSOLIDAÇÃO ====
    with tab2:
        st.markdown("### <span class='step-badge step-final'>FINAL</span> Após Consolidação", unsafe_allow_html=True)
        col_title, col_btn = st.columns([3, 1])
        with col_title:
            st.markdown("Mapa da distribuição após consolidação de UTPs unitárias e limpeza territorial.")
        with col_btn:
            if st.button("Rodar Pipeline", width='stretch', key="btn_tab_run"):
                with st.spinner("Executando..."):
                    if run_consolidation():
                        st.success("Feito!")
                        st.rerun()
                    else:
                        st.error("Erro!")
        st.markdown("---")
        
        
        if consolidation_loader.is_executed():
            # IMPORTANTE: Usar snapshot pós-unitárias (Steps 5+7) ao invés do resultado completo
            # Isso garante que as consolidações de sedes NÃO apareçam nesta aba
            post_unitary_consolidations = consolidation_loader.get_post_unitary_consolidations()
            post_unitary_mapping = consolidation_loader.get_post_unitary_mapping()
            
            # Calcular estatísticas baseadas no snapshot pós-unitárias
            # MUDANÇA CRÍTICA: Usar Snapshot Step 5 (Post-Unitary) direto
            # Isso garante que vemos exatamente o que foi salvo
            df_consolidated = snapshot_loader.get_snapshot_dataframe('step5')
            if df_consolidated.empty:
                # Fallback se snapshot nao existir (ainda nao rodou pipeline novo)
                df_consolidated = consolidation_loader.apply_post_unitary_to_dataframe(df_filtered)
            else:
                # Filtrar DF do snapshot pelos filtros da UI se necessario (ex: UFs)
                if selected_ufs:
                     df_consolidated = df_consolidated[df_consolidated['cd_mun'].isin(df_filtered['cd_mun'])].copy()
            
            # === MÉTRICAS PADRONIZADAS ===
            if not df_consolidated.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    # Contagem única de municípios
                    current_unique = df_consolidated['cd_mun'].nunique() if 'cd_mun' in df_consolidated.columns else len(df_consolidated)
                    # Total depende do contexto, aqui usamos o filtered original como referência total ou o próprio consolidado
                    total_unique = df_municipios['cd_mun'].nunique()
                    st.metric("Municípios", current_unique, f"{total_unique} total")
                with col2:
                    current_utps = df_consolidated['utp_id'].nunique()
                    st.metric("UTPs", current_utps, f"{len(utps_list)} total")
                with col3:
                    current_ufs = df_consolidated['uf'].nunique()
                    st.metric("Estados", current_ufs, f"{len(ufs)} total")
            
            st.markdown("---")
            st.markdown("#### Mapa Pós-Consolidação")
            
            # Controle de visualização de contornos de RM
            show_rm_borders_tab2 = st.checkbox(
                "Mostrar contornos de Regiões Metropolitanas",
                value=False,
                key='show_rm_tab2',
                help="Ativa/desativa a visualização dos contornos das Regiões Metropolitanas sobre o mapa de UTPs"
            )

            if gdf is not None:
                # Tentar carregar GDF do snapshot
                gdf_consolidated = snapshot_loader.get_geodataframe_for_step('step5', gdf[gdf['uf'].isin(selected_ufs)].copy())
                
                if gdf_consolidated is None:
                    # Fallback
                    gdf_consolidated = consolidation_loader.apply_post_unitary_to_dataframe(
                        gdf[gdf['uf'].isin(selected_ufs)].copy()
                    )
                
                if selected_utps:
                    gdf_consolidated = gdf_consolidated[
                        gdf_consolidated['utp_id'].isin(selected_utps)
                    ]
                
                st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_consolidated)} Municípios")
                
                st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_consolidated)} Municípios")
                
                # Calcular coloração CONSOLIDADA sobre o frame consolidado (ou usar do snapshot)
                # O snapshot loader já traz color_id. Podemos usar ele.
                colors_consolidated = {}
                
                # Check if we have valid coloring in the snapshot
                has_valid_coloring = False
                if 'color_id' in gdf_consolidated.columns:
                     unique_colors = gdf_consolidated['color_id'].unique()
                     # If we have more than 1 color, OR if we have 1 color but it's not 0 (which is default/fallback)
                     # actually 0 is a valid color index, but if ALL are 0, it's suspicious for a large map.
                     if len(unique_colors) > 1:
                         has_valid_coloring = True
                     elif len(unique_colors) == 1 and unique_colors[0] != 0:
                         has_valid_coloring = True
                     
                     if has_valid_coloring:
                         for _, row in gdf_consolidated.iterrows():
                             # Access CD_MUN safely (standardized in loader)
                             col_name = 'CD_MUN' if 'CD_MUN' in row else 'cd_mun'
                             colors_consolidated[int(row[col_name])] = int(row['color_id'])
                
                # Fallback: Load from external cache if snapshot coloring is missing or seems invalid (monochromatic 0)
                if not has_valid_coloring:
                     logging.warning("⚠️ Snapshot coloring seems invalid or missing. Loading from consolidated_coloring.json fallback.")
                     colors_consolidated = load_or_compute_coloring(gdf_consolidated, "consolidated_coloring.json")
                
                # Renderizar mapa com opção de mostrar contornos de RM
                render_map(gdf_consolidated, title="Distribuição Consolidada (Snapshot)", graph=graph,
                          global_colors=colors_consolidated,
                          gdf_rm=gdf_rm, show_rm_borders=show_rm_borders_tab2)
            
            st.markdown("---")
            st.markdown("#### Registro de Consolidações")
            st.caption("Consolidações de UTPs unitárias (Steps 5+7) - SEM incluir consolidação de sedes")
            
            # Preparar dados para planilha - USAR DADOS PÓS-UNITÁRIAS
            if post_unitary_consolidations:
                df_consolidations = pd.DataFrame([
                    {
                        "ID": i + 1,
                        "UTP Origem": c["source_utp"],
                        "UTP Destino": c["target_utp"],
                        "Motivo": c.get("reason", "N/A"),
                        "Data": c["timestamp"][:10],
                        "Hora": c["timestamp"][11:19]
                    }
                    for i, c in enumerate(post_unitary_consolidations)
                ])
                st.dataframe(df_consolidations, width='stretch', hide_index=True)
            
            # Download do resultado
            result_json = json.dumps(consolidation_loader.result, ensure_ascii=False, indent=2)
            st.download_button(
                label="Baixar Resultado de Consolidação",
                data=result_json,
                file_name=f"consolidation_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
            

    
    # ==== TAB 3: ANÁLISE DE DEPENDÊNCIAS ====
    with tab3:
        st.markdown("### <span class='step-badge step-final'>ANÁLISE</span> Dependências entre Sedes", unsafe_allow_html=True)
        st.markdown("Análise sede-a-sede para identificar hierarquias e dependências entre UTPs usando dados socioeconômicos e fluxos.")
        st.markdown("---")
        
        # Carregar análise de dependências do cache JSON
        @st.cache_data(show_spinner="Carregando análise de dependências...", hash_funcs={pd.DataFrame: id})
        def load_sede_analysis_from_cache():
            """
            Carrega análise de dependências do JSON pré-processado.
            
            Retorna a tabela completa de origem-destino se disponível.
            """
            cache_file = Path(__file__).parent.parent.parent / "data" / "sede_analysis_cache.json"
            
            # Tentar carregar do cache
            if cache_file.exists():
                try:
                    import json
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Reconstruir DataFrame do JSON
                    df_raw = pd.DataFrame(data['sede_analysis'])
                    
                    # Carregar tabela completa se existir
                    df_comprehensive = pd.DataFrame()
                    if 'comprehensive_dependency_table' in data:
                        df_comprehensive = pd.DataFrame(data['comprehensive_dependency_table'])
                        logging.info(f"✅ Tabela completa carregada do cache: {len(df_comprehensive)} linhas")
                    
                    # Criar SedeAnalyzer temporário apenas para formatar tabela simples
                    analyzer = SedeAnalyzer()
                    analyzer.df_sede_analysis = df_raw
                    df_display = analyzer.export_sede_comparison_table()
                    
                    logging.info(f"✅ Análise carregada do cache: {len(df_raw)} sedes")
                    
                    return data['summary'], df_display, df_raw, df_comprehensive
                    
                except Exception as e:
                    logging.warning(f"⚠️ Erro ao carregar cache, executando análise: {e}")
            
            # Fallback: executar análise se cache não existir
            logging.info("ℹ️ Cache não encontrado, executando análise...")
            analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
            summary = analyzer.analyze_sede_dependencies()
            
            if summary.get('success'):
                df_table = analyzer.export_sede_comparison_table()
                # Tentar gerar tabela completa
                df_comp = analyzer.export_comprehensive_dependency_table()
                return summary, df_table, analyzer.df_sede_analysis, df_comp
            else:
                return summary, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        # Carregar análise (do cache ou executar fallback)
        # Carregar análise (do cache ou executar fallback)
        try:
            summary, df_display, df_raw, df_comprehensive = load_sede_analysis_from_cache()
            
            if summary.get('success'):
                # === MÉTRICAS GERAIS ===
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total de Sedes", summary['total_sedes'])
                
                with col2:
                    st.metric("Alertas de Dependência", summary['total_alertas'])
                
                with col3:
                    st.metric("População Total", f"{summary['populacao_total']:,}")
                
                with col4:
                    st.metric("Sedes com Aeroporto", summary['sedes_com_aeroporto'])
                
                st.markdown("---")
                
                # === ALERTAS ===
                sede_comparison.render_dependency_alerts(df_display)
                

                
                st.markdown("---")
                
                # === FILTROS E TABELA ===
                st.markdown("#### Tabela Comparativa de Sedes (Expandida)")
                
                # Filtros acima da tabela
                col_filter1, col_filter2, col_filter3, col_filter4 = st.columns(4)
                
                with col_filter1:
                    # Seletor de modo de visualização
                    view_mode = st.radio(
                        "Modo de Visualização",
                        ["Individual", "Origem-Destino"],
                        help="Individual: dados básicos. Origem-Destino: tabela completa expandida"
                    )
                
                with col_filter2:
                    show_alerts_only = st.checkbox("Apenas Alertas", value=False)
                
                with col_filter3:
                    # Filtro por REGIC
                    regic_options = ['Todos'] + sorted(df_raw[df_raw['regic'] != '']['regic'].unique().tolist())
                    selected_regic = st.selectbox("Filtrar por REGIC", regic_options)
                
                with col_filter4:
                    # Filtro por aeroporto
                    filter_airport = st.selectbox("Filtrar Aeroporto", ["Todos", "Apenas com aeroporto", "Sem aeroporto"])
                
                # Renderizar tabela conforme modo selecionado
                if view_mode == "Origem-Destino":
                    # Usar dados abrangentes se disponíveis, senão calcular
                    if not df_comprehensive.empty:
                        df_origin_dest = df_comprehensive
                    else:
                        # Fallback: recalcular se não estiver no cache
                        analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
                        analyzer.df_sede_analysis = df_raw
                        df_origin_dest = analyzer.export_comprehensive_dependency_table()
                    
                    # Aplicar filtros ao dataframe origem-destino
                    df_filtered_od = df_origin_dest.copy()
                    
                    if selected_regic != 'Todos':
                        # Filtrar por REGIC de origem OU destino (usando novos nomes de coluna)
                        # Nota: Verifica se colunas existem antes de filtrar para evitar erro em dados mistos
                        col_orig = 'REGIC_ORIGEM' if 'REGIC_ORIGEM' in df_filtered_od.columns else 'Origem_REGIC'
                        col_dest = 'REGIC_DESTINO' if 'REGIC_DESTINO' in df_filtered_od.columns else 'Destino_REGIC'
                        
                        mask = (df_filtered_od[col_orig] == selected_regic) | (df_filtered_od[col_dest] == selected_regic)
                        df_filtered_od = df_filtered_od[mask]
                    
                    if filter_airport == "Apenas com aeroporto":
                        col_orig = 'Aeroporto_Origem' if 'Aeroporto_Origem' in df_filtered_od.columns else 'Origem_Aeroporto'
                        col_dest = 'Aeroporto_Destino' if 'Aeroporto_Destino' in df_filtered_od.columns else 'Destino_Aeroporto'
                        
                        # Filtrar onde origem OU destino tem aeroporto
                        mask = (df_filtered_od[col_orig] == 'Sim') | (df_filtered_od[col_dest] == 'Sim')
                        df_filtered_od = df_filtered_od[mask]
                    elif filter_airport == "Sem aeroporto":
                        col_orig = 'Aeroporto_Origem' if 'Aeroporto_Origem' in df_filtered_od.columns else 'Origem_Aeroporto'
                        col_dest = 'Aeroporto_Destino' if 'Aeroporto_Destino' in df_filtered_od.columns else 'Destino_Aeroporto'
                        
                        # Filtrar onde AMBOS não têm aeroporto
                        mask = (df_filtered_od[col_orig] == '') & (df_filtered_od[col_dest] == '')
                        df_filtered_od = df_filtered_od[mask]
                    
                    # Renderizar tabela COMPLETA origem-destino
                    sede_comparison.render_comprehensive_table(df_filtered_od, show_alerts_only)
                    
                    # Usar df_display original para gráficos (não filtramos no modo origem-destino)
                    df_filtered_display = df_display.copy()
                    
                else:
                    # Modo Individual (atual)
                    # Aplicar filtros ao dataframe
                    df_filtered_display = df_display.copy()
                    df_filtered_raw = df_raw.copy()
                    
                    if selected_regic != 'Todos':
                        mask = df_raw['regic'] == selected_regic
                        df_filtered_display = df_display[mask]
                        df_filtered_raw = df_raw[mask]
                    
                    if filter_airport == "Apenas com aeroporto":
                        mask = df_display['Aeroporto'] == 'Sim'
                        df_filtered_display = df_filtered_display[mask]
                        df_filtered_raw = df_filtered_raw[df_raw['tem_aeroporto'] == True]
                    elif filter_airport == "Sem aeroporto":
                        mask = df_display['Aeroporto'] == ''
                        df_filtered_display = df_filtered_display[mask]
                        df_filtered_raw = df_filtered_raw[df_raw['tem_aeroporto'] == False]
                    
                    # Renderizar tabela individual
                    sede_comparison.render_sede_table(df_filtered_display, show_alerts_only)
                
                st.markdown("---")
                
                # === VISUALIZAÇÕES ===
                st.markdown("#### Análises Visuais")
                
                # Gráficos socioeconômicos (usa dados filtrados)
                sede_comparison.render_socioeconomic_charts(df_filtered_display)
                
                st.markdown("---")
                
                # Distribuição REGIC
                sede_comparison.render_regic_distribution(df_filtered_display)
                
                st.markdown("---")
                
                # === EXPORTAR DADOS ===
                st.markdown("#### Exportar Dados")
                
                col_exp1, col_exp2 = st.columns(2)
                
                with col_exp1:
                    # Download CSV da tabela
                    csv = df_display.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="Baixar Tabela (CSV)",
                        data=csv,
                        file_name=f"analise_sedes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                
                with col_exp2:
                    # Download JSON dos alertas
                    if summary['total_alertas'] > 0:
                        alertas_json = df_raw[df_raw['tem_alerta_dependencia']]['alerta_detalhes'].tolist()
                        json_str = json.dumps(alertas_json, ensure_ascii=False, indent=2)
                        st.download_button(
                            label="Baixar Alertas (JSON)",
                            data=json_str,
                            file_name=f"alertas_dependencia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
            
            else:
                st.error(f"Erro ao executar análise: {summary.get('error', 'Erro desconhecido')}")
                st.info("""
                ### Como usar:
                
                1. Certifique-se de que o arquivo `data/initialization.json` está presente
                2. Verifique se a matriz de impedância está disponível em `data/01_raw/impedance/impedancias_filtradas_2h.csv`
                3. Recarregue o dashboard (F5)
                """)
        
        except Exception as e:
            st.error(f"Erro ao carregar análise de sedes: {e}")
            import traceback
            with st.expander("Ver detalhes do erro"):
                st.code(traceback.format_exc())
    
    # === FOOTER ===
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.9rem; margin-top: 2rem;'>
        <p><strong>GeoValida</strong> • Consolidação Territorial de UTPs</p>
        <p>Laboratório de Transportes • UFSC</p>
        <p style='font-size: 0.8rem; color: #999;'>Cache de consolidação em: <code>data/consolidation_result.json</code></p>
    </div>
    """, unsafe_allow_html=True)


    # ==== TAB 4: ANÁLISE INTERESTADUAL ====
    with tab4:
        st.markdown("### <span class='step-badge step-final'>ANÁLISE</span> UTPs Interestaduais", unsafe_allow_html=True)
        st.markdown("Identificação de UTPs que abrangem municípios de múltiplos estados.")
        st.markdown("---")
        
        # 1. Identificar UTPs interestaduais
        # Agrupar por UTP e contar UFs únicos
        stats_interestadual = df_municipios.groupby('utp_id')['uf'].nunique().reset_index()
        utps_interestaduais_ids = stats_interestadual[stats_interestadual['uf'] > 1]['utp_id'].tolist()
        
        if not utps_interestaduais_ids:
            st.info("Nenhuma UTP interestadual encontrada.")
        else:
            # Filtrar dados para essas UTPs
            df_interestadual = df_municipios[df_municipios['utp_id'].isin(utps_interestaduais_ids)].copy()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total de UTPs Interestaduais", len(utps_interestaduais_ids))
            with col2:
                st.metric("Municípios Envolvidos", len(df_interestadual))
            
            # --- MAPA COM COLORAÇÃO ESPECÍFICA ---
            st.markdown("#### Mapa de Discrepância de Estado")
            st.caption("🔴 Tons de Vermelho: Município em estado diferente da Sede | ⚪ Tons de Cinza: Município no mesmo estado da Sede")
            st.caption("*(Cores variam para distinguir UTPs vizinhas)*")
            
            # Criar mapa de cores customizado
            custom_colors = {} # cd_mun -> hex color
            
            # Paletas topológicas (5 cores cada) para garantir contraste entre vizinhos
            # Reds para "Fora do Estado"
            REDS = ['#FF0000', '#B22222', '#CD5C5C', '#8B0000', '#FF4500']
            # Grays para "Dentro do Estado"
            GRAYS = ['#D3D3D3', '#A9A9A9', '#808080', '#696969', '#C0C0C0']
            
            # Iterar por UTP para determinar cores
            for utp_id, group in df_interestadual.groupby('utp_id'):
                # Achar a sede
                sede_row = group[group['sede_utp'] == True]
                if sede_row.empty:
                    # Se não tem sede definida (raro), usa a moda da UF
                    sede_uf = group['uf'].mode().iloc[0]
                else:
                    sede_uf = sede_row.iloc[0]['uf']
                
                # Colorir
                for idx, row in group.iterrows():
                    cd_mun = int(row['cd_mun'])
                    
                    # Obter índice de coloração topológica (0-4)
                    # Isso garante que UTPs vizinhas tenham índices diferentes
                    color_idx = global_colors_initial.get(cd_mun, 0)
                    palette_idx = color_idx % 5
                    
                    if row['uf'] != sede_uf:
                        custom_colors[cd_mun] = REDS[palette_idx] # Variação de vermelho
                    else:
                        custom_colors[cd_mun] = GRAYS[palette_idx] # Variação de cinza
            
            if gdf is not None:
                # Filtrar GDF
                gdf_inter = gdf[gdf['utp_id'].isin(utps_interestaduais_ids)].copy()
                
                # Calcular Cores Reais
                def get_color(row):
                    cd_mun_int = int(row['CD_MUN'])
                    # Retorna a cor calculada ou branco se erro
                    return custom_colors.get(cd_mun_int, '#FFFFFF')
                
                gdf_inter['color'] = gdf_inter.apply(get_color, axis=1)
                
                # Renderizar Folium
                m_inter = folium.Map(
                    location=[-15, -55], zoom_start=4,
                    tiles="CartoDB positron", prefer_canvas=True
                )
                
                if not gdf_inter.empty:
                    bounds = gdf_inter.total_bounds
                    m_inter.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]], padding=(0.05, 0.05))
                
                # Estilo
                folium.GeoJson(
                    gdf_inter.to_json(),
                    style_function=lambda x: {
                        'fillColor': x['properties'].get('color', '#cccccc'),
                        'color': '#ffffff', # Borda branca para destacar municípios
                        'weight': 0.5,
                        'fillOpacity': 0.8
                    },
                    tooltip=folium.GeoJsonTooltip(
                        fields=['NM_MUN', 'utp_id', 'uf', 'nm_sede'],
                        aliases=['Município:', 'UTP:', 'UF:', 'Sede:'],
                        localize=True
                    )
                ).add_to(m_inter)
                
                # Adicionar Contorno das UTPs (Preto)
                try:
                    # Preparar geometria para dissolver
                    # Usar projeção métrica para buffer mais preciso (3857 ou 5880) e evitar warning
                    # 100 metros de tolerância para fechar buracos
                    
                    # 1. Projetar para CRS métrico (EPSG:3857 - Pseudo-Mercator é rápido e suficiente aqui)
                    gdf_metric = gdf_inter.to_crs(epsg=3857)
                    
                    # 2. Buffer positivo (expandir) em metros
                    gdf_metric['geometry'] = gdf_metric.geometry.buffer(200)
                    
                    # 3. Dissolver por UTP
                    gdf_dissolved_metric = gdf_metric.dissolve(by='utp_id')
                    
                    # 4. Buffer negativo (contrair) em metros para restaurar forma
                    gdf_dissolved_metric['geometry'] = gdf_dissolved_metric.geometry.buffer(-200)
                    
                    # 5. Projetar de volta para WGS84 (Folium)
                    gdf_utp_outlines = gdf_dissolved_metric.to_crs(epsg=4326).reset_index()
                    gdf_utp_outlines = gdf_utp_outlines.reset_index()
                    
                    folium.GeoJson(
                        gdf_utp_outlines,
                        style_function=lambda x: {
                            'fillColor': 'none',
                            'color': '#000000', # Preto
                            'weight': 2.0,      # Mais espesso que os municípios
                            'fillOpacity': 0
                        },
                        interactive=False, # Não atrapalhar tooltip dos municípios
                        name="Contornos de UTP"
                    ).add_to(m_inter)
                except Exception as e:
                    logging.warning(f"Erro ao gerar contornos de UTP: {e}")
                
                map_html_inter = m_inter._repr_html_()
                st.components.v1.html(map_html_inter, height=600, scrolling=False)
            
            st.markdown("---")
            st.markdown("#### Detalhamento")
            
            # Tabela detalhada
            table_data = []
            for utp_id, group in df_interestadual.groupby('utp_id'):
                # Identificar UFs presentes
                ufs_presentes = sorted(group['uf'].unique().tolist())
                sede_row = group[group['sede_utp'] == True]
                sede_nm = sede_row.iloc[0]['nm_mun'] if not sede_row.empty else "N/A"
                sede_uf = sede_row.iloc[0]['uf'] if not sede_row.empty else "N/A"
                
                # Contar municípios fora do estado da sede
                muns_fora = group[group['uf'] != sede_uf]
                qtd_fora = len(muns_fora)
                
                table_data.append({
                    "UTP": utp_id,
                    "Sede": f"{sede_nm} ({sede_uf})",
                    "UFs Envolvidas": ", ".join(ufs_presentes),
                    "Qtd. Municípios Fora do Estado da Sede": qtd_fora,
                    "Total Municípios": len(group)
                })
            
            df_table_inter = pd.DataFrame(table_data).sort_values("Qtd. Municípios Fora do Estado da Sede", ascending=False)
            st.dataframe(df_table_inter, hide_index=True, width='stretch')


    # ==== TAB CONSOLIDAÇÃO SEDES (NOVA) ====
    with tab_sedes:
        st.markdown("### <span class='step-badge step-final'>NOVO</span> Consolidação de Sedes", unsafe_allow_html=True)
        st.markdown("Comparativo entre o cenário Pós-Limpeza (Base) e Pós-Consolidação de Sedes (Final).")
        st.markdown("Nesta etapa, sedes dependentes (fluxo principal + 2h distância) são anexadas a sedes mais fortes.")
        st.markdown("---")

        # Verificar se existe resultado dedicado de Sedes
        sede_executed = consolidation_loader.is_sede_executed()
        
        if sede_executed:
             sede_result = consolidation_loader.get_sede_result()
             sede_consolidations = sede_result.get('consolidations', [])
             sede_mapping = sede_result.get('utps_mapping', {})
             
             if gdf is not None:
                 # Filtrar GDF
                 gdf_sliced = gdf[gdf['uf'].isin(selected_ufs)].copy()
                 if selected_utps:
                     gdf_sliced = gdf_sliced[gdf_sliced['utp_id'].isin(selected_utps)]
                     
                 # Aplicar consolidação TOTAL via Snapshot Step 6
                 gdf_final = snapshot_loader.get_geodataframe_for_step('step6', gdf_sliced)
                 
                 if gdf_final is None:
                     # Fallback
                     gdf_final = consolidation_loader.apply_consolidations_to_dataframe(gdf_sliced, custom_mapping=total_mapping)
                 
                 # === MÉTRICAS PADRONIZADAS ===
                 if not gdf_final.empty:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        # Contagem única de municípios (coluna pode variar caixa dependendo da fonte, normalizar)
                        col_cd = 'CD_MUN' if 'CD_MUN' in gdf_final.columns else 'cd_mun'
                        current_unique = gdf_final[col_cd].astype(str).nunique()
                        total_unique = df_municipios['cd_mun'].nunique()
                        st.metric("Municípios", current_unique, f"{total_unique} total")
                    with col2:
                        current_utps = gdf_final['utp_id'].nunique()
                        st.metric("UTPs", current_utps, f"{len(utps_list)} total")
                    with col3:
                        current_ufs = gdf_final['uf'].nunique()
                        st.metric("Estados", current_ufs, f"{len(ufs)} total")
                 
                 st.markdown("---")

                 # Renderizar
                 # Tentar carregar coloração final específica se existir, senão usa a consolidada padrão
                 colors_final = {}
                 if 'color_id' in gdf_final.columns:
                     for _, row in gdf_final.iterrows():
                         col_name = 'CD_MUN' if 'CD_MUN' in row else 'cd_mun'
                         colors_final[int(row[col_name])] = int(row['color_id'])
                 else:
                     colors_final = load_or_compute_coloring(gdf_final, "post_sede_coloring.json")

                 render_map(gdf_final, title="Final (Snapshot)", 
                           global_colors=colors_final, 
                           gdf_rm=gdf_rm, show_rm_borders=True)
             else:
                 st.warning("Mapa indisponível")
             
             st.markdown("---")
             # Tabela de Mudanças Específicas desta Etapa
             st.markdown("#### Detalhes das Alterações de Sedes")
             
             # Criar tabela detalhada
             changes_data = []
             for c in sede_consolidations:
                 # Detalhes estão em 'details'
                 details = c.get('details', {})
                 mun_id = details.get('mun_id')
                 nm_mun = details.get('nm_mun', str(mun_id))
                 
                 changes_data.append({
                     "Município": nm_mun,
                     "UTP Origem": c['source_utp'],
                     "UTP Destino": c['target_utp'],
                     "Tipo": "Sede da UTP" if details.get('is_sede') else "Município Componente",
                     "Motivo": c['reason']
                 })
             
             st.dataframe(pd.DataFrame(changes_data), hide_index=True, width='stretch')

        else:
             st.info("Nenhuma consolidação de sedes encontrada.")
             st.caption("Execute a Etapa 6 do pipeline e certifique-se que houve consolidações.")
    
    # ==== TAB BORDERS: VALIDAÇÃO DE FRONTEIRAS ====
    with tab_borders:
        st.markdown("### <span class='step-badge step-final'>STEP 8</span> Validação de Fronteiras", unsafe_allow_html=True)
        st.markdown("Análise e refinamento iterativo de fronteiras entre UTPs baseado em fluxos principais.")
        st.markdown("---")
        
        # Visualizar Mapa Snapshot Step 8
        if gdf is not None:
             gdf_borders = snapshot_loader.get_geodataframe_for_step('step8', gdf[gdf['uf'].isin(selected_ufs)].copy())
             
             if gdf_borders is not None:
                 if selected_utps:
                     gdf_borders = gdf_borders[gdf_borders['utp_id'].isin(selected_utps)]
                     
                 st.subheader("Estado Final Pós-Validação (Snapshot)")
                 
                 # === MÉTRICAS PADRONIZADAS (MOVIDO PARA CIMA DO MAPA) ===
                 if not gdf_borders.empty:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        col_cd = 'CD_MUN' if 'CD_MUN' in gdf_borders.columns else 'cd_mun'
                        current_unique = gdf_borders[col_cd].astype(str).nunique()
                        total_unique = df_municipios['cd_mun'].nunique()
                        st.metric("Municípios", current_unique, f"{total_unique} total")
                    with col2:
                        current_utps = gdf_borders['utp_id'].nunique()
                        st.metric("UTPs", current_utps, f"{len(utps_list)} total")
                    with col3:
                        current_ufs = gdf_borders['uf'].nunique()
                        st.metric("Estados", current_ufs, f"{len(ufs)} total")
                    
                    st.markdown("---")
                 
                 colors_borders = {}
                 if 'color_id' in gdf_borders.columns:
                     for _, row in gdf_borders.iterrows():
                         col_name = 'CD_MUN' if 'CD_MUN' in row else 'cd_mun'
                         colors_borders[int(row[col_name])] = int(row['color_id'])
                 
                 render_map(gdf_borders, title="Validação Fronteiras (Snapshot)", 
                           global_colors=colors_borders,
                           gdf_rm=gdf_rm, show_rm_borders=True)
                 st.markdown("---")
        
        # Carregar dados de validação de fronteiras
        borders_json_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "border_validation_result.json"
        
        if borders_json_path.exists():
            try:
                with open(borders_json_path, 'r', encoding='utf-8') as f:
                    borders_data = json.load(f)
                
                metadata = borders_data.get('metadata', {})
                relocations = borders_data.get('relocations', [])
                rejections = borders_data.get('rejections', [])
                transitive_chains = borders_data.get('transitive_chains', [])
                
                # === MÉTRICAS PADRONIZADAS (REMOVIDO DAQUI) ===
                # (Movido para cima do mapa)
                
                st.markdown("---")
                
                # === CADEIAS TRANSITIVAS ===
                if transitive_chains:
                    st.markdown("#### 🔗 Cadeias Transitivas Detectadas")
                    st.caption(f"{len(transitive_chains)} cadeia(s) transitiva(s) resolvida(s)")
                    
                    for i, chain in enumerate(transitive_chains, 1):
                        chain_str = " → ".join(chain['chain'])
                        final_utp = chain['final_utp']
                        st.info(f"**Cadeia {i}:** {chain_str} → **UTP {final_utp}**")
                    
                    st.markdown("---")
                
                # === TABS INTERNAS: REALOCAÇÕES VS REJEIÇÕES ===
                subtab1, subtab2 = st.tabs(["Realocações", "Rejeições"])
                
                with subtab1:
                    st.markdown("#### Municípios Realocados")
                    
                if relocations:
                    # Preparar dados para tabela (Usando lista atual)
                    relocations_df = pd.DataFrame([
                        {
                            "Município": r['mun_name'],
                            "CD_MUN": r['mun_id'],
                            "UTP Origem": r['utp_origem'],
                            "UTP Destino": r['utp_destino'],
                            "Iteração": r.get('iteration', 1),
                            "Motivo": r.get('reason', 'N/A')
                        }
                        for r in relocations
                    ])
                else:
                    # Tentar carregar HISTÓRICO da ConsolidationManager se a lista atual estiver vazia
                    # Isso garante que a tabela mostre o histórico mesmo se a última execução foi limpa
                    
                    history_consolidations = consolidation_loader.get_consolidations()
                    border_history = []
                    
                    for c in history_consolidations:
                        # Identificar registros de validação de fronteira pelo "reason" ou "details"
                        reason = str(c.get('reason', ''))
                        details = c.get('details', {})
                        
                        if "Border validation" in reason or details.get('step') == 'border_validation':
                             # Extrair dados do formato do ConsolidationManager
                             mun_nm = details.get('municipality_name', str(details.get('municipality_id', 'Unknown')))
                             
                             border_history.append({
                                "Município": mun_nm,
                                "CD_MUN": details.get('municipality_id', 0),
                                "UTP Origem": c['source_utp'],
                                "UTP Destino": c['target_utp'],
                                "Iteração": details.get('iteration', 1),
                                "Motivo": reason
                             })
                    
                    if border_history:
                        relocations_df = pd.DataFrame(border_history)
                        st.info(f"Mostrando histórico acumulado de {len(relocations_df)} realocações (execuções anteriores)")
                    else:
                        relocations_df = pd.DataFrame()

                # Só renderizar se tiver dados (seja atual ou histórico)
                if not relocations_df.empty:
                        
                    # Estatísticas
                    st.markdown(f"**{len(relocations_df)} municípios realocados**")
                        
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("UTPs Origem Únicas", relocations_df['UTP Origem'].nunique())
                    with col2:
                        st.metric("UTPs Destino Únicas", relocations_df['UTP Destino'].nunique())
                    
                    # Distribuição por iteração
                    st.markdown("**Distribuição por Iteração:**")
                    iter_counts = relocations_df['Iteração'].value_counts().sort_index()
                    for iter_num, count in iter_counts.items():
                        st.write(f"- Iteração {iter_num}: {count} realocações")
                    
                    st.markdown("---")
                    st.markdown("**Detalhes:**")
                    st.dataframe(relocations_df, hide_index=True, width='stretch', height=400)
                    
                    # Visualização no mapa
                    if gdf is not None:
                        st.markdown("---")
                        st.markdown("#### Mapa de Municípios Realocados")
                        
                        # Destacar municípios realocados
                        # Precisamos extrair IDs da tabela consolidada
                        if 'CD_MUN' in relocations_df.columns:
                             relocated_ids = set(relocations_df['CD_MUN'].unique())
                             gdf_highlight = gdf[gdf['CD_MUN'].isin(relocated_ids)].copy()
                             
                             if not gdf_highlight.empty:
                                 st.caption(f"Destacando {len(gdf_highlight)} municípios realocados")
                                 
                                 # Criar mapa básico
                                 m = folium.Map(
                                     location=[-15, -55],
                                     zoom_start=4,
                                     tiles="CartoDB positron"
                                 )
                                 
                                 # Adicionar municípios realocados em destaque
                                 folium.GeoJson(
                                     gdf_highlight.to_json(),
                                     style_function=lambda x: {
                                         'fillColor': '#FF6B6B',
                                         'color': '#C92A2A',
                                         'weight': 2,
                                         'fillOpacity': 0.7
                                     },
                                     tooltip=folium.GeoJsonTooltip(
                                         fields=['NM_MUN', 'utp_id', 'uf'],
                                         aliases=['Município:', 'UTP:', 'UF:']
                                     )
                                 ).add_to(m)
                                 
                                 # Fit bounds
                                 if not gdf_highlight.empty:
                                     bounds = gdf_highlight.total_bounds
                                     m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
                                 
                                 map_html = m._repr_html_()
                                 st.components.v1.html(map_html, height=500, scrolling=False)

                else:
                    if not relocations and not 'border_history' in locals():
                         st.info("Nenhuma realocação foi realizada.")
                         st.caption("Todas as fronteiras já estão otimizadas!")
                
                with subtab2:
                    st.markdown("#### Propostas de Realocação Rejeitadas")
                    
                    if rejections:
                        # Preparar dados para tabela
                        rejections_df = pd.DataFrame([
                            {
                                "Município": r['mun_name'],
                                "CD_MUN": r['mun_id'],
                                "UTP Atual": r['utp_origem'],
                                "UTP Proposta": r.get('proposed_utp', 'N/A'),
                                "Motivo": r.get('reason', 'N/A'),
                                "Detalhes": r.get('details', ''),
                                "Iteração": r.get('iteration', 1)
                            }
                            for r in rejections
                        ])
                        
                        st.markdown(f"**{len(rejections_df)} propostas rejeitadas**")
                        
                        # Análise de motivos
                        st.markdown("**Distribuição de Motivos de Rejeição:**")
                        reason_counts = rejections_df['Motivo'].value_counts()
                        for reason, count in reason_counts.items():
                            percentage = (count / len(rejections_df)) * 100
                            st.write(f"- {reason}: {count} ({percentage:.1f}%)")
                        
                        st.markdown("---")
                        st.markdown("**Detalhes:**")
                        st.dataframe(rejections_df, hide_index=True, width='stretch', height=400)
                    else:
                        st.success("✅ Nenhuma rejeição! Todas as propostas foram aceitas.")
                
                # === DOWNLOAD ===
                st.markdown("---")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Download JSON
                    borders_json = json.dumps(borders_data, ensure_ascii=False, indent=2)
                    st.download_button(
                        label="Baixar Resultados (JSON)",
                        data=borders_json,
                        file_name=f"border_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
                
                with col2:
                    # Download CSV
                    csv_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "border_validation_result.csv"
                    if csv_path.exists():
                        with open(csv_path, 'r', encoding='utf-8-sig') as f:
                            csv_data = f.read()
                        st.download_button(
                            label="Baixar Resultados (CSV)",
                            data=csv_data,
                            file_name=f"border_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
            except Exception as e:
                st.error(f"Erro ao carregar dados de validação de fronteiras: {e}")
                import traceback
                st.code(traceback.format_exc())
        else:
            st.warning("⚠️ Dados de validação de fronteiras não encontrados")
            st.info("""
            **Como gerar os dados:**
            1. Execute o pipeline completo: `python main.py`
            2. Ou execute apenas a consolidação: `python src/run_consolidation.py`
            
            O Step 8 (Validação de Fronteiras) será executado automaticamente e gerará os arquivos:
            - `data/03_processed/border_validation_result.json`
            - `data/03_processed/border_validation_result.csv`
            """)



