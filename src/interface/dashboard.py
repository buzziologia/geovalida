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
PASTEL_PALETTE = [
    '#8dd3c7', '#ffffb3', '#bebada', '#fb8072', '#80b1d3', '#fdb462', 
    '#b3de69', '#fccde5', '#d9d9d9', '#bc80bd', '#ccebc5', '#ffed6f',
    '#a6cee3', '#b2df8a', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99'
]


@st.cache_data(show_spinner=True, hash_funcs={gpd.GeoDataFrame: id})
def get_geodataframe(shapefile_path, df_municipios):
    """Carrega, processa e simplifica o shapefile com cache."""
    if not shapefile_path.exists():
        return None

    try:
        gdf = gpd.read_file(shapefile_path)
        
        # Reprojetar para WGS84 (EPSG:4326) - Folium espera este CRS
        # O shapefile original está em SIRGAS2000 (EPSG:4674)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        
        # Converter IDs para string
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        # Garantir cópia para não afetar cache original do pandas se houver
        df_mun_copy = df_municipios.copy()
        df_mun_copy['cd_mun'] = df_mun_copy['cd_mun'].astype(str)
        
        # Juntar dados
        gdf = gdf.merge(df_mun_copy[['cd_mun', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_mun']], 
                       left_on='CD_MUN', right_on='cd_mun', how='left')
        
        # Identificar nomes das sedes
        df_sedes = df_mun_copy[df_mun_copy['sede_utp'] == True][['utp_id', 'nm_mun']].set_index('utp_id')
        sede_mapper = df_sedes['nm_mun'].to_dict()
        gdf['nm_sede'] = gdf['utp_id'].map(sede_mapper).fillna('')
        
        # Simplificar geometria com preservação de topologia - tolerance de 0.002 graus (~200m)
        gdf['geometry'] = gdf.geometry.simplify(tolerance=0.002, preserve_topology=True)
        gdf['regiao_metropolitana'] = gdf['regiao_metropolitana'].fillna('')
        
        # Manter apenas colunas essenciais
        cols_to_keep = ['NM_MUN', 'CD_MUN', 'geometry', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_sede']
        existing_cols = [c for c in cols_to_keep if c in gdf.columns]
        gdf = gdf[existing_cols]
        
        return gdf
    except Exception as e:
        st.error(f"Erro no processamento do mapa: {e}")
        return None


@st.cache_data(show_spinner=True, hash_funcs={gpd.GeoDataFrame: id})
def get_rm_geodataframe(shapefile_path):
    """Carrega e processa o shapefile de Regiões Metropolitanas com cache."""
    if not shapefile_path.exists():
        return None
    
    try:
        gdf_rm = gpd.read_file(shapefile_path)
        
        # Reprojetar para WGS84 (EPSG:4326) se necessário
        if gdf_rm.crs and gdf_rm.crs.to_epsg() != 4326:
            gdf_rm = gdf_rm.to_crs(epsg=4326)
        
        # Simplificar geometria para melhor performance
        gdf_rm['geometry'] = gdf_rm.geometry.simplify(tolerance=0.005, preserve_topology=True)
        
        # Manter apenas colunas essenciais
        cols_to_keep = ['NOME', 'UF_SIGLA', 'MUNICIPIO', 'AREA_KM2', 'POP_2010', 
                       'POLO', 'POLO_POP', 'geometry']
        existing_cols = [c for c in cols_to_keep if c in gdf_rm.columns]
        gdf_rm = gdf_rm[existing_cols]
        
        return gdf_rm
    except Exception as e:
        st.error(f"Erro ao carregar shapefile de Regiões Metropolitanas: {e}")
        return None



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
    
    # Garantir tipos numéricos
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


def render_map(gdf_filtered, title="Mapa", graph=None, gdf_rm=None, show_rm_borders=False):
    """Função auxiliar para renderizar um mapa folium com coloração por grafo."""
    if gdf_filtered is None or gdf_filtered.empty:
        st.info("Nenhum dado para visualizar neste filtro.")
        return
    
    gdf_filtered = gdf_filtered.copy()
    
    # Se o grafo foi fornecido, usar coloração mínima baseada em adjacência
    if graph is not None:
        try:
            # Preparar GeoDataFrame para coloração (precisa de UTP_ID e CD_MUN como int)
            gdf_for_coloring = gdf_filtered.copy()
            
            # Garantir que CD_MUN existe e é inteiro
            if 'CD_MUN' not in gdf_for_coloring.columns:
                logging.warning("Coluna CD_MUN não encontrada, usando coloração simples")
                raise ValueError("Missing CD_MUN")
            
            gdf_for_coloring['CD_MUN'] = gdf_for_coloring['CD_MUN'].astype(str)
            gdf_for_coloring['UTP_ID'] = gdf_for_coloring['utp_id'].astype(str)
            
            # Calcular coloração usando algoritmo de grafo
            coloring = graph.compute_graph_coloring(gdf_for_coloring)
            
            # Mapear cores: cd_mun (int) -> color_idx -> cor hex
            color_map = {}
            for _, row in gdf_filtered.iterrows():
                try:
                    cd_mun = int(row['CD_MUN'])
                    color_idx = coloring.get(cd_mun, 0) % len(PASTEL_PALETTE)
                    color_map[row['utp_id']] = PASTEL_PALETTE[color_idx]
                except (ValueError, KeyError):
                    color_map[row['utp_id']] = PASTEL_PALETTE[0]
            
            gdf_filtered['color'] = gdf_filtered['utp_id'].map(color_map)
            logging.info(f"Coloração por grafo aplicada: {len(set(coloring.values()))} cores distintas")
        
        except Exception as e:
            logging.warning(f"Erro ao aplicar coloração por grafo, usando fallback: {e}")
            # Fallback: usar coloração simples por UTP
            utps_unique = gdf_filtered['utp_id'].dropna().unique()
            colors = {utp: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] 
                     for i, utp in enumerate(sorted(utps_unique))}
            gdf_filtered['color'] = gdf_filtered['utp_id'].map(colors)
    else:
        # Sem grafo: usar coloração simples
        utps_unique = gdf_filtered['utp_id'].dropna().unique()
        colors = {utp: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] 
                 for i, utp in enumerate(sorted(utps_unique))}
        gdf_filtered['color'] = gdf_filtered['utp_id'].map(colors)
    
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
        logging.info(f"DEBUG RM: show_rm_borders={show_rm_borders}, gdf_rm.shape={gdf_rm.shape}")
        
        # ESTRATÉGIA: Matching espacial - verificar quais municípios estão dentro de cada RM do shapefile
        # Isso evita problema de correspondência de nomes entre JSON e shapefile
        
        if 'regiao_metropolitana' in gdf_filtered.columns:
            # Encontrar municípios que têm RM definida no JSON
            municipios_com_rm = gdf_filtered[gdf_filtered['regiao_metropolitana'].notna() & 
                                             (gdf_filtered['regiao_metropolitana'] != '')].copy()
            
            if not municipios_com_rm.empty:
                logging.info(f"DEBUG RM: {len(municipios_com_rm)} municípios com RM no mapa")
                
                # Garantir mesma projeção para spatial join
                if gdf_rm.crs != municipios_com_rm.crs:
                    gdf_rm_proj = gdf_rm.to_crs(municipios_com_rm.crs)
                else:
                    gdf_rm_proj = gdf_rm
                
                # Fazer spatial join para encontrar qual RM do shapefile contém cada município
                # Usar intersects em vez de within pois municípios podem estar parcialmente em RMs
                try:
                    import geopandas as gpd
                    joined = gpd.sjoin(municipios_com_rm, gdf_rm_proj, how='left', predicate='intersects')
                    
                    # Encontrar RMs do shapefile que têm municípios
                    rms_com_municipios = joined['index_right'].dropna().unique()
                    
                    logging.info(f"DEBUG RM: {len(rms_com_municipios)} RMs do shapefile têm municípios visíveis")
                    
                    if len(rms_com_municipios) > 0:
                        # Desenhar contornos das RMs relevantes
                        gdf_rm_filtered = gdf_rm_proj.iloc[rms_com_municipios].copy()
                        
                        for idx, row in gdf_rm_filtered.iterrows():
                            nome_rm = row.get('NOME', 'N/A')
                            uf = row.get('UF_SIGLA', 'N/A')
                            num_municipios = int(row.get('MUNICIPIO', 0)) if pd.notna(row.get('MUNICIPIO')) else 0
                            
                            logging.info(f"DEBUG RM: Adicionando contorno para {nome_rm} ({uf})")
                            
                            # Tooltip para RM
                            tooltip_rm = f"RM: {nome_rm} ({uf}) - {num_municipios} municípios"
                            
                            # Adicionar contorno da RM
                            folium.GeoJson(
                                row.geometry,
                                style_function=lambda x: {
                                    'fillColor': 'none',
                                    'color': '#FF0000',  # Vermelho para destacar
                                    'weight': 3,
                                    'fillOpacity': 0,
                                    'dashArray': '10, 5'  # Linha tracejada
                                },
                                tooltip=tooltip_rm,
                                name=f"RM: {nome_rm}"
                            ).add_to(m)
                        
                        logging.info(f"DEBUG RM: {len(gdf_rm_filtered)} contornos de RM adicionados ao mapa")
                    else:
                        logging.warning("DEBUG RM: Nenhuma RM do shapefile intersecta com municípios visíveis")
                
                except Exception as e:
                    logging.error(f"DEBUG RM: Erro ao fazer spatial join: {e}")
            else:
                logging.info("DEBUG RM: Nenhum município com RM no mapa filtrado")
        else:
            logging.warning("DEBUG RM: Coluna 'regiao_metropolitana' não encontrada em gdf_filtered")
    else:
        if not show_rm_borders:
            logging.info("DEBUG RM: show_rm_borders é False - RMs não serão exibidas")
        elif gdf_rm is None:
            logging.warning("DEBUG RM: gdf_rm é None - shapefile não foi carregado")
        elif gdf_rm.empty:
            logging.warning("DEBUG RM: gdf_rm está vazio - shapefile carregado mas sem dados")
    
    map_html = m._repr_html_()
    st.components.v1.html(map_html, height=600, scrolling=False)


def render_dashboard(manager):
    """Dashboard com visualização do pipeline de consolidação territorial."""
    
    # === LOAD CONSOLIDATION CACHE ===
    consolidation_loader = ConsolidationLoader()
    
    # === HEADER ===
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
        
        # Filtro por UTP
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
            selected_utps = st.multiselect("UTPs", utps_list, default=[])
        
        st.markdown("---")
        st.caption(f"Dados de: {metadata.get('timestamp', 'N/A')[:10]}")
        
        # === SEÇÃO DE CONSOLIDAÇÃO ===
        st.markdown("---")
        st.markdown("### Consolidação")
        
        if consolidation_loader.is_executed():
            summary = consolidation_loader.get_summary()
            
            # Verifica se houve mudanças ou não
            if summary['total_consolidations'] > 0:
                st.success("Consolidação em cache")
                st.metric("Consolidações", summary['total_consolidations'])
                st.metric("UTPs Reduzidas", f"{summary['unique_sources']} → {summary['unique_targets']}")
            else:
                st.info("Consolidação executada - nenhuma mudança necessária")
                st.caption("Todos os municípios já estão corretamente organizados.")
            
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
    df_filtered = df_municipios[df_municipios['uf'].isin(selected_ufs)].copy()
    if selected_utps:
        df_filtered = df_filtered[df_filtered['utp_id'].isin(selected_utps)]
    
    # Carregar shapefile
    shapefile_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"
    gdf = get_geodataframe(shapefile_path, df_municipios)
    
    # Carregar shapefile de Regiões Metropolitanas
    rm_shapefile_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "shapefiles" / "reg. metrop.shp"
    gdf_rm = get_rm_geodataframe(rm_shapefile_path)
    
    # Criar instância do grafo territorial para coloração
    try:
        graph = TerritorialGraph()
        # Carregar estrutura do grafo a partir dos dados
        for _, row in df_municipios.iterrows():
            cd_mun = int(row['cd_mun'])
            nm_mun = row.get('nm_mun', str(cd_mun))
            utp_id = str(row.get('utp_id', 'SEM_UTP'))
            rm_name = row.get('regiao_metropolitana', '')
            
            if not rm_name or rm_name.strip() == '':
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
    except Exception as e:
        logging.error(f"Erro ao criar grafo territorial: {e}")
        graph = None
    
    # === TABS ===
    tab1, tab2, tab3 = st.tabs([
        "Distribuição Inicial",
        "Pós-Consolidação",
        "Análise de Dependências"
    ])
    
    # ==== TAB 1: DISTRIBUIÇÃO INICIAL ====
    with tab1:
        st.markdown("### <span class='step-badge step-initial'>INICIAL</span> Situação Atual", unsafe_allow_html=True)
        st.markdown("Mapa da distribuição atual das UTPs antes da consolidação.")
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Municípios", len(df_filtered), f"{len(df_municipios)} total")
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
        
        if gdf is not None:
            gdf_filtered = gdf[gdf['uf'].isin(selected_ufs)].copy()
            if selected_utps:
                gdf_filtered = gdf_filtered[gdf_filtered['utp_id'].isin(selected_utps)]
            
            st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_filtered)} Municípios")
            
            # Renderizar mapa com opção de mostrar contornos de RM
            render_map(gdf_filtered, title="Distribuição por UTP", graph=graph, 
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
            consolidations = consolidation_loader.get_consolidations()
            stats_summary = consolidation_loader.get_summary()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Consolidações", stats_summary['total_consolidations'])
            with col2:
                st.metric("UTPs Reduzidas", f"{stats_summary['unique_sources']} → {stats_summary['unique_targets']}")
            with col3:
                reduction = (stats_summary['unique_sources'] - stats_summary['unique_targets']) / stats_summary['unique_sources'] * 100
                st.metric("% Redução", f"{reduction:.1f}%")
            
            st.markdown("---")
            st.markdown("#### Mapa Pós-Consolidação")
            
            # Controle de visualização de contornos de RM
            show_rm_borders_tab2 = st.checkbox(
                "Mostrar contornos de Regiões Metropolitanas",
                value=False,
                key='show_rm_tab2',
                help="Ativa/desativa a visualização dos contornos das Regiões Metropolitanas sobre o mapa de UTPs"
            )
            
            # Aplicar consolidações ao dataframe
            df_consolidated = consolidation_loader.apply_consolidations_to_dataframe(df_filtered)
            
            if gdf is not None:
                gdf_consolidated = consolidation_loader.apply_consolidations_to_dataframe(
                    gdf[gdf['uf'].isin(selected_ufs)].copy()
                )
                
                if selected_utps:
                    gdf_consolidated = gdf_consolidated[
                        gdf_consolidated['utp_id'].isin(selected_utps)
                    ]
                
                st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_consolidated)} Municípios")
                
                # Renderizar mapa com opção de mostrar contornos de RM
                render_map(gdf_consolidated, title="Distribuição Consolidada", graph=graph,
                          gdf_rm=gdf_rm, show_rm_borders=show_rm_borders_tab2)
            
            st.markdown("---")
            st.markdown("#### Registro de Consolidações")
            
            # Preparar dados para planilha
            df_consolidations = consolidation_loader.export_as_dataframe()
            st.dataframe(df_consolidations, width='stretch', hide_index=True)
            
            # Download do resultado
            result_json = json.dumps(consolidation_loader.result, ensure_ascii=False, indent=2)
            st.download_button(
                label="Baixar Resultado de Consolidação",
                data=result_json,
                file_name=f"consolidation_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
            
            # === ANÁLISE DE QUALIDADE DAS UTPs ===
            st.markdown("---")
            st.markdown("#### Análise de Qualidade das UTPs")
            st.caption("Identificação de problemas que podem requerer atenção adicional")
            
            # Análise de UTPs unitárias
            df_unitary = analyze_unitary_utps(df_consolidated)
            
            # Análise de UTPs não-contíguas
            non_contiguous = analyze_non_contiguous_utps(gdf_consolidated) if gdf is not None else {}
            
            # Métricas de qualidade
            col1, col2, col3 = st.columns(3)
            with col1:
                total_utps = df_consolidated['utp_id'].nunique()
                st.metric("Total de UTPs", total_utps)
            with col2:
                unitary_count = len(df_unitary)
                st.metric("UTPs Unitárias", unitary_count, 
                         delta="Requer atenção" if unitary_count > 0 else "OK",
                         delta_color="inverse")
            with col3:
                non_contiguous_count = len(non_contiguous)
                st.metric("UTPs Não-Contíguas", non_contiguous_count,
                         delta="Requer atenção" if non_contiguous_count > 0 else "OK",
                         delta_color="inverse")
            
            # Detalhamento das UTPs Unitárias
            if not df_unitary.empty:
                st.markdown("---")
                st.markdown("##### UTPs Unitárias")
                st.caption(f"⚠️ {len(df_unitary)} UTP(s) com apenas 1 município")
                
                st.dataframe(
                    df_unitary,
                    width='stretch',
                    hide_index=True
                )
                
                st.info("""
                **UTPs Unitárias** podem indicar:
                - Municípios que não puderam ser consolidados devido a restrições
                - Municípios com características muito distintas dos vizinhos
                - Possíveis candidatos para nova rodada de consolidação
                """)
            else:
                st.success("✅ Nenhuma UTP unitária encontrada")
            
            # Detalhamento das UTPs Não-Contíguas
            if non_contiguous:
                st.markdown("---")
                st.markdown("##### UTPs com Municípios Não-Contíguos")
                st.caption(f"⚠️ {len(non_contiguous)} UTP(s) com municípios geograficamente desconectados")
                
                # Criar tabela formatada
                non_contiguous_data = []
                for utp_id, info in non_contiguous.items():
                    # Formatar componentes
                    components_str = []
                    for i, comp in enumerate(info['components'], 1):
                        comp_munic = ", ".join(comp[:3])  # Primeiros 3 municípios
                        if len(comp) > 3:
                            comp_munic += f" (+{len(comp)-3})"
                        components_str.append(f"Grupo {i}: {comp_munic}")
                    
                    non_contiguous_data.append({
                        'UTP': utp_id,
                        'Total Municípios': info['num_municipalities'],
                        'Componentes Desconectados': info['num_components'],
                        'Detalhes': " | ".join(components_str)
                    })
                
                df_non_contiguous = pd.DataFrame(non_contiguous_data)
                st.dataframe(
                    df_non_contiguous,
                    width='stretch',
                    hide_index=True
                )
                
                st.warning("""
                **UTPs Não-Contíguas** indicam que alguns municípios da UTP estão geograficamente separados:
                - Isso pode ser resultado de consolidações baseadas em fluxos funcionais
                - Municípios podem ter forte relação funcional mas não compartilham fronteiras
                - Considere revisar se a consolidação faz sentido do ponto de vista territorial
                """)
            else:
                st.success("✅ Todas as UTPs são geograficamente contíguas")
        
        else:
            st.info("Nenhuma consolidação em cache ainda.")
            st.markdown("""
            ### Como usar:
            
            1. **Execute a consolidação** via seu código (etapas 0-7)
            2. **O arquivo `consolidation_result.json` será criado** em `data/`
            3. **Recarregue o dashboard** (F5 ou refresh)
            4. Os mapas comparativos aparecerão automaticamente
            
            O cache permanecerá enquanto você não clicar em "Limpar" na sidebar.
            """)
    
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
            
            Este método carrega resultados da análise que foram salvos quando o pipeline
            foi executado, resultando em carregamento ~100x mais rápido que recalcular.
            
            Se o cache não existir, executa a análise como fallback.
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
                    
                    # Criar SedeAnalyzer temporário apenas para formatar tabela
                    analyzer = SedeAnalyzer()
                    analyzer.df_sede_analysis = df_raw
                    df_display = analyzer.export_sede_comparison_table()
                    
                    logging.info(f"✅ Análise carregada do cache: {len(df_raw)} sedes")
                    
                    return data['summary'], df_display, df_raw
                    
                except Exception as e:
                    logging.warning(f"⚠️ Erro ao carregar cache, executando análise: {e}")
            
            # Fallback: executar análise se cache não existir
            logging.info("ℹ️ Cache não encontrado, executando análise...")
            analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
            summary = analyzer.analyze_sede_dependencies()
            
            if summary.get('success'):
                df_table = analyzer.export_sede_comparison_table()
                return summary, df_table, analyzer.df_sede_analysis
            else:
                return summary, pd.DataFrame(), pd.DataFrame()
        
        # Carregar análise (do cache ou executar fallback)
        try:
            summary, df_display, df_raw = load_sede_analysis_from_cache()
            
            if summary.get('success'):
                # === MÉTRICAS GERAIS ===
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total de Sedes", summary['total_sedes'])
                
                with col2:
                    alert_count = summary['total_alertas']
                    st.metric("Alertas", alert_count, 
                             delta="Dependências" if alert_count > 0 else "Nenhum",
                             delta_color="inverse")
                
                with col3:
                    st.metric("População Total", f"{summary['populacao_total']:,}")
                
                with col4:
                    st.metric("Com Aeroporto", summary['sedes_com_aeroporto'])
                
                st.markdown("---")
                
                # === ALERTAS DE DEPENDÊNCIA ===
                st.markdown("#### Alertas de Dependência Funcional")
                st.caption("Sedes cujo principal fluxo vai para outra sede a até 2h de distância")
                
                sede_comparison.render_dependency_alerts(df_display)
                
                st.markdown("---")
                
                # === FILTROS E TABELA ===
                st.markdown("#### Tabela Comparativa de Sedes")
                
                # Filtros acima da tabela
                col_filter1, col_filter2, col_filter3, col_filter4 = st.columns(4)
                
                with col_filter1:
                    # Seletor de modo de visualização
                    view_mode = st.radio(
                        "Modo de Visualização",
                        ["Individual", "Origem-Destino"],
                        help="Individual: uma linha por sede. Origem-Destino: pares comparativos lado a lado"
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
                    # Gerar dados origem-destino
                    analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
                    # Reutilizar análise já feita
                    analyzer.df_sede_analysis = df_raw
                    df_origin_dest = analyzer.export_origin_destination_comparison()
                    
                    # Aplicar filtros ao dataframe origem-destino
                    df_filtered_od = df_origin_dest.copy()
                    
                    if selected_regic != 'Todos':
                        # Filtrar por REGIC de origem OU destino
                        mask = (df_filtered_od['Origem_REGIC'] == selected_regic) | (df_filtered_od['Destino_REGIC'] == selected_regic)
                        df_filtered_od = df_filtered_od[mask]
                    
                    if filter_airport == "Apenas com aeroporto":
                        # Filtrar onde origem OU destino tem aeroporto
                        mask = (df_filtered_od['Origem_Aeroporto'] == 'Sim') | (df_filtered_od['Destino_Aeroporto'] == 'Sim')
                        df_filtered_od = df_filtered_od[mask]
                    elif filter_airport == "Sem aeroporto":
                        # Filtrar onde AMBOS não têm aeroporto
                        mask = (df_filtered_od['Origem_Aeroporto'] == '') & (df_filtered_od['Destino_Aeroporto'] == '')
                        df_filtered_od = df_filtered_od[mask]
                    
                    # Renderizar tabela origem-destino
                    sede_comparison.render_origin_destination_table(df_filtered_od, show_alerts_only)
                    
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
