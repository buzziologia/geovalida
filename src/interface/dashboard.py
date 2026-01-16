# src/interface/dashboard.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import json
from pathlib import Path
from datetime import datetime
from src.utils import DataLoader
from src.interface.consolidation_loader import ConsolidationLoader
from src.run_consolidation import run_consolidation
from src.pipeline.sede_analyzer import SedeAnalyzer
from src.interface.components import sede_comparison


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
</style>
""", unsafe_allow_html=True)


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


def render_map(gdf_filtered, title="Mapa"):
    """Função auxiliar para renderizar um mapa folium."""
    if gdf_filtered is None or gdf_filtered.empty:
        st.info("Nenhum dado para visualizar neste filtro.")
        return
    
    # Criar cores por UTP com Paleta Pastel
    utps_unique = gdf_filtered['utp_id'].dropna().unique()
    colors = {}
    PASTEL_PALETTE = [
        '#8dd3c7', '#ffffb3', '#bebada', '#fb8072', '#80b1d3', '#fdb462', 
        '#b3de69', '#fccde5', '#d9d9d9', '#bc80bd', '#ccebc5', '#ffed6f',
        '#a6cee3', '#b2df8a', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99'
    ]
    
    for i, utp in enumerate(sorted(utps_unique)):
        colors[utp] = PASTEL_PALETTE[i % len(PASTEL_PALETTE)]
    
    gdf_filtered = gdf_filtered.copy()
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
        utps_list = sorted(df_municipios['utp_id'].unique().tolist())
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
                if st.button("Rodar Agora", use_container_width=True, help="Executa o pipeline completo de consolidação (Fluxos + REGIC)"):
                    with st.spinner("Executando pipeline..."):
                        if run_consolidation():
                            st.success("Sucesso!")
                            st.rerun()
                        else:
                            st.error("Falha na execução.")
            
            with col2:
                if st.button("Limpar Cache", use_container_width=True):
                    consolidation_loader.clear()
                    st.rerun()
        else:
            st.warning("Nenhuma consolidação em cache")
            if st.button("Executar Consolidação", use_container_width=True):
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
        
        if gdf is not None:
            gdf_filtered = gdf[gdf['uf'].isin(selected_ufs)].copy()
            if selected_utps:
                gdf_filtered = gdf_filtered[gdf_filtered['utp_id'].isin(selected_utps)]
            
            st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_filtered)} Municípios")
            render_map(gdf_filtered, title="Distribuição Inicial")
        
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
                use_container_width=True,
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
            if st.button("Rodar Pipeline", use_container_width=True, key="btn_tab_run"):
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
            
            # Aplicar consolidações ao dataframe
            df_consolidated = consolidation_loader.apply_consolidations_to_dataframe(df_filtered)
            
            if gdf is not None:
                gdf_consolidated = consolidation_loader.apply_consolidations_to_dataframe(
                    gdf[gdf['uf'].isin(selected_ufs)].copy()
                )
                
                if selected_utps:
                    target_utps = set(c['target_utp'] for c in consolidations)
                    gdf_consolidated = gdf_consolidated[
                        gdf_consolidated['utp_id'].isin(target_utps | set(selected_utps))
                    ]
                
                st.subheader(f"{len(selected_ufs)} Estado(s) | {len(gdf_consolidated)} Municípios")
                render_map(gdf_consolidated, title="Distribuição Consolidada")
            
            st.markdown("---")
            st.markdown("#### Registro de Consolidações")
            
            # Preparar dados para planilha
            df_consolidations = consolidation_loader.export_as_dataframe()
            st.dataframe(df_consolidations, use_container_width=True, hide_index=True)
            
            # Download do resultado
            result_json = json.dumps(consolidation_loader.result, ensure_ascii=False, indent=2)
            st.download_button(
                label="Baixar Resultado de Consolidação",
                data=result_json,
                file_name=f"consolidation_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        
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
        
        # Inicializar analisador com cache
        @st.cache_data(show_spinner="Analisando dependências entre sedes...")
        def run_sede_analysis():
            """
            Executa análise de dependências e retorna resultados.
            
            IMPORTANTE: A análise é feita sobre a configuração territorial APÓS consolidação,
            se houver consolidações em cache. Caso contrário, usa a configuração inicial.
            """
            # Passar consolidation_loader para usar dados consolidados
            analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
            summary = analyzer.analyze_sede_dependencies()
            
            if summary.get('success'):
                df_table = analyzer.export_sede_comparison_table()
                return summary, df_table, analyzer.df_sede_analysis
            else:
                return summary, pd.DataFrame(), pd.DataFrame()
        
        # Executar análise
        try:
            summary, df_display, df_raw = run_sede_analysis()
            
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
                col_filter1, col_filter2, col_filter3 = st.columns(3)
                
                with col_filter1:
                    show_alerts_only = st.checkbox("Apenas Alertas", value=False)
                
                with col_filter2:
                    # Filtro por REGIC
                    regic_options = ['Todos'] + sorted(df_raw[df_raw['regic'] != '']['regic'].unique().tolist())
                    selected_regic = st.selectbox("Filtrar por REGIC", regic_options)
                
                with col_filter3:
                    # Filtro por aeroporto
                    filter_airport = st.selectbox("Filtrar Aeroporto", ["Todos", "Apenas com aeroporto", "Sem aeroporto"])
                
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
                
                # Renderizar tabela
                sede_comparison.render_sede_table(df_filtered_display, show_alerts_only)
                
                st.markdown("---")
                
                # === VISUALIZAÇÕES ===
                st.markdown("#### Análises Visuais")
                
                # Gráficos socioeconômicos
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
