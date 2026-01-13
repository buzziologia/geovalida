# src/interface/dashboard.py
import streamlit as st
from src.interface.components import sidebar, metrics, map_viewer


def render_dashboard(manager):
    """Dashboard profissional do GeoValida seguindo Padrão Digital de Governo."""
    
    # Configuração da página
    st.set_page_config(
        page_title="GeoValida",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # CSS customizado para Padrão Gov.br
    st.markdown("""
    <style>
    /* Padrão Digital de Governo - Cores oficiais */
    :root {
        --primary-color: #1351B4;
        --success-color: #168821;
        --warning-color: #FFCD07;
        --error-color: #E52207;
        --info-color: #155BCB;
        --text-color: #333333;
        --bg-color: #FFFFFF;
        --surface-color: #F8F8F8;
    }
    
    /* Header customizado */
    .header-title {
        color: #1351B4;
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
    }
    
    .header-subtitle {
        color: #333333;
        font-size: 0.95rem;
        font-weight: 400;
    }
    
    /* Card estilizado */
    .gov-card {
        border-left: 4px solid #1351B4;
        padding: 1rem;
        background-color: #F8F8F8;
        border-radius: 0.25rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header com branding Gov.br
    col1, col2 = st.columns([0.8, 0.2])
    with col1:
        st.markdown('<p class="header-title">GeoValida</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="header-subtitle">Sistema de Validação e Regionalização de Unidades de Planejamento Territorial</p>',
            unsafe_allow_html=True
        )
    
    # Sidebar
    selected_step = sidebar.render_sidebar(manager)
    
    # Métricas no topo
    st.divider()
    metrics.render_top_metrics(manager)
    st.divider()
    
    # Layout principal: 2 colunas (Conteúdo + Mapa)
    col_content, col_map = st.columns([0.4, 0.6])
    
    with col_content:
        st.subheader("Processamento", divider="blue")
        
        if selected_step == "0. Carga de Dados":
            st.write("Carregue os dados para começar a análise territorial")
            if st.button("Carregar Dados", use_container_width=True, type="primary"):
                with st.spinner("Carregando dados..."):
                    if manager.step_0_initialize_data():
                        st.success("Dados carregados com sucesso!")
                        st.rerun()
                    else:
                        st.error("Erro ao carregar dados")
        
        elif selected_step == "1. Mapa Inicial":
            st.write("Visualizando a situação inicial das Unidades de Planejamento Territorial")
            st.info("Total de 5.573 municípios em 747 UTPs")
        
        elif selected_step == "2. Análise de Fluxos":
            st.write("Análise da Matriz Origem-Destino de transporte")
            if st.button("Analisar Fluxos", use_container_width=True, type="primary"):
                with st.spinner("Processando fluxos..."):
                    df_flows = manager.step_2_analyze_flows()
                    if df_flows is not None and not df_flows.empty:
                        st.dataframe(df_flows, use_container_width=True)
                    else:
                        st.info("Nenhum fluxo detectado nos dados")
        
        elif selected_step == "5. Consolidação Funcional":
            st.write("Consolidação de UTPs baseada em fluxos de transporte")
            if st.button("Executar Consolidação", use_container_width=True, type="primary"):
                with st.spinner("Processando consolidação funcional..."):
                    changes = manager.step_5_consolidate_functional()
                    st.success(f"Consolidação concluída: {changes} uniões realizadas")
        
        elif selected_step == "7. Limpeza Territorial":
            st.write("Consolidação final via REGIC + Adjacência Geográfica")
            if st.button("Executar Limpeza", use_container_width=True, type="primary"):
                with st.spinner("Processando limpeza territorial..."):
                    changes = manager.step_7_territorial_cleanup()
                    st.success(f"Limpeza concluída: {changes} consolidações realizadas")
        
        st.divider()
        st.subheader("Exportação", divider="blue")
        
        col_export1, col_export2 = st.columns(2)
        with col_export1:
            if st.button("Exportar CSV", use_container_width=True):
                st.info("Função em desenvolvimento")
        with col_export2:
            if st.button("Exportar Mapa", use_container_width=True):
                st.info("Função em desenvolvimento")
    
    with col_map:
        st.subheader("Visualização Espacial", divider="blue")
        map_viewer.render_maps(selected_step, manager)
