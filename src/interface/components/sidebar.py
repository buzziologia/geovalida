# src/interface/components/sidebar.py
import streamlit as st

def render_sidebar(manager):
    """Sidebar profissional seguindo Padrão Digital de Governo."""
    
    st.sidebar.markdown("## Painel de Controle")
    st.sidebar.divider()
    
    # Menu de seleção
    step = st.sidebar.radio(
        "Selecione a Etapa de Processamento:",
        [
            "0. Carga de Dados",
            "1. Mapa Inicial",
            "2. Análise de Fluxos",
            "5. Consolidação Funcional",
            "7. Limpeza Territorial"
        ],
        key="sidebar_step",
        help="Escolha uma etapa para visualizar e processar"
    )
    
    st.sidebar.divider()
    
    # Informações do sistema - Padrão Gov.br
    st.sidebar.markdown("### Status do Sistema", help="Informações atualizadas do carregamento de dados")
    if manager.graph and manager.graph.hierarchy:
        mun_count = sum(1 for n, d in manager.graph.hierarchy.nodes(data=True) 
                        if d.get('type') == 'municipality')
        utp_count = sum(1 for n, d in manager.graph.hierarchy.nodes(data=True) 
                        if d.get('type') == 'utp')
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Municípios", f"{mun_count:,}".replace(',', '.'), help="Total de municípios brasileiros")
        with col2:
            st.metric("UTPs", f"{utp_count:,}".replace(',', '.'), help="Total de Unidades de Planejamento")
        
        st.sidebar.success("Sistema pronto para uso")
    else:
        st.sidebar.warning("Carregue os dados para começar")
    
    st.sidebar.divider()
    
    # Botões de controle - Padrão Gov.br
    st.sidebar.markdown("### Ações")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Recarregar", use_container_width=True, key="btn_reload"):
            st.rerun()
    with col2:
        if st.button("Limpar Cache", use_container_width=True, key="btn_clear"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()
    
    st.sidebar.divider()
    
    # Rodapé com informações - Padrão Gov.br
    st.sidebar.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.85rem; margin-top: 1rem;'>
        <p><strong>GeoValida</strong> v1.0</p>
        <p>Laboratório de Transportes • UFSC</p>
        <p>Padrão Digital de Governo</p>
    </div>
    """, unsafe_allow_html=True)
        
    return step

