# src/interface/components/map_viewer.py
import streamlit as st
import leafmap.foliumap as leafmap
import geopandas as gpd
import logging
from typing import Dict, Any

# Paleta de cores oficial Padrão Digital de Governo (Gov.br)
GOVBR_COLORS = [
    "#1351B4", "#168821", "#E52207", "#FFCD07", "#155BCB", 
    "#00A871", "#0076D6", "#0C326F", "#5AB9B3", "#8289FF", 
    "#AD79E9", "#BE32D0"
]

def create_interactive_map(gdf: gpd.GeoDataFrame, 
                           coloring: Dict[int, int],
                           seats: Dict[Any, int]) -> leafmap.Map:
    """
    Cria um mapa interativo utilizando leafmap para melhor performance.
    """
    
    # 1. Inicializar o mapa (centralizado no Brasil)
    # O leafmap facilita a configuração inicial e o controlo de camadas
    m = leafmap.Map(
        center=[-15.78, -47.93], 
        zoom=4, 
        draw_control=False,
        measure_control=False,
        fullscreen_control=True
    )
    
    # Adiciona uma camada de fundo leve (CartoDB Positron)
    m.add_basemap("CartoDB.Positron")

    # Conjunto de IDs de municípios que são sedes
    seat_ids = set(seats.values())

    # 2. Definição da lógica de estilo (Cores e Bordas)
    def style_fn(feature):
        cd_mun = feature['properties'].get('CD_MUN')
        # Obtém a cor baseada na coloração de grafo
        color_idx = coloring.get(cd_mun, 0) % len(GOVBR_COLORS)
        color = GOVBR_COLORS[color_idx]
        
        is_seed = cd_mun in seat_ids
        
        return {
            'fillColor': color,
            'color': '#1351B4' if is_seed else '#333333', # Destaque azul para sedes
            'weight': 3 if is_seed else 0.5,
            'fillOpacity': 0.7
        }

    # 3. Adicionar os dados GeoJSON com o leafmap
    # O leafmap lida com a conversão para GeoJSON de forma otimizada
    m.add_gdf(
        gdf,
        layer_name="Unidades de Planeamento",
        style_function=style_fn,
        highlight_function=lambda x: {'fillOpacity': 0.9, 'weight': 4, 'color': '#FFFFFF'},
        info_mode=None, # Desativamos o popup padrão para usar o tooltip
        tooltip=leafmap.folium.GeoJsonTooltip(
            fields=['NM_MUN', 'CD_MUN', 'UTP_ID'], # COLUNA CORRETA: UTP_ID
            aliases=['Município:', 'Código IBGE:', 'ID UTP:'],
            localize=True,
            sticky=False,
            labels=True,
            style="""
                background-color: #FFFFFF;
                border: 2px solid #1351B4;
                border-radius: 5px;
                box-shadow: 3px 3px rgba(0,0,0,0.2);
                font-family: sans-serif;
            """
        )
    )

    # 4. Adicionar Legenda de forma simples com leafmap
    # Podemos criar um dicionário de categorias para a legenda
    legend_dict = {
        'Sede de UTP (Destaque Azul)': '#1351B4',
        'Município Membro': '#CCCCCC'
    }
    m.add_legend(title="Legenda Territorial", legend_dict=legend_dict)

    return m

def render_maps(selected_step: str, manager=None):
    """Renderiza os mapas no Streamlit utilizando a integração nativa do leafmap."""
    if manager is None or manager.gdf is None or manager.gdf.empty:
        st.warning("Dados não carregados para visualização.", icon="⚠️")
        return

    try:
        with st.spinner("A processar mapa otimizado com leafmap..."):
            # 1. Sincronizar dados do Grafo com o GeoDataFrame
            manager.map_generator.sync_with_graph(manager.graph)
            
            # 2. Garantir projeção WGS84
            gdf_map = manager.map_generator.gdf_complete.copy()
            if gdf_map.crs != "EPSG:4326":
                gdf_map = gdf_map.to_crs(epsg=4326)

            # 3. Calcular coloração de grafo
            coloring = manager.graph.compute_graph_coloring(gdf_map)
            seats = manager.graph.utp_seeds

            # 4. Criar o mapa leafmap
            m = create_interactive_map(gdf_map, coloring, seats)

            # 5. Renderizar no Streamlit
            # O leafmap possui um método nativo para isto
            m.to_streamlit(height=650)
            
            st.caption(f"Visualização Otimizada: {selected_step} | Total: {len(gdf_map)} municípios")

    except Exception as e:
        st.error(f"Erro na renderização leafmap: {str(e)}")
        logging.error(f"Erro no map_viewer: {e}", exc_info=True)