# src/interface/components/map_viewer.py
import streamlit as st
import folium
import geopandas as gpd
import logging
from typing import Dict, Any

# Paleta de Alto Contraste (Cores bem distintas para evitar confusão)
DISTINCT_COLORS = [
    "#E6194B", "#3CB44B", "#FFE119", "#4363D8", "#F58231", 
    "#911EB4", "#42E4FF", "#F032E6", "#BFEF45", "#FABEBE",
    "#008080", "#E6BEFF", "#9A6324", "#FFFAC8", "#800000"
]

def create_interactive_map(gdf: gpd.GeoDataFrame, 
                           coloring: Dict[int, int],
                           seats: Dict[Any, int]) -> folium.Map:
    """
    Cria um mapa interativo com cores sólidas e alto contraste.
    Resolve a diferença de tons usando fillOpacity: 1.0.
    """
    m = folium.Map(location=[-15.78, -47.93], zoom_start=4, tiles="CartoDB positron")
    seat_ids = set(seats.values())
    
    # Simplificação leve para performance
    gdf_simplified = gdf.copy()
    gdf_simplified['geometry'] = gdf_simplified.geometry.simplify(tolerance=0.003, preserve_topology=True)
    
    for _, row in gdf_simplified.iterrows():
        try:
            cd_mun = int(row.get('CD_MUN'))
            nm_mun = row.get('NM_MUN', 'N/A')
            utp_id = row.get('UTP_ID', 'N/A')
            
            # Busca cor sólida do dicionário
            color_idx = coloring.get(cd_mun, 0) % len(DISTINCT_COLORS)
            fill_color = DISTINCT_COLORS[color_idx]
            is_seed = cd_mun in seat_ids

            # Popups e Tooltips
            popup_text = f"<b>{nm_mun}</b><br>UTP: {utp_id}<br>Status: {'Sede' if is_seed else 'Membro'}"
            
            # Desenho com Opacidade 1.0 (Resolve a diferença de tons)
            if row.geometry and row.geometry.geom_type in ['Polygon', 'MultiPolygon']:
                folium.GeoJson(
                    row.geometry,
                    style_function=lambda x, fc=fill_color, seed=is_seed: {
                        'fillColor': fc,
                        'color': 'black' if seed else '#999999',
                        'weight': 2.0 if seed else 1.0,
                        'fillOpacity': 1.0 # COR SÓLIDA: Resolve o tom diferente
                    },
                    tooltip=f"{nm_mun} (CD: {cd_mun})",
                    popup=folium.Popup(popup_text, max_width=300)
                ).add_to(m)
        except Exception as e:
            logging.error(f"Erro ao processar município no mapa: {e}")
            continue
    
    # Adicionar Legenda
    legend_html = f"""
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 220px; height: auto;
                background-color: white; border: 2px solid grey; z-index: 9999; font-size: 14px;
                padding: 10px; opacity: 0.9; border-radius: 5px;">
        <p><b>Legenda</b></p>
        <p><i class="fa fa-square" style="color:black"></i> Sede de UTP (Borda Preta)</p>
        <p><i class="fa fa-square" style="color:#666666"></i> Município Membro</p>
        <p><i>Cores sólidas representam diferentes UTPs.</i></p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m

def render_maps(selected_step: str, manager=None):
    """Renderiza os mapas no Streamlit."""
    if manager is None or manager.gdf is None:
        st.warning("Dados não carregados.")
        return
    try:
        with st.spinner("Gerando visualização geográfica..."):
            manager.map_generator.sync_with_graph(manager.graph)
            gdf_map = manager.map_generator.gdf_complete.copy()
            if gdf_map.crs != "EPSG:4326":
                gdf_map = gdf_map.to_crs(epsg=4326)
            coloring = manager.graph.compute_graph_coloring(gdf_map)
            seats = manager.graph.utp_seeds
            
            m = create_interactive_map(gdf_map, coloring, seats)
            
            from streamlit.components.v1 import html
            html(m._repr_html_(), height=700)
            
            st.caption(f"Mapa: {selected_step} | Cores distintas: {len(set(coloring.values()))}")
    except Exception as e:
        st.error(f"Erro no mapa: {e}")
        logging.error(f"Erro na renderização do mapa: {e}", exc_info=True)

def render_maps_filtered(selected_step: str, manager, gdf_filtered: gpd.GeoDataFrame, 
                         coloring: Dict[int, int], seats: Dict[Any, int]):
    """Renderiza o mapa filtrado no Streamlit."""
    if gdf_filtered is None or gdf_filtered.empty:
        st.warning("Nenhum dado para visualizar com os filtros atuais.")
        return

    try:
        if gdf_filtered.crs != "EPSG:4326":
            gdf_filtered = gdf_filtered.to_crs(epsg=4326)
            
        m = create_interactive_map(gdf_filtered, coloring, seats)

        from streamlit.components.v1 import html
        html(m._repr_html_(), height=700)
    except Exception as e:
        st.error(f"Erro no mapa filtrado: {e}")