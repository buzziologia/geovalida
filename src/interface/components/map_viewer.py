# src/interface/components/map_viewer.py
import streamlit as st
import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import logging
from typing import Optional, Dict
import io


# Paleta de cores Gov.br para coloração de grafo
GOVBR_COLORS = [
    "#1351B4",  # Azul Primário
    "#168821",  # Verde Sucesso
    "#E52207",  # Vermelho Erro
    "#FFCD07",  # Amarelo Alerta
    "#155BCB",  # Azul Info
    "#00A871",  # Verde Vivid
    "#0076D6",  # Azul Vivid
    "#0C326F",  # Azul Dark
    "#5AB9B3",  # Cyan
    "#8289FF",  # Indigo
    "#AD79E9",  # Violet
    "#BE32D0",  # Magenta
]


@st.cache_data
def get_color_for_municipality(cd_mun: int, coloring: Dict[int, int]) -> str:
    """Retorna cor hex para um município baseado na coloração de grafo."""
    if cd_mun not in coloring:
        return "#CCCCCC"  # Cinza padrão
    
    color_idx = coloring[cd_mun] % len(GOVBR_COLORS)
    return GOVBR_COLORS[color_idx]


def create_interactive_map(gdf: gpd.GeoDataFrame, 
                           coloring: Dict[int, int],
                           seats: Dict[int, int],  # {utp_id -> cd_mun_seed}
                           title: str = "Mapa de UTPs do Brasil") -> folium.Map:
    """
    Cria mapa interativo com folium contendo:
    - Municipios coloridos por coloração de grafo
    - Sedes com contorno em destaque
    - Tooltips com dados ao passar mouse
    """
    
    # 1. Calcular centro do mapa (centróide do Brasil)
    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()
    
    # 2. Criar mapa base
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=4,
        tiles="OpenStreetMap",
        prefer_canvas=True
    )
    
    # 3. Adicionar municípios como camada
    for idx, row in gdf.iterrows():
        cd_mun = row['CD_MUN']
        nm_mun = row.get('NM_MUN', f"Município {cd_mun}")
        utp_id = row.get('UTP_ID', 'SEM_UTP')
        
        # Obter cor baseada em coloração de grafo
        color = get_color_for_municipality(cd_mun, coloring)
        
        # Verificar se é sede de UTP
        is_seed = cd_mun in seats.values()
        weight = 3 if is_seed else 1
        dash_array = [5, 5] if is_seed else []
        
        # Criar popup/tooltip com informações
        popup_html = f"""
        <div style="font-family: Arial; font-size: 12px; width: 200px;">
            <b>{nm_mun}</b><br>
            Código: {cd_mun}<br>
            UTP: {utp_id}<br>
            {'<b style="color: #1351B4;">★ SEDE de UTP</b><br>' if is_seed else ''}
            Cor: {color}
        </div>
        """
        
        popup = folium.Popup(popup_html, max_width=250)
        
        # Adicionar feature ao mapa
        try:
            if row['geometry'].geom_type == 'Polygon':
                folium.Polygon(
                    locations=[(lat, lon) for lon, lat in row['geometry'].exterior.coords],
                    popup=popup,
                    tooltip=nm_mun,
                    color=color,
                    weight=weight,
                    opacity=0.7,
                    fill_opacity=0.6,
                    fill_color=color
                ).add_to(m)
            elif row['geometry'].geom_type == 'MultiPolygon':
                for poly in row['geometry'].geoms:
                    folium.Polygon(
                        locations=[(lat, lon) for lon, lat in poly.exterior.coords],
                        popup=popup,
                        tooltip=nm_mun,
                        color=color,
                        weight=weight,
                        opacity=0.7,
                        fill_opacity=0.6,
                        fill_color=color
                    ).add_to(m)
        except Exception as e:
            logging.warning(f"Erro ao adicionar geometria para {nm_mun}: {e}")
            continue
    
    # 4. Adicionar legenda
    legend_html = '''
    <div style="position: fixed; 
            bottom: 50px; right: 50px; width: 250px; height: auto; 
            background-color: white; border: 2px solid #1351B4; z-index: 9999;
            border-radius: 5px; padding: 10px; font-size: 12px; font-family: Arial;">
        <p style="margin: 0; font-weight: bold; color: #1351B4;">Legenda</p>
        <hr style="margin: 5px 0;">
        <p style="margin: 3px 0;">
            <span style="display: inline-block; width: 15px; height: 15px; 
                  background-color: #1351B4; border: 2px solid #1351B4;"></span>
            Municípios (Coloração Grafo)
        </p>
        <p style="margin: 3px 0;">
            <span style="display: inline-block; width: 15px; height: 15px; 
                  border: 3px solid #1351B4; background-color: transparent;"></span>
            Sede de UTP
        </p>
        <p style="margin: 3px 0; font-size: 11px; color: #666;">
            Cores: Nenhuma UTP vizinha <br>compartilha cor
        </p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


def render_maps(selected_step: str, manager=None):
    """Renderiza mapas conforme a etapa selecionada."""
    
    if manager is None:
        st.warning("Manager não inicializado", icon="⚠")
        return
    
    if manager.gdf is None or manager.gdf.empty:
        st.warning("Carregue os dados para visualizar o mapa", icon="⚠")
        return
    
    # Computar coloração de grafo
    if manager.graph is None:
        st.error("Grafo não inicializado")
        return
    
    try:
        st.info("Computando coloração de grafo...")
        coloring = manager.graph.compute_graph_coloring(manager.gdf)
        st.success(f"Coloração concluída: {len(coloring)} municípios com cores")
    except Exception as e:
        st.error(f"Erro ao computar coloração: {e}")
        logging.error(f"Erro na coloração: {e}", exc_info=True)
        return
    
    # Mapa de sedes (utp_id -> cd_mun)
    seats = manager.graph.utp_seeds if hasattr(manager.graph, 'utp_seeds') else {}
    
    try:
        st.info("Gerando mapa interativo...")
        # Criar mapa interativo
        m = create_interactive_map(
            gdf=manager.gdf,
            coloring=coloring,
            seats=seats,
            title=f"Mapa - {selected_step}"
        )
        
        st.info("Renderizando mapa no navegador...")
        # Renderizar mapa com streamlit components
        map_html = m._repr_html_()
        st.components.v1.html(map_html, width=725, height=600)
        
        st.success("Mapa renderizado com sucesso!")
        
    except Exception as e:
        st.error(f"Erro ao gerar mapa: {e}")
        logging.error(f"Erro ao renderizar mapa: {e}", exc_info=True)


def render_map(gdf: gpd.GeoDataFrame, title: str = "Mapa UTPs") -> None:
    """Renderiza um mapa estático em Streamlit."""
    if gdf is None or gdf.empty:
        st.warning("Nenhum dado geográfico para visualizar")
        return
    
    fig, ax = plt.subplots(figsize=(12, 10))
    try:
        gdf.plot(ax=ax, column='UTP_ID', cmap='tab20', legend=True, alpha=0.7)
        ax.set_title(title, fontsize=16, fontweight='bold')
        st.pyplot(fig)
    except Exception as e:
        st.error(f"Erro ao renderizar mapa: {e}")
    finally:
        plt.close(fig)


