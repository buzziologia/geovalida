"""
Helper function to render map with flow information popups for border validation tab.
"""
import folium
import logging
import pandas as pd
from src.interface.flow_utils import get_top_destinations_for_municipality, format_flow_popup_html

logger = logging.getLogger(__name__)



def render_map_with_flow_popups(gdf_filtered, df_municipios, title="Mapa", 
                                  global_colors=None, gdf_rm=None, show_rm_borders=False, 
                                  show_state_borders=False, gdf_states=None,
                                  PASTEL_PALETTE=None, df_impedance=None):
    """
    Renderiza mapa folium com popups informativos de fluxo.
    
    Args:
        gdf_filtered: GeoDataFrame filtrado com municípios
        df_municipios: DataFrame completo com dados dos municípios (para lookup de fluxos)
        title: Título do mapa
        global_colors: Dict com coloração pré-calculada
        gdf_rm: GeoDataFrame com limites de RMs
        show_rm_borders: Se deve mostrar contornos de RMs
        show_state_borders: Se deve mostrar contornos estaduais
        gdf_states: GeoDataFrame opcional com contornos de Estados pré-calculados
        PASTEL_PALETTE: Lista de cores para coloração
        df_impedance: Optional DataFrame com dados de tempo de viagem (origem_6, destino_6, tempo_horas)
    """
    if gdf_filtered is None or gdf_filtered.empty:
        return None

    # ... (rest of function body remains same until end) ...


    
    gdf_filtered = gdf_filtered.copy()
    gdf_filtered = gdf_filtered.reset_index(drop=True)
    gdf_filtered['color'] = '#cccccc'
    
    # Aplicar coloração (simplificada)
    # Aplicar coloração
    coloring_applied = False
    
    if global_colors and PASTEL_PALETTE:
        try:
            for idx, row in gdf_filtered.iterrows():
                # Tenta acessar CD_MUN ou cd_mun
                cd_mun_val = row.get('CD_MUN') if 'CD_MUN' in row else row.get('cd_mun')
                if cd_mun_val is None: continue
                
                cd_mun = int(cd_mun_val)
                color_idx = global_colors.get(cd_mun, 0) % len(PASTEL_PALETTE)
                gdf_filtered.at[idx, 'color'] = PASTEL_PALETTE[color_idx]
            
            coloring_applied = True
        except Exception as e:
            logger.warning(f"Erro ao aplicar coloração global: {e}")
    
    # Fallback: Coloração simples por UTP se global falhar
    if not coloring_applied:
        try:
            utps_unique = gdf_filtered['utp_id'].dropna().unique()
            if len(utps_unique) > 0 and PASTEL_PALETTE:
                colors = {utp: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] 
                         for i, utp in enumerate(sorted(utps_unique))}
                gdf_filtered['color'] = gdf_filtered['utp_id'].map(colors).fillna('#cccccc')
        except Exception as e:
            logger.warning(f"Erro ao aplicar coloração fallback: {e}")
    
    # Criar mapa
    m = folium.Map(
        location=[-15, -55],
        zoom_start=4,
        tiles="CartoDB positron",
        prefer_canvas=True,
        control_scale=True
    )
    
    # Fit bounds
    if not gdf_filtered.empty:
        bounds = gdf_filtered.total_bounds
        m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]], padding=(0.05, 0.05))
    
    
    # Load impedance data if not provided
    if df_impedance is None:
        try:
            from pathlib import Path
            impedance_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
            if impedance_path.exists():
                df_impedance = pd.read_csv(impedance_path, sep=';', encoding='latin-1')
                df_impedance = df_impedance.dropna(axis=1, how='all')
                df_impedance = df_impedance.rename(columns={
                    'COD_IBGE_ORIGEM_1': 'origem_6',
                    'COD_IBGE_DESTINO_1': 'destino_6',
                    'Tempo': 'tempo_horas'
                })
                # Convert tempo_horas to float
                df_impedance['tempo_horas'] = (
                    df_impedance['tempo_horas'].astype(str).str.replace(',', '.').astype(float)
                )
                # Ensure 6-digit keys are int
                df_impedance['origem_6'] = pd.to_numeric(df_impedance['origem_6'], errors='coerce').fillna(0).astype(int)
                df_impedance['destino_6'] = pd.to_numeric(df_impedance['destino_6'], errors='coerce').fillna(0).astype(int)
                logger.info(f"Loaded {len(df_impedance)} impedance pairs for popup times")
            else:
                logger.warning(f"Impedance file not found: {impedance_path}")
        except Exception as e:
            logger.warning(f"Could not load impedance data for popups: {e}")
            df_impedance = None
    
    # Pre-calcular popups para performance (evitar loop de 5000+ layers)
    logging.info("Calculando popups de fluxo...")
    
    # Criar coluna popup_html
    # Usar dict lookup para performance
    df_lookup = df_municipios.set_index('cd_mun') if 'cd_mun' in df_municipios.columns else pd.DataFrame()
    
    # Pre-fetch colors to properties if not already
    # (Existing logic puts color column in gdf_filtered, which to_json handles)
    
    def get_popup_content(row):
        try:
            cd_mun_val = row.get('CD_MUN') if 'CD_MUN' in row else row.get('cd_mun')
            cd_mun = str(cd_mun_val)
            nm_mun = row.get('NM_MUN', row.get('nm_mun', 'Desconhecido'))
            utp_id = str(row.get('utp_id', ''))
            
            # Lookup data using fast index
            mun_data = {}
            found = False
            
            # Try exact match (string)
            if cd_mun in df_lookup.index:
                mun_entry = df_lookup.loc[cd_mun]
                found = True
            # Try integer match if string failed
            elif cd_mun.isdigit() and int(cd_mun) in df_lookup.index:
                mun_entry = df_lookup.loc[int(cd_mun)]
                found = True
                
            if found:
                # Handle duplicate index if any (return series or dataframe)
                if isinstance(mun_entry, pd.DataFrame):
                    mun_data = mun_entry.iloc[0].to_dict()
                else:
                    # CRITICAL FIX: When converting Series to dict, pandas may not
                    # preserve nested dicts/lists correctly. We need to ensure
                    # modal_matriz is copied as-is.
                    mun_data = mun_entry.to_dict()
                    
                    # Explicitly preserve modal_matriz if it's a nested dict
                    if 'modal_matriz' in mun_entry.index:
                        mun_data['modal_matriz'] = mun_entry['modal_matriz']
                
                # CRITICAL FIX #2: cd_mun is the INDEX, so it's not in the dict!
                # We need to add it back for origem_cd lookup in get_top_destinations
                mun_data['cd_mun'] = cd_mun
                
                # Add debugging for first few lookups
                if not hasattr(get_popup_content, '_debug_counter'):
                    get_popup_content._debug_counter = 0
                
                if get_popup_content._debug_counter < 3:  # Debug first 3 only
                    modal_type = type(mun_data.get('modal_matriz')).__name__
                    has_data = bool(mun_data.get('modal_matriz'))
                    logger.info(f"DEBUG popup {get_popup_content._debug_counter}: cd_mun={cd_mun}, modal_matriz type={modal_type}, has_data={has_data}")
                    get_popup_content._debug_counter += 1
                
                top_destinations = get_top_destinations_for_municipality(
                    mun_data, df_municipios, top_n=5, df_impedance=df_impedance
                )
            else:
                logger.warning(f"Municipality {cd_mun} not found in df_municipios index")
                top_destinations = []
            
            # Extract extra fields for header
            regiao_metropolitana = row.get('regiao_metropolitana', mun_data.get('regiao_metropolitana', '-'))
            regic = row.get('regic', mun_data.get('regic', '-'))
            populacao = row.get('populacao_2022', mun_data.get('populacao_2022', 0))
            uf = row.get('uf', mun_data.get('uf', ''))
            
            # Calculate total flow if not available
            from src.interface.flow_utils import get_municipality_total_flow
            total_viagens = get_municipality_total_flow(mun_data)
            
            return format_flow_popup_html(
                nm_mun=nm_mun, 
                cd_mun=cd_mun, 
                utp_id=utp_id, 
                top_destinations=top_destinations,
                regiao_metropolitana=regiao_metropolitana,
                regic=regic,
                populacao=populacao,
                total_viagens=total_viagens,
                uf=uf
            )
        except Exception as e:
            return f"Erro: {str(e)}"

    # Apply calculation
    gdf_filtered['popup_html'] = gdf_filtered.apply(get_popup_content, axis=1)
    
    # Separar municípios regulares e sedes (AGORA COM POPUP_HTML)
    gdf_members = gdf_filtered[~gdf_filtered['sede_utp']].copy()
    gdf_seats = gdf_filtered[gdf_filtered['sede_utp']].copy()
    
    # Adicionar camada ÚNICA de Municípios Regulares
    if not gdf_members.empty:
        # Filtrar apenas colunas necessárias para o GeoJSON (reduz tamanho)
        cols_to_keep = ['geometry', 'popup_html', 'color', 'NM_MUN', 'utp_id']
        members_json = gdf_members[cols_to_keep].to_json()
        
        folium.GeoJson(
            members_json,
            name="Municípios",
            style_function=lambda feature: {
                'fillColor': feature['properties'].get('color', '#cccccc'),
                'color': '#ffffff',
                'weight': 0.3,
                'fillOpacity': 0.9
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['NM_MUN', 'utp_id'],
                aliases=['Município:', 'UTP:'],
                localize=True,
                sticky=False
            ),
            popup=folium.GeoJsonPopup(
                fields=['popup_html'],
                labels=False,  # Não mostrar label do campo
                localize=False, # Não alterar o HTML
                min_width=520,
                max_width=600
            )
        ).add_to(m)
    
    # Adicionar camada ÚNICA de Sedes (com estilo diferente)
    if not gdf_seats.empty:
        cols_to_keep = ['geometry', 'popup_html', 'color', 'NM_MUN', 'utp_id']
        seats_json = gdf_seats[cols_to_keep].to_json()
        
        folium.GeoJson(
            seats_json,
            name="Sedes",
            style_function=lambda feature: {
                'fillColor': feature['properties'].get('color', '#cccccc'),
                'color': '#000000',
                'weight': 3.0,
                'fillOpacity': 1.0
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['NM_MUN', 'utp_id'],
                aliases=['🏛️ SEDE:', 'UTP:'],
                localize=True,
                sticky=False
            ),
            popup=folium.GeoJsonPopup(
                fields=['popup_html'],
                labels=False,
                localize=False,
                min_width=520,
                max_width=600
            )
        ).add_to(m)
    
    # Adicionar camada de contornos de Regiões Metropolitanas (opcional)
    if show_rm_borders and gdf_rm is not None and not gdf_rm.empty:
        try:
            rms_visible = gdf_filtered['regiao_metropolitana'].unique()
            gdf_rm_filtered = gdf_rm[gdf_rm['regiao_metropolitana'].isin(rms_visible)].copy()
            
            if not gdf_rm_filtered.empty:
                folium.map.CustomPane("rm_borders", z_index=450).add_to(m)
                
                # Para RMs, são poucos objetos, loop é aceitável e permite tooltip customizado fácil
                # Mas melhor fazer single layer se possível, porém tooltip varia.
                # Como são < 50 RMs, loop é OK.
                for idx, row in gdf_rm_filtered.iterrows():
                    nome_rm = row['regiao_metropolitana']
                    uf = row['uf']
                    num_municipios = row['count']
                    
                    tooltip_rm = f"RM: {nome_rm} ({uf}) - {num_municipios} municípios"
                    
                    folium.GeoJson(
                        row.geometry,
                        style_function=lambda x: {
                            'fillColor': 'none',
                            'color': '#FF0000',
                            'weight': 3,
                            'fillOpacity': 0,
                            'dashArray': '4, 4'
                        },
                        tooltip=tooltip_rm,
                        name=f"RM: {nome_rm}",
                        pane="rm_borders"
                    ).add_to(m)
            
        except Exception as e:
            logger.error(f"Erro ao renderizar RMs: {e}")

    # Adicionar camada de contornos de Estados (se solicitado)
    if show_state_borders:
        try:
            # Preferir GDF de estados fornecido (mais completo) ou calcular on-the-fly (apenas visíveis)
            if gdf_states is not None and not gdf_states.empty:
                gdf_states_to_render = gdf_states
            elif not gdf_filtered.empty and 'uf' in gdf_filtered.columns:
                # Fallback: Dissolver por UF para obter contornos dos municípios visíveis
                gdf_states_to_render = gdf_filtered[['uf', 'geometry']].dissolve(by='uf').reset_index()
            else:
                gdf_states_to_render = None
            
            if gdf_states_to_render is not None and not gdf_states_to_render.empty:
                folium.map.CustomPane("state_borders", z_index=460).add_to(m)
                
                folium.GeoJson(
                    gdf_states_to_render.to_json(),
                    name="Limites Estaduais",
                    style_function=lambda x: {
                        'fillColor': 'none',
                        'color': '#0000FF', # Azul (destaque solicitado)
                        'weight': 3,        # Aumentado para melhor visualização
                        'fillOpacity': 0
                    },
                    tooltip=folium.GeoJsonTooltip(
                        fields=['uf'],
                        aliases=['Estado:'],
                        localize=True
                    ),
                    pane="state_borders"
                ).add_to(m)
            
        except Exception as e:
            logger.error(f"Erro ao renderizar contornos estaduais: {e}")
            
    return m
