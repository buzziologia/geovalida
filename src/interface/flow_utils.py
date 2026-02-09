"""
Utility functions for flow analysis and visualization
"""
import pandas as pd
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def get_municipality_total_flow(municipal_data: Dict) -> int:
    """
    Calculate total flow (all modals) for a municipality.
    
    Args:
        municipal_data: Dictionary containing municipality data with 'modal_matriz'
        
    Returns:
        Total number of trips across all modals
    """
    total = 0
    modal_matriz = municipal_data.get('modal_matriz', {})
    
    if not isinstance(modal_matriz, dict):
        return 0
    
    # Sum all flows across all modals
    for modal_name, destinations in modal_matriz.items():
        if isinstance(destinations, dict):
            total += sum(destinations.values())
    
    return total


def get_top_municipalities_in_utp(
    df_municipios: pd.DataFrame,
    utp_id: str,
    top_n: int = 10
) -> pd.DataFrame:
    """
    Get top N municipalities by total flow within a UTP.
    
    Args:
        df_municipios: DataFrame with municipality data
        utp_id: UTP ID to filter
        top_n: Number of top municipalities to return
        
    Returns:
        DataFrame with top municipalities, their flows, and modal breakdown
    """
    # Filter municipalities in the UTP
    utp_municipalities = df_municipios[df_municipios['utp_id'] == utp_id].copy()
    
    if utp_municipalities.empty:
        return pd.DataFrame()
    
    # Calculate total flow for each municipality
    flow_data = []
    
    for _, mun in utp_municipalities.iterrows():
        cd_mun = mun['cd_mun']
        nm_mun = mun['nm_mun']
        modal_matriz = mun.get('modal_matriz', {})
        
        # Calculate flows by modal
        flows_by_modal = {}
        total_flow = 0
        
        if isinstance(modal_matriz, dict):
            for modal_name, destinations in modal_matriz.items():
                if isinstance(destinations, dict):
                    modal_total = sum(destinations.values())
                    flows_by_modal[modal_name] = modal_total
                    total_flow += modal_total
        
        flow_data.append({
            'cd_mun': cd_mun,
            'nm_mun': nm_mun,
            'total_flow': total_flow,
            'rodoviaria_coletiva': flows_by_modal.get('rodoviaria_coletiva', 0),
            'rodoviaria_particular': flows_by_modal.get('rodoviaria_particular', 0),
            'aeroviaria': flows_by_modal.get('aeroviaria', 0),
            'ferroviaria': flows_by_modal.get('ferroviaria', 0),
            'hidroviaria': flows_by_modal.get('hidroviaria', 0)
        })
    
    # Create DataFrame and sort by total flow
    df_flows = pd.DataFrame(flow_data)
    df_flows = df_flows.sort_values('total_flow', ascending=False).head(top_n)
    
    return df_flows


def get_top_destinations_for_municipality(
    municipal_data: Dict,
    df_municipios: pd.DataFrame,
    top_n: int = 5,
    df_impedance: pd.DataFrame = None
) -> List[Tuple[str, str, str, str, int, float, Optional[float]]]:
    """
    Get top N destination flows for a municipality (aggregated across all modals).
    
    Args:
        municipal_data: Dictionary containing municipality data with 'modal_matriz'
        df_municipios: DataFrame to lookup destination names
        top_n: Number of top destinations to return
        df_impedance: Optional DataFrame with travel time data (origem_6, destino_6, tempo_horas)
        
    Returns:
        List of tuples: (dest_cd, dest_name, dest_uf, dest_utp, dest_rm, dest_pop, dest_regic, flow_count, percentage, tempo_horas)
    """
    modal_matriz = municipal_data.get('modal_matriz', {})
    
    if not isinstance(modal_matriz, dict):
        return []
    
    # Aggregate flows by destination across all modals
    destination_flows = {}
    
    for modal_name, destinations in modal_matriz.items():
        if isinstance(destinations, dict):
            for dest_cd, flow_count in destinations.items():
                dest_cd_int = int(dest_cd)
                destination_flows[dest_cd_int] = destination_flows.get(dest_cd_int, 0) + flow_count
    
    # Calculate total for percentages
    total_flow = sum(destination_flows.values())
    
    if total_flow == 0:
        return []
    
    # Sort by flow and get top N
    sorted_destinations = sorted(
        destination_flows.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]
    
    # Get origin code for travel time lookup
    origem_cd = municipal_data.get('cd_mun')
    
    # Lookup destination names and travel times
    result = []
    for dest_cd, flow_count in sorted_destinations:
        # Find destination name - try string first, then int
        dest_row = df_municipios[df_municipios['cd_mun'] == str(dest_cd)]
        
        if dest_row.empty and str(dest_cd).isdigit():
            dest_row = df_municipios[df_municipios['cd_mun'] == int(dest_cd)]
        
        if not dest_row.empty:
            dest_name = dest_row.iloc[0]['nm_mun']
            dest_uf = dest_row.iloc[0].get('uf', '')
            dest_utp = dest_row.iloc[0].get('utp_id', '')
            dest_rm = dest_row.iloc[0].get('regiao_metropolitana', '')
            # Handle NaN/None for RM
            if pd.isna(dest_rm) or str(dest_rm).strip() == '':
                dest_rm = '-'
                
            dest_pop = dest_row.iloc[0].get('populacao_2022', 0)
            dest_regic = dest_row.iloc[0].get('regic', '-')
            if pd.isna(dest_regic) or str(dest_regic).strip() == '':
                dest_regic = '-'
        else:
            dest_name = f"Município {dest_cd}"
            dest_uf = ""
            dest_utp = "-"
            dest_rm = "-"
            dest_pop = 0
            dest_regic = "-"
        
        # Calculate travel time if impedance data is available
        tempo_horas = None
        if df_impedance is not None and origem_cd is not None:
            try:
                origem_6 = int(origem_cd) // 10
                destino_6 = int(dest_cd) // 10
                
                # Look up travel time in impedance matrix
                impedance_row = df_impedance[
                    (df_impedance['origem_6'] == origem_6) & 
                    (df_impedance['destino_6'] == destino_6)
                ]
                
                if not impedance_row.empty:
                    tempo_horas = float(impedance_row.iloc[0]['tempo_horas'])
            except (ValueError, KeyError, TypeError):
                pass
        
        # If impedance data was provided, only show destinations with travel time ≤2h
        if df_impedance is not None and tempo_horas is None:
            continue  # Skip destinations without travel time data
        
        percentage = (flow_count / total_flow) * 100
        result.append((str(dest_cd), dest_name, dest_uf, dest_utp, dest_rm, dest_pop, dest_regic, flow_count, percentage, tempo_horas))
        
        #Stop once we have top_n results
        if len(result) >= top_n:
            break
    
    return result



def format_flow_popup_html(
    nm_mun: str,
    cd_mun: str,
    utp_id: str,
    top_destinations: List[Tuple],
    regiao_metropolitana: str = "-",
    regic: str = "-",
    populacao: int = 0,
    total_viagens: int = 0,
    uf: str = ""
) -> str:
    """
    Format flow data as HTML for Folium popup.
    
    Args:
        nm_mun: Municipality name
        cd_mun: Municipality code
        utp_id: UTP ID
        top_destinations: List of destination tuples
        regiao_metropolitana: Metropolitan region name
        regic: REGIC classification
        populacao: Population count
        total_viagens: Total flow trips
        uf: State abbreviation
        
    Returns:
        HTML string for popup
    """
    
    # Format numbers
    pop_fmt = f"{int(populacao):,}".replace(",", ".") if populacao else "-"
    viagens_fmt = f"{int(total_viagens):,}".replace(",", ".") if total_viagens else "0"
    
    # Handle NaN/None strings
    if str(regiao_metropolitana).lower() in ['nan', 'none', '']:
        regiao_metropolitana = "-"
    if str(regic).lower() in ['nan', 'none', '', '0', '0.0']:
        regic = "-"
        
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; width: 500px; max-width: 90vw;">
        <div style="background-color: #f8f9fa; padding: 10px; border-bottom: 2px solid #1351B4; margin-bottom: 10px;">
            <h3 style="margin: 0; color: #1351B4; font-size: 16px;">
                {nm_mun} ({uf})
            </h3>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 8px; font-size: 11px;">
                <div>
                    <strong>Código IBGE:</strong> {cd_mun}<br>
                    <strong>UTP:</strong> {utp_id}<br>
                    <strong>RM:</strong> {regiao_metropolitana}
                </div>
                <div>
                    <strong>REGIC:</strong> {regic}<br>
                    <strong>População:</strong> {pop_fmt}<br>
                    <strong>Total Viagens:</strong> {viagens_fmt}
                </div>
            </div>
        </div>
    """
    
    if top_destinations:
        html += """
        <h4 style="margin: 10px 0 5px 0; color: #333; font-size: 13px;">
            Top 5 Destinos de Fluxo
        </h4>
        <table style="width: 100%; border-collapse: collapse; font-size: 10px; table-layout: fixed;">
            <thead>
                <tr style="background-color: #f0f0f0; border-bottom: 1px solid #ddd;">
                    <th style="text-align: left; padding: 4px; width: 5%;">#</th>
                    <th style="text-align: left; padding: 4px; width: 25%;">Município</th>
                    <th style="text-align: left; padding: 4px; width: 25%;">Dados Ter.</th>
                    <th style="text-align: left; padding: 4px; width: 20%;">Dados Soc.</th>
                    <th style="text-align: right; padding: 4px; width: 13%;">Viagens</th>
                    <th style="text-align: right; padding: 4px; width: 12%;">% / Tempo</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for i, item in enumerate(top_destinations, 1):
            # Unpack based on length to handle potential legacy calls
            if len(item) == 10:
                 dest_cd, dest_name, dest_uf, dest_utp, dest_rm, dest_pop, dest_regic, flow, pct, tempo = item
            elif len(item) == 7:
                 # Legacy 7 items
                 dest_cd, dest_name, dest_uf, dest_utp, flow, pct, tempo = item
                 dest_rm, dest_pop, dest_regic = "-", 0, "-"
            else:
                 # Fallback
                 continue
            
            # Formatting
            flow_fmt = f"{flow:,}".replace(",", ".")
            pop_fmt = f"{int(dest_pop):,}".replace(",", ".") if dest_pop else "-"
            
            if pd.isna(dest_rm) or str(dest_rm).strip() == '': dest_rm = '-'
            if pd.isna(dest_regic) or str(dest_regic).strip() == '': dest_regic = '-'
            
            # Format travel time
            if tempo is not None:
                if tempo < 1.0:
                    tempo_str = f"{int(tempo * 60)}min"
                else:
                    tempo_str = f"{tempo:.1f}h"
            else:
                tempo_str = "-"
            
            html += f"""
                <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 4px; color: #666;">{i}</td>
                    <td style="padding: 4px;">
                        <strong>{dest_name}</strong> ({dest_uf})<br>
                        <span style="color: #666;">CD: {dest_cd}</span>
                    </td>
                    <td style="padding: 4px; vertical-align: top;">
                        UTP: <strong>{dest_utp}</strong><br>
                        <div style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 110px;" title="{dest_rm}">
                            RM: {dest_rm}
                        </div>
                    </td>
                    <td style="padding: 4px; vertical-align: top;">
                        Pop: {pop_fmt}<br>
                        <div style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 90px;" title="{dest_regic}">
                            {dest_regic}
                        </div>
                    </td>
                    <td style="padding: 4px; text-align: right; font-weight: bold;">
                        {flow_fmt}
                    </td>
                    <td style="padding: 4px; text-align: right;">
                        <span style="color: #1351B4;">{pct:.1f}%</span><br>
                        <span style="color: #666;">{tempo_str}</span>
                    </td>
                </tr>
            """
        
        html += """
            </tbody>
        </table>
        """
    else:
        html += """
        <p style="margin: 15px 0; color: #888; font-style: italic;">
            Sem dados de fluxo disponíveis
        </p>
        """
    
    html += """
    </div>
    """
    
    return html


def format_utp_flow_summary_html(df_flows: pd.DataFrame) -> str:
    """
    Format UTP flow summary as HTML table.
    
    Args:
        df_flows: DataFrame with municipality flows in UTP
        
    Returns:
        HTML string for display
    """
    if df_flows.empty:
        return "<p style='color: #888;'>Nenhum dado de fluxo disponível para esta UTP.</p>"
    
    html = """
    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
        <thead>
            <tr style="background-color: #1351B4; color: white;">
                <th style="text-align: left; padding: 10px; border: 1px solid #ddd;">#</th>
                <th style="text-align: left; padding: 10px; border: 1px solid #ddd;">Município</th>
                <th style="text-align: right; padding: 10px; border: 1px solid #ddd;">Fluxo Total</th>
                <th style="text-align: right; padding: 10px; border: 1px solid #ddd;">Rod. Coletiva</th>
                <th style="text-align: right; padding: 10px; border: 1px solid #ddd;">Rod. Particular</th>
                <th style="text-align: right; padding: 10px; border: 1px solid #ddd;">Aérea</th>
            </tr>
        </thead>
        <tbody>
    """
    
    for i, row in enumerate(df_flows.itertuples(), 1):
        bg_color = "#f9f9f9" if i % 2 == 0 else "white"
        
        html += f"""
            <tr style="background-color: {bg_color};">
                <td style="padding: 8px; border: 1px solid #ddd;">{i}</td>
                <td style="padding: 8px; border: 1px solid #ddd;"><strong>{row.nm_mun}</strong></td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: right; font-weight: bold;">
                    {row.total_flow:,}
                </td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">
                    {row.rodoviaria_coletiva:,}
                </td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">
                    {row.rodoviaria_particular:,}
                </td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">
                    {row.aeroviaria:,}
                </td>
            </tr>
        """
    
    html += """
        </tbody>
    </table>
    """
    
    return html
