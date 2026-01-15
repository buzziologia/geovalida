# src/interface/components/sede_comparison.py
"""
Componentes de visualização para análise comparativa entre sedes.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional


def render_sede_table(df: pd.DataFrame, show_alerts_only: bool = False) -> None:
    """
    Renderiza tabela interativa de sedes com formatação condicional.
    
    Args:
        df: DataFrame com dados das sedes
        show_alerts_only: Se True, mostra apenas sedes com alerta
    """
    if df.empty:
        st.info("Nenhum dado disponível para visualização.")
        return
    
    df_display = df.copy()
    
    # Filtrar apenas alertas se solicitado
    if show_alerts_only:
        df_display = df_display[df_display['Alerta'] == 'SIM']
        
        if df_display.empty:
            st.success("Nenhum alerta de dependência detectado!")
            return
    
    # Ordenar por população (padrão)
    df_display = df_display.sort_values('População', ascending=False)
    
    # Exibir tabela com formatação
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            'UTP': st.column_config.TextColumn('UTP', width='small'),
            'Sede': st.column_config.TextColumn('Sede', width='medium'),
            'UF': st.column_config.TextColumn('UF', width='small'),
            'REGIC': st.column_config.TextColumn('REGIC', width='medium'),
            'População': st.column_config.NumberColumn('População', format='%d'),
            'Nº Municípios': st.column_config.NumberColumn('Nº Mun.', width='small'),
            'Viagens': st.column_config.NumberColumn('Viagens', format='%d'),
            'Aeroporto': st.column_config.TextColumn('Aeroporto', width='small'),
            'Turismo': st.column_config.TextColumn('Turismo', width='small'),
            'Principal Destino': st.column_config.TextColumn('Principal Destino', width='medium'),
            'Fluxo (%)': st.column_config.NumberColumn('Fluxo (%)', format='%.1f%%'),
            'Tempo (h)': st.column_config.NumberColumn('Tempo (h)', format='%.2f'),
            'Alerta': st.column_config.TextColumn('Alerta', width='small')
        }
    )


def render_dependency_alerts(df: pd.DataFrame) -> None:
    """
    Renderiza cards de alertas de dependência com destaque visual.
    
    Args:
        df: DataFrame com dados das sedes
    """
    df_alerts = df[df['Alerta'] == 'SIM'].copy()
    
    if df_alerts.empty:
        st.success("**Nenhuma dependência funcional detectada**")
        st.caption("Todas as sedes têm autonomia ou fluxos principais para destinos >2h de distância")
        return
    
    st.warning(f"**{len(df_alerts)} alertas de dependência detectados**")
    
    # Exibir cada alerta em um expander
    for _, row in df_alerts.iterrows():
        with st.expander(f"ALERTA: {row['Sede']} ({row['UF']}) → {row['Principal Destino']}"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Sede de Origem", row['Sede'])
                st.caption(f"UTP: {row['UTP']}")
                st.caption(f"REGIC: {row['REGIC']}")
            
            with col2:
                st.metric("Principal Destino", row['Principal Destino'])
                st.caption(f"Proporção do Fluxo: {row['Fluxo (%)']}%")
                st.caption(f"Tempo de Viagem: {row['Tempo (h)']}h")
            
            with col3:
                st.metric("População UTP", f"{int(row['População']):,}")
                st.caption(f"Municípios: {row['Nº Municípios']}")
                st.caption(f"Total Viagens: {int(row['Viagens']):,}")
            
            st.markdown("---")
            st.markdown("""
            **Recomendação:** Esta sede apresenta forte dependência funcional de outro centro urbano. 
            Considere avaliar a consolidação ou reclassificação desta UTP.
            """)


def render_socioeconomic_charts(df: pd.DataFrame) -> None:
    """
    Renderiza gráficos de comparação socioeconômica usando Plotly.
    
    Args:
        df: DataFrame com dados das sedes
    """
    if df.empty:
        return
    
    # Gráfico 1: Top 15 Sedes por População
    st.markdown("#### Top 15 Sedes por População")
    
    df_top_pop = df.nlargest(15, 'População').copy()
    
    # Adicionar cor baseada em alerta
    df_top_pop['cor'] = df_top_pop['Alerta'].map({
        'SIM': '#ff6b6b',  # Vermelho
        '': '#4CAF50'  # Verde
    })
    
    fig_pop = go.Figure()
    fig_pop.add_trace(go.Bar(
        x=df_top_pop['População'],
        y=df_top_pop['Sede'],
        orientation='h',
        marker=dict(color=df_top_pop['cor']),
        text=df_top_pop['População'].apply(lambda x: f'{x:,.0f}'),
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>População: %{x:,.0f}<extra></extra>'
    ))
    
    fig_pop.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title='População Total da UTP',
        yaxis_title='',
        height=500,
        showlegend=False,
        margin=dict(l=10, r=10, t=30, b=10)
    )
    
    st.plotly_chart(fig_pop, use_container_width=True)


def render_flow_matrix(df_raw: pd.DataFrame, top_n: int = 15) -> None:
    """
    Renderiza heatmap de fluxos principais entre sedes.
    
    Args:
        df_raw: DataFrame bruto com informações de fluxo
        top_n: Número de principais sedes a incluir
    """
    if df_raw.empty:
        return
    
    st.markdown("#### Matriz de Fluxos Principais entre Sedes")
    
    # Selecionar top N sedes por população
    df_top = df_raw.nlargest(top_n, 'População')[['Sede', 'Principal Destino', 'Fluxo (%)']].copy()
    
    # Criar matriz pivot
    # Vamos criar uma visualização simplificada mostrando os principais fluxos
    
    # Lista de sedes únicas
    sedes = df_top['Sede'].unique().tolist()
    
    # Criar matriz zerada
    matrix = pd.DataFrame(0, index=sedes, columns=sedes)
    
    # Preencher com fluxos conhecidos
    for _, row in df_top.iterrows():
        origem = row['Sede']
        destino = row['Principal Destino']
        fluxo = row['Fluxo (%)']
        
        # Só preencher se o destino também estiver no top N
        if destino in sedes:
            matrix.loc[origem, destino] = fluxo
    
    # Criar heatmap
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=matrix.values,
        x=matrix.columns,
        y=matrix.index,
        colorscale='YlOrRd',
        text=matrix.values.round(1),
        texttemplate='%{text}%',
        textfont={"size": 10},
        hovertemplate='Origem: %{y}<br>Destino: %{x}<br>Fluxo: %{z:.1f}%<extra></extra>',
        colorbar=dict(title='Fluxo (%)')
    ))
    
    fig_heatmap.update_layout(
        xaxis_title='Destino',
        yaxis_title='Origem',
        height=600,
        margin=dict(l=150, r=10, t=30, b=100),
        xaxis={'side': 'bottom'},
        yaxis={'autorange': 'reversed'}
    )
    
    fig_heatmap.update_xaxes(tickangle=45)
    
    st.plotly_chart(fig_heatmap, use_container_width=True)
    
    st.caption("**Nota:** Valores representam a % do fluxo total da sede de origem que vai para o destino indicado. Apenas os principais fluxos são mostrados.")


def render_regic_distribution(df: pd.DataFrame) -> None:
    """
    Renderiza distribuição de sedes por classificação REGIC.
    
    Args:
        df: DataFrame com dados das sedes
    """
    if df.empty or 'REGIC' not in df.columns:
        return
    
    st.markdown("#### Distribuição por Classificação REGIC")
    
    # Filtrar apenas sedes com classificação REGIC
    df_regic = df[df['REGIC'] != ''].copy()
    
    if df_regic.empty:
        st.info("Nenhuma sede com classificação REGIC disponível")
        return
    
    # Contar por classificação
    regic_counts = df_regic.groupby('REGIC').size().reset_index(name='Quantidade')
    regic_counts = regic_counts.sort_values('Quantidade', ascending=False)
    
    # Criar gráfico de barras
    fig_regic = px.bar(
        regic_counts,
        x='REGIC',
        y='Quantidade',
        text='Quantidade',
        color='Quantidade',
        color_continuous_scale='Blues'
    )
    
    fig_regic.update_traces(textposition='outside')
    fig_regic.update_layout(
        xaxis_title='Classificação REGIC',
        yaxis_title='Número de Sedes',
        showlegend=False,
        height=400,
        margin=dict(l=10, r=10, t=10, b=10)
    )
    
    fig_regic.update_xaxes(tickangle=45)
    
    st.plotly_chart(fig_regic, use_container_width=True)
