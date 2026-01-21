#!/usr/bin/env python3
"""
Simula o algoritmo de coloracao para a UTP 677 especificamente.
Verifica cada etapa do processo para entender onde esta falhando.
"""

import json
import geopandas as gpd
import pandas as pd
from pathlib import Path
import networkx as nx

DATA_DIR = Path(__file__).parent / "data"
SHAPEFILE_PATH = DATA_DIR / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"

# Carregar dados
with open(DATA_DIR / "initialization.json", 'r', encoding='utf-8') as f:
    init_data = json.load(f)

municipios = init_data['municipios']

# Filtrar apenas municipios da UTP 677
utp_677_data = [m for m in municipios if str(m['utp_id']) == "677"]

print("=" * 80)
print("SIMULACAO DO ALGORITMO DE COLORACAO PARA UTP 677")
print("=" * 80)

print(f"\nMunicipios da UTP 677 no initialization.json:")
for m in utp_677_data:
    print(f"  - {m['nm_mun']} (CD_MUN: {m['cd_mun']})")

# Carregar shapefile
print(f"\nCarregando shapefile...")
gdf = gpd.read_file(SHAPEFILE_PATH)

# Criar DataFrame de municipios
df_mun = pd.DataFrame(utp_677_data)
df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)
gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)

# Merge
gdf_677 = gdf.merge(
    df_mun[['cd_mun', 'utp_id']], 
    left_on='CD_MUN', 
    right_on='cd_mun',
    how='inner'
)

print(f"\nMunicipios encontrados no shapefile: {len(gdf_677)}")
print(f"CDs_MUN: {sorted(gdf_677['CD_MUN'].unique().tolist())}")

# Simular o algoritmo de coloracao
print(f"\n{'='*80}")
print("ETAPA 1: Limpeza e Preparacao")
print(f"{'='*80}")

gdf_clean = gdf_677.dropna(subset=['utp_id', 'geometry']).copy()
gdf_clean['UTP_ID'] = gdf_clean['utp_id'].astype(str)

print(f"Apos limpeza: {len(gdf_clean)} registros")
print(f"UTPs unicas: {gdf_clean['UTP_ID'].unique().tolist()}")

# ETAPA 2: Dissolve por UTP
print(f"\n{'='*80}")
print("ETAPA 2: Dissolve por UTP")
print(f"{'='*80}")

gdf_utps = gdf_clean[['UTP_ID', 'geometry']].dissolve(by='UTP_ID')
print(f"Apos dissolve: {len(gdf_utps)} UTPs")
print(f"UTP index: {gdf_utps.index.tolist()}")

# ETAPA 3: Construir grafo
print(f"\n{'='*80}")
print("ETAPA 3: Construir Grafo de Adjacencia")
print(f"{'='*80}")

# Projetar para metrico
gdf_projected = gdf_utps.to_crs(epsg=5880)

G = nx.Graph()
G.add_nodes_from(gdf_projected.index)

print(f"Nos do grafo: {G.nodes()}")

# Buffer e verificar adjacencia
gdf_left = gdf_projected.copy()
gdf_left['geometry'] = gdf_left.geometry.buffer(100)

gdf_right = gdf_projected.reset_index()[['UTP_ID', 'geometry']].rename(
    columns={'UTP_ID': 'ID_RIGHT'}
)

joins = gpd.sjoin(gdf_left, gdf_right, predicate='intersects', how='inner')

print(f"\nAdjacencias encontradas:")
for idx_left, row in joins.iterrows():
    idx_right = row['ID_RIGHT']
    if str(idx_left) != str(idx_right):
        print(f"  UTP {idx_left} <-> UTP {idx_right}")
        G.add_edge(str(idx_left), str(idx_right))

print(f"\nGrafo final: {G.number_of_nodes()} nos, {G.number_of_edges()} arestas")

# ETAPA 4: Coloracao
print(f"\n{'='*80}")
print("ETAPA 4: Coloracao Minima (DSATUR)")
print(f"{'='*80}")

utp_color_map = nx.coloring.greedy_color(G, strategy='DSATUR')
print(f"Cores atribuidas as UTPs:")
for utp_id, color in utp_color_map.items():
    print(f"  UTP {utp_id}: Cor {color}")

# ETAPA 5: Mapeamento final
print(f"\n{'='*80}")
print("ETAPA 5: Mapeamento CD_MUN -> Cor")
print(f"{'='*80}")

final_coloring = {}
for _, row in gdf_clean.iterrows():
    cd_mun = int(row['CD_MUN'])
    utp_id = str(row['UTP_ID'])
    final_coloring[cd_mun] = utp_color_map.get(utp_id, 0)

print(f"\nColoracao final:")
for cd_mun, color in sorted(final_coloring.items()):
    mun_name = next((m['nm_mun'] for m in utp_677_data if m['cd_mun'] == cd_mun), 'N/A')
    print(f"  CD_MUN {cd_mun} ({mun_name}): Cor {color}")

# Verificar consistencia
colors_used = set(final_coloring.values())
print(f"\n{'='*80}")
print("RESULTADO")
print(f"{'='*80}")

if len(colors_used) == 1:
    print(f"[OK] Todos os municipios da UTP 677 tem a mesma cor: {list(colors_used)[0]}")
else:
    print(f"[X] PROBLEMA: UTP 677 tem {len(colors_used)} cores diferentes: {sorted(colors_used)}")
    print(f"\nISSO NAO DEVERIA ACONTECER!")
    print(f"Todos os municipios da mesma UTP deveriam ter a mesma cor apos o dissolve.")
