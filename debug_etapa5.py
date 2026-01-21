#!/usr/bin/env python3
"""
Debug detalhado da etapa 5 do algoritmo de coloracao.
"""

import json
import geopandas as gpd
import pandas as pd
import networkx as nx
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
SHAPEFILE_PATH = DATA_DIR / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"

# Carregar dados
with open(DATA_DIR / "initialization.json", 'r', encoding='utf-8') as f:
    init_data = json.load(f)

municipios = init_data['municipios']

# Limpar dados (como no script corrigido)
df_mun = pd.DataFrame(municipios)
original_size = len(df_mun)
df_mun = df_mun[df_mun['utp_id'].notna()].copy()
df_mun = df_mun.drop_duplicates(subset=['cd_mun'], keep='first')

print(f"Limpeza: {original_size} -> {len(df_mun)} municipios")

# Carregar shapefile
gdf = gpd.read_file(SHAPEFILE_PATH)
gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)

# Merge
gdf_merged = gdf.merge(
    df_mun[['cd_mun', 'utp_id']], 
    left_on='CD_MUN', 
    right_on='cd_mun',
    how='inner'
)
gdf_merged = gdf_merged.rename(columns={'utp_id': 'UTP_ID'})

print(f"Merged: {len(gdf_merged)} registros\n")

# Simular algoritmo
gdf_clean = gdf_merged.dropna(subset=['UTP_ID', 'geometry']).copy()
gdf_clean['UTP_ID'] = gdf_clean['UTP_ID'].astype(str)

# Filtrar UTP 677
gdf_677 = gdf_clean[gdf_clean['UTP_ID'] == '677'].copy()

print("="*80)
print("UTP 677 - APOS LIMPEZA E MERGE")
print("="*80)
print(f"\nRegistros: {len(gdf_677)}")
print(f"\nCD_MUNs unicos: {gdf_677['CD_MUN'].unique().tolist()}")
print(f"\nDetalhes:")
print(gdf_677[['CD_MUN', 'NM_MUN', 'UTP_ID']].to_string())

# Verificar duplicatas
duplicates = gdf_677[gdf_677.duplicated(subset=['CD_MUN'], keep=False)]
if not duplicates.empty:
    print(f"\n[!] AINDA HA {len(duplicates)} DUPLICATAS!")
    print(duplicates[['CD_MUN', 'NM_MUN', 'UTP_ID']].to_string())
else:
    print(f"\n[OK] Sem duplicatas")

# Dissolve
gdf_utps = gdf_clean[['UTP_ID', 'geometry']].dissolve(by='UTP_ID')
gdf_677_dissolved = gdf_utps.loc[['677']] if '677' in gdf_utps.index else None

if gdf_677_dissolved is not None:
    print(f"\n{'='*80}")
    print("APOS DISSOLVE")
    print(f"{'='*80}")
    print(f"Registros: {len(gdf_677_dissolved)}")
    print(f"Geometry type: {gdf_677_dissolved.geometry.iloc[0].geom_type}")

# Simular coloracao
G = nx.Graph()
G.add_node('677')
utp_color_map = {'677': 0}  # Simulando que UTP 677 recebe cor 0

print(f"\n{'='*80}")
print("ETAPA 5: MAPEAMENTO FINAL (CODIGO ATUAL)")
print(f"{'='*80}")

final_coloring = {}
for idx, row in gdf_clean.iterrows():
    cd_mun = int(row['CD_MUN'])
    utp_id = str(row['UTP_ID'])
    
    if utp_id == '677':  # Apenas UTP 677
        old_color = final_coloring.get(cd_mun, None)
        new_color = utp_color_map.get(utp_id, 0)
        
        if old_color is not None and old_color != new_color:
            print(f"[!] SOBRESCRITA: CD_MUN {cd_mun} tinha cor {old_color}, agora {new_color}")
        
        final_coloring[cd_mun] = new_color

print(f"\nCores finais da UTP 677:")
for cd_mun, color in sorted(final_coloring.items()):
    print(f"  CD_MUN {cd_mun}: cor {color}")

# Verificar se todos tem a mesma cor
colors_677 = set(final_coloring.values())
if len(colors_677) == 1:
    print(f"\n[OK] Todos tem cor {list(colors_677)[0]}")
else:
    print(f"\n[X] PROBLEMA: {len(colors_677)} cores diferentes: {sorted(colors_677)}")
