#!/usr/bin/env python3
"""
Investiga o DataFrame ANTES do dissolve para identificar o problema.
"""

import json
import geopandas as gpd
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
SHAPEFILE_PATH = DATA_DIR / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"

# Carregar dados
with open(DATA_DIR / "initialization.json", 'r', encoding='utf-8') as f:
    init_data = json.load(f)

municipios = init_data['municipios']

# Carregar shapefile
gdf = gpd.read_file(SHAPEFILE_PATH)
df_mun = pd.DataFrame(municipios)

# Converter tipos
gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)

# Merge
gdf_merged = gdf.merge(
    df_mun[['cd_mun', 'utp_id']], 
    left_on='CD_MUN', 
    right_on='cd_mun',
    how='inner'
)

# Renomear
gdf_merged = gdf_merged.rename(columns={'utp_id': 'UTP_ID'})

print("=" * 80)
print("INVESTIGACAO DO DATAFRAME PRE-DISSOLVE")
print("=" * 80)

# Filtrar UTP 677
gdf_clean = gdf_merged.dropna(subset=['UTP_ID', 'geometry']).copy()
gdf_clean['UTP_ID'] = gdf_clean['UTP_ID'].astype(str)

gdf_677 = gdf_clean[gdf_clean['UTP_ID'] == '677'].copy()

print(f"\nTotal de registros da UTP 677 no gdf_clean: {len(gdf_677)}")
print(f"\nColunas: {gdf_677.columns.tolist()}\n")

print("Detalhes dos registros:")
print(gdf_677[['CD_MUN', 'NM_MUN', 'UTP_ID', 'cd_mun']].to_string())

print(f"\n{'='*80}")
print("ANALISE")
print(f"{'='*80}")

# Verificar duplicatas
duplicates = gdf_677[gdf_677.duplicated(subset=['CD_MUN'], keep=False)]

if not duplicates.empty:
    print(f"\n[!] ENCONTRADAS {len(duplicates)} LINHAS DUPLICADAS!")
    print("\nLinhas duplicadas:")
    print(duplicates[['CD_MUN', 'NM_MUN', 'UTP_ID']].to_string())
    
    print(f"\n[!] PROBLEMA ENCONTRADO:")
    print(f"    O shapefile tem municipios duplicados!")
    print(f"    Quando fazemos dissolve, ele tenta unir geometrias duplicadas")
    print(f"    mas as geometrias podem nao estar exatamente na mesma posicao,")
    print(f"    criando multiplos poligonos para a mesma UTP!")
else:
    print(f"\n[OK] Nenhuma duplicata encontrada")

# Verificar apos dissolve
print(f"\n{'='*80}")
print("TESTANDO DISSOLVE")
print(f"{'='*80}")

gdf_utps = gdf_677[['UTP_ID', 'geometry']].dissolve(by='UTP_ID')

print(f"\nRegistros apos dissolve: {len(gdf_utps)}")
print(f"Geometries type: {gdf_utps.geometry.geom_type.tolist()}")

# Verificar se a geometria e MultiPolygon (multiplos poligonos)
if len(gdf_utps) > 0:
    geom = gdf_utps.iloc[0].geometry
    print(f"\nGeometria da UTP 677:")
    print(f"  Tipo: {geom.geom_type}")
    if geom.geom_type == 'MultiPolygon':
        print(f"  Numero de poligonos: {len(geom.geoms)}")
        print(f"\n  [!] UTP 677 tem {len(geom.geoms)} poligonos separados!")
        print(f"      Isso pode fazer com que cada poligono seja tratado seperadamente")
        print(f"      pelo algoritmo de adjacencia, resultando em cores diferentes!")
