#!/usr/bin/env python3
"""
Compara o cache atual com o que o algoritmo deveria produzir.
Identifica onde esta a divergencia.
"""

import json
import geopandas as gpd
import pandas as pd
from pathlib import Path
import sys

# Adicionar raiz do projeto ao path
PROJECT_ROOT = Path(__file__).parent
sys.path.append(str(PROJECT_ROOT))

from src.core.graph import TerritorialGraph

DATA_DIR = PROJECT_ROOT / "data"
SHAPEFILE_PATH = DATA_DIR / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"

print("=" * 80)
print("COMPARACAO: CACHE vs ALGORITMO CORRETO")
print("=" * 80)

# Carregar dados
with open(DATA_DIR / "initialization.json", 'r', encoding='utf-8') as f:
    init_data = json.load(f)

with open(DATA_DIR / "initial_coloring.json", 'r', encoding='utf-8') as f:
    cached_colors = json.load(f)

municipios = init_data['municipios']

print(f"\nCarregando shapefile e dados...")
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

# Renomear para formato esperado
gdf_merged = gdf_merged.rename(columns={'utp_id': 'UTP_ID'})

print(f"Total de municipios no shapefile merged: {len(gdf_merged)}")

# Calcular cores usando o algoritmo
print(f"\nCalculando coloracao usando TerritorialGraph...")
graph = TerritorialGraph()
computed_colors = graph.compute_graph_coloring(gdf_merged)

print(f"Cores calculadas: {len(computed_colors)} municipios")

# Comparar com cache
print(f"\n{'='*80}")
print("COMPARACAO COM CACHE")
print(f"{'='*80}")

# Focar na UTP 677
utp_677_muns = [m for m in municipios if str(m['utp_id']) == "677"]
cd_muns_677 = [str(m['cd_mun']) for m in utp_677_muns]

print(f"\nUTP 677 - Municipios:")
print(f"  CD_MUNs: {sorted(set(cd_muns_677))}")

print(f"\nUTP 677 - Cores no CACHE:")
for cd_mun in sorted(set(cd_muns_677)):
    color_cached = cached_colors.get(cd_mun, cached_colors.get(int(cd_mun), 'N/A'))
    print(f"  {cd_mun}: {color_cached}")

print(f"\nUTP 677 - Cores CALCULADAS agora:")
for cd_mun in sorted(set(cd_muns_677)):
    color_computed = computed_colors.get(int(cd_mun), 'N/A')
    print(f"  {cd_mun}: {color_computed}")

# Verificar consistencia
cached_colors_677 = set()
computed_colors_677 = set()

for cd_mun in set(cd_muns_677):
    cached_colors_677.add(cached_colors.get(cd_mun, cached_colors.get(int(cd_mun), None)))
    computed_colors_677.add(computed_colors.get(int(cd_mun), None))

print(f"\n{'='*80}")
print("RESULTADO")
print(f"{'='*80}")

print(f"\nCache tem {len(cached_colors_677)} cores diferentes: {sorted(cached_colors_677)}")
print(f"Algoritmo atual retorna {len(computed_colors_677)} cores: {sorted(computed_colors_677)}")

if len(cached_colors_677) > 1:
    print(f"\n[X] CACHE INVALIDO: UTP 677 tem multiplas cores no cache")
    print(f"    O cache precisa ser regerado!")

if len(computed_colors_677) == 1:
    print(f"\n[OK] ALGORITMO CORRETO: Todos tem a mesma cor")
else:
    print(f"\n[X] ALGORITMO COM PROBLEMA: Multiplas cores geradas")

# Verificar UTPs globalmente
print(f"\n{'='*80}")
print("VERIFICACAO GLOBAL")
print(f"{'='*80}")

# Agrupar por UTP e verificar consistencia
utp_cached_colors = {}
utp_computed_colors = {}

for m in municipios:
    utp_id = str(m['utp_id'])
    cd_mun = str(m['cd_mun'])
    
    if utp_id not in utp_cached_colors:
        utp_cached_colors[utp_id] = set()
        utp_computed_colors[utp_id] = set()
    
    # Cor do cache
    color_cached = cached_colors.get(cd_mun, cached_colors.get(int(m['cd_mun']), None))
    if color_cached is not None:
        utp_cached_colors[utp_id].add(color_cached)
    
    # Cor computada
    color_computed = computed_colors.get(int(m['cd_mun']), None)
    if color_computed is not None:
        utp_computed_colors[utp_id].add(color_computed)

# Contar UTPs inconsistentes
inconsistent_cached = sum(1 for colors in utp_cached_colors.values() if len(colors) > 1)
inconsistent_computed = sum(1 for colors in utp_computed_colors.values() if len(colors) > 1)

print(f"\nUTPs com cores inconsistentes:")
print(f"  Cache: {inconsistent_cached} UTPs")
print(f"  Calculado agora: {inconsistent_computed} UTPs")

if inconsistent_cached > 0:
    print(f"\n[!] ACAO NECESSARIA: O cache esta invalido e precisa ser regerado")
    print(f"    Execute: python main.py")
