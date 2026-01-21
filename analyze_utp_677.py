#!/usr/bin/env python3
"""
Analise detalhada da UTP 677 e suas cores.
"""

import json
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"

# Carregar dados
with open(DATA_DIR / "initialization.json", 'r', encoding='utf-8') as f:
    init_data = json.load(f)

with open(DATA_DIR / "initial_coloring.json", 'r', encoding='utf-8') as f:
    initial_colors = json.load(f)

with open(DATA_DIR / "consolidated_coloring.json", 'r', encoding='utf-8') as f:
    consolidated_colors = json.load(f)

municipios = init_data['municipios']

# Analisar UTP 677
print("=" * 80)
print("ANALISE DETALHADA DA UTP 677")
print("=" * 80)

utp_677_muns = [m for m in municipios if str(m['utp_id']) == "677"]

print(f"\nTotal de municipios: {len(utp_677_muns)}\n")

print("Municipio | CD_MUN | Cor Inicial | Cor Consolidada")
print("-" * 70)

for mun in utp_677_muns:
    cd_mun = str(mun['cd_mun'])
    nm_mun = mun['nm_mun']
    color_init = initial_colors.get(cd_mun, initial_colors.get(int(cd_mun), 'N/A'))
    color_cons = consolidated_colors.get(cd_mun, consolidated_colors.get(int(cd_mun), 'N/A'))
    
    match = "OK" if color_init == color_cons else "MUDOU"
    print(f"{nm_mun:30} | {cd_mun} | {color_init:14} | {color_cons:15} [{match}]")

# Verificar consistencia
colors_init = set()
colors_cons = set()

for mun in utp_677_muns:
    cd_mun = str(mun['cd_mun'])
    colors_init.add(initial_colors.get(cd_mun, initial_colors.get(int(cd_mun), None)))
    colors_cons.add(consolidated_colors.get(cd_mun, consolidated_colors.get(int(cd_mun), None)))

print("\n" + "=" * 80)
print("RESUMO")
print("=" * 80)
print(f"Cores diferentes no mapa INICIAL: {len(colors_init)} - {sorted(colors_init)}")
print(f"Cores diferentes no mapa CONSOLIDADO: {len(colors_cons)} - {sorted(colors_cons)}")

if len(colors_init) > 1:
    print(f"\n[X] PROBLEMA: UTP 677 tem {len(colors_init)} cores diferentes no mapa inicial!")
    print("   Todos os municipios da mesma UTP deveriam ter a mesma cor.")

if len(colors_cons) > 1:
    print(f"\n[X] PROBLEMA: UTP 677 tem {len(colors_cons)} cores diferentes no mapa consolidado!")
    print("   Todos os municipios da mesma UTP deveriam ter a mesma cor.")
