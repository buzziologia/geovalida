#!/usr/bin/env python3
"""Verifica dados de região metropolitana no initialization.json"""
import json
from pathlib import Path

json_path = Path('data/initialization.json')

if not json_path.exists():
    print(f"ERROR: {json_path} não encontrado!")
    exit(1)

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

municipios = data.get('municipios', [])

# Contar RMs
com_rm = [m for m in municipios if m.get('regiao_metropolitana', '').strip()]
sem_rm = [m for m in municipios if not m.get('regiao_metropolitana', '').strip()]

print(f"\n{'='*80}")
print("VERIFICAÇÃO DE REGIÃO METROPOLITANA")
print(f"{'='*80}\n")

print(f"Total de municípios: {len(municipios)}")
print(f"Municípios COM RM: {len(com_rm)} ({len(com_rm)/len(municipios)*100:.1f}%)")
print(f"Municípios SEM RM: {len(sem_rm)} ({len(sem_rm)/len(municipios)*100:.1f}%)")

print(f"\n{'='*80}")
print("EXEMPLOS COM RM:")
print(f"{'='*80}\n")
for m in com_rm[:10]:
    print(f"  {m['nm_mun']:30s} (UF: {m.get('uf', 'N/A'):2s}) - RM: {m.get('regiao_metropolitana', 'N/A')}")

print(f"\n{'='*80}")
print("EXEMPLOS SEM RM:")
print(f"{'='*80}\n")
for m in sem_rm[:10]:
    print(f"  {m['nm_mun']:30s} (UF: {m.get('uf', 'N/A'):2s}) - RM: '{m.get('regiao_metropolitana', 'N/A')}'")

# Contar RMs únicas
rms_unicas = set(m.get('regiao_metropolitana', '') for m in com_rm if m.get('regiao_metropolitana', '').strip())
print(f"\n{'='*80}")
print(f"Total de RMs únicas: {len(rms_unicas)}")
print(f"{'='*80}\n")

# Listar as primeiras 20 RMs
for i, rm in enumerate(sorted(rms_unicas)[:20], 1):
    count = sum(1 for m in com_rm if m.get('regiao_metropolitana') == rm)
    print(f"{i:2d}. {rm:50s} ({count} municípios)")

print(f"\n{'='*80}")
print("ANÁLISE CONCLUÍDA")
print(f"{'='*80}\n")
