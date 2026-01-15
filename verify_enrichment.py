import json

# Load and verify enriched data
with open('data/initialization.json', encoding='utf-8') as f:
    data = json.load(f)

# Get a sample municipality
m = data['municipios'][1]

print("=" * 60)
print("DADOS ENRIQUECIDOS - AMOSTRA")
print("=" * 60)
print(f"Município: {m['nm_mun']} ({m['uf']})")
print(f"\nDados Básicos:")
print(f"  População 2022: {m.get('populacao_2022', 'N/A'):,}")
print(f"  Área (km²): {m.get('area_km2', 'N/A')}")
print(f"  Categoria Turismo: {m.get('turismo_classificacao', 'N/A')}")

print(f"\nInfraestrutura:")
print(f"  Aeroportos 100km: {m.get('aeroportos_100km', 'N/A')}")
print(f"  Rodoviárias: {m.get('rodoviarias', 'N/A')}")

print(f"\nEconômico:")
print(f"  Renda per capita: R$ {m.get('renda_per_capita', 'N/A')}")
print(f"  Estabelecimentos formais/1000 hab: {m.get('estabelecimentos_formais_mil_hab', 'N/A')}")

print(f"\nConectividade:")
print(f"  Cobertura 4G: {m.get('cobertura_4g_pct', 'N/A')}%")
print(f"  Cobertura 5G: {m.get('cobertura_5g_pct', 'N/A')}%")

print(f"\nSaúde:")
print(f"  Médicos/100mil hab: {m.get('medicos_100mil_hab', 'N/A')}")
print(f"  Leitos hospitalares/100mil hab: {m.get('leitos_hospitalares_100mil_hab', 'N/A')}")

# Count how many municipalities have each field populated
print("\n" + "=" * 60)
print("ESTATÍSTICAS DE PREENCHIMENTO")
print("=" * 60)

fields_to_check = [
    ('populacao_2022', 'População 2022'),
    ('turismo_classificacao', 'Categoria de Turismo'),
    ('aeroportos_100km', 'Aeroportos 100km'),
    ('renda_per_capita', 'Renda per capita'),
    ('cobertura_4g_pct', 'Cobertura 4G'),
]

for field, label in fields_to_check:
    count = sum(1 for m in data['municipios'] if m.get(field) not in [None, '', 0, '0'])
    total = len(data['municipios'])
    pct = (count / total) * 100
    print(f"{label:30s}: {count:,} de {total:,} ({pct:.1f}%)")
