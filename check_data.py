import json

with open('data/initialization.json', encoding='utf-8') as f:
    data = json.load(f)

muns = data['municipios']
print(f'Total municipalities: {len(muns)}')

with_airport = [m for m in muns if m.get('aeroporto')]
with_turismo = [m for m in muns if m.get('turismo_classificacao')]

print(f'\nWith airport: {len(with_airport)}')
print(f'With tourism classification: {len(with_turismo)}')

if with_airport:
    print(f'\nSample with airport:')
    for i, m in enumerate(with_airport[:3]):
        print(f"  {m['nm_mun']} ({m['uf']}) - {m['aeroporto']}")

if with_turismo:
    print(f'\nSample with tourism:')
    for i, m in enumerate(with_turismo[:3]):
        print(f"  {m['nm_mun']} ({m['uf']}) - {m['turismo_classificacao']}")

# Check what the population field looks like
print(f'\nPopulation data check:')
for m in muns[:3]:
    print(f"  {m['nm_mun']}: {m.get('populacao_2022', 'N/A')}")
