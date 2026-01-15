import pandas as pd

# Read the Base_Categorização file
df = pd.read_csv(
    'data/01_raw/Base_Categorização(Base Organizada Normalizada).csv',
    sep=';',
    encoding='utf-8',
    skiprows=2
)

print(f"Total columns: {len(df.columns)}\n")
print("=" * 80)
print("ALL AVAILABLE COLUMNS")
print("=" * 80)

# Organize columns by category based on prefix
categories = {
    'md_': 'Metadados/Informações Básicas',
    'go_': 'Governança',
    'rc_': 'Recursos Culturais e Naturais',
    'st_': 'Serviços Turísticos',
    'in_': 'Infraestrutura de Transporte',
    'ee_': 'Estrutura Econômica',
    'et_': 'Especialização Turística',
    'ci_': 'Conectividade à Internet',
    'se_': 'Segurança',
    'sa_': 'Saúde',
    'de_': 'Demanda',
    'Categoria': 'Categoria de Turismo'
}

for prefix, category_name in categories.items():
    cols = [col for col in df.columns if col.startswith(prefix)]
    if cols:
        print(f"\n{category_name} ({len(cols)} colunas):")
        for col in cols:
            # Get some sample non-null values
            sample_vals = df[col].dropna().head(3).tolist()
            if sample_vals:
                print(f"  • {col}: {sample_vals[0]}")
            else:
                print(f"  • {col}: (sem dados)")

# Show some key statistics
print("\n" + "=" * 80)
print("DADOS MAIS RELEVANTES PARA ANÁLISE DE SEDES")
print("=" * 80)

relevant_cols = {
    'md_populacao_2022': 'População 2022',
    'Categoria': 'Categoria de Turismo',
    'md_area_km2': 'Área (km²)',
    'rc_area_conserv': 'Área de Conservação Ambiental (%)',
    'st_dens_uni_habit': 'Densidade de Unidades Habitacionais Hoteleiras',
    'st_dens_leitos_hospedagem': 'Densidade de Leitos de Hospedagem',
    'st_dens_estab_hospedagem': 'Densidade de Estabelecimentos de Hospedagem',
    'in_aeroportos_100km': 'Quantidade de Aeroportos em 100km',
    'in_aeroportos_inter_100km': 'Aeroportos Internacionais em 100km',
    'in_rodoviarias': 'Quantidade de Rodoviárias',
    'ee_estab_formais': 'Estabelecimentos Formais (por mil hab)',
    'ee_ocup_formais': 'Ocupações Formais (por mil hab)',
    'ee_renda_pc': 'Renda Domiciliar Per Capita',
    'ci_part_rede_4g': 'Cobertura 4G (%)',
    'ci_dens_banda_fixa': 'Densidade Banda Larga Fixa',
    'sa_medicos': 'Médicos (por 100 mil hab)',
    'sa_leitos_hospitalar': 'Leitos Hospitalares (por 100 mil hab)',
    'de_demanda_turistica': 'Demanda Turística'
}

print("\nColunas sugeridas para integração:")
for col, descricao in relevant_cols.items():
    if col in df.columns:
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        print(f"  ✓ {col:30s} - {descricao}")
        print(f"    {non_null:,} valores ({pct:.1f}% preenchido)")
