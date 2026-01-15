import pandas as pd

# Read the Base_Categorização file
df = pd.read_csv(
    'data/01_raw/Base_Categorização(Base Organizada Normalizada).csv',
    sep=';',
    encoding='utf-8',
    skiprows=2  # Skip the metadata header rows
)

print(f"Total rows: {len(df)}")
print(f"\nColumns with data:")
print(df.columns.tolist()[:15])  # Show first 15 columns

# Check key columns
print(f"\n\nKey data columns:")
print(f"- Município code: {df['md_cod_mun'].dtype}")
print(f"- População 2022: {df['md_populacao_2022'].dtype}")
print(f"- Categoria: {df['Categoria'].dtype}")

print(f"\n\nSample data:")
print(df[['md_cod_mun', 'md_nome_mun', 'md_sigla_uf', 'md_populacao_2022', 'Categoria']].head(10))

print(f"\n\nUnique tourism categories:")
print(df['Categoria'].value_counts())

print(f"\n\nPopulation stats:")
print(df['md_populacao_2022'].describe())
