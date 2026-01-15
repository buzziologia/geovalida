import pandas as pd

# Try different encodings and separators
file_path = 'data/01_raw/Base_Categorização(Base Organizada Normalizada).csv'

for sep in [';', ',', '\t']:
    for encoding in ['utf-8', 'latin1', 'cp1252']:
        try:
            df = pd.read_csv(file_path, sep=sep, encoding=encoding, nrows=5)
            print(f"✓ SUCCESS with sep='{sep}' encoding='{encoding}'")
            print(f"\nColumns ({len(df.columns)}):")
            print(df.columns.tolist())
            print(f"\nShape: {df.shape}")
            print(f"\nFirst 3 rows:")
            print(df.head(3))
            
            # Check for population and tourism columns
            print("\n\nLooking for population/tourism columns:")
            pop_cols = [col for col in df.columns if 'pop' in col.lower() or 'hab' in col.lower()]
            turismo_cols = [col for col in df.columns if 'turis' in col.lower() or 'categ' in col.lower()]
            print(f"Population-related: {pop_cols}")
            print(f"Tourism-related: {turismo_cols}")
            
            break
        except Exception as e:
            continue
    else:
        continue
    break
else:
    print("Failed to read file with any combination")
