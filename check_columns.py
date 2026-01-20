import pandas as pd
from pathlib import Path

# Path to the file
file_path = Path("data/01_raw/Composicao_RM_2024.xlsx")

try:
    df = pd.read_excel(file_path)
    print("Columns found:", df.columns.tolist())
    if "NOME_CATMETROPOL" in df.columns:
        print("SUCCESS: NOME_CATMETROPOL found.")
    else:
        print("FAILURE: NOME_CATMETROPOL not found.")
except Exception as e:
    print(f"Error reading file: {e}")
