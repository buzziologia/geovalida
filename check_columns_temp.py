import pandas as pd
from pathlib import Path

file_path = Path(r"c:\Users\vinicios.buzzi\buzzi\GeoValida\data\01_raw\Composicao_RM_2024.xlsx")

try:
    df = pd.read_excel(file_path)
    print("Columns found:", df.columns.tolist())
    if "NOME_CATMETROPOL" in df.columns:
        print("SUCCESS: NOME_CATMETROPOL column exists.")
    else:
        print("FAILURE: NOME_CATMETROPOL column NOT found.")
except Exception as e:
    print(f"Error reading file: {e}")
