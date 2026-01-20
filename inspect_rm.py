
import pandas as pd
import os

file_path = r'c:\Users\vinicios.buzzi\buzzi\GeoValida\data\01_raw\Composicao_RM_2024.xlsx'

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
else:
    try:
        df = pd.read_excel(file_path)
        print("Columns:", df.columns.tolist())
        print("First 5 rows:")
        print(df.head())
    except Exception as e:
        print(f"Error reading file: {e}")
