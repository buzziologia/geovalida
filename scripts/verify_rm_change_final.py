import pandas as pd
import json
from pathlib import Path

def verify():
    # Load Excel
    print("Loading Excel...")
    excel_path = Path(r"c:\Users\vinicios.buzzi\buzzi\GeoValida\data\01_raw\Composicao_RM_2024.xlsx")
    df = pd.read_excel(excel_path)
    
    # Get a sample
    # Filter for where NOME_CATMETROPOL is not null
    sample = df[df['NOME_CATMETROPOL'].notna()].iloc[0]
    cod_mun = int(sample['COD_MUN'])
    expected_name = str(sample['NOME_CATMETROPOL'])
    print(f"Checking COD_MUN: {cod_mun}")
    print(f"Expected RM Name (from Excel): {expected_name}")
    
    # Load JSON
    print("Loading initialization.json...")
    json_path = Path(r"c:\Users\vinicios.buzzi\buzzi\GeoValida\data\initialization.json")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Find municipality
    found = False
    for mun in data['municipios']:
        if mun['cd_mun'] == cod_mun:
            actual_name = mun['regiao_metropolitana']
            print(f"Actual RM Name (from JSON): {actual_name}")
            
            if actual_name == expected_name:
                print("SUCCESS: Names match!")
            else:
                print("FAILURE: Names do NOT match.")
            found = True
            break
    
    if not found:
        print("FAILURE: Municipality not found in JSON.")

if __name__ == "__main__":
    verify()
