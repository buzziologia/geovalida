import pandas as pd
import json
from pathlib import Path

def analyze():
    # Paths
    base_dir = Path(r"c:\Users\vinicios.buzzi\buzzi\GeoValida")
    data_dir = base_dir / "data"
    raw_dir = data_dir / "01_raw"
    
    # 1. Analyze UTP_FINAL.xlsx
    excel_path = raw_dir / "UTP_FINAL.xlsx"
    print(f"--- Analyzing {excel_path} ---")
    try:
        df_utp = pd.read_excel(excel_path)
        print(f"Total rows: {len(df_utp)}")
        print(f"Columns: {list(df_utp.columns)}")
        
        if 'UTPs_PAN_3' in df_utp.columns:
            unique_utps = df_utp['UTPs_PAN_3'].unique()
            print(f"Unique UTPs_PAN_3 count: {len(unique_utps)}")
            print(f"First 10 distinct UTPs: {unique_utps[:10]}")
            
            # Check relation with CD_MUN
            municipalities_count = df_utp['CD_MUN'].nunique()
            print(f"Unique CD_MUN count: {municipalities_count}")
        else:
            print("Column 'UTPs_PAN_3' NOT FOUND!")
            
    except Exception as e:
        print(f"Error reading Excel: {e}")

    # 2. Analyze initialization.json
    json_path = data_dir / "initialization.json"
    print(f"\n--- Analyzing {json_path} ---")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        utps = data.get('utps', [])
        print(f"Number of UTPs in JSON: {len(utps)}")
        
        # Check if municipalities have unique utp_ids
        municipios = data.get('municipios', [])
        print(f"Number of Municipios in JSON: {len(municipios)}")
        
        utp_ids = [m.get('utp_id') for m in municipios]
        unique_json_utps = set(utp_ids)
        print(f"Unique utp_ids in municipios list: {len(unique_json_utps)}")
        
    except Exception as e:
        print(f"Error reading JSON: {e}")

if __name__ == "__main__":
    analyze()
