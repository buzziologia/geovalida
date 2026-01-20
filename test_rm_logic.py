
import logging
import pandas as pd
import geopandas as gpd
from pathlib import Path
from src.utils import DataLoader
from src.interface.dashboard import get_derived_rm_geodataframe, get_geodataframe

# Mock streamlit cache
import streamlit as st
st.cache_data = lambda **kwargs: lambda func: func
st.error = print

def test_rm_derivation():
    print("Loading data...")
    loader = DataLoader()
    df_municipios = loader.get_municipios_dataframe()
    
    if df_municipios.empty:
        print("ERROR: df_municipios is empty!")
        return

    print(f"Loaded {len(df_municipios)} municipalities.")
    
    # Check if RM field exists and has data
    if 'regiao_metropolitana' not in df_municipios.columns:
        print("ERROR: regiao_metropolitana column missing!")
        return
        
    rm_counts = df_municipios['regiao_metropolitana'].value_counts()
    print("\nRM Distribution in DataFrame:")
    print(rm_counts.head())
    
    # Check specifically for a known RM name if possible, or just non-empty
    valid_rms = df_municipios[
        df_municipios['regiao_metropolitana'].notna() & 
        (df_municipios['regiao_metropolitana'] != '')
    ]
    print(f"\nMunicipalities with RM assigned: {len(valid_rms)}")
    
    if valid_rms.empty:
        print("ERROR: No municipalities have RM assigned!")
        return

    # Load shapefile (using the path logic from dashboard.py)
    shapefile_path = Path("data/01_raw/shapefiles/BR_Municipios_2024.shp")
    if not shapefile_path.exists():
        print(f"ERROR: Shapefile not found at {shapefile_path}")
        return
        
    print("Loading GeoDataFrame...")
    gdf = get_geodataframe(shapefile_path, df_municipios)
    
    if gdf is None:
        print("ERROR: Failed to load GeoDataFrame")
        return
        
    print(f"GeoDataFrame loaded: {len(gdf)} rows")
    
    # Test derivation
    print("Deriving RM geometries...")
    gdf_rm = get_derived_rm_geodataframe(gdf)
    
    if gdf_rm is None or gdf_rm.empty:
        print("ERROR: Derived RM GeoDataFrame is empty!")
    else:
        print(f"SUCCESS: Derived {len(gdf_rm)} Metropolitan Regions.")
        print("RMs found:")
        for idx, row in gdf_rm.iterrows():
            print(f" - {row['regiao_metropolitana']} ({row['uf']}): {row['count']} municipalities")

if __name__ == "__main__":
    test_rm_derivation()
