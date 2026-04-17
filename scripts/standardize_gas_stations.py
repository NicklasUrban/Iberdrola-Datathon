import json
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os
import sys

def main(
    raw_path="data/raw/gas_stations/gas_stations.json",
    output_path="data/standardized/gas_stations.parquet",
    metric_crs="EPSG:25830",
    **kwargs
):
    """
    Standardizes gas stations data with English names and specific selection.
    """
    print(f"🚀 Standardizing Gas Stations from {raw_path}...")
    
    if not os.path.exists(raw_path):
        print(f"Error: {raw_path} not found.")
        sys.exit(1)

    # 1. Load JSON
    try:
        with open(raw_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        stations = raw_data.get('ListaEESSPrecio', [])
        df = pd.DataFrame(stations)
    except Exception as e:
        print(f"Error reading JSON: {e}")
        sys.exit(1)

    if df.empty:
        print("Warning: No stations found in JSON.")
        return

    # 2. Convert to GeoPandas
    print(" - Converting coordinates to GeoPandas points...")
    df['Latitud'] = df['Latitud'].str.replace(',', '.').astype(float)
    df['Longitud (WGS84)'] = df['Longitud (WGS84)'].str.replace(',', '.').astype(float)
    
    geometry = [Point(xy) for xy in zip(df['Longitud (WGS84)'], df['Latitud'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # 3. Project & Rename
    print(f" - Projecting to {metric_crs} and renaming columns...")
    gdf = gdf.to_crs(metric_crs)
    
    # Column mapping to English
    mapping = {
        'IDEESS': 'station_id',
        'Municipio': 'city',
        'Provincia': 'province'
    }
    gdf = gdf.rename(columns=mapping)
    
    # Selection
    columns_to_keep = ['station_id', 'city', 'province', 'geometry']
    gdf = gdf[columns_to_keep]
    
    # 4. Save
    print(f" - Saving to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Gas Stations standardized ({len(gdf)} stations).")

if __name__ == "__main__":
    main()
