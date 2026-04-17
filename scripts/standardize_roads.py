import geopandas as gpd
import pandas as pd
import fiona
import os
import sys

# Enable KML driver for GeoPandas (via fiona)
fiona.drvsupport.supported_drivers['KML'] = 'rw'

def main(
    kmz_path="data/raw/roads/roads.kmz",
    output_path="data/standardized/roads.parquet",
    metric_crs="EPSG:25830",
    **kwargs
):
    """
    Standardizes road backbone data from KMZ into a projected tabular format.
    Extracts road name and type from HTML description and calculates length in meters.
    """
    print(f"🚀 Standardizing Roads from {kmz_path}...")
    
    if not os.path.exists(kmz_path):
        print(f"Error: {kmz_path} not found.")
        sys.exit(1)

    # 1. Load KMZ
    print(" - Loading KMZ data...")
    try:
        gdf = gpd.read_file(kmz_path, driver='KML')
    except Exception as e:
        print(f"Error reading KMZ: {e}")
        sys.exit(1)

    if gdf.empty:
        print("Warning: Loaded GeoDataFrame is empty.")
        return

    # 2. Extract Metadata from HTML Description
    print(" - Extracting metadata from description...")
    gdf["road_name"] = gdf["description"].str.extract(
        r"<td>Carretera</td>\s*<td>([^<]+)</td>", expand=False
    )
    gdf["road_type"] = gdf["description"].str.extract(
        r"<td>Tipo_de_via</td>\s*<td>([^<]+)</td>", expand=False
    )
    
    # 3. Project to Metric CRS
    print(f" - Projecting to {metric_crs}...")
    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", inplace=True)
    gdf = gdf.to_crs(metric_crs)
    
    # 4. Calculate Length in meters
    print(" - Calculating length_m...")
    gdf["length_m"] = gdf.geometry.length
    
    # 5. Column Renaming & Selection
    # KMZ 'id' -> 'road_id'
    gdf = gdf.rename(columns={'id': 'road_id'})
    
    columns_to_keep = ["road_id", "road_name", "road_type", "length_m", "geometry"]
    final_cols = [c for c in columns_to_keep if c in gdf.columns]
    gdf = gdf[final_cols]
    
    # 6. Save as Parquet
    print(f" - Saving to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Roads standardized ({len(gdf)} segments).")

if __name__ == "__main__":
    main()
