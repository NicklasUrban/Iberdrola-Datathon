import polars as pl
import geopandas as gpd
import pandas as pd
import os
import glob
import re
import sys

def main(
    raw_dir="data/raw/traffic/informacion_tramo",
    shp_path="data/raw/traffic/geometria/Geometria_tramos.shp",
    output_path="data/standardized/traffic.parquet",
    metric_crs="EPSG:25830",
    **kwargs
):
    # Map raw_dir to what the code uses internally
    traffic_dir = raw_dir
    """
    Standardizes traffic data by:
    1. Merging all daily 'informacion_tramo' CSVs (Total and Short trips).
    2. Joining with 'geometria' SHP.
    3. Calculating global max metrics.
    4. Projecting to metric CRS.
    """
    print(f"🚀 Standardizing Traffic from {traffic_dir} and {shp_path}...")
    
    # 1. Merge Informacion Tramo CSVs
    print(" - Merging daily traffic CSVs...")
    csv_files = glob.glob(os.path.join(traffic_dir, "*_info_odmatrix.csv.gz"))
    if not csv_files:
        csv_files = glob.glob(os.path.join(traffic_dir, "*.csv"))
    
    if not csv_files:
        print(f"Error: No traffic CSVs found in {traffic_dir}")
        sys.exit(1)
        
    csv_files.sort()
    
    dfs = []
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        match = re.search(r"(\d{8})", filename)
        if not match: continue
        
        date_str = match.group(1)
        total_col = f"total_{date_str}"
        short_col = f"short_{date_str}"
        
        try:
            df = pl.scan_csv(file_path, separator=';') \
                .select([
                    pl.col("tramo").cast(pl.Utf8),
                    pl.col("total").alias(total_col),
                    pl.col("corto").alias(short_col)
                ])
            dfs.append(df)
        except Exception as e:
            print(f"   Error reading {filename}: {e}")

    if not dfs:
        print("Error: No valid traffic dataframes to merge.")
        sys.exit(1)

    print(f" - Joining {len(dfs)} datasets...")
    merged_traffic = dfs[0]
    for i in range(1, len(dfs)):
        merged_traffic = merged_traffic.join(dfs[i], on="tramo", how="full", coalesce=True)
    
    # Calculate Max metrics
    final_traffic_df = merged_traffic.collect()
    total_cols = [c for c in final_traffic_df.columns if c.startswith("total_")]
    short_cols = [c for c in final_traffic_df.columns if c.startswith("short_")]
    
    final_traffic_df = final_traffic_df.with_columns([
        pl.max_horizontal(total_cols).alias("total_max"),
        pl.max_horizontal(short_cols).alias("short_max")
    ])

    # 2. Load Geometry
    print(" - Loading Geometry SHP...")
    if not os.path.exists(shp_path):
        print(f"Error: SHP file not found at {shp_path}")
        sys.exit(1)
        
    gdf_geom = gpd.read_file(shp_path)
    
    # 3. Project to Metric CRS
    print(f" - Projecting to {metric_crs}...")
    if gdf_geom.crs is None:
        gdf_geom.set_crs("EPSG:3042", inplace=True, allow_override=True)
    gdf_geom = gdf_geom.to_crs(metric_crs)
    
    # 4. Join Geometry + Traffic
    print(" - Joining geometry and traffic data...")
    df_traffic_pd = final_traffic_df.to_pandas()
    
    # Rename id_tramo to traffic_segment_id and join
    gdf_geom = gdf_geom.rename(columns={'id_tramo': 'traffic_segment_id'})
    gdf_final = gdf_geom.merge(df_traffic_pd, left_on='traffic_segment_id', right_on='tramo', how='inner')
    
    # Drop duplicate 'tramo'
    if 'tramo' in gdf_final.columns:
        gdf_final = gdf_final.drop(columns=['tramo'])
    
    # 5. Save
    print(f" - Saving to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_final.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Traffic standardized ({len(gdf_final)} segments).")

if __name__ == "__main__":
    main()
