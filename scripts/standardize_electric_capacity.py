import pandas as pd
import polars as pl
import geopandas as gpd
import os
import sys

def clean_coordinate(val):
    if isinstance(val, str):
        try:
            return float(val.replace(',', '.'))
        except ValueError:
            return 0.0
    return float(val) if val is not None else 0.0

def load_and_clean_data(file_path, company_name):
    print(f" - Loading {company_name} from {os.path.basename(file_path)}...")
    # Common columns
    common_cols = ['Gestor de red', 'Provincia', 'Municipio', 'Coordenada UTM X', 'Coordenada UTM Y', 'Subestación']
    capacity_col = 'Capacidad firme disponible (MW)' if company_name == 'Iberdrola' else 'Capacidad disponible (MW)'
    
    try:
        # Load everything first to avoid "not found in axis" errors with usecols
        df_pd = pd.read_excel(file_path)
        
        # Filter columns that actually exist
        available_cols = [c for c in common_cols + [capacity_col] if c in df_pd.columns]
        df_pd = df_pd[available_cols]
    except Exception as e:
        print(f"   Error reading {file_path}: {e}")
        return None
    
    df = pl.from_pandas(df_pd)
    
    # Standardize types
    str_cols = ['Gestor de red', 'Provincia', 'Municipio', 'Subestación']
    df = df.with_columns([
        pl.col(c).cast(pl.Utf8) for c in str_cols if c in df.columns
    ])

    if company_name == 'Iberdrola' and capacity_col in df.columns:
        df = df.rename({capacity_col: 'Capacidad disponible (MW)'})
    
    # Clean capacity helper
    def clean_val(val):
        if val is None: return 0.0
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, str):
            cleaned = "".join(c for c in val if c.isdigit() or c in ',.')
            if not cleaned: return 0.0
            return float(cleaned.replace(',', '.'))
        return 0.0

    df = df.with_columns([
        pl.col('Coordenada UTM X').map_elements(clean_coordinate, return_dtype=pl.Float64),
        pl.col('Coordenada UTM Y').map_elements(clean_coordinate, return_dtype=pl.Float64),
        (pl.col('Capacidad disponible (MW)').map_elements(clean_val, return_dtype=pl.Float64) * 1000.0).alias('capacity_kw'),
        pl.lit(company_name).alias('company')
    ])
    
    # Rename to English
    df = df.rename({
        'Gestor de red': 'grid_operator',
        'Provincia': 'province',
        'Municipio': 'city',
        'Subestación': 'substation'
    })
    
    # Drop raw capacity column to avoid type conflicts during merge
    if 'Capacidad disponible (MW)' in df.columns:
        df = df.drop('Capacidad disponible (MW)')
    
    return df

def main(
    raw_dir="data/raw/electric_capacity",
    output_path="data/standardized/electric_capacity.parquet",
    metric_crs="EPSG:25830",
    **kwargs
):
    """
    Standardizes electric capacity data with English names and capacity in kW.
    """
    print(f"🚀 Standardizing Electric Capacity from {raw_dir}...")
    
    files = {
        'Endesa': 'Endesa_2026_04_01.xlsx',
        'Iberdrola': 'Iberdrola_2026_04_01.xlsx',
        'Viesgo': 'Viesgo_2026_04_01.xlsx'
    }
    
    dfs = []
    for company, filename in files.items():
        file_path = os.path.join(raw_dir, filename)
        if os.path.exists(file_path):
            df = load_and_clean_data(file_path, company)
            if df is not None: dfs.append(df)

    if not dfs:
        print("Error: No capacity data loaded.")
        sys.exit(1)

    merged_df = pl.concat(dfs)
    
    # Add row_id (substation + index)
    merged_df = merged_df.with_columns([
        (pl.col("substation") + "_" + pl.arange(0, pl.count()).cast(pl.Utf8)).alias("row_id")
    ])
    
    pdf = merged_df.to_pandas()
    
    # Convert to GeoPandas
    gdf = gpd.GeoDataFrame(
        pdf, 
        geometry=gpd.points_from_xy(pdf['Coordenada UTM X'], pdf['Coordenada UTM Y']),
        crs="EPSG:25830"
    )
    
    # Project & Drop redundant
    gdf = gdf.to_crs(metric_crs)
    gdf = gdf.drop(columns=['Coordenada UTM X', 'Coordenada UTM Y'])
    
    # Save
    print(f" - Saving to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Electric Capacity standardized ({len(gdf)} entries).")

if __name__ == "__main__":
    main()
