import pandas as pd
import geopandas as gpd
import numpy as np
from scipy.spatial import KDTree
import os

def load_data():
    """Loads standardized electric capacity and optimized sites."""
    print("📂 Loading datasets...")
    sites_path = "data/processed/grid_aware_optimized_sites.parquet"
    grid_path = "data/standardized/electric_capacity.parquet"
    
    gdf_sites = gpd.read_parquet(sites_path)
    gdf_sites = gdf_sites[gdf_sites['is_open'] == 1].copy() # Filter for open sites
    gdf_grid = gpd.read_parquet(grid_path)
    
    # Ensure CRS match (EPSG:3042 used for Spanish backbone usually)
    if gdf_sites.crs.is_geographic:
        gdf_sites = gdf_sites.to_crs(epsg=3042)
    if gdf_grid.crs != gdf_sites.crs:
        gdf_grid = gdf_grid.to_crs(gdf_sites.crs)
        
    return gdf_sites, gdf_grid

def analyze_feasibility(gdf_sites, gdf_grid):
    """Performs spatial join and cumulative capacity check."""
    print("🧠 Analyzing Grid Feasibility...")
    
    # 1. Incremental demand (only new chargers)
    gdf_sites['new_kw'] = (gdf_sites['final_n'] - gdf_sites['initial_n']) * 150
    
    # 2. Assign sites to nearest substation (within 10km)
    sub_coords = list(zip([g.x for g in gdf_grid.geometry], [g.y for g in gdf_grid.geometry]))
    site_coords = list(zip([g.x for g in gdf_sites.geometry], [g.y for g in gdf_sites.geometry]))
    
    tree = KDTree(sub_coords)
    dists, indices = tree.query(site_coords, k=1, distance_upper_bound=10000)
    
    gdf_sites['substation_index'] = indices
    gdf_sites['dist_to_substation'] = dists
    
    # Mask for points with no substation within 10km
    no_grid_mask = np.isinf(dists)
    
    # Map back to substation IDs (using the unique row_id)
    valid_indices = gdf_sites.loc[~no_grid_mask, 'substation_index'].astype(int)
    gdf_sites.loc[~no_grid_mask, 'substation_id'] = gdf_grid.iloc[valid_indices]['row_id'].values
    gdf_sites.loc[~no_grid_mask, 'substation_cap_kw'] = gdf_grid.iloc[valid_indices]['capacity_kw'].values
    
    # Aggregate demand
    substation_loads = gdf_sites.groupby('substation_id')['new_kw'].sum().reset_index()
    substation_loads.rename(columns={'new_kw': 'total_assigned_demand_kw'}, inplace=True)
    
    # Merge loads back to sites
    gdf_sites = gdf_sites.merge(substation_loads, on='substation_id', how='left')
    
    # 4. Classification
    def classify(row):
        if np.isinf(row['dist_to_substation']):
            return 'No Grid Access (>10km)'
        if row['total_assigned_demand_kw'] > row['substation_cap_kw']:
            return 'Grid Bottleneck'
        return 'Feasible'
    
    gdf_sites['grid_status'] = gdf_sites.apply(classify, axis=1)
    
    return gdf_sites, substation_loads, gdf_grid

def report(gdf_sites, sub_loads, gdf_grid):
    print("\n📊 --- GRID FEASIBILITY SUMMARY ---")
    
    feasible = gdf_sites[gdf_sites.grid_status == 'Feasible']
    infeasible = gdf_sites[gdf_sites.grid_status != 'Feasible']
    
    print(f"Total Proposed Sites: {len(gdf_sites)}")
    print(f"Feasible Charging Stations: {len(feasible)} (Total Chargers: {feasible.final_n.sum()})")
    print(f"Infeasible Charging Stations: {len(infeasible)} (Total Chargers: {infeasible.final_n.sum()})")
    
    stats = gdf_sites.grid_status.value_counts()
    for status, count in stats.items():
        print(f"  - {status}: {count}")
        
    # Need to merge with original capacity for the top 10 view
    overloaded = sub_loads.merge(gdf_grid[['row_id', 'capacity_kw']].drop_duplicates(), left_on='substation_id', right_on='row_id')
    overloaded['deficit_kw'] = overloaded['total_assigned_demand_kw'] - overloaded['capacity_kw']
    overloaded = overloaded[overloaded.deficit_kw > 0].sort_values('deficit_kw', ascending=False)
    
    if not overloaded.empty:
        print(overloaded[['substation_id', 'capacity_kw', 'total_assigned_demand_kw', 'deficit_kw']].head(10).to_string(index=False))
    else:
        print("None! All assigned demand fits within linked substation capacities.")
    print("------------------------------------\n")

def main():
    gdf_sites, gdf_grid = load_data()
    gdf_res, sub_loads, gdf_grid_full = analyze_feasibility(gdf_sites, gdf_grid)
    report(gdf_res, sub_loads, gdf_grid_full)
    
    # Save the augmented results
    os.makedirs("data/processed", exist_ok=True)
    gdf_res.to_parquet("data/processed/grid_feasibility_results.parquet")
    print("✨ SUCCESS: Saved grid analysis results to data/processed/grid_feasibility_results.parquet")

if __name__ == "__main__":
    main()
