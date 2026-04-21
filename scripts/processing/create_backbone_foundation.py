import geopandas as gpd
import pandas as pd
import numpy as np
import os
import time

def discretize_backbone_roads(gdf_roads, sampling_interval_m=200):
    """
    Converts LineStrings from a GeoDataFrame into a series of Points along their path.
    Each point stores the distance from the line's start (m_ref).
    
    Args:
        gdf_roads: GeoDataFrame containing standardized backbone roads (from roads.parquet).
                   Expected columns: 'road_id', 'geometry'.
        sampling_interval_m: Interval in meters between successive points.
    """
    print(f" - Discretizing backbones into points (Interval={sampling_interval_m}m)...")
    
    # Ensure metric CRS (assuming roads are already projected in standardization)
    if gdf_roads.crs is None:
        gdf_roads.set_crs(epsg=3042, inplace=True)
    
    points_data = []
    for _, row in gdf_roads.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        
        length = geom.length
        if length <= 0:
            distances = [0.0]
        else:
            distances = np.arange(0, length, sampling_interval_m)
            if len(distances) == 0 or distances[-1] < length:
                distances = np.append(distances, length)
            
        for d in distances:
            pt = geom.interpolate(d)
            entry = row.to_dict()
            entry['geometry'] = pt
            entry['m_ref'] = round(d, 2)
            points_data.append(entry)
            
    gdf_pts = gpd.GeoDataFrame(points_data, crs=gdf_roads.crs)
    
    # Maintain columns or ensure they exist
    if 'road_id' in gdf_pts.columns:
        gdf_pts = gdf_pts.rename(columns={'road_id': 'backbone_id'})
    
    # Create unique point IDs
    gdf_pts['point_idx'] = gdf_pts.groupby('backbone_id').cumcount()
    gdf_pts['point_id'] = (
        gdf_pts['backbone_id'].astype(str) + 
        "_" + 
        gdf_pts['point_idx'].astype(str)
    )
    
    return gdf_pts

def map_traffic_to_points(gdf_points, gdf_traffic, traffic_columns=["total_max"], buffer_radius_m=50):
    """
    Maps traffic intensity metrics from standardized traffic segments to backbone points.
    
    Args:
        gdf_points: GeoDataFrame of discretized backbone points.
        gdf_traffic: GeoDataFrame of standardized traffic segments (from traffic.parquet).
        traffic_columns: List of traffic intensity columns to map.
        buffer_radius_m: Radius to buffer points for spatial intersection.
    """
    print(f" - Mapping traffic columns {traffic_columns} (Buffer={buffer_radius_m}m)...")
    
    if gdf_traffic.crs != gdf_points.crs:
        gdf_traffic = gdf_traffic.to_crs(gdf_points.crs)
        
    # Validate requested columns
    available_cols = [c for c in traffic_columns if c in gdf_traffic.columns]
    if not available_cols:
        print(f"   Warning: None of the requested columns {traffic_columns} were found. Skipping traffic mapping.")
        return gdf_points
        
    print(f"   Mapping columns: {available_cols}")

    # Ensure ID columns are strings for merging
    if 'traffic_segment_id' in gdf_traffic.columns:
        gdf_traffic['traffic_segment_id'] = gdf_traffic['traffic_segment_id'].astype(str)
    
    # 1. Spatial Join with Buffer
    gdf_pts_buffered = gdf_points.copy()
    gdf_pts_buffered['geometry'] = gdf_pts_buffered.geometry.buffer(buffer_radius_m)
    
    # Use spatial join to find segments near points
    joined = gpd.sjoin(
        gdf_pts_buffered[['point_id', 'backbone_id', 'point_idx', 'geometry']], 
        gdf_traffic[['traffic_segment_id', 'geometry'] + available_cols], 
        how='inner', 
        predicate='intersects'
    )
    
    if joined.empty:
        print("   Warning: No segments matched the backbone points.")
        for col in available_cols:
            gdf_points[col] = 0.0
        return gdf_points

    # 2. Neighbor Validation Filter (Longitudinal Persistence)
    # A segment is valid for a backbone road if it touches at least two adjacent points
    joined['has_neighbor'] = joined.groupby(['traffic_segment_id', 'backbone_id'])['point_idx'].transform(
        lambda x: x.isin(x + 1) | x.isin(x - 1)
    )
    joined_filtered = joined[joined['has_neighbor']].copy()
    
    if joined_filtered.empty:
        print("   Warning: No segments passed the neighbor-validation filter.")
        for col in available_cols:
            gdf_points[col] = 0.0
        return gdf_points

    # 3. Sum Traffic per Point
    traffic_summary = joined_filtered.groupby('point_id')[available_cols].sum().reset_index()
    
    # Drop existing traffic columns if they exist to avoid suffixes (_x, _y) during merge
    existing_traffic_cols = [c for c in available_cols if c in gdf_points.columns]
    if existing_traffic_cols:
        gdf_points = gdf_points.drop(columns=existing_traffic_cols)

    # Merge back to original points
    gdf_final = gdf_points.merge(traffic_summary, on='point_id', how='left')
    gdf_final[available_cols] = gdf_final[available_cols].fillna(0)
    
    # 4. Gap Filling (Interpolation for single-point gaps)
    gdf_final = gdf_final.sort_values(['backbone_id', 'point_idx'])
    for col in available_cols:
        prev_val = gdf_final.groupby('backbone_id')[col].shift(1)
        next_val = gdf_final.groupby('backbone_id')[col].shift(-1)
        
        mask = (gdf_final[col] == 0) & (prev_val > 0) & (next_val > 0)
        gdf_final.loc[mask, col] = (prev_val + next_val) / 2
    
    return gdf_final

def assign_nearest_charging_stations(gdf_points, gdf_chargers, max_distance=None):
    """
    Assigns the nearest ultra-fast charging station site_id and distance.
    """
    print(f" - Assigning nearest charging stations (MaxDist={max_distance})...")
    
    if gdf_chargers.crs != gdf_points.crs:
        gdf_chargers = gdf_chargers.to_crs(gdf_points.crs)
        
    cols_to_keep = ['site_id', 'geometry']
    gdf_chargers_subset = gdf_chargers[cols_to_keep].rename(columns={
        'site_id': 'nearest_charger_id'
    })
    
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_chargers_subset,
        how="left",
        max_distance=max_distance,
        distance_col="dist_charger_m"
    )
    
    gdf_result = gdf_result.drop_duplicates(subset=['point_id'])
    if 'index_right' in gdf_result.columns:
        gdf_result = gdf_result.drop(columns=['index_right'])
        
    return gdf_result

def assign_nearest_gas_stations(gdf_points, gdf_gas, max_distance=None):
    """
    Assigns the nearest gas station station_id and distance.
    """
    print(f" - Assigning nearest gas stations (MaxDist={max_distance})...")
    
    if gdf_gas.crs != gdf_points.crs:
        gdf_gas = gdf_gas.to_crs(gdf_points.crs)
        
    gdf_gas_subset = gdf_gas[['station_id', 'geometry']].rename(columns={
        'station_id': 'nearest_gas_station_id'
    })
    
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_gas_subset,
        how="left",
        max_distance=max_distance,
        distance_col="dist_gas_station_m"
    )
    
    gdf_result = gdf_result.drop_duplicates(subset=['point_id'])
    if 'index_right' in gdf_result.columns:
        gdf_result = gdf_result.drop(columns=['index_right'])
        
    return gdf_result

def assign_grid_capacity(gdf_points, gdf_capacity):
    """
    Assigns the nearest electrical substation capacity to each backbone point.
    """
    print(" - Assigning nearest electrical grid capacity...")
    
    if gdf_capacity.crs != gdf_points.crs:
        gdf_capacity = gdf_capacity.to_crs(gdf_points.crs)
        
    gdf_result = gpd.sjoin_nearest(
        gdf_points,
        gdf_capacity[['capacity_kw', 'row_id', 'geometry']],
        how='left',
        distance_col='dist_substation_m'
    )
    
    gdf_result = gdf_result.drop_duplicates(subset=['point_id'])
    if 'index_right' in gdf_result.columns:
        gdf_result = gdf_result.drop(columns=['index_right'])

    # Rename row_id to substation_id for clarity in the foundation dataset
    if 'row_id' in gdf_result.columns:
        gdf_result = gdf_result.rename(columns={'row_id': 'substation_id'})
        
    return gdf_result

def main(
    roads_path, 
    traffic_path, 
    chargers_path, 
    gas_stations_path, 
    capacity_path,
    output_path,
    sub_steps=["all"],
    traffic_columns=["total_max", "short_max", "medio_max"],
    sampling_interval_m=200,
    buffer_radius_m=50,
    max_distance_proximity=None
):
    """
    Orchestrates the creation of the backbone foundation points loading from paths.
    """
    start_time = time.time()
    print("🚀 Starting Backbone Foundation Creation...")
    
    run_all = "all" in sub_steps
    
    # 1. Road Discretization
    if run_all or "discretize" in sub_steps:
        print(f" - Loading standardized roads from {roads_path}...")
        gdf_roads = gpd.read_parquet(roads_path)
        gdf_points = discretize_backbone_roads(gdf_roads, sampling_interval_m)
    else:
        print(f" - Skipping discretization. Loading existing points from {output_path}...")
        gdf_points = gpd.read_parquet(output_path)

    # 2. Traffic Mapping
    if run_all or "traffic" in sub_steps:
        print(f" - Loading standardized traffic from {traffic_path}...")
        gdf_traffic = gpd.read_parquet(traffic_path)
        gdf_points = map_traffic_to_points(gdf_points, gdf_traffic, traffic_columns, buffer_radius_m)

    # 3. Charger Proximity
    if run_all or "chargers" in sub_steps:
        print(f" - Loading standardized chargers from {chargers_path}...")
        gdf_chargers = gpd.read_parquet(chargers_path)
        gdf_points = assign_nearest_charging_stations(gdf_points, gdf_chargers, max_distance_proximity)

    # 4. Gas Station Proximity
    if run_all or "gas_stations" in sub_steps:
        print(f" - Loading standardized gas stations from {gas_stations_path}...")
        gdf_gas = gpd.read_parquet(gas_stations_path)
        gdf_points = assign_nearest_gas_stations(gdf_points, gdf_gas, max_distance_proximity)

    # 5. Grid Capacity
    if run_all or "capacity" in sub_steps:
        print(f" - Loading standardized capacity from {capacity_path}...")
        gdf_capacity = gpd.read_parquet(capacity_path)
        gdf_points = assign_grid_capacity(gdf_points, gdf_capacity)

    print(f" - Saving final foundation dataset to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_points.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Created foundation with {len(gdf_points)} points.")
    print(f"   Time elapsed: {time.time() - start_time:.1f}s")
    return gdf_points

if __name__ == "__main__":
    main(
        roads_path="data/standardized/roads.parquet",
        traffic_path="data/standardized/traffic.parquet",
        chargers_path="data/standardized/chargers.parquet",
        gas_stations_path="data/standardized/gas_stations.parquet",
        capacity_path="data/standardized/electric_capacity.parquet",
        output_path="data/processed/backbone_foundation.parquet"
    )
