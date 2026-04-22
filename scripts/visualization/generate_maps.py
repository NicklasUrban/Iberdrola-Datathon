import geopandas as gpd
import pandas as pd
import numpy as np
import folium
from scipy.spatial import KDTree
import os
from branca.element import Template, MacroElement

def calculate_supply_demand_ratio(gdf_backbone, gdf_sites, ev_traffic_pct=0.14, need_charge_pct=0.38, sessions_per_charger=24, coverage_threshold_m=30000):
    print("Calculating demand-supply ratio...")
    demand_weight = gdf_backbone['total_max'] * ev_traffic_pct * need_charge_pct
    
    backbone_coords = list(zip(gdf_backbone.geometry.x, gdf_backbone.geometry.y))
    valid_sites = gdf_sites[gdf_sites['n_chargers'] > 0].copy()
    if valid_sites.empty:
        supply = np.zeros(len(gdf_backbone))
    else:
        site_coords = list(zip(valid_sites.geometry.x, valid_sites.geometry.y))
        tree = KDTree(site_coords)
        neighbors_list = tree.query_ball_point(backbone_coords, coverage_threshold_m)
        
        n_chargers = valid_sites['n_chargers'].values
        supply = np.array([sum([n_chargers[j] * sessions_per_charger for j in neighbors]) for neighbors in neighbors_list])
    
    met_pct = np.where(demand_weight.values > 0, (supply / demand_weight.values) * 100, 100.0)
    return demand_weight, supply, met_pct

def get_color_for_met_pct(pct):
    if pct < 100.0: return '#d73027' # Red (Congested / Unmet Demand)
    elif pct < 125.0: return '#fdae61' # Orange (Moderate)
    else: return '#82B300' # Iberdrola Green (Sufficient)

def get_color_for_grid(status):
    if 'Congested' in str(status): return '#d73027'
    elif 'Moderate' in str(status): return '#fdae61'
    else: return '#82B300'

def add_legend(map_obj, title, items):
    """Adds a custom HTML legend to a Folium map."""
    lines = []
    for label, color in items:
        lines.append(f'<tr><td><span style="background:{color}; opacity:0.8; width:12px; height:12px; display:inline-block; border-radius:50%; margin-right:8px;"></span></td><td style="padding:2px 0;">{label}</td></tr>')
    
    lines_html = "".join(lines)
    
    template = f"""
    {{% macro html(this, kwargs) %}}
    <div style="
        position: fixed; 
        bottom: 50px; right: 50px; width: 220px; 
        background-color: white; border: 1px solid #e2e8f0; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-radius: 8px; 
        z-index: 9999; font-size: 13px; font-family: 'Inter', sans-serif;
        padding: 12px; color: #1a202c;
    ">
        <h4 style="margin: 0 0 8px 0; font-size: 14px; font-weight: 600; color: #002B5C;">{title}</h4>
        <table style="width: 100%; border-collapse: collapse;">
            {lines_html}
        </table>
    </div>
    {{% endmacro %}}
    """
    macro = MacroElement()
    macro._template = Template(template)
    map_obj.get_root().add_child(macro)


def generate_maps():
    print("Loading datasets...")
    backbone_path = "data/processed/backbone_foundation.parquet"
    chargers_path = "data/standardized/chargers.parquet"
    optimized_path = "data/processed/grid_aware_optimized_sites.parquet"
    file2_path = "data/outputs/File 2.csv"
    
    gdf_backbone = gpd.read_parquet(backbone_path)
    gdf_chargers = gpd.read_parquet(chargers_path)
    gdf_opt = gpd.read_parquet(optimized_path)
    
    df_file2 = pd.read_csv(file2_path)
    gdf_file2 = gpd.GeoDataFrame(
        df_file2, 
        geometry=gpd.points_from_xy(df_file2['longitude'], df_file2['latitude']),
        crs="EPSG:3042"
    ).to_crs("EPSG:4326")
    
    df_before = gdf_opt[gdf_opt['type'] == 'existing'].copy()
    df_before['n_chargers'] = df_before['initial_n']
    
    df_after = gdf_opt[gdf_opt['is_open'] == 1].copy()
    df_after['n_chargers'] = df_after['final_n']
    
    _, _, met_pct_before = calculate_supply_demand_ratio(gdf_backbone, df_before)
    _, _, met_pct_after = calculate_supply_demand_ratio(gdf_backbone, df_after)
    
    gdf_backbone['met_pct_before'] = met_pct_before
    gdf_backbone['met_pct_after'] = met_pct_after
    
    if len(gdf_backbone) > 5000:
        gdf_backbone_sample = gdf_backbone.sample(5000, random_state=42)
    else:
        gdf_backbone_sample = gdf_backbone
        
    gdf_backbone_sample = gdf_backbone_sample.to_crs(epsg=4326)
    df_before = df_before.to_crs(epsg=4326)
    df_after = df_after.to_crs(epsg=4326)
    
    spain_center = [40.463667, -3.74922]
    tile_theme = "CartoDB positron"
    
    # Map 1
    print("Generating Map 1...")
    m1 = folium.Map(location=spain_center, zoom_start=6, tiles=tile_theme)
    for _, row in df_before.iterrows():
        if row['n_chargers'] > 0:
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=3, color='#002B5C', fill=True, fill_color='#002B5C', fill_opacity=0.7,
                tooltip=f"Site: {row['site_id']}<br>Chargers: {row['n_chargers']}"
            ).add_to(m1)
    add_legend(m1, "Map Legend", [("Existing EV Stations", "#002B5C")])
    m1.save("docs/maps/map_1_current_stations.html")
    
    # Map 2
    print("Generating Map 2...")
    m2 = folium.Map(location=spain_center, zoom_start=6, tiles=tile_theme)
    for _, row in gdf_backbone_sample.iterrows():
        folium.Circle(
            location=[row.geometry.y, row.geometry.x],
            radius=1500, color=get_color_for_met_pct(row['met_pct_before']), fill=True, fill_opacity=0.6, weight=0,
            tooltip=f"Demand Met: {row['met_pct_before']:.1f}%"
        ).add_to(m2)
    add_legend(m2, "% of EV Demand Met", [
        ("Road Points: Congested (< 100%)", "#d73027"),
        ("Road Points: Moderate (100% - 125%)", "#fdae61"),
        ("Road Points: Sufficient (> 125%)", "#82B300")
    ])
    m2.save("docs/maps/map_2_demand_before.html")
    
    # Map 3
    print("Generating Map 3...")
    m3 = folium.Map(location=spain_center, zoom_start=6, tiles=tile_theme)
    for _, row in gdf_file2.iterrows():
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=5, color=get_color_for_grid(row['grid_status']), fill=True, fill_opacity=0.9,
            tooltip=f"Site: {row['location_id']}<br>Chargers Proposed: {row['n_chargers_proposed']}<br>Grid Status: {row['grid_status']}"
        ).add_to(m3)
    add_legend(m3, "Grid Feasibility", [
        ("Proposed Station: Congested", "#d73027"),
        ("Proposed Station: Moderate", "#fdae61"),
        ("Proposed Station: Sufficient", "#82B300")
    ])
    m3.save("docs/maps/map_3_proposed_stations.html")
    
    # Map 4
    print("Generating Map 4...")
    m4 = folium.Map(location=spain_center, zoom_start=6, tiles=tile_theme)
    for _, row in gdf_backbone_sample.iterrows():
        folium.Circle(
            location=[row.geometry.y, row.geometry.x],
            radius=1500, color=get_color_for_met_pct(row['met_pct_after']), fill=True, fill_opacity=0.6, weight=0,
            tooltip=f"Demand Met: {row['met_pct_after']:.1f}%"
        ).add_to(m4)
    add_legend(m4, "% of EV Demand Met (Post-Opt)", [
        ("Road Points: Congested (< 100%)", "#d73027"),
        ("Road Points: Moderate (100% - 125%)", "#fdae61"),
        ("Road Points: Sufficient (> 125%)", "#82B300")
    ])
    m4.save("docs/maps/map_4_demand_after.html")
    print("Successfully generated all 4 maps!")

if __name__ == "__main__":
    os.makedirs("docs/maps", exist_ok=True)
    generate_maps()
