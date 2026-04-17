import xml.etree.ElementTree as ET
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os
import sys

def parse_xml(xml_path):
    """Parses the DGT Datex2 XML file extracting location details and power."""
    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"XML file not found: {xml_path}")
        
    print(f" - Parsing XML from {xml_path}...")
    tree = ET.parse(xml_path)
    root = tree.getroot()

    ns = {
        'com': 'http://datex2.eu/schema/3/common',
        'fac': 'http://datex2.eu/schema/3/facilities',
        'loc': 'http://datex2.eu/schema/3/locationReferencing',
        'egi': 'http://datex2.eu/schema/3/energyInfrastructure',
        'locx': 'http://datex2.eu/schema/3/energyInfrastructureExtension' # Extension for address
    }

    data = []
    sites = root.findall('.//egi:energyInfrastructureSite', ns)
    
    for site in sites:
        site_id = site.get('id')
        site_name_elem = site.find('./fac:name/com:values/com:value', ns)
        site_name = site_name_elem.text if site_name_elem is not None else "Unknown"
        
        # Coordinates
        location_ref = site.find('.//loc:coordinatesForDisplay', ns)
        lat = location_ref.find('loc:latitude', ns).text if location_ref is not None else None
        lon = location_ref.find('loc:longitude', ns).text if location_ref is not None else None
        
        # Location Meta (City, Province)
        city, province = None, None
        address_lines = site.findall('.//locx:addressLine/locx:text/com:values/com:value', ns)
        for line in address_lines:
            text = line.text if line.text else ""
            if "Municipio:" in text:
                city = text.replace("Municipio:", "").strip()
            elif "Provincia:" in text:
                province = text.replace("Provincia:", "").strip()
        
        connectors = site.findall('.//egi:connector', ns)
        for conn in connectors:
            power_val = conn.find('egi:maxPowerAtSocket', ns)
            power_w = float(power_val.text) if power_val is not None and power_val.text else 0.0
            
            data.append({
                'site_name': site_name,
                'site_id': site_id,
                'latitude': float(lat) if lat else None,
                'longitude': float(lon) if lon else None,
                'city': city,
                'province': province,
                'max_power_w': power_w
            })
            
    return pd.DataFrame(data)

def main(
    raw_path="data/raw/chargers/chargers.xml",
    output_path="data/standardized/chargers.parquet",
    metric_crs="EPSG:25830",
    **kwargs
):
    """
    Standardizes EV chargers data:
    1. Filter ultra-fast (>100kW).
    2. Convert power to kW.
    3. Group by site and location.
    4. Project to metric CRS.
    """
    print(f"🚀 Standardizing Chargers from {raw_path}...")
    
    # 1. Parse XML
    df = parse_xml(raw_path)
    if df.empty:
        print("Warning: Parsed dataframe is empty.")
        return

    # 2. Filter & Transform Power
    # Convert Watts to kW
    df['max_power_kw'] = df['max_power_w'] / 1000.0
    
    # Filter only ultra-fast (> 100kW)
    print(" - Filtering ultra-fast chargers (> 100kW)...")
    df = df[df['max_power_kw'] > 100].copy()
    
    if df.empty:
        print("No chargers found matching >100kW threshold.")
        return

    # 3. Clean & Convert to GeoPandas
    df = df.dropna(subset=['latitude', 'longitude'])
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # 4. Project to Metric CRS
    print(f" - Projecting to {metric_crs}...")
    gdf = gdf.to_crs(metric_crs)
    
    # 5. Group by Site & Location
    # We group by site_id and geometry to consolidate chargers at the same location
    print(" - Grouping by site and location...")
    # Add a string representation of geometry for grouping
    gdf['geom_str'] = gdf.geometry.apply(lambda x: x.wkt)
    
    grouped = gdf.groupby(['site_id', 'geom_str']).agg({
        'site_name': 'first',
        'city': 'first',
        'province': 'first',
        'max_power_kw': 'max',
        'geometry': 'first',
        'site_id': 'count' # Use this as charger_count
    }).rename(columns={'site_id': 'charger_count'})
    
    # Reset index and drop helper
    gdf_final = gpd.GeoDataFrame(grouped.reset_index(drop=True), crs=metric_crs)
    
    # 6. Save
    print(f" - Saving to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_final.to_parquet(output_path)
    
    print(f"✨ SUCCESS: Chargers standardized ({len(gdf_final)} sites).")

if __name__ == "__main__":
    main()
