import os
import sys
import tomllib
import requests

def load_config(config_path="config.toml"):
    """Loads the central configuration."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def download_file(url, local_path, force=False):
    """Downloads a file from a URL to a local path."""
    if os.path.exists(local_path) and not force:
        print(f" - Skipping {os.path.basename(local_path)} (already exists).")
        return True
    
    print(f" - Downloading {os.path.basename(local_path)} from cloud...")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"   Error downloading {url}: {e}")
        return False

def sync_standardized_data(config_path="config.toml"):
    """
    Synchronizes standardized data from GCP bucket based on config.toml.
    Can be called from a notebook or CLI.
    """
    print("=== Iberdrola Datathon: Cloud Data Sync ===\n")
    
    config = load_config(config_path)
    cloud_cfg = config.get("cloud_sync", {})
    base_url = cloud_cfg.get("base_url")
    force = cloud_cfg.get("force", False)
    
    if not base_url:
        print("Error: [cloud_sync].base_url not found in config.toml")
        return False

    # Standardized steps from which to derive file names
    standard_steps = ["roads", "traffic", "chargers", "electric_capacity", "vehicle_registrations", "gas_stations"]
    
    success_count = 0
    fail_count = 0
    
    for step in standard_steps:
        step_cfg = config.get("steps", {}).get(step, {})
        local_path = step_cfg.get("output_path")
        
        if not local_path:
            # Fallback to default if not in config
            local_path = f"data/standardized/{step}.parquet"
            
        filename = os.path.basename(local_path)
        remote_url = f"{base_url}/{filename}"
        
        if download_file(remote_url, local_path, force=force):
            success_count += 1
        else:
            fail_count += 1
            
    print(f"\n✅ Sync complete: {success_count} succeeded, {fail_count} failed.")
    return fail_count == 0

if __name__ == "__main__":
    sync_standardized_data()
