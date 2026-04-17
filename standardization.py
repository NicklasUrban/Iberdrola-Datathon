import sys
import os
import tomllib
import importlib.util

# Add scripts directory to path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), 'scripts')
sys.path.append(SCRIPTS_DIR)

def load_config(config_path="config.toml"):
    """Loads the central configuration."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def run_standardization_step(step_name, module_name, config_params):
    """Dynamically imports and runs a standardization script's main function with config params."""
    print(f"\n>>> RUNNING STANDARDIZATION: {step_name} ({module_name}.py)")
    
    script_path = os.path.join(SCRIPTS_DIR, f"{module_name}.py")
    if not os.path.exists(script_path):
        print(f"Error: Script {script_path} not found.")
        return False

    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'main'):
        try:
            # Pass all parameters from the TOML step block as keyword arguments
            module.main(**config_params)
            return True
        except Exception as e:
            print(f"FAILED step '{step_name}': {e}")
            import traceback
            traceback.print_exc()
            return False
    else:
        print(f"Error: Module '{module_name}' has no main() function.")
        return False

def main():
    print("=== Iberdrola Datathon: Data Standardization Orchestrator ===\n")
    
    config = load_config()
    
    # Get execution control
    exec_cfg = config.get("standardization_execution", {})
    glob_cfg = config.get("standardization_config", {})
    requested_steps = exec_cfg.get("steps", ["all"])
    
    # Define mapping and logical order
    step_map = {
        "roads": ("Roads", "standardize_roads"),
        "traffic": ("Traffic", "standardize_traffic"),
        "chargers": ("EV Chargers", "standardize_chargers"),
        "electric_capacity": ("Electric Capacity", "standardize_electric_capacity"),
        "vehicle_registrations": ("Vehicle Registrations", "standardize_vehicle_registrations"),
        "gas_stations": ("Gas Stations", "standardize_gas_stations")
    }
    
    # Determine steps to execute
    if "all" in requested_steps:
        steps_to_run = list(step_map.keys())
    else:
        steps_to_run = [s for s in requested_steps if s in step_map]

    for step_key in steps_to_run:
        step_label, module_name = step_map[step_key]
        
        # Get step-specific params from config.toml
        step_params = config.get("steps", {}).get(step_key, {}).copy()
        
        # Inject global standardization config (like metric_crs)
        step_params.update(glob_cfg)
        
        # Remove 'depends_on' as it's not a function parameter
        if 'depends_on' in step_params:
            del step_params['depends_on']

        if not run_standardization_step(step_label, module_name, step_params):
            print(f"\nStandardization ABORTED at step: {step_label}")
            sys.exit(1)

    print("\n✅ All requested standardization steps completed successfully.")
    print("Files available in: data/standardized/")

if __name__ == "__main__":
    main()
