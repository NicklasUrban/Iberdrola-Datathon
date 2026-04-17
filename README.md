# Iberdrola Datathon 2026: Route to Electrification of Mobility

## Project Overview
This project aims to optimize the deployment of electric vehicle (EV) charging infrastructure across Spain's **interurban road network**. The objective is to identify the most strategic locations for high-power charging stations by balancing mobility demand, 2027 projection scenarios, and the physical constraints of the electrical distribution grid.

---

## 🚀 Quick Start: Data Access
To ensure immediate reproducibility without needing to scrape external portals, all raw and standardized datasets are mirrored in our **GCP Data Bucket**. 

You can synchronize the official standardized "Silver Layer" directly into your local `data/` directory using our sync utility:

```bash
# Synchronize standardized datasets from GCP
python3 sync_cloud_data.py
```

> [!TIP]
> If you are starting the repository from scratch, using `sync_cloud_data.py` is the fastest way to begin analysis. It skips the ingestion and standardization phases by fetching pre-computed results.

---

## 🛠 Project Architecture & Orchestration

The project uses a modular "Medallion Architecture" driven by specialized orchestrator scripts:

### 1. Cloud Data Sync (`sync_cloud_data.py`)
The primary entry point for researchers. It fetches pre-processed standardized datasets from the project's cloud storage.
- **Source**: `https://storage.googleapis.com/iberdrola-datathon/data/standardized/`
- **Logic**: Intelligent skipping (only downloads missing files unless `force = true` in config).

### 2. Raw Acquisition (`download.py`)
Manages the ingestion of raw data from public sources and ministry portals. Use this if you want to refresh the source data from the ministries.
- **Inputs**: Ministry Portals (MITMA, DGT, CNMC).
- **Outputs**: `data/raw/<step_name>/`

### 3. Tabular Standardization (`standardization.py`)
The "Silver Layer" processor. It transforms raw formats into clean, metric-projected (**EPSG:25830**) tabular datasets.
- **Key Logic**: Advanced filtering (e.g., ultra-fast >100kW only), propulsion mapping, and spatial normalization.
- **Outputs**: `data/standardized/` (GeoParquet / Parquet).

---

## 📊 Data Catalog: Standardized Layer

All standardized files are stored in `data/standardized/`. These represent the high-quality unified inputs for strategic analysis.

| File Name | Description | Key Features |
| :--- | :--- | :--- |
| `roads.parquet` | Road backbone network. | `road_id`, `length_m`, metric geometry. |
| `traffic.parquet` | Longitudinal traffic demand. | Joined geometry + daily `total` and `short` trip metrics. |
| `chargers.parquet` | High-power infrastructure. | Filtered for **>100kW**; grouped by site. |
| `electric_capacity.parquet` | Grid hosting capacity. | Unified Iberdrola, Endesa, and Viesgo maps in **kW**. |
| `vehicle_registrations.parquet` | Filtered DGT Registrations. | Filtered for passenger cars and active propulsions. |
| `gas_stations.parquet` | Fuel station network. | Standardized English attributes and IDs. |

---

## 📓 Research & Analysis
Advanced analysis is performed in the `notebooks/` and legacy `scripts/`:
- `analyze_charging_sites_proximity.py`: Links sites to road segments.
- `analyze_segment_intervals.py`: Calculates distance-based gaps ("Range Anxiety").
- `EV_forecast.ipynb`: Time-series projections to 2027.

---

## 🛠 Reproducibility & Environment

This project uses `uv` for high-performance dependency management.

### 1. Environment Setup
```bash
# Sync dependencies and create .venv
uv sync
```

### 2. Configuration (`config.toml`)
All orchestrators are controlled via the `config.toml` file. This allows you to:
- **`cloud_sync`**: Toggle `force` download of cloud data.
- **`standardization_execution`**: Select specific steps to rerun.
- **`standardization_config`**: Centralize parameters like `metric_crs`.
- **`steps`**: Modify source paths or processing parameters without touching code.

---

## 📅 Final Deliverables (March 2026 Strategy)
- **KPI Scorecard**: Global impact metrics of the proposed station network.
- **Optimal Deployment Map**: Coordinates and charger counts for 2027 readiness.
- **Grid Strategy**: Roadmap for electrical infrastructure reinforcements.
