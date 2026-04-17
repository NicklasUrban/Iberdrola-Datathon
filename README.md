# Iberdrola Datathon 2026: Route to Electrification of Mobility

## Project Overview
This project aims to optimize the deployment of electric vehicle (EV) charging infrastructure across Spain's **interurban road network**. The objective is to identify the most strategic locations for high-power charging stations by balancing mobility demand, 2027 projection scenarios, and the physical constraints of the electrical distribution grid.

---

## 🚀 Quick Start: Data Access
To ensure immediate reproducibility without needing to scrape external portals, all raw and standardized datasets are mirrored in our **GCP Data Bucket**:

**🔗 [Cloud Data Access](https://storage.googleapis.com/iberdrola-datathon/data/)**

> [!TIP]
> If you are starting the repository from scratch, you **do not need to rerun** the download or standardization scripts. You can simply download the contents of the bucket into your local `data/` directory to begin analysis immediately.

---

## 🛠 Project Architecture & Orchestration

The project uses a modular "Medallion Architecture" driven by two specialized orchestrator scripts:

### 1. Raw Acquisition (`download.py`)
This orchestrator manages the ingestion of raw data from public sources and ministry portals. It hardcodes all source URLs and handles formatting (KMZ, XML, XLSX, ZIP, CSV).
- **Inputs**: Ministry Portals (MITMA, DGT, CNMC).
- **Outputs**: `data/raw/<step_name>/`
- **Execution**:
  ```bash
  python3 download.py
  ```

### 2. Tabular Standardization (`standardization.py`)
The "Silver Layer" of the pipeline. It transforms raw, heterogeneous formats into clean, metric-projected (**EPSG:25830**) tabular datasets.
- **Key Logic**: Advanced filtering (e.g., ultra-fast chargers only), propulsion type mapping, and spatial normalization.
- **Outputs**: `data/standardized/` (GeoParquet / Parquet).
- **Execution**:
  ```bash
  python3 standardization.py
  ```

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
Advanced analysis is performed in the `notebooks/` and legacy `scripts/` (to be refactored into the new standardization layer):
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
Both orchestrators are controlled via the `config.toml` file. This allows you to:
- **`standardization_execution`**: Select specific steps to rerun.
- **`standardization_config`**: Centralize parameters like `metric_crs`.
- **`steps`**: Modify source paths or processing parameters without touching code.

---

## 📅 Final Deliverables (March 2026 Strategy)
- **KPI Scorecard**: Global impact metrics of the proposed station network.
- **Optimal Deployment Map**: Coordinates and charger counts for 2027 readiness.
- **Grid Strategy**: Roadmap for electrical infrastructure reinforcements.
