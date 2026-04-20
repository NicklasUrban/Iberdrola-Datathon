import pandas as pd
import geopandas as gpd
import numpy as np
from scipy.spatial import KDTree
from scipy.optimize import milp, LinearConstraint, Bounds
from scipy.sparse import csc_matrix
from joblib import Parallel, delayed
import os
import time

# --- CONFIGURABLE PARAMETERS ---
COVERAGE_THRESHOLD_M = 20000
SUBSTATION_THRESHOLD_M = 10000
EV_TRAFFIC_PCT = 0.15
NEED_CHARGE_PCT = 0.20
SESSIONS_PER_CHARGER = 24
MAX_CHARGERS_PER_SITE = 30
POWER_PER_CHARGER_KW = 150

# Penalties
PENALTY_COVERAGE = 1e6
PENALTY_SUPPLY = 1e4
PENALTY_GRID_UPGRADE = 1e2 # Lower than coverage -> prefers to upgrade grid than leave road gaps

SAMPLING_STEP = 25 # 5km intervals for speed

def load_data():
    """Loads all necessary datasets for grid-aware optimization."""
    print("📂 Loading datasets...")
    backbone_path = "data/processed/backbone_foundation.parquet"
    chargers_path = "data/standardized/chargers.parquet"
    gas_path = "data/standardized/gas_stations.parquet"
    grid_path = "data/standardized/electric_capacity.parquet"
    
    gdf_backbone = gpd.read_parquet(backbone_path)
    gdf_chargers = gpd.read_parquet(chargers_path)
    gdf_gas = gpd.read_parquet(gas_path)
    gdf_grid = gpd.read_parquet(grid_path)
    
    if gdf_backbone.crs.is_geographic:
        gdf_backbone = gdf_backbone.to_crs(epsg=3042)
    for gdf in [gdf_chargers, gdf_gas, gdf_grid]:
        if gdf.crs != gdf_backbone.crs:
            gdf.to_crs(gdf_backbone.crs, inplace=True)
            
    gdf_backbone['demand_weight'] = gdf_backbone['total_max'] * EV_TRAFFIC_PCT * NEED_CHARGE_PCT
    return gdf_backbone, gdf_chargers, gdf_gas, gdf_grid

def generate_smart_candidates(gdf_backbone, gdf_chargers, gdf_gas):
    """Generates greenfield candidates for both dead zones and supply gap areas."""
    print("📍 Generating Smart Candidates...")
    candidates = []
    for _, row in gdf_chargers.iterrows():
        candidates.append({'site_id': row['site_id'], 'type': 'existing', 'initial_n': row['charger_count'], 'geometry': row.geometry})
    for _, row in gdf_gas.iterrows():
        candidates.append({'site_id': row['station_id'], 'type': 'gas', 'initial_n': 0, 'geometry': row.geometry})
    df_cand = pd.DataFrame(candidates)
    
    cand_coords = list(zip([g.x for g in df_cand.geometry], [g.y for g in df_cand.geometry]))
    tree = KDTree(cand_coords)
    backbone_coords = list(zip(gdf_backbone.geometry.x, gdf_backbone.geometry.y))
    
    dists, _ = tree.query(backbone_coords, k=1, distance_upper_bound=COVERAGE_THRESHOLD_M)
    neighbor_indices = tree.query_ball_point(backbone_coords, COVERAGE_THRESHOLD_M)
    
    dead_mask = np.isinf(dists)
    max_neighbor_supply = np.array([len(indices) * MAX_CHARGERS_PER_SITE * SESSIONS_PER_CHARGER for indices in neighbor_indices])
    gap_mask = gdf_backbone['demand_weight'] > max_neighbor_supply
    
    greenfield_points = gdf_backbone[dead_mask | gap_mask].copy()
    if not greenfield_points.empty:
        gf = []
        for _, row in greenfield_points.iterrows():
            gf.append({'site_id': f"GF_{row['point_id']}", 'type': 'greenfield', 'initial_n': 0, 'geometry': row.geometry})
        df_cand = pd.concat([df_cand, pd.DataFrame(gf)], ignore_index=True)
        
    return df_cand

def build_road_constraints_chunk(chunk_start, chunk_end, neighbors_chunk, demands_chunk, M, B):
    """Builds the spatial road constraints in parallel."""
    rows, cols, vals = [], [], []
    c_lb, c_ub = [], []
    for i, neighbors in enumerate(neighbors_chunk):
        pt_idx = chunk_start + i
        row_cov, row_sup = pt_idx * 2, pt_idx * 2 + 1
        sc_idx, ss_idx = 2 * M + pt_idx, 2 * M + B + pt_idx
        # Coverage
        for j in neighbors: rows.append(row_cov); cols.append(j); vals.append(1)
        rows.append(row_cov); cols.append(sc_idx); vals.append(1); c_lb.append(1); c_ub.append(np.inf)
        # Supply
        for j in neighbors: rows.append(row_sup); cols.append(j+M); vals.append(SESSIONS_PER_CHARGER)
        rows.append(row_sup); cols.append(ss_idx); vals.append(1); c_lb.append(demands_chunk[i]); c_ub.append(np.inf)
    return rows, cols, vals, c_lb, c_ub

def solve_grid_aware_optimization(gdf_backbone, df_cand, gdf_grid):
    """Unified solver with Grid Capacity constraints and Upgrade Slacks."""
    gdf_sampled = gdf_backbone.iloc[::SAMPLING_STEP].copy()
    M, B, K = len(df_cand), len(gdf_sampled), len(gdf_grid)
    
    print(f"🧠 Formulating Grid-Aware optimization (Vars: {2*M + 2*B + K})...")
    
    # 1. Assignment: Sites to Nearest Substations
    sub_coords = list(zip([g.x for g in gdf_grid.geometry], [g.y for g in gdf_grid.geometry]))
    cand_coords = list(zip([g.x for g in df_cand.geometry], [g.y for g in df_cand.geometry]))
    sub_tree = KDTree(sub_coords)
    dists_sub, indices_sub = sub_tree.query(cand_coords, k=1, distance_upper_bound=SUBSTATION_THRESHOLD_M)
    df_cand['substation_idx'] = indices_sub
    df_cand['dist_to_grid'] = dists_sub
    
    # 2. Linear Relaxation Variables
    integrality = np.zeros(2 * M + 2 * B + K)
    lb = np.zeros(2 * M + 2 * B + K)
    ub = np.zeros(2 * M + 2 * B + K)
    for i, row in df_cand.iterrows():
        if np.isinf(row['dist_to_grid']): ub[i] = 0; ub[i+M] = 0 # No Grid Access sites are banned
        elif row['type'] == 'existing': lb[i], ub[i] = 1, 1; lb[i+M], ub[i+M] = row['initial_n'], MAX_CHARGERS_PER_SITE
        else: ub[i] = 1; ub[i+M] = MAX_CHARGERS_PER_SITE
    ub[2*M:] = np.inf # Slacks
    
    # 3. Objective
    c = np.zeros(2 * M + 2 * B + K)
    for i, row in df_cand.iterrows():
        if row['type'] != 'existing': c[i] = 1.0 # Min new sites
        c[i+M] = 0.001 
    c[2*M : 2*M+B] = PENALTY_COVERAGE
    c[2*M+B : 2*M+2*B] = PENALTY_SUPPLY
    c[2*M+2*B : ] = PENALTY_GRID_UPGRADE
    
    # 4. Constraints Construction
    rows, cols, vals = [], [], []
    c_lb, c_ub = [], []
    curr_row = 0
    
    # 4a. Road (Parallel)
    print("   Building road constraints...")
    road_tree = KDTree(cand_coords)
    backbone_coords = list(zip(gdf_sampled.geometry.x, gdf_sampled.geometry.y))
    neighbors_list = road_tree.query_ball_point(backbone_coords, COVERAGE_THRESHOLD_M)
    
    res_parallel = Parallel(n_jobs=-1)(delayed(build_road_constraints_chunk)(
        chunk[0], chunk[-1]+1, [neighbors_list[i] for i in chunk], gdf_sampled.iloc[chunk]['demand_weight'].tolist(), M, B
    ) for chunk in np.array_split(np.arange(B), 8))
    
    for r, cl, v, clb, cub in res_parallel:
        rows.extend([x + curr_row for x in r]); cols.extend(cl); vals.extend(v); c_lb.extend(clb); c_ub.extend(cub)
    curr_row += 2 * B
    
    # 4b. Coupling: 4x-n <= 0, n-30x <=0
    for i in range(M):
        rows.extend([curr_row, curr_row]); cols.extend([i, i+M]); vals.extend([4, -1]); c_lb.append(-np.inf); c_ub.append(0); curr_row += 1
        rows.extend([curr_row, curr_row]); cols.extend([i, i+M]); vals.extend([-MAX_CHARGERS_PER_SITE, 1]); c_lb.append(-np.inf); c_ub.append(0); curr_row += 1
        
    # 4c. NEW Grid: sum(150(n - init)) <= cap + slack
    print("   Building grid constraints...")
    # Group sites by substation_idx
    sub_groups = {}
    for i, row in df_cand.iterrows():
        if np.isinf(row['dist_to_grid']): continue
        s_idx = int(row['substation_idx'])
        if s_idx not in sub_groups: sub_groups[s_idx] = []
        sub_groups[s_idx].append(i)
        
    for s_idx, site_indices in sub_groups.items():
        slack_idx = 2 * M + 2 * B + s_idx
        cap_k = gdf_grid.iloc[s_idx]['capacity_kw']
        init_power_sum = sum([df_cand.iloc[i]['initial_n'] * POWER_PER_CHARGER_KW for i in site_indices])
        
        # sum(150n) - slack <= cap + init_power
        for i in site_indices: rows.append(curr_row); cols.append(i+M); vals.append(POWER_PER_CHARGER_KW)
        rows.append(curr_row); cols.append(slack_idx); vals.append(-1)
        c_lb.append(-np.inf); c_ub.append(cap_k + init_power_sum); curr_row += 1
        
    A = csc_matrix((vals, (rows, cols)), shape=(curr_row, 2 * M + 2 * B + K))
    
    print("🚀 Solving Grid-Aware Linear Relaxation...")
    t0 = time.time()
    res = milp(c=c, bounds=Bounds(lb, ub), constraints=LinearConstraint(A, c_lb, c_ub), integrality=integrality)
    print(f"   Optimization Successful! (Time: {time.time()-t0:.2f}s)")
    
    # 5. Extract results
    df_cand['is_open'] = (res.x[:M] > 1e-5).astype(int)
    df_cand['final_n'] = np.ceil(res.x[M:2*M]).astype(int)
    df_cand['added_chargers'] = df_cand['final_n'] - df_cand['initial_n']
    
    # Extract Grid Slack
    grid_slacks = res.x[2*M+2*B:]
    
    return df_cand, grid_slacks

def report(gdf_res, grid_slacks):
    print("\n📊 --- GRID-AWARE OPTIMIZATION SUMMARY ---")
    open_sites = gdf_res[gdf_res.is_open == 1].copy()
    upgrades_needed = (grid_slacks > 1.0).sum()
    total_upgrade_kw = grid_slacks.sum()
    
    print(f"Total Stations Recommendation = {len(open_sites)}")
    print(f"Total New Chargers = {gdf_res.added_chargers.sum()}")
    print(f"Substations requiring UPGRADES = {upgrades_needed}")
    print(f"Total Grid Capacity Gap = {total_upgrade_kw:.0f} kW")
    
    # Classify sites
    gdf_res['grid_feasibility'] = 'Feasible'
    for i, row in gdf_res.iterrows():
        if np.isinf(row['dist_to_grid']): gdf_res.at[i, 'grid_feasibility'] = 'No Grid Access (>10km)'
        elif row['substation_idx'] < len(grid_slacks) and grid_slacks[int(row['substation_idx'])] > 1.0:
            gdf_res.at[i, 'grid_feasibility'] = 'Grid Upgrade Required'
            
    stats = gdf_res[gdf_res.is_open==1].grid_feasibility.value_counts()
    for s, c in stats.items(): print(f"  - {s}: {c}")
    print("-------------------------------------------\n")

def main():
    gdf_backbone, gdf_chargers, gdf_gas, gdf_grid = load_data()
    df_cand = generate_smart_candidates(gdf_backbone, gdf_chargers, gdf_gas)
    df_res, grid_slacks = solve_grid_aware_optimization(gdf_backbone, df_cand, gdf_grid)
    report(df_res, grid_slacks)
    
    gdf_res = gpd.GeoDataFrame(df_res, geometry='geometry', crs=gdf_backbone.crs)
    gdf_res.to_parquet("data/processed/grid_aware_optimized_sites.parquet")
    print("✨ SUCCESS: Saved result to data/processed/grid_aware_optimized_sites.parquet")

if __name__ == "__main__":
    main()
