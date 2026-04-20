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
EV_TRAFFIC_PCT = 0.15
NEED_CHARGE_PCT = 0.20
SESSIONS_PER_CHARGER = 24
MIN_CHARGERS_PER_SITE = 4
MAX_CHARGERS_PER_SITE = 30
MIN_SUPPLY_DEMAND_RATIO = 1.0

# Penalties for Slacks (Deficit Tracking)
PENALTY_COVERAGE = 1e6
PENALTY_SUPPLY = 1e3 # Per charging session missed

# Performance: NO SAMPLING (Full precision)
SAMPLING_STEP = 1 

def load_data():
    """Loads backbone, existing chargers, and gas station candidates."""
    print("📂 Loading datasets...")
    backbone_path = "data/processed/backbone_foundation.parquet"
    chargers_path = "data/standardized/chargers.parquet"
    gas_path = "data/standardized/gas_stations.parquet"
    
    gdf_backbone = gpd.read_parquet(backbone_path)
    gdf_chargers = gpd.read_parquet(chargers_path)
    gdf_gas = gpd.read_parquet(gas_path)
    
    if gdf_backbone.crs.is_geographic:
        gdf_backbone = gdf_backbone.to_crs(epsg=3042)
    for gdf in [gdf_chargers, gdf_gas]:
        if gdf.crs != gdf_backbone.crs:
            gdf.to_crs(gdf_backbone.crs, inplace=True)
            
    gdf_backbone['demand_weight'] = gdf_backbone['total_max'] * EV_TRAFFIC_PCT * NEED_CHARGE_PCT
    return gdf_backbone, gdf_chargers, gdf_gas

def generate_smart_candidates(gdf_backbone, gdf_chargers, gdf_gas):
    """Generates greenfield candidates for both dead zones and supply gap areas."""
    print("📍 Analyzing coverage and supply gaps...")
    
    candidates = []
    for _, row in gdf_chargers.iterrows():
        candidates.append({'site_id': row['site_id'], 'type': 'existing', 'initial_n': row['charger_count'], 'geometry': row.geometry})
    for _, row in gdf_gas.iterrows():
        candidates.append({'site_id': row['station_id'], 'type': 'gas', 'initial_n': 0, 'geometry': row.geometry})
    df_cand = pd.DataFrame(candidates)
    
    cand_coords = list(zip([g.x for g in df_cand.geometry], [g.y for g in df_cand.geometry]))
    tree = KDTree(cand_coords)
    backbone_coords = list(zip(gdf_backbone.geometry.x, gdf_backbone.geometry.y))
    
    print("   Detecting coverage gaps...")
    dists, _ = tree.query(backbone_coords, k=1, distance_upper_bound=COVERAGE_THRESHOLD_M)
    print("   Detecting supply gaps...")
    neighbor_indices = tree.query_ball_point(backbone_coords, COVERAGE_THRESHOLD_M)
    
    dead_mask = np.isinf(dists)
    max_neighbor_supply = np.array([len(indices) * MAX_CHARGERS_PER_SITE * SESSIONS_PER_CHARGER for indices in neighbor_indices])
    gap_mask = gdf_backbone['demand_weight'] > max_neighbor_supply
    
    combined_mask = dead_mask | gap_mask
    greenfield_points = gdf_backbone[combined_mask].copy()
    
    if not greenfield_points.empty:
        print(f"   Generating {len(greenfield_points)} Smart Greenfield candidates...")
        gf = []
        for _, row in greenfield_points.iterrows():
            gf.append({'site_id': f"GF_{row['point_id']}", 'type': 'greenfield', 'initial_n': 0, 'geometry': row.geometry})
        df_cand = pd.concat([df_cand, pd.DataFrame(gf)], ignore_index=True)
        
    return df_cand

def build_constraints_chunk(chunk_start, chunk_end, neighbor_indices_chunk, demands_chunk, M, B):
    """Helper to build constraints for a chunk of the backbone in parallel."""
    rows, cols, vals = [], [], []
    c_lb, c_ub = [], []
    
    for i, neighbors in enumerate(neighbor_indices_chunk):
        point_idx = chunk_start + i
        demand_i = demands_chunk[i]
        
        sc_idx = 2 * M + point_idx
        ss_idx = 2 * M + B + point_idx
        
        # Row 1: Coverage (sum x + sc >= 1)
        # We'll use a local row offset and then shift it in the main thread? 
        # Easier: return everything relative to backbone index.
        row_coverage = point_idx * 2
        row_supply = point_idx * 2 + 1
        
        for j in neighbors:
            rows.append(row_coverage); cols.append(j); vals.append(1)
        rows.append(row_coverage); cols.append(sc_idx); vals.append(1)
        c_lb.append(1); c_ub.append(np.inf)
        
        # Row 2: Supply (sum 24n + ss >= demand)
        for j in neighbors:
            rows.append(row_supply); cols.append(j+M); vals.append(SESSIONS_PER_CHARGER)
        rows.append(row_supply); cols.append(ss_idx); vals.append(1)
        c_lb.append(demand_i * MIN_SUPPLY_DEMAND_RATIO); c_ub.append(np.inf)
        
    return rows, cols, vals, c_lb, c_ub

def solve_linear_optimization(gdf_backbone, df_cand):
    """Solves the Linear Relaxation using Sparse Matrices and Parallel Construction."""
    M = len(df_cand)
    B = len(gdf_backbone)
    print(f"🧠 Formulating Linear Relaxation (Vars: {2*M + 2*B}, Backbone: {B})...")
    
    # 1. Variables Vector & integrality (0 = continuous for speed)
    integrality = np.zeros(2 * M + 2 * B) 
    
    lb = np.zeros(2 * M + 2 * B)
    ub = np.zeros(2 * M + 2 * B)
    for i, row in df_cand.iterrows():
        if row['type'] == 'existing':
            lb[i], ub[i] = 1, 1 
            lb[i+M], ub[i+M] = row['initial_n'], MAX_CHARGERS_PER_SITE
        else:
            ub[i] = 1 
            ub[i+M] = MAX_CHARGERS_PER_SITE
    ub[2*M:] = np.inf # Slacks
    
    # 2. Objective
    c = np.zeros(2 * M + 2 * B)
    for i, row in df_cand.iterrows():
        if row['type'] != 'existing': c[i] = 1.0 
        c[i+M] = 0.001 
    c[2*M : 2*M+B] = PENALTY_COVERAGE
    c[2*M+B : 2*M+2*B] = PENALTY_SUPPLY
    
    # 3. Parallel Constraint Generation
    print(f"   Building spatial mapping for {B} points in parallel...")
    cand_coords = list(zip([g.x for g in df_cand.geometry], [g.y for g in df_cand.geometry]))
    tree = KDTree(cand_coords)
    backbone_coords = list(zip(gdf_backbone.geometry.x, gdf_backbone.geometry.y))
    neighbor_indices = tree.query_ball_point(backbone_coords, COVERAGE_THRESHOLD_M)
    
    num_chunks = 8
    chunks = np.array_split(np.arange(B), num_chunks)
    
    results = Parallel(n_jobs=-1)(
        delayed(build_constraints_chunk)(
            chunk[0], chunk[-1] + 1, 
            [neighbor_indices[i] for i in chunk], 
            gdf_backbone.iloc[chunk]['demand_weight'].tolist(),
            M, B
        ) for chunk in chunks
    )
    
    # Flatten parallel results
    all_rows, all_cols, all_vals, all_c_lb, all_c_ub = [], [], [], [], []
    # Offsetting row indices from parallel chunks is not needed because we used point_idx*2
    for r, cl, v, clb, cub in results:
        all_rows.extend(r); all_cols.extend(cl); all_vals.extend(v)
        all_c_lb.extend(clb); all_c_ub.extend(cub)
        
    # Offset spatial rows to come AFTER coupling constraints
    spatial_row_offset = 2 * M
    all_rows = [r + spatial_row_offset for r in all_rows]
    
    # Add Coupling: 4x <= n, n <= 30x
    coupling_rows, coupling_cols, coupling_vals = [], [], []
    coupling_lb, coupling_ub = [], []
    for i in range(M):
        r1, r2 = 2*i, 2*i+1
        # 4x - n <= 0
        coupling_rows.extend([r1, r1]); coupling_cols.extend([i, i+M]); coupling_vals.extend([4, -1]); coupling_lb.append(-np.inf); coupling_ub.append(0)
        # n - 30x <= 0
        coupling_rows.extend([r2, r2]); coupling_cols.extend([i, i+M]); coupling_vals.extend([-MAX_CHARGERS_PER_SITE, 1]); coupling_lb.append(-np.inf); coupling_ub.append(0)
        
    A_rows = coupling_rows + all_rows
    A_cols = coupling_cols + all_cols
    A_vals = coupling_vals + all_vals
    A = csc_matrix((A_vals, (A_rows, A_cols)), shape=(spatial_row_offset + 2 * B, 2 * M + 2 * B))
    
    final_lb = coupling_lb + all_c_lb
    final_ub = coupling_ub + all_c_ub
    
    print("🚀 Solving Linear Relaxation (HiGHS)...")
    t0 = time.time()
    res = milp(c=c, bounds=Bounds(lb, ub), constraints=LinearConstraint(A, final_lb, final_ub), integrality=integrality)
    
    if not res.success:
        print(f"   ⚠️ ERROR: {res.message}")
        return df_cand, 0, 0
    
    print(f"   Optimization Successful! (Time: {time.time()-t0:.2f}s)")
    
    # 4. Result Extraction & Heuristic Rounding
    df_cand['is_open'] = (res.x[:M] > 1e-5).astype(int) # Round up tiny fractional uses
    df_cand['final_n'] = np.ceil(res.x[M:2*M]).astype(int) # Round up chargers
    df_cand['added_chargers'] = df_cand['final_n'] - df_cand['initial_n']
    
    total_demand = gdf_backbone['demand_weight'].sum()
    total_deficit = res.x[2*M+B : 2*M+2*B].sum()
    sat_pct = max(0, 100 * (1 - total_deficit / total_demand))
    
    return df_cand, sat_pct, total_deficit

def report(df_res, sat_pct, deficit):
    print("\n📊 --- FINAL OPTIMIZATION REPORT (FULL SCALE) ---")
    gs = df_res[df_res['type'] == 'gas']
    gf = df_res[df_res['type'] == 'greenfield']
    
    print(f"Number of new charging stations created = {int(gs[gs.is_open>0.5].shape[0] + gf[gf.is_open>0.5].shape[0])}")
    print(f"Number of new chargers created = {int(df_res.added_chargers.sum())}")
    print(f"Percent demand satisfied = {sat_pct:.2f}%")
    print(f"Max capacity sites (30 chargers) = {int(df_res[df_res.final_n >= 30].shape[0])}")
    print(f"Residual deficit (sessions/day) = {int(deficit)}")
    print("--------------------------------------------------\n")

def main():
    gdf_backbone, gdf_chargers, gdf_gas = load_data()
    df_cand = generate_smart_candidates(gdf_backbone, gdf_chargers, gdf_gas)
    df_res, sat_pct, deficit = solve_linear_optimization(gdf_backbone, df_cand)
    report(df_res, sat_pct, deficit)
    
    gdf_res = gpd.GeoDataFrame(df_res, geometry='geometry', crs=gdf_backbone.crs)
    os.makedirs("data/processed", exist_ok=True)
    gdf_res.to_parquet("data/processed/optimized_sites.parquet")
    print("✨ SUCCESS: Saved results to data/processed/optimized_sites.parquet")

if __name__ == "__main__":
    main()
