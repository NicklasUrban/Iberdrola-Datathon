import geopandas as gpd
import pandas as pd
import numpy as np
import math


def size_site(aadt, ev_share=0.15, avg_session_min=20.0,
              peak_hour_frac=0.10, utilization=0.40,
              kw_per_stall=200, min_stalls=4, headroom=0.15):
    """Return (stalls, site_kw) for a given AADT under assumed EV-share scenario."""
    peak_ev_per_hour = aadt * ev_share * peak_hour_frac
    sessions_per_stall_per_hour = 60.0 / avg_session_min * utilization
    stalls = max(min_stalls, math.ceil(peak_ev_per_hour / sessions_per_stall_per_hour))
    site_kw = stalls * kw_per_stall * (1.0 + headroom)
    return stalls, site_kw


def select_corridor_sites(
    gdf_points: gpd.GeoDataFrame,
    min_spacing_m: float = 50_000,
    high_traffic_spacing_m: float = 25_000,
    high_traffic_quantile: float = 0.75,
    ev_share: float = 0.15,
    kw_per_stall: int = 200,
    min_stalls: int = 4,
    gas_merge_m: float = 150.0,
    gas_merge_bonus: float = 0.20,
    urban_exclusion_m: float | None = None,
    urban_mask_col: str | None = None,
) -> gpd.GeoDataFrame:
    """
    Walk each backbone in order and place charging sites based on a gap-constrained
    priority greedy selector.

    Expects `gdf_points` produced by create_backbone_foundation with columns:
        backbone_id, point_idx, point_id, geometry, m_ref,
        total_max, dist_charger_m, dist_gas_station_m,
        capacity_kw, dist_substation_m
    """
    required = {'backbone_id', 'point_idx', 'point_id', 'm_ref',
                'total_max', 'dist_charger_m', 'dist_gas_station_m', 'capacity_kw'}
    missing = required - set(gdf_points.columns)
    if missing:
        raise ValueError(f"gdf_points missing columns: {missing}")

    df = gdf_points.copy()

    # Apply urban exclusion if a boolean mask column is provided
    if urban_mask_col and urban_mask_col in df.columns:
        df = df[~df[urban_mask_col]].copy()

    # Normalize components
    tmax = df['total_max'].clip(lower=0)
    dmax = df['dist_charger_m'].fillna(df['dist_charger_m'].max())
    cmax = df['capacity_kw'].fillna(0).clip(lower=0)

    df['priority'] = (
        (tmax / (tmax.max() or 1.0)) *
        (dmax / (dmax.max() or 1.0)) *
        (cmax / (cmax.max() or 1.0))
    )

    # Co-location bonus
    gas_close = df['dist_gas_station_m'].fillna(np.inf) <= gas_merge_m
    df.loc[gas_close, 'priority'] *= (1.0 + gas_merge_bonus)

    high_traffic_threshold = df['total_max'].quantile(high_traffic_quantile)

    selected_rows = []
    for bid, group in df.sort_values(['backbone_id', 'm_ref']).groupby('backbone_id'):
        last_m = -math.inf
        for _, pt in group.iterrows():
            m = pt['m_ref']
            spacing = (high_traffic_spacing_m
                       if pt['total_max'] >= high_traffic_threshold
                       else min_spacing_m)
            if (m - last_m) < spacing:
                continue

            # Within the spacing window ahead, pick the local-max priority point
            window_end = m + spacing
            window = group[(group['m_ref'] >= m) & (group['m_ref'] < window_end)]
            best = window.loc[window['priority'].idxmax()]

            stalls, site_kw = size_site(best['total_max'], ev_share=ev_share,
                                        kw_per_stall=kw_per_stall,
                                        min_stalls=min_stalls)

            nearest_cap = best['capacity_kw'] if pd.notna(best['capacity_kw']) else 0
            grid_ok = nearest_cap >= site_kw

            selected_rows.append({
                'site_point_id': best['point_id'],
                'backbone_id': bid,
                'm_ref': best['m_ref'],
                'aadt': best['total_max'],
                'dist_to_prev_charger_m': best['dist_charger_m'],
                'dist_to_gas_m': best['dist_gas_station_m'],
                'colocate_with_gas': bool(best['dist_gas_station_m'] <= gas_merge_m
                                          if pd.notna(best['dist_gas_station_m']) else False),
                'stalls': stalls,
                'required_kw': round(site_kw, 0),
                'substation_capacity_kw': float(nearest_cap),
                'grid_ok': bool(grid_ok),
                'priority': best['priority'],
                'geometry': best['geometry'],
            })
            last_m = best['m_ref']

    gdf_sites = gpd.GeoDataFrame(selected_rows, geometry='geometry', crs=gdf_points.crs)
    return gdf_sites
