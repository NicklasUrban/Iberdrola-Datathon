import geopandas as gpd
import pandas as pd
import numpy as np
import math


def size_site(aadt, ev_share=0.15, avg_session_min=20.0,
              peak_hour_frac=0.10, trip_charge_frac=0.15,
              utilization=0.40, kw_per_stall=150,
              min_stalls=4, max_stalls=12, headroom=0.15):
    """Return (stalls, site_kw) for a given AADT. Stalls clamped to [min, max]."""
    peak_ev_needing_charge = aadt * ev_share * peak_hour_frac * trip_charge_frac
    sessions_per_stall_per_hour = 60.0 / avg_session_min * utilization
    raw_stalls = math.ceil(peak_ev_needing_charge / sessions_per_stall_per_hour)
    stalls = max(min_stalls, min(max_stalls, raw_stalls))
    site_kw = stalls * kw_per_stall * (1.0 + headroom)
    return stalls, site_kw


def _snap_chargers_to_backbone(gdf_chargers, gdf_points, max_snap_m=500.0):
    """
    Snap each existing charger to the nearest backbone point, keeping only
    chargers within `max_snap_m` of the backbone. Returns a DataFrame with
    columns: site_id, backbone_id, m_ref, dist_snap_m, geometry.
    """
    if gdf_chargers.crs != gdf_points.crs:
        gdf_chargers = gdf_chargers.to_crs(gdf_points.crs)

    snapped = gpd.sjoin_nearest(
        gdf_chargers[['site_id', 'max_power_kw', 'geometry']],
        gdf_points[['backbone_id', 'm_ref', 'point_id', 'geometry']],
        how='left',
        distance_col='dist_snap_m',
        max_distance=max_snap_m,
    ).dropna(subset=['backbone_id']).drop(columns=['index_right'], errors='ignore')

    # If a charger snaps to multiple equidistant points, keep one
    snapped = snapped.drop_duplicates(subset=['site_id'])
    return snapped


def _fill_gap(group, gap_start_m, gap_end_m, max_spacing_m,
              gas_anchor_m, ev_share, kw_per_stall,
              min_stalls, max_stalls, backbone_id):
    """
    Place new sites within a gap (gap_start_m, gap_end_m] on a single backbone
    such that no sub-gap exceeds max_spacing_m. Uses priority + gas anchoring
    within each 60 km sub-window.
    """
    new_sites = []
    cur = gap_start_m
    while (gap_end_m - cur) > max_spacing_m:
        window_upper = min(cur + max_spacing_m, gap_end_m)
        window = group[(group['m_ref'] > cur) & (group['m_ref'] <= window_upper)]
        if window.empty:
            break

        gas_candidates = window[window['dist_gas_station_m'].fillna(np.inf) <= gas_anchor_m]
        pool = gas_candidates if not gas_candidates.empty else window
        best = pool.loc[pool['priority'].idxmax()]

        stalls, site_kw = size_site(
            best['total_max'], ev_share=ev_share,
            kw_per_stall=kw_per_stall,
            min_stalls=min_stalls, max_stalls=max_stalls,
        )
        nearest_cap = best['capacity_kw'] if pd.notna(best['capacity_kw']) else 0.0
        is_gas = bool(pd.notna(best['dist_gas_station_m'])
                      and best['dist_gas_station_m'] <= gas_anchor_m)

        new_sites.append({
            'site_point_id': best['point_id'],
            'backbone_id': backbone_id,
            'm_ref': best['m_ref'],
            'aadt': best['total_max'],
            'dist_to_prev_charger_m': best['dist_charger_m'],
            'dist_to_gas_m': best['dist_gas_station_m'],
            'colocate_with_gas': is_gas,
            'stalls': stalls,
            'required_kw': round(site_kw, 0),
            'substation_capacity_kw': float(nearest_cap),
            'grid_ok': bool(nearest_cap >= site_kw),
            'priority': best['priority'],
            'is_existing': False,
            'existing_charger_id': None,
            'existing_charger_kw': np.nan,
            'geometry': best['geometry'],
        })
        cur = best['m_ref']
    return new_sites


def select_corridor_sites(
    gdf_points: gpd.GeoDataFrame,
    gdf_chargers: gpd.GeoDataFrame | None = None,
    max_spacing_m: float = 60_000,
    ev_share: float = 0.15,
    kw_per_stall: int = 150,
    min_stalls: int = 4,
    max_stalls: int = 12,
    gas_anchor_m: float = 150.0,
    gas_merge_bonus: float = 0.20,
    ultra_fast_kw_threshold: float = 150.0,
    existing_snap_m: float = 500.0,
) -> gpd.GeoDataFrame:
    """
    Place ultra-fast charging sites along each backbone such that consecutive
    sites are no more than `max_spacing_m` apart, measured along the road via
    `m_ref`. Existing ultra-fast chargers (max_power_kw >= ultra_fast_kw_threshold)
    are snapped onto the backbone and counted as pre-placed anchors; new sites
    only fill gaps that remain > max_spacing_m.

    Returns a GeoDataFrame of sites with `is_existing` flag distinguishing
    existing chargers from proposed new sites.
    """
    required = {'backbone_id', 'point_idx', 'point_id', 'm_ref',
                'total_max', 'dist_charger_m', 'dist_gas_station_m', 'capacity_kw'}
    missing = required - set(gdf_points.columns)
    if missing:
        raise ValueError(f"gdf_points missing columns: {missing}")

    df = gdf_points.copy()

    # Priority score
    tmax = df['total_max'].clip(lower=0)
    dmax = df['dist_charger_m'].fillna(df['dist_charger_m'].max())
    cmax = df['capacity_kw'].fillna(0).clip(lower=0)
    df['priority'] = (
        (tmax / (tmax.max() or 1.0)) *
        (dmax / (dmax.max() or 1.0)) *
        (cmax / (cmax.max() or 1.0))
    )
    gas_close = df['dist_gas_station_m'].fillna(np.inf) <= gas_anchor_m
    df.loc[gas_close, 'priority'] *= (1.0 + gas_merge_bonus)

    # Snap existing ultra-fast chargers to backbone
    snapped = None
    if gdf_chargers is not None and len(gdf_chargers):
        ultra = gdf_chargers[gdf_chargers['max_power_kw'] >= ultra_fast_kw_threshold].copy()
        if len(ultra):
            snapped = _snap_chargers_to_backbone(ultra, df, max_snap_m=existing_snap_m)
            print(f" - Snapped {len(snapped)}/{len(ultra)} existing ultra-fast chargers "
                  f"(>= {ultra_fast_kw_threshold} kW) onto backbone.")

    rows = []
    for bid, group in df.sort_values(['backbone_id', 'm_ref']).groupby('backbone_id'):
        group = group.reset_index(drop=True)
        backbone_len = float(group['m_ref'].max())

        # Collect existing anchors on this backbone
        if snapped is not None:
            existing_on_bb = snapped[snapped['backbone_id'] == bid].sort_values('m_ref')
        else:
            existing_on_bb = pd.DataFrame(columns=['m_ref', 'site_id', 'max_power_kw'])

        # Emit existing chargers into the result set
        for _, ex in existing_on_bb.iterrows():
            # Find the backbone point closest to the snapped m_ref for geometry/attrs
            bb_pt = group.iloc[(group['m_ref'] - ex['m_ref']).abs().idxmin()]
            rows.append({
                'site_point_id': bb_pt['point_id'],
                'backbone_id': bid,
                'm_ref': float(ex['m_ref']),
                'aadt': bb_pt['total_max'],
                'dist_to_prev_charger_m': 0.0,
                'dist_to_gas_m': bb_pt['dist_gas_station_m'],
                'colocate_with_gas': bool(pd.notna(bb_pt['dist_gas_station_m'])
                                          and bb_pt['dist_gas_station_m'] <= gas_anchor_m),
                'stalls': np.nan,
                'required_kw': np.nan,
                'substation_capacity_kw': float(bb_pt['capacity_kw']) if pd.notna(bb_pt['capacity_kw']) else 0.0,
                'grid_ok': True,
                'priority': bb_pt['priority'],
                'is_existing': True,
                'existing_charger_id': ex['site_id'],
                'existing_charger_kw': float(ex['max_power_kw']),
                'geometry': bb_pt['geometry'],
            })

        # Build anchor list: virtual 0, existing chargers, virtual backbone_len
        anchors = [0.0] + existing_on_bb['m_ref'].tolist() + [backbone_len]

        for i in range(len(anchors) - 1):
            gap = anchors[i + 1] - anchors[i]
            if gap <= max_spacing_m:
                continue
            rows.extend(_fill_gap(
                group, anchors[i], anchors[i + 1], max_spacing_m,
                gas_anchor_m=gas_anchor_m, ev_share=ev_share,
                kw_per_stall=kw_per_stall,
                min_stalls=min_stalls, max_stalls=max_stalls,
                backbone_id=bid,
            ))

    gdf_sites = gpd.GeoDataFrame(rows, geometry='geometry', crs=gdf_points.crs)
    return gdf_sites
