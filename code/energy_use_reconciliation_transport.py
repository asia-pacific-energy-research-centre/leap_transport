from __future__ import annotations
import pandas as pd
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple


from energy_use_reconciliation import (
    LEAP_MEASURE_CONFIG,
    _apply_proportional_adjustment,
    get_adjustment_year_columns,
    build_branch_path,
)
from transport_branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
)
from esto_transport_data import (
    extract_esto_energy_use_for_leap_branches,
)


def _get_scalar(
    df: pd.DataFrame,
    base_year: int | str,
    path: str,
    variable: str,
    allow_missing: bool = False,
    default: Optional[float] = None,
) -> float:
    """Return a single scalar for Branch Path/Variable in base_year, with explicit erroring on duplicates."""

    series = df.loc[(df["Branch Path"] == path) & (df["Variable"] == variable), base_year]
    if series.empty:
        if allow_missing:
            return default if default is not None else 0.0
        raise ValueError(f"No values found for {variable} at {path}")
    if len(series) > 1:
        raise ValueError(f"Expected exactly one value for {variable} at {path}, found {len(series)}")
    value = float(series.iloc[0])
    if pd.isna(value):
        if allow_missing:
            return default if default is not None else 0.0
        raise ValueError(f"Value for {variable} at {path} is NaN")
    return value


##########################################################
# TRANSPORT ENERGY RECONCILIATION FRAMEWORK - MAIN FUNCTIONS
######################################################################
def _scale_series_if_present(
    df: pd.DataFrame,
    mask: pd.Series,
    year_columns: Sequence[int | str],
    scale_factor: float,
) -> None:
    """Multiply values in the provided year columns by scale_factor where mask is True."""
    if not mask.any():
        return

    for year_col in year_columns:
        if year_col not in df.columns:
            continue
        df.loc[mask, year_col] = df.loc[mask, year_col] * scale_factor
        
def transport_adjustment_fn(
    export_df: pd.DataFrame,
    base_year: int | str,
    rule: Mapping[str, object],
    scale_factor: float,
    strategies: Mapping[str, Sequence[str]],
    year_columns: Optional[Sequence[int | str]] = None,
    apply_to_future_years: bool = False,
) -> None:
    """Scale inputs used by transport energy_fn across all relevant branch paths.

    Interpretation:
      - scale_factor is the desired factor for ENERGY on this rule.
      - For Stock branches, energy ∝ (device stock) * Mileage * Fuel Economy
        for this device, so we:
          * scale device stock by f = scale_factor**(1/3) (via stock & share
            adjustments), and
          * scale Mileage and Fuel Economy for this device by the same f.
        => device-level energy scales by f^3 = scale_factor.

        - For Intensity branches (non-road):
        energy ∝ Activity_level_effective * Final Energy Intensity, where
        Activity_level_effective is derived from a total Activity Level and
        one or two levels of Activity Level “shares”. We:
          * scale the target leaf’s effective Activity Level by
            f = scale_factor**0.5 (via activity/share adjustment), and
          * scale its Final Energy Intensity by the same f.
        => leaf-level energy scales by f^2 = scale_factor.
    """
    branch_path = build_branch_path(rule["branch_tuple"], root=rule.get("root", "Demand"))
    years_to_adjust = list(year_columns) if year_columns is not None else get_adjustment_year_columns(
        export_df, base_year, include_future_years=apply_to_future_years
    )
    parts = branch_path.split("\\")
    strategy = rule.get("calculation_strategy")
    path_lower = branch_path.lower()

    if strategy == "Stock":
        # We assume this is a *device* branch under a road mode, e.g.
        #   parent_path = Demand\Transport\Passenger road
        #   mode_path   = Demand\Transport\Passenger road\Buses
        #   device_path = Demand\Transport\Passenger road\Buses\Diesel
        if len(parts) >= 3 and "road" in path_lower and "non road" not in path_lower:
            parent_path = "\\".join(parts[:-2])
            mode_path = "\\".join(parts[:-1])
            device_path = branch_path
        else:
            breakpoint()
            raise NotImplementedError(
                "Stock-based adjustment only implemented for road branches with at least three parts."
            )

        # We want energy on this device to scale by `scale_factor`.
        # energy_device ∝ Stock_device * Mileage_device * FuelEconomy_device
        # so we scale each by f = scale_factor**(1/3).
        f = scale_factor ** (1.0 / 3.0)

        # 1) Adjust stock & shares so device stock is multiplied by f,
        #    other device stocks unchanged, shares re-normalised.
        _adjust_device_stock_and_shares_exact(
            export_df,
            base_year=base_year,
            parent_path=parent_path,
            mode_path=mode_path,
            device_path=device_path,
            device_stock_factor=f,
            year_columns=years_to_adjust,
        )

        # 2) Scale mileage and fuel economy for THIS device branch by f.
        for variable in ("Mileage", "Fuel Economy"):
            _scale_series_if_present(
                export_df,
                (export_df["Branch Path"] == device_path)
                & (export_df["Variable"] == variable),
                years_to_adjust,
                f,
            )

        # Note: Device Share is already handled inside
        # _adjust_device_stock_and_shares_exact, so we do not scale it again.

    elif strategy == "Intensity":

        if len(parts) >= 4 and "non road" in path_lower:
            # Non-road intensity branches:
            #   parent_path    = total activity (e.g. ...\Non-road X)
            #   share1_path    = first-level share (e.g. ...\Non-road X\Mode)
            #   leaf_path      = this branch (e.g. ...\Non-road X\Mode\Fuel)
            parent_path = "\\".join(parts[:-2])
            share1_path = "\\".join(parts[:-1])
            leaf_path = branch_path

            # We want *energy* on this leaf to scale by `scale_factor`.
            # energy ∝ A_eff * Intensity
            # so we scale A_eff and Intensity both by f, with f^2 = scale_factor.
            f = scale_factor ** 0.5
            if 'rail' in path_lower:
                if 'electric' in path_lower:
                    if 'passenger' in path_lower:
                        breakpoint()#double check we are getting the number we expect here
            # 1) Adjust activity hierarchy so A_eff(leaf) is multiplied by f,
            #    while other leaves' activities are unchanged and shares normalised.
            _adjust_activity_and_shares_exact(
                export_df,
                base_year=base_year,
                parent_path=parent_path,
                share1_path=share1_path,
                leaf_path=leaf_path,
                leaf_activity_factor=f,
                year_columns=years_to_adjust,
            )

            if 'rail' in path_lower:
                if 'electric' in path_lower:
                    if 'passenger' in path_lower:
                        breakpoint()#double check we are getting the number we expect here
            # 2) Scale Final Energy Intensity at this leaf by f.
            _scale_series_if_present(
                export_df,
                (export_df["Branch Path"] == branch_path)
                & (export_df["Variable"] == "Final Energy Intensity"),
                years_to_adjust,
                f,
            )
            
            if 'rail' in path_lower:
                if 'electric' in path_lower:
                    if 'passenger' in path_lower:
                        breakpoint()#double check we are getting the number we expect here
        elif len(parts) < 4 and ("pipeline" in path_lower or "nonspecified" in path_lower):
            parent_path = "\\".join(parts[:-1])   # total activity
            share1_path = parent_path             # children are the share1 nodes
            leaf_path   = branch_path

            _adjust_activity_and_shares_exact(
                export_df,
                base_year=base_year,
                parent_path=parent_path,
                share1_path=share1_path,
                leaf_path=leaf_path,
                leaf_activity_factor=scale_factor,
                year_columns=years_to_adjust,
            )

            # Intensity is 1, but safe to scale
            _scale_series_if_present(
                export_df,
                (export_df["Branch Path"] == branch_path)
                & (export_df["Variable"] == "Final Energy Intensity"),
                years_to_adjust,
                scale_factor,
            )
        else:
            breakpoint()
            raise NotImplementedError(
                f"Intensity-based adjustment not implemented for branch {branch_path}"
            )
    else:
        _apply_proportional_adjustment(export_df, base_year, rule, scale_factor, strategies, years_to_adjust)
        

def transport_energy_fn(
    export_df: pd.DataFrame,
    base_year: int | str,
    rule: Mapping[str, object],
    strategies: Mapping[str, Sequence[str]],
    combination_fn: Optional[Callable[[List[pd.Series]], pd.Series]] = None,
) -> float:
    """Dispatcher for transport-specific energy calculations."""
    strategy = rule.get("calculation_strategy")
    if strategy == "Stock":
        return transport_stock_energy_fn(export_df, base_year, rule, strategies, combination_fn)
    if strategy == "Intensity":
        return transport_intensity_energy_fn(export_df, base_year, rule, strategies, combination_fn)
    breakpoint()
    raise ValueError(f"Unsupported calculation strategy '{strategy}' for rule {rule}")


###################################################################
# TRANSPORT-SPECIFIC ENERGY FUNCTIONS
####################################################################

def transport_intensity_energy_fn(
    export_df: pd.DataFrame,
    base_year: int | str,
    rule: Mapping[str, object],
    strategies: Mapping[str, Sequence[str]],
    combination_fn: Optional[Callable[[List[pd.Series]], pd.Series]] = None,
) -> float:
    """Intensity-based energy calculation compatible with reconcile_energy_use."""

    branch_path = build_branch_path(rule["branch_tuple"], root=rule.get("root", "Demand"))
    parts = branch_path.split("\\")

    # Determine activity level (with or without shares)
    if len(parts) < 4:
        # Expect pipeline or nonspecified (one activity shares)
        if "nonspecified" not in branch_path.lower() and "pipeline" not in branch_path.lower():
            # Optional debug
            breakpoint()
            raise ValueError(
                f"Branch path {branch_path} not recognised for an intensity branch with fewer than four parts."
            )
            
        branch_path_up_one = "\\".join(parts[:-1])

        activity_level = _get_scalar(export_df, base_year, branch_path_up_one, "Activity Level")
        activity_level_share1 = _get_scalar(export_df, base_year, branch_path_up_one, "Activity Level")
        
        activity_level = (activity_level * activity_level_share1) / 100  #divide by 100 to convert from percentages to shares (e.g. 25% -> 0.25)
    else:
        # Expect non-road branches with two levels of activity shares
        if "non road" not in branch_path.lower():
            # Optional debug
            breakpoint()
            raise ValueError(
                f"Branch path {branch_path} not recognised for a non-road branch with more than four parts."
            )

        branch_path_up_one = "\\".join(parts[:-1])
        branch_path_up_two = "\\".join(parts[:-2])

        activity_level = _get_scalar(export_df, base_year, branch_path_up_two, "Activity Level")
        activity_level_share1 = _get_scalar(export_df, base_year, branch_path_up_one, "Activity Level")
        activity_level_share2 = _get_scalar(export_df, base_year, branch_path, "Activity Level")
        # Convert from % to shares (e.g. 25% * 50% -> 0.125)
        activity_level = (activity_level * activity_level_share1 * activity_level_share2) / 10000

    intensity = _get_scalar(export_df, base_year, branch_path, "Final Energy Intensity")

    if intensity == 0:
        breakpoint()
        raise ValueError(f"Intensity data missing or zero for branch {branch_path}")#testing out raising error here instead of just warning
        # print(f"WARNING: intensity data missing or zero for branch {branch_path}")
        # return 0.0

    path_lower = branch_path.lower()
    if "non road" in path_lower:
        intensity_scale = LEAP_MEASURE_CONFIG["Fuel (non-road)"]["Final Energy Intensity"]["factor"]
        activity_scale = LEAP_MEASURE_CONFIG["Fuel (non-road)"]["Activity Level"]["factor"]
    elif "road" in path_lower:
        breakpoint()
        raise NotImplementedError("Road intensity-based branches not yet implemented in energy use calculation.")
    elif "pipeline" in path_lower or "nonspecified" in path_lower:
        # For these, intensity should be 1 (unitless) and activity carries the energy scaling
        if intensity != 1:
            breakpoint()
            raise ValueError(f"Expected intensity of 1 for branch {branch_path}, got {intensity}")
        intensity_scale = 1.0
        activity_scale = LEAP_MEASURE_CONFIG["Others (level 2)"]["Activity Level"]["factor"]
    else:
        breakpoint()
        raise ValueError(f"Unknown branch type in path: {branch_path}")

    intensity = intensity / intensity_scale
    activity_level = activity_level / activity_scale

    energy_use = activity_level * intensity
    if 'rail' in path_lower:
        if 'electric' in path_lower:
            if 'passenger' in path_lower:
                breakpoint()#double check we are getting the number we expect here
    return float(energy_use)


def transport_stock_energy_fn(
    export_df: pd.DataFrame,
    base_year: int | str,
    rule: Mapping[str, object],
    strategies: Mapping[str, Sequence[str]],
    combination_fn: Optional[Callable[[List[pd.Series]], pd.Series]] = None,
) -> float:
    """Stock-based energy calculation compatible with reconcile_energy_use.This may be useful for other sectors with stock-based energy calculations - the way that we have to access branch paths up x levels is tricky but important for getting the right numbers. Similarly the scaling factors from LEAP_MEASURE_CONFIG are important to ensure we are working in the right units - they will need to be adapted for other sectors. Latly it should be noted that since th unit for efficiency is mj/km, efficiency is timesed by mileage rather than divided as it would be if efficiency were in km/mj which is the standard unit... Leap things!"""

    branch_path = build_branch_path(rule["branch_tuple"], root=rule.get("root", "Demand"))
    parts = branch_path.split("\\")
    if len(parts) < 3:
        return 0.0

    branch_path_up_one = "\\".join(parts[:-1])
    branch_path_up_two = "\\".join(parts[:-2])
    stock_share = _get_scalar(export_df, base_year, branch_path_up_one, "Stock Share")
    device_share = _get_scalar(export_df, base_year, branch_path, "Device Share")
    stocks = _get_scalar(export_df, base_year, branch_path_up_two, "Stock")
    mileage = _get_scalar(export_df, base_year, branch_path, "Mileage")
    efficiency = _get_scalar(export_df, base_year, branch_path, "Fuel Economy")

    if efficiency == 0:
        print(f"WARNING: efficiency data missing or zero for branch {branch_path}")
        return 0.0

    efficiency_safe = efficiency

    path_lower = branch_path.lower()
    if "non road" in path_lower:
        breakpoint()
        raise NotImplementedError("Non-road stock-based branches not yet implemented in energy use calculation.")
    if "road" in path_lower:
        efficiency_scale = LEAP_MEASURE_CONFIG["Fuel (road)"]["Final On-Road Fuel Economy"]["factor"]
        mileage_scale = LEAP_MEASURE_CONFIG["Fuel (road)"]["Mileage"]["factor"]
        stock_scale = LEAP_MEASURE_CONFIG["Vehicle type (road)"]["Stock"]["factor"]
    else:
        breakpoint()
        raise ValueError(f"Unknown branch type in path: {branch_path}")

    stocks_new = (stocks * stock_share * device_share) / 10000
    stocks_new /= stock_scale
    mileage /= mileage_scale
    efficiency_safe /= efficiency_scale

    energy_use = stocks_new * mileage * efficiency_safe
    return float(energy_use)


###################################################################
# TRANSPORT-SPECIFIC ADJUSTMENT FUNCTIONS
####################################################################

def _adjust_device_stock_and_shares_exact(
    df: pd.DataFrame,
    base_year: int | str,
    parent_path: str,
    mode_path: str,
    device_path: str,
    device_stock_factor: float,
    year_columns: Optional[Sequence[int | str]] = None,
) -> None:
    r"""
    Adjust Stock at `parent_path` (e.g. Demand\\Transport\\Passenger road),
    Stock Share of all modes under it, and Device Share of all devices under
    each mode, so that:

    - The stock of the target device (e.g. diesel buses) is multiplied by
      `device_stock_factor`.
    - All other devices' absolute stocks are preserved.
    - Stock Shares and Device Shares are re-normalised to their original
      totals (e.g. 100).

    Assumes:
    - Parent has a single 'Stock' row at `parent_path`.
    - Modes are direct children of `parent_path` with Variable == 'Stock Share'.
    - Devices are direct children of each mode with Variable == 'Device Share'.
    """

    # 1. Parent stock
    parent_mask = (df["Branch Path"] == parent_path) & (df["Variable"] == "Stock")
    if not parent_mask.any():
        return

    # 2. Mode-level stock shares (direct children of parent_path)
    depth_parent = parent_path.count("\\")
    depth_mode = depth_parent + 1
    
    modes_mask = (
        (df["Variable"] == "Stock Share")
        & df["Branch Path"].str.startswith(parent_path + "\\")
        & (df["Branch Path"].str.count(r"\\") == depth_mode)
    )
    if not modes_mask.any():
        return

    mode_paths = df.loc[modes_mask, "Branch Path"].tolist()
    dev_masks: dict[str, pd.Series] = {}
    for m_path in mode_paths:
        mode_depth = m_path.count("\\")
        device_depth = mode_depth + 1
        dev_masks[m_path] = (
            (df["Variable"] == "Device Share")
            & df["Branch Path"].str.startswith(m_path + "\\")
            & (df["Branch Path"].str.count(r"\\") == device_depth)
        )

    years = list(year_columns or [base_year])

    for year_col in years:
        if year_col not in df.columns:
            continue

        S_tot0 = float(df.loc[parent_mask, year_col].iloc[0])

        mode_shares = df.loc[modes_mask, year_col].astype(float)
        T_s = mode_shares.sum()  # e.g. 100 or 1

        # 3. Collect device shares per mode and compute original device stocks
        #    Stock_mode0[m] = S_tot0 * (s_m0 / T_s)
        #    Stock_device0[(m, j)] = Stock_mode0[m] * (d_mj0 / D_m)
        mode_info: dict[str, dict] = {}
        Stock_device0: dict[tuple[str, str], float] = {}
        
        for m_path, s_m0 in zip(mode_paths, mode_shares):
            dev_mask = dev_masks[m_path]

            dev_series = df.loc[dev_mask, year_col].astype(float)
            dev_paths = df.loc[dev_mask, "Branch Path"].tolist()
            D_m = float(dev_series.sum()) if not dev_series.empty else 0.0

            mode_info[m_path] = {
                "share0": float(s_m0),
                "device_paths": dev_paths,
                "device_shares0": dev_series,
                "D_m": D_m,
            }

            Stock_mode0 = S_tot0 * (float(s_m0) / T_s) if T_s != 0 else 0.0
            if D_m > 0 and Stock_mode0 != 0:
                for d_path, d_share0 in zip(dev_paths, dev_series):
                    Stock_device0[(m_path, d_path)] = Stock_mode0 * (float(d_share0) / D_m)
            else:
                # No device split or zero stock -> treat as no devices
                for d_path in dev_paths:
                    Stock_device0[(m_path, d_path)] = 0.0

        # Ensure the target device exists
        if (mode_path not in mode_info) or ((mode_path, device_path) not in Stock_device0):
            continue

        # 4. Apply scaling to target device; keep others unchanged
        Stock_device1: dict[tuple[str, str], float] = {}
        for key, val in Stock_device0.items():
            if key == (mode_path, device_path):
                Stock_device1[key] = device_stock_factor * val
            else:
                Stock_device1[key] = val

        # 5. New total stock
        S_tot1 = sum(Stock_device1.values())
        if S_tot1 <= 0:
            continue

        # 6. New mode-level stocks and Stock Shares
        Stock_mode1: dict[str, float] = {}
        for m_path in mode_paths:
            Stock_mode1[m_path] = sum(
                Stock_device1[(m_path, d_path)]
                for d_path in mode_info[m_path]["device_paths"]
                if (m_path, d_path) in Stock_device1
            )

        new_mode_shares: dict[str, float] = {}
        for m_path in mode_paths:
            if S_tot1 > 0:
                new_mode_shares[m_path] = T_s * Stock_mode1[m_path] / S_tot1
            else:
                new_mode_shares[m_path] = mode_info[m_path]["share0"]

        # 7. New device shares, per mode, preserving per-device stocks
        new_device_shares: dict[tuple[str, str], float] = {}
        for m_path in mode_paths:
            dev_paths = mode_info[m_path]["device_paths"]
            D_m = mode_info[m_path]["D_m"]
            Stock_m1 = Stock_mode1[m_path]

            if Stock_m1 > 0 and D_m > 0:
                for d_path in dev_paths:
                    key = (m_path, d_path)
                    stock_d1 = Stock_device1.get(key, 0.0)
                    new_device_shares[key] = D_m * stock_d1 / Stock_m1
            else:
                # Fall back to original shares if we cannot safely rescale
                dev_series = mode_info[m_path]["device_shares0"]
                for d_path, d_share0 in zip(dev_paths, dev_series):
                    new_device_shares[(m_path, d_path)] = float(d_share0)

        # 8. Write back parent stock, mode Stock Shares, and device Device Shares
        df.loc[parent_mask, year_col] = S_tot1

        # Mode Stock Shares
        for m_path in mode_paths:
            mask_m = (df["Branch Path"] == m_path) & (df["Variable"] == "Stock Share")
            df.loc[mask_m, year_col] = new_mode_shares[m_path]

        # Device Shares
        for (m_path, d_path), d_share1 in new_device_shares.items():
            mask_d = (df["Branch Path"] == d_path) & (df["Variable"] == "Device Share")
            df.loc[mask_d, year_col] = d_share1
        

def _adjust_activity_and_shares_exact(
    df: pd.DataFrame,
    base_year: int | str,
    parent_path: str,
    share1_path: str,
    leaf_path: str,
    leaf_activity_factor: float,
    year_columns: Optional[Sequence[int | str]] = None,
) -> None:
    r"""
    Adjust Activity Level at `parent_path` (e.g. Demand\\Transport\\Non-road X),
    and Activity Level "shares" at one or two levels below it, so that:

    - The effective activity for the target leaf is multiplied by
      `leaf_activity_factor`.
    - The effective activity for all other leaves is unchanged.
    - Activity "shares" at each level remain normalised (sum preserved).

    Structure assumed (non-road case):
      parent_path (total activity, Variable='Activity Level')
        -> share1 nodes (Variable='Activity Level', act shares)
            -> share2 nodes (Variable='Activity Level', act shares)

    If a share1 node has no children of deeper depth with Activity Level,
    it is treated as a leaf itself (one-share case).
    """

    # 1. Parent activity
    parent_mask = (df["Branch Path"] == parent_path) & (df["Variable"] == "Activity Level")
    if not parent_mask.any():
        return

    # 2. First-level shares (share1) under parent_path
    depth_parent = parent_path.count("\\")
    depth_share1 = depth_parent + 1

    share1_mask = (
        (df["Variable"] == "Activity Level")
        & df["Branch Path"].str.startswith(parent_path + "\\")
        & (df["Branch Path"].str.count(r"\\") == depth_share1)
    )
    if not share1_mask.any():
        return

    share1_paths = df.loc[share1_mask, "Branch Path"].tolist()
    share2_masks: dict[str, pd.Series] = {}
    for s1_path in share1_paths:
        s1_depth = s1_path.count("\\")
        depth_share2 = s1_depth + 1
        share2_masks[s1_path] = (
            (df["Variable"] == "Activity Level")
            & df["Branch Path"].str.startswith(s1_path + "\\")
            & (df["Branch Path"].str.count(r"\\") == depth_share2)
        )

    years = list(year_columns or [base_year])

    for year_col in years:
        if year_col not in df.columns:
            continue

        A0 = float(df.loc[parent_mask, year_col].iloc[0])
        share1_vals = df.loc[share1_mask, year_col].astype(float)
        T1 = share1_vals.sum()  # e.g. 100

        # 3. Build leaf activities before adjustment
        #    Keyed as (share1_path, leaf_path)
        leaf_act0: dict[tuple[str, str], float] = {}
        share1_info: dict[str, dict] = {}

        for s1_path, s1_val in zip(share1_paths, share1_vals):
            share2_mask = share2_masks[s1_path]
            share2_paths = df.loc[share2_mask, "Branch Path"].tolist()
            share2_vals = df.loc[share2_mask, year_col].astype(float) if share2_mask.any() else pd.Series([], dtype=float)

            if share2_paths:
                Dm = share2_vals.sum()
                share1_info[s1_path] = {
                    "share1_val": float(s1_val),
                    "leaf_paths": share2_paths,
                    "leaf_shares0": share2_vals,
                    "D_m": float(Dm),
                }
                # leaf with two levels of shares
                if T1 != 0 and Dm != 0:
                    for l_path, l_share in zip(share2_paths, share2_vals):
                        leaf_act0[(s1_path, l_path)] = A0 * (float(s1_val) / T1) * (float(l_share) / Dm)
                else:
                    for l_path in share2_paths:
                        leaf_act0[(s1_path, l_path)] = 0.0
            else:
                # One-share case: treat share1 node itself as a leaf.
                Dm = 1.0
                share1_info[s1_path] = {
                    "share1_val": float(s1_val),
                    "leaf_paths": [s1_path],
                    "leaf_shares0": pd.Series([Dm]),
                    "D_m": Dm,
                }
                if T1 != 0:
                    leaf_act0[(s1_path, s1_path)] = A0 * (float(s1_val) / T1)
                else:
                    leaf_act0[(s1_path, s1_path)] = 0.0

        # Ensure target leaf exists
        # In one-share case, leaf_path == share1_path and key is (share1_path, share1_path).
        target_key = (share1_path, leaf_path if (share1_path, leaf_path) in leaf_act0 else share1_path)
        if target_key not in leaf_act0:
            continue

        # 4. Apply scaling to target leaf; keep others unchanged
        leaf_act1: dict[tuple[str, str], float] = {}
        for key, val in leaf_act0.items():
            if key == target_key:
                leaf_act1[key] = leaf_activity_factor * val
            else:
                leaf_act1[key] = val

        # 5. New total activity
        A1 = sum(leaf_act1.values())
        if A1 <= 0:
            continue

        # 6. New share1 activities and shares
        leaf_act1_by_s1: dict[str, float] = {s1_path: 0.0 for s1_path in share1_paths}
        for (s1_path, l_path), v in leaf_act1.items():
            leaf_act1_by_s1[s1_path] += v

        new_share1_vals: dict[str, float] = {}
        for s1_path in share1_paths:
            new_share1_vals[s1_path] = T1 * leaf_act1_by_s1[s1_path] / A1 if A1 > 0 else share1_info[s1_path]["share1_val"]

        # 7. New share2 (leaf-level shares) per share1 group
        new_leaf_shares: dict[tuple[str, str], float] = {}
        for s1_path in share1_paths:
            info = share1_info[s1_path]
            leaf_paths_s1 = info["leaf_paths"]
            Dm = info["D_m"]

            total_act_s1 = leaf_act1_by_s1[s1_path]
            if total_act_s1 > 0 and Dm > 0:
                for l_path in leaf_paths_s1:
                    key = (s1_path, l_path)
                    act_leaf = leaf_act1.get(key, 0.0)
                    new_leaf_shares[key] = Dm * act_leaf / total_act_s1
            else:
                # Fall back to original shares if we cannot rescale safely
                for l_path, l_share0 in zip(leaf_paths_s1, info["leaf_shares0"]):
                    new_leaf_shares[(s1_path, l_path)] = float(l_share0)

        # 8. Write back parent activity, share1 and leaf shares
        df.loc[parent_mask, year_col] = A1

        # share1 (first-level) Activity Level
        for s1_path in share1_paths:
            mask_s1 = (df["Branch Path"] == s1_path) & (df["Variable"] == "Activity Level")
            df.loc[mask_s1, year_col] = new_share1_vals[s1_path]

        # leaf (second-level) Activity Level
        for (s1_path, l_path), l_share1 in new_leaf_shares.items():
            if l_path == s1_path:
                # one-share case, already handled via share1 value
                continue
            mask_leaf = (df["Branch Path"] == l_path) & (df["Variable"] == "Activity Level")
            df.loc[mask_leaf, year_col] = l_share1


###################################################################
#VERIFICATION FUNCTIONS
####################################################################

def verify_nonroad_intensity_leaf_adjustment(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    base_year: int | str,
    parent_path: str,
    share1_path: str,
    leaf_path: str,
    energy_scale_factor: float,
    tol: float = 1e-8,
) -> bool:
    """
    Verify non-road intensity-based adjustment for a single leaf branch.

    Structure:
      parent_path (Activity Level total)
        -> share1 nodes (Activity Level shares)
            -> share2 nodes = leaves (Activity Level shares), OR
               share1 itself is leaf if no share2 exists.

    Assumes you did:
      - effective_activity(leaf) *= f
      - Final Energy Intensity(leaf) *= f
      with f = sqrt(energy_scale_factor),
      and left other leaves' activities and intensities unchanged (except for
      necessary renormalisation of shares).

    Checks:
      - Effective activity for target leaf scaled by f.
      - Effective activity for all other leaves unchanged.
      - Final Energy Intensity for target leaf scaled by f, unchanged elsewhere.
      - Activity Level shares normalised at share1 and share2 levels.
      - Parent Activity Level consistent with sum of leaf activities.
    """

    f = energy_scale_factor ** 0.5

    def get_val(df, path, var):
        mask = (df["Branch Path"] == path) & (df["Variable"] == var)
        if not mask.any():
            return None
        return float(df.loc[mask, base_year].iloc[0])

    # ---- reconstruct leaf activities ----
    def reconstruct_leaf_activities(df):
        A0 = get_val(df, parent_path, "Activity Level")
        if A0 is None:
            return {}, None, None

        depth_parent = parent_path.count("\\")
        depth_s1 = depth_parent + 1

        s1_mask = (
            (df["Variable"] == "Activity Level")
            & df["Branch Path"].str.startswith(parent_path + "\\")
            & (df["Branch Path"].str.count(r"\\") == depth_s1)
        )
        s1_paths = df.loc[s1_mask, "Branch Path"].tolist()
        s1_vals = [get_val(df, p, "Activity Level") for p in s1_paths]
        T1 = sum(s1_vals) if s1_vals else None

        leaf_acts = {}
        group_info = {}

        for s1_path, s1_val in zip(s1_paths, s1_vals):
            depth_s2 = s1_path.count("\\") + 1
            s2_mask = (
                (df["Variable"] == "Activity Level")
                & df["Branch Path"].str.startswith(s1_path + "\\")
                & (df["Branch Path"].str.count(r"\\") == depth_s2)
            )
            s2_paths = df.loc[s2_mask, "Branch Path"].tolist()
            s2_vals = [get_val(df, p, "Activity Level") for p in s2_paths]

            if s2_paths:  # two-level share
                Dm = sum(s2_vals) if s2_vals else None
                group_info[s1_path] = {
                    "share1_val": s1_val,
                    "leaf_paths": s2_paths,
                    "leaf_shares": s2_vals,
                    "D_m": Dm,
                }

                if T1 and T1 != 0 and Dm and Dm != 0:
                    for lp, lv in zip(s2_paths, s2_vals):
                        leaf_acts[(s1_path, lp)] = A0 * (s1_val / T1) * (lv / Dm)
                else:
                    for lp in s2_paths:
                        leaf_acts[(s1_path, lp)] = 0.0
            else:  # one-share case: s1 itself is leaf
                Dm = 1.0
                group_info[s1_path] = {
                    "share1_val": s1_val,
                    "leaf_paths": [s1_path],
                    "leaf_shares": [Dm],
                    "D_m": Dm,
                }
                if T1 and T1 != 0:
                    leaf_acts[(s1_path, s1_path)] = A0 * (s1_val / T1)
                else:
                    leaf_acts[(s1_path, s1_path)] = 0.0

        return leaf_acts, group_info, A0

    leaf_act0, group0, A0 = reconstruct_leaf_activities(df_before)
    leaf_act1, group1, A1 = reconstruct_leaf_activities(df_after)

    if not leaf_act0 or not leaf_act1:
        print("ERROR: could not reconstruct leaf activities.")
        return False

    # Resolve target key (handles one-share case)
    target_key = (share1_path, leaf_path)
    if target_key not in leaf_act0:
        # one-share case: leaf is the share1 node itself
        target_key = (share1_path, share1_path)
        if target_key not in leaf_act0:
            print("ERROR: target leaf not found in leaf activities.")
            return False

    # ---- 1. effective activity ----
    print("Checking effective leaf activities...")
    for key in leaf_act0:
        if key == target_key:
            if abs(leaf_act1[key] - leaf_act0[key] * f) > tol:
                print(f"ERROR: target leaf activity scaled incorrectly.")
                print(f"  Before:   {leaf_act0[key]}")
                print(f"  Expected: {leaf_act0[key] * f}")
                print(f"  After:    {leaf_act1[key]}")
                return False
        else:
            if abs(leaf_act1[key] - leaf_act0[key]) > tol:
                print(f"ERROR: non-target leaf activity changed: {key}")
                print(f"  Before: {leaf_act0[key]}")
                print(f"  After:  {leaf_act1[key]}")
                return False
    print("✓ Leaf activities OK")

    # ---- 2. share normalisation (share1 and share2) ----
    print("Checking Activity Level share normalisation...")

    # share1 sums
    s1_paths0 = list(group0.keys())
    T1_0 = sum(group0[p]["share1_val"] for p in s1_paths0)
    T1_1 = sum(group1[p]["share1_val"] for p in group1.keys())
    if abs(T1_0 - T1_1) > tol:
        print("ERROR: share1 sums changed.")
        print(f"  Before: {T1_0}, After: {T1_1}")
        return False

    # share2 sums for each group (two-level case only)
    for s1_path in s1_paths0:
        leaf_paths0 = group0[s1_path]["leaf_paths"]
        leaf_paths1 = group1[s1_path]["leaf_paths"]
        if set(leaf_paths0) != set(leaf_paths1):
            print(f"ERROR: leaf set changed for {s1_path}.")
            return False

        Dm0 = group0[s1_path]["D_m"]
        Dm1 = group1[s1_path]["D_m"]
        if abs(Dm0 - Dm1) > tol:
            print(f"ERROR: share2 sum changed for {s1_path}.")
            print(f"  Before: {Dm0}, After: {Dm1}")
            return False

    print("✓ Activity Level shares normalised")

    # ---- 3. parent Activity Level consistency ----
    if abs(A0 - sum(leaf_act0.values())) > tol or abs(A1 - sum(leaf_act1.values())) > tol:
        print("ERROR: parent Activity Level mismatch with sum of leaf activities.")
        print(f"  Parent before: {A0}, sum(leaves): {sum(leaf_act0.values())}")
        print(f"  Parent after:  {A1}, sum(leaves): {sum(leaf_act1.values())}")
        return False
    print("✓ Parent Activity Level reconstruction matches")

    # ---- 4. intensity scaling ----
    print("Checking Final Energy Intensity scaling...")

    def get_intensity_map(df):
        mask = df["Variable"] == "Final Energy Intensity"
        return df.loc[mask, ["Branch Path", base_year]].set_index("Branch Path")[base_year]

    inten0 = get_intensity_map(df_before)
    inten1 = get_intensity_map(df_after)

    # target leaf intensity
    if leaf_path in inten0.index:
        i0 = float(inten0.loc[leaf_path])
        i1 = float(inten1.loc[leaf_path])
        if abs(i1 - i0 * f) > tol:
            print("ERROR: intensity at target leaf did not scale by f.")
            print(f"  Before:   {i0}")
            print(f"  Expected: {i0 * f}")
            print(f"  After:    {i1}")
            return False

    # all others unchanged
    for path in inten0.index:
        if path == leaf_path:
            continue
        v0 = float(inten0.loc[path])
        v1 = float(inten1.loc[path])
        if abs(v1 - v0) > tol:
            print(f"ERROR: intensity changed for non-target branch {path}.")
            print(f"  Before: {v0}, After: {v1}")
            return False

    print("✓ Final Energy Intensity scaling OK")

    # ---- final ----
    print("\n=== NON-ROAD INTENSITY VERIFICATION PASSED ===")
    print(f"Energy scale factor: {energy_scale_factor}")
    print(f"Implied factor f = {f}")
    print(f"Parent Activity: {A0} → {A1}")
    return True

def verify_device_stock_adjustment_with_mileage_fe(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    base_year: int | str,
    parent_path: str,
    mode_path: str,
    device_path: str,
    energy_scale_factor: float,
    tol: float = 1e-8,
) -> bool:
    """
    Verify Stock-based adjustment for a single device branch.

    Assumes:
      - energy_device ∝ Stock_device * Mileage_device * FuelEconomy_device
      - You applied:
          f = energy_scale_factor**(1/3)
        to:
          - device stock (via stock/share adjustment)
          - Mileage at device_path
          - Fuel Economy at device_path

    Checks:
      - Target device stock scaled by f.
      - All other device stocks unchanged.
      - Mileage and Fuel Economy at device_path scaled by f, unchanged elsewhere.
      - Mode Stock Shares still normalised (sum preserved).
      - Device Shares still normalised (sum preserved per mode).
      - Parent stock reconstruction consistent with parent Stock value.
    """

    # ---- basic helper ----
    def get_val(df, path, var):
        mask = (df["Branch Path"] == path) & (df["Variable"] == var)
        if not mask.any():
            return None
        return float(df.loc[mask, base_year].iloc[0])

    f = energy_scale_factor ** (1.0 / 3.0)

    # ---- parent stock ----
    S0 = get_val(df_before, parent_path, "Stock")
    S1 = get_val(df_after, parent_path, "Stock")

    # ---- modes (Stock Share) ----
    depth_parent = parent_path.count("\\")
    depth_mode = depth_parent + 1

    modes_mask = (
        (df_before["Variable"] == "Stock Share")
        & df_before["Branch Path"].str.startswith(parent_path + "\\")
        & (df_before["Branch Path"].str.count(r"\\") == depth_mode)
    )
    mode_paths = df_before.loc[modes_mask, "Branch Path"].tolist()

    # ---- rebuild device stocks before/after ----
    def rebuild_device_stocks(df):
        stocks = {}
        S_tot = get_val(df, parent_path, "Stock")

        mode_shares = {
            m: get_val(df, m, "Stock Share")
            for m in mode_paths
        }
        T_s = sum(mode_shares.values()) if mode_shares else None

        for m_path in mode_paths:
            s_m = mode_shares[m_path]
            if T_s and T_s != 0:
                mode_stock = S_tot * (s_m / T_s)
            else:
                mode_stock = 0.0

            depth_dev = m_path.count("\\") + 1
            dev_mask = (
                (df["Variable"] == "Device Share")
                & df["Branch Path"].str.startswith(m_path + "\\")
                & (df["Branch Path"].str.count(r"\\") == depth_dev)
            )
            dev_paths = df.loc[dev_mask, "Branch Path"].tolist()
            dev_shares = [
                get_val(df, d, "Device Share") for d in dev_paths
            ]
            D_m = sum(dev_shares) if dev_shares else None

            for d_path, d_share in zip(dev_paths, dev_shares):
                if D_m and D_m != 0:
                    stocks[(m_path, d_path)] = mode_stock * (d_share / D_m)
                else:
                    stocks[(m_path, d_path)] = 0.0

        return stocks, mode_shares, S_tot

    stocks0, mode_shares0, S0_check = rebuild_device_stocks(df_before)
    stocks1, mode_shares1, S1_check = rebuild_device_stocks(df_after)

    # ---- 1. device stocks ----
    print("Checking device-level absolute stock consistency...")
    target_key = (mode_path, device_path)
    for key in stocks0:
        if key == target_key:
            # target device must scale by f
            if abs(stocks1[key] - stocks0[key] * f) > tol:
                print(f"ERROR: target device {device_path} stock scaled incorrectly.")
                print(f"  Before:   {stocks0[key]}")
                print(f"  Expected: {stocks0[key] * f}")
                print(f"  After:    {stocks1[key]}")
                return False
        else:
            # others must be unchanged
            if abs(stocks1[key] - stocks0[key]) > tol:
                print(f"ERROR: non-target device changed: {key}")
                print(f"  Before: {stocks0[key]}")
                print(f"  After:  {stocks1[key]}")
                return False
    print("✓ Device stocks OK")

    # ---- 2. mode stock-share normalisation ----
    T_s0 = sum(mode_shares0.values())
    T_s1 = sum(mode_shares1.values())
    if abs(T_s0 - T_s1) > tol:
        print("ERROR: Mode Stock Shares lost normalisation.")
        print(f"  Before total: {T_s0}  After total: {T_s1}")
        return False
    print("✓ Mode Stock Shares normalised")

    # ---- 3. device-share normalisation per mode ----
    print("Checking Device Share normalisation per mode...")
    for m_path in mode_paths:
        depth_dev = m_path.count("\\") + 1

        dev_mask_before = (
            (df_before["Variable"] == "Device Share")
            & df_before["Branch Path"].str.startswith(m_path + "\\")
            & (df_before["Branch Path"].str.count("\\") == depth_dev)
        )
        dev_paths_m = df_before.loc[dev_mask_before, "Branch Path"].tolist()

        Dm0 = sum(get_val(df_before, d, "Device Share") for d in dev_paths_m)
        Dm1 = sum(get_val(df_after, d, "Device Share") for d in dev_paths_m)

        if abs(Dm0 - Dm1) > tol:
            print(f"ERROR: Device Shares under mode {m_path} lost normalisation.")
            print(f"  Before: {Dm0}  After: {Dm1}")
            return False
    print("✓ Device Shares normalised")

    # ---- 4. parent stock reconstruction ----
    if abs(S0 - S0_check) > tol or abs(S1 - S1_check) > tol:
        print("ERROR: Parent Stock mismatch between direct and reconstructed values.")
        print(f"  Direct before: {S0}, reconstructed: {S0_check}")
        print(f"  Direct after:  {S1}, reconstructed: {S1_check}")
        return False
    print("✓ Parent Stock reconstruction matches")

    # ---- 5. mileage & fuel economy scaling ----
    print("Checking Mileage and Fuel Economy scaling...")

    def get_series(df, var):
        mask = df["Variable"] == var
        return df.loc[mask, ["Branch Path", base_year]].set_index("Branch Path")[base_year]

    mileage0 = get_series(df_before, "Mileage")
    mileage1 = get_series(df_after, "Mileage")
    fe0 = get_series(df_before, "Fuel Economy")
    fe1 = get_series(df_after, "Fuel Economy")

    # target device: scaled by f
    for series0, series1, name in ((mileage0, mileage1, "Mileage"),
                                   (fe0, fe1, "Fuel Economy")):
        if device_path in series0.index:
            v0 = float(series0.loc[device_path])
            v1 = float(series1.loc[device_path])
            if abs(v1 - v0 * f) > tol:
                print(f"ERROR: {name} at {device_path} did not scale by f.")
                print(f"  Before:   {v0}")
                print(f"  Expected: {v0 * f}")
                print(f"  After:    {v1}")
                return False

    # all other branches: unchanged
    for series0, series1, name in ((mileage0, mileage1, "Mileage"),
                                   (fe0, fe1, "Fuel Economy")):
        for path in series0.index:
            if path == device_path:
                continue
            v0 = float(series0.loc[path])
            v1 = float(series1.loc[path])
            if abs(v1 - v0) > tol:
                print(f"ERROR: {name} changed for non-target branch {path}.")
                print(f"  Before: {v0}, After: {v1}")
                return False

    print("✓ Mileage and Fuel Economy scaling OK")

    # ---- final ----
    print("\n=== STOCK-BASED VERIFICATION PASSED ===")
    print(f"Energy scale factor: {energy_scale_factor}")
    print(f"Implied device/mileage/FE factor f = {f}")
    print(f"Parent Stock: {S0} → {S1}")
    return True



########################################
#OTHER TRANSPORT UTILITIES
########################################


def build_transport_esto_energy_totals(
    esto_df: pd.DataFrame,
    economy: str,
    original_scenario: str,
    base_year: int | str,
    final_year: int | str,
    SUBTOTAL_COLUMN: str = 'subtotal_layout',
) -> Dict[Tuple[str, ...], float]:
    """
    Build {ESTO_key -> ESTO_energy_total} for a given economy/scenario/base_year,
    handling both:
      - normal sector–fuel combos (simple one-row lookup), and
      - any ESTO keys whose LEAP branches include 'Nonspecified transport',
        using extract_esto_energy_use_for_leap_branches.

    This is the 'ESTO side' input for reconcile_energy_use.
    """

    # 1. Filter ESTO for economy + scenario, same as your old code
    esto_filtered = esto_df[
        (esto_df["economy"] == economy)
        & (esto_df["scenarios"] == original_scenario.lower())
        & (esto_df[SUBTOTAL_COLUMN] == False)
    ][["sectors", "sub1sectors", "sub2sectors", "sub3sectors", "sub4sectors", "fuels", "subfuels", base_year]]

    esto_energy_totals: Dict[Tuple[str, ...], float] = {}

    for esto_key, leap_branches in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
        # esto_key like ('15_02_road', '07_petroleum_products', '07_01_motor_gasoline')
        # leap_branches is a list of tuples like ('Passenger road', 'Cars', 'ICE', 'Gasoline')

        # Does this ESTO key map to any Nonspecified transport branch?
        has_nonspecified = any(
            "nonspecified transport" in " ".join(branch).lower()
            for branch in leap_branches
        )

        if has_nonspecified:
            # 2A. Nonspecified handling using your existing helper
            try:
                # This is exactly the call pattern from your old code, just factored out
                esto_energy_total_list = extract_esto_energy_use_for_leap_branches(
                    leap_branches,
                    esto_df,
                    economy,
                    original_scenario,
                    base_year,
                    final_year,
                )

                # Convert list[dict] -> DataFrame and take base year slice
                tmp = pd.DataFrame()
                for item in esto_energy_total_list:
                    tmp = pd.concat([tmp, pd.DataFrame(item)], ignore_index=True)

                # Expect a 'Date' column and an 'Energy' column, as in your old code
                tmp_base = tmp.loc[tmp["Date"] == base_year]

                if tmp_base.empty:
                    print(
                        f"ℹ️  No nonspecified ESTO rows for key {esto_key} at {base_year}; using 0.0."
                    )
                    esto_energy_totals[esto_key] = 0.0
                else:
                    # If there are multiple rows for that year, sum them
                    esto_energy_totals[esto_key] = float(tmp_base["Energy"].sum())

            except Exception as e:
                # Keep the same defensive behaviour you had
                print(
                    f"Error converting ESTO nonspecified data to totals for key {esto_key}: {e}"
                )
                esto_energy_totals[esto_key] = 0.0

            continue  # move on to next ESTO key

        # 2B. Normal (not-nonspecified) ESTO key: do the simple sub1/sub2/fuels/subfuels lookup
        mask = (
            (esto_filtered["sub1sectors"] == esto_key[0])
            & (esto_filtered["sub2sectors"] == "x")
            & (esto_filtered["fuels"] == esto_key[1])
            & (esto_filtered["subfuels"] == esto_key[2])
        )
        esto_rows = esto_filtered.loc[mask]

        if len(esto_rows) == 1:
            esto_energy_totals[esto_key] = float(esto_rows.iloc[0][base_year])
        elif len(esto_rows) == 0:
            # This matches your “no row, LEAP may be zero” handling, but we
            # don’t know the LEAP side here, so just record zero and warn.
            print(
                f"ℹ️  No ESTO data found for key {esto_key} in {economy}/{original_scenario} "
                f"for {base_year}. Using 0.0."
            )
            esto_energy_totals[esto_key] = 0.0
        else:
            # Multiple rows is an error – same behaviour as before.
            breakpoint()
            raise ValueError(
                f"Multiple ESTO rows found for key {esto_key} in {economy}/{original_scenario} for {base_year}."
            )

    return esto_energy_totals
















