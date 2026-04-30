#%%
"""Disaggregated pre-reconciliation vs raw-source comparison for transport.

Major points:
1. Build per-branch/year rows by mapping each LEAP branch to its source tuple.
2. Compute LEAP-side effective metrics from pre-recon checkpoints.
3. Compute raw-side aggregates from the input transport model data.
4. Harmonize units before diffs so values are comparable.
5. Optionally add a stock-family view that collapses alternative fuels into
   fossil families for clearer diagnostics.

Efficiency unit rule used here:
- LEAP fuel economy in checkpoints is MJ/100km.
- Raw source Efficiency is Billion_km_per_pj and must be converted (inverse +
  scaling) to MJ/100km before comparison.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from configurations.basic_mappings import add_fuel_column
from configurations.branch_mappings import KM_PER_PJ_TO_MJ_PER_100KM, LEAP_BRANCH_TO_SOURCE_MAP
from configurations.measure_catalog import LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP
from configurations.measure_metadata import SOURCE_MEASURE_TO_UNIT
from functions.path_utils import resolve_path
from functions.transport_branch_paths import (
    branch_tuple_depth,
    build_transport_branch_path,
    is_non_road_transport_branch_path,
    is_pipeline_or_nonspecified_branch_path,
)
from configurations.transport_economy_config import load_transport_run_config


SOURCE_DIMS = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"]
# Unit harmonization from pre-reconciliation FOR_VIEWING values to raw-model-like magnitudes:
# - Mileage: FOR_VIEWING uses Kilometer, raw source is effectively thousand-km.
# - Fuel economy: keep in MJ/100km for both pre and raw in this table.
# - Non-road intensity: FOR_VIEWING uses GJ per activity unit, raw source is effectively MJ per unit.
MILEAGE_PRE_TO_RAW = 1.0 / 1000.0
INTENSITY_PRE_TO_RAW = 1000.0
RAW_STOCKS_TO_ABSOLUTE = float(SOURCE_MEASURE_TO_UNIT.get("Stocks", (None, 1.0))[1] or 1.0)
RAW_ACTIVITY_TO_ABSOLUTE = float(SOURCE_MEASURE_TO_UNIT.get("Activity", (None, 1.0))[1] or 1.0)
RAW_EFFICIENCY_TO_KM_PER_PJ = float(SOURCE_MEASURE_TO_UNIT.get("Efficiency", (None, 1.0))[1] or 1.0)
ALT_FUEL_TO_BASE_FUEL = {
    "biodiesel": "Gas and diesel oil",
    "biogasoline": "Motor gasoline",
    "bio jet kerosene": "Kerosene type jet fuel",
    "biogas": "Natural gas",
}


@dataclass
class RawAgg:
    stocks: float | None = None
    mileage: float | None = None
    efficiency: float | None = None
    activity: float | None = None
    intensity: float | None = None


def _parse_source_tuple(value: Any) -> tuple[Any, ...] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, tuple):
        return value
    try:
        parsed = ast.literal_eval(str(value))
        return parsed if isinstance(parsed, tuple) else None
    except (ValueError, SyntaxError):
        return None


def _reverted_stock_family_fuel_label(fuel: Any, drive: Any) -> str | None:
    fuel_l = str(fuel).strip().lower()
    drive_l = str(drive).strip().lower()
    if not fuel_l:
        return None
    if fuel_l in ALT_FUEL_TO_BASE_FUEL:
        return ALT_FUEL_TO_BASE_FUEL[fuel_l]
    if fuel_l == "efuel":
        if ("jet" in drive_l) or ("air" in drive_l):
            return "Kerosene type jet fuel"
        if drive_l.endswith("_d") or ("diesel" in drive_l):
            return "Gas and diesel oil"
        if drive_l.endswith("_g") or ("gasoline" in drive_l):
            return "Motor gasoline"
    return str(fuel)


def _safe_pct_diff(lhs: float | None, rhs: float | None) -> float | None:
    if lhs is None or rhs is None:
        return None
    if pd.isna(lhs) or pd.isna(rhs):
        return None
    if abs(rhs) <= 1e-12:
        return None
    return float((lhs - rhs) / rhs)


def _raw_efficiency_to_mj_per_100km(raw_efficiency_value: Any) -> float | None:
    """Convert raw source Efficiency (Billion_km_per_pj) -> MJ/100km."""
    eff = _to_float_or_none(raw_efficiency_value)
    if eff is None or eff <= 0:
        return None
    km_per_pj = eff * RAW_EFFICIENCY_TO_KM_PER_PJ
    if km_per_pj <= 0:
        return None
    return float(KM_PER_PJ_TO_MJ_PER_100KM / km_per_pj)


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    value_num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(value_num):
        return None
    return float(value_num)


def _build_pre_lookup(pre_df: pd.DataFrame, scenario: str) -> dict[tuple[str, str, int], float]:
    work = pre_df.copy()
    work["Scenario_norm"] = work["Scenario"].astype(str).str.strip().str.lower()
    year_cols = [c for c in work.columns if str(c).isdigit() and len(str(c)) == 4]

    def _to_long(df_slice: pd.DataFrame) -> pd.DataFrame:
        if df_slice.empty:
            return pd.DataFrame(columns=["Branch Path", "Variable", "Date", "Value"])
        out = df_slice.melt(
            id_vars=["Branch Path", "Variable"],
            value_vars=year_cols,
            var_name="Date",
            value_name="Value",
        )
        out["Date"] = pd.to_numeric(out["Date"], errors="coerce")
        out["Value"] = pd.to_numeric(out["Value"], errors="coerce")
        out = out.dropna(subset=["Date", "Value"]).copy()
        out["Date"] = out["Date"].astype(int)
        out = out.drop_duplicates(subset=["Branch Path", "Variable", "Date"], keep="first")
        return out

    scenario_long = _to_long(work[work["Scenario_norm"] == scenario.lower()].copy())
    current_accounts_long = _to_long(work[work["Scenario_norm"] == "current accounts"].copy())

    lookup: dict[tuple[str, str, int], float] = {}
    # Fill with Current Accounts first, then overwrite with scenario-specific rows.
    for df_part in (current_accounts_long, scenario_long):
        for _, r in df_part.iterrows():
            lookup[(str(r["Branch Path"]), str(r["Variable"]), int(r["Date"]))] = float(r["Value"])
    return lookup


def _build_pre_lookup_from_long(pre_long_df: pd.DataFrame) -> dict[tuple[str, str, int], float]:
    required_cols = {"Branch_Path", "Measure", "Date", "Value"}
    if not required_cols.issubset(set(pre_long_df.columns)):
        return {}
    work = pre_long_df[list(required_cols)].copy()
    work["Date"] = pd.to_numeric(work["Date"], errors="coerce")
    work["Value"] = pd.to_numeric(work["Value"], errors="coerce")
    work = work.dropna(subset=["Branch_Path", "Measure", "Date", "Value"]).copy()
    if work.empty:
        return {}
    work["Date"] = work["Date"].astype(int)
    work = work.drop_duplicates(subset=["Branch_Path", "Measure", "Date"], keep="last")
    lookup: dict[tuple[str, str, int], float] = {}
    for _, row in work.iterrows():
        lookup[(str(row["Branch_Path"]), str(row["Measure"]), int(row["Date"]))] = float(row["Value"])
    return lookup


def _build_parent_share_sums(
    lookup: dict[tuple[str, str, int], float],
    variable: str,
) -> tuple[dict[tuple[str, int], float], dict[tuple[str, int], int]]:
    sums: dict[tuple[str, int], float] = {}
    counts: dict[tuple[str, int], int] = {}
    for (branch_path, var, date), value in lookup.items():
        if var != variable:
            continue
        parent = branch_path.rsplit("\\", 1)[0] if "\\" in branch_path else ""
        key = (parent, int(date))
        sums[key] = sums.get(key, 0.0) + float(value)
        counts[key] = counts.get(key, 0) + 1
    return sums, counts


def _build_pre_normalization_device_share_sums(
    economy: str,
    scenario: str,
) -> tuple[dict[tuple[str, int], float], dict[tuple[str, int], int]]:
    long_path = resolve_path(f"intermediate_data/export_df_checkpoint_{economy}_{scenario}.pkl")
    assert long_path is not None
    if not long_path.exists():
        return {}, {}
    long_df = pd.read_pickle(long_path)
    required_cols = {"Branch_Path", "Measure", "Date", "Value"}
    if not required_cols.issubset(set(long_df.columns)):
        return {}, {}

    sub = long_df[long_df["Measure"] == "Device Share"].copy()
    if sub.empty:
        return {}, {}
    sub["Date"] = pd.to_numeric(sub["Date"], errors="coerce")
    sub["Value"] = pd.to_numeric(sub["Value"], errors="coerce")
    sub = sub.dropna(subset=["Date", "Value", "Branch_Path"]).copy()
    sub["Date"] = sub["Date"].astype(int)
    sub["parent_path"] = sub["Branch_Path"].astype(str).str.rsplit("\\", n=1).str[0]

    sums = (
        sub.groupby(["parent_path", "Date"], as_index=False)["Value"]
        .sum()
        .set_index(["parent_path", "Date"])["Value"]
        .to_dict()
    )
    counts = (
        sub.groupby(["parent_path", "Date"], as_index=False)["Value"]
        .count()
        .set_index(["parent_path", "Date"])["Value"]
        .astype(int)
        .to_dict()
    )
    return sums, counts


def _pre_value(
    lookup: dict[tuple[str, str, int], float],
    branch_path: str,
    variable: str,
    date: int,
) -> float | None:
    return lookup.get((branch_path, variable, date))


def _pre_metrics_for_branch(
    lookup: dict[tuple[str, str, int], float],
    device_share_parent_sum_lookup: dict[tuple[str, int], float],
    device_share_parent_count_lookup: dict[tuple[str, int], int],
    leap_branch_tuple: tuple[str, ...],
    analysis_type: str,
    date: int,
    root: str = "Demand",
    device_share_is_absolute_million_stock: bool = False,
) -> dict[str, float | None]:
    def _looks_like_percent_share(value: float | None) -> bool:
        if value is None or pd.isna(value):
            return False
        # Allow only slight over-100 totals from rounding/normalization noise.
        return 0.0 <= float(value) <= 105.0

    branch_path = build_transport_branch_path(leap_branch_tuple, root=root)
    parts = branch_path.split("\\")
    if analysis_type == "Stock":
        if len(parts) < 4:
            return {
                "pre_effective_stock": None,
                "pre_parent_stock": None,
                "pre_mode_stock_share": None,
                "pre_device_share": None,
                "pre_device_share_sibling_sum": None,
                "pre_device_share_sibling_count": None,
                "pre_mileage": None,
                "pre_efficiency": None,
                "pre_effective_activity": None,
                "pre_intensity": None,
            }
        mode_path = "\\".join(parts[:-1])
        parent_path = "\\".join(parts[:-2])

        stock = _pre_value(lookup, parent_path, "Stock", date)
        stock_share = _pre_value(lookup, mode_path, "Stock Share", date)
        device_share = _pre_value(lookup, branch_path, "Device Share", date)
        mileage = _pre_value(lookup, branch_path, "Mileage", date)
        efficiency = _pre_value(lookup, branch_path, "Final On-Road Fuel Economy", date)
        if efficiency is None:
            efficiency = _pre_value(lookup, branch_path, "Fuel Economy", date)
        sibling_key = (mode_path, int(date))
        device_share_sibling_sum = device_share_parent_sum_lookup.get(sibling_key)
        device_share_sibling_count = device_share_parent_count_lookup.get(sibling_key)

        effective_stock = None
        if device_share_is_absolute_million_stock and device_share is not None:
            # In export_df_checkpoint_* long checkpoints, Device Share rows are already
            # leaf stock amounts in million vehicles (not percentages). Convert to
            # absolute vehicles to align with raw-side stock units.
            effective_stock = float(device_share * 1_000_000.0)
        elif None not in (stock, stock_share, device_share):
            effective_stock = float(stock * stock_share * device_share / 10000.0)

        mileage_norm = None if mileage is None else float(mileage * MILEAGE_PRE_TO_RAW)
        # Checkpoint fuel economy is already MJ/100km; keep as-is so it matches
        # raw efficiency after raw-side conversion to MJ/100km.
        efficiency_norm = None if efficiency is None else float(efficiency)

        return {
            "pre_effective_stock": effective_stock,
            "pre_parent_stock": stock,
            "pre_mode_stock_share": stock_share,
            "pre_device_share": device_share,
            "pre_device_share_sibling_sum": device_share_sibling_sum,
            "pre_device_share_sibling_count": device_share_sibling_count,
            "pre_mileage": mileage_norm,
            "pre_efficiency": efficiency_norm,
            "pre_effective_activity": None,
            "pre_intensity": None,
        }

    if analysis_type == "Intensity":
        intensity = _pre_value(lookup, branch_path, "Final Energy Intensity", date)
        branch_lower = branch_path.lower()
        effective_activity = None
        logical_depth = branch_tuple_depth(branch_path)
        if logical_depth >= 3 and is_non_road_transport_branch_path(branch_path):
            up_one = "\\".join(parts[:-1])
            up_two = "\\".join(parts[:-2])
            parent_activity = _pre_value(lookup, up_two, "Activity Level", date)
            share_1 = _pre_value(lookup, up_one, "Activity Level", date)
            share_2 = _pre_value(lookup, branch_path, "Activity Level", date)
            leaf_activity_abs = (
                float(share_2 * RAW_ACTIVITY_TO_ABSOLUTE)
                if share_2 is not None and not pd.isna(share_2)
                else None
            )
            if (
                None not in (parent_activity, share_1, share_2)
                and _looks_like_percent_share(share_1)
                and _looks_like_percent_share(share_2)
            ):
                effective_activity = float(parent_activity * share_1 * share_2 / 10000.0)
                # Guard against mixed-unit rows where leaf "Activity Level" is actually
                # already absolute-like (billions). In those cases the product formula
                # can overstate by an order of magnitude; prefer direct leaf scaling.
                if (
                    leaf_activity_abs is not None
                    and leaf_activity_abs > 0
                    and (
                        effective_activity / leaf_activity_abs > 10.0
                        or effective_activity / leaf_activity_abs < 0.1
                    )
                ):
                    effective_activity = leaf_activity_abs
            elif leaf_activity_abs is not None:
                # Some non-road checkpoint rows store leaf Activity Level as an absolute
                # activity amount (in billions), not a percent share.
                effective_activity = leaf_activity_abs
        elif is_pipeline_or_nonspecified_branch_path(branch_path):
            up_one = "\\".join(parts[:-1])
            parent_activity = _pre_value(lookup, up_one, "Activity Level", date)
            share_1 = _pre_value(lookup, branch_path, "Activity Level", date)
            if (
                None not in (parent_activity, share_1)
                and _looks_like_percent_share(share_1)
            ):
                effective_activity = float(parent_activity * share_1 / 100.0)
            elif share_1 is not None:
                effective_activity = float(share_1 * RAW_ACTIVITY_TO_ABSOLUTE)

        effective_activity_norm = effective_activity
        intensity_norm = None if intensity is None else float(intensity * INTENSITY_PRE_TO_RAW)

        return {
            "pre_effective_stock": None,
            "pre_parent_stock": None,
            "pre_mode_stock_share": None,
            "pre_device_share": None,
            "pre_device_share_sibling_sum": None,
            "pre_device_share_sibling_count": None,
            "pre_mileage": None,
            "pre_efficiency": None,
            "pre_effective_activity": effective_activity_norm,
            "pre_intensity": intensity_norm,
        }

    return {
        "pre_effective_stock": None,
        "pre_parent_stock": None,
        "pre_mode_stock_share": None,
        "pre_device_share": None,
        "pre_device_share_sibling_sum": None,
        "pre_device_share_sibling_count": None,
        "pre_mileage": None,
        "pre_efficiency": None,
        "pre_effective_activity": None,
        "pre_intensity": None,
    }


def _prepare_raw_grouped(raw_with_fuel: pd.DataFrame) -> pd.DataFrame:
    work = raw_with_fuel.copy()
    work["Date"] = pd.to_numeric(work["Date"], errors="coerce")
    work = work[work["Date"].notna()].copy()
    work["Date"] = work["Date"].astype(int)

    stocks = pd.to_numeric(work["Stocks"], errors="coerce").fillna(0.0) * RAW_STOCKS_TO_ABSOLUTE
    # Raw source activity is in "Billion_passenger_km_or_freight_tonne_km";
    # convert to absolute activity units for comparison against LEAP-side
    # Activity Level values from checkpoints.
    activity = pd.to_numeric(work["Activity"], errors="coerce").fillna(0.0) * RAW_ACTIVITY_TO_ABSOLUTE
    mileage = pd.to_numeric(work["Mileage"], errors="coerce")
    efficiency_raw = pd.to_numeric(work["Efficiency"], errors="coerce")
    efficiency_mj_per_100km = efficiency_raw.map(_raw_efficiency_to_mj_per_100km)
    intensity = pd.to_numeric(work["Intensity"], errors="coerce")

    work["stocks_v"] = stocks
    work["activity_v"] = activity
    work["mileage_num"] = mileage.fillna(0.0) * stocks
    work["efficiency_num"] = efficiency_mj_per_100km.fillna(0.0) * stocks
    work["intensity_num"] = intensity.fillna(0.0) * activity

    grouped = (
        work.groupby(["Date", *SOURCE_DIMS], as_index=False)[
            ["stocks_v", "activity_v", "mileage_num", "efficiency_num", "intensity_num"]
        ]
        .sum()
    )
    return grouped


def _stock_cause_flag(
    pre_effective_stock: float | None,
    raw_stocks: float | None,
    pre_device_share_sibling_sum: float | None,
) -> str | None:
    if pre_effective_stock is None or raw_stocks is None:
        return None
    if pd.isna(pre_effective_stock) or pd.isna(raw_stocks):
        return None
    if pre_effective_stock > 0 and abs(raw_stocks) <= 1e-9:
        return "raw_stock_zero_pre_positive"
    if pre_device_share_sibling_sum is not None and pd.notna(pre_device_share_sibling_sum):
        share_total = float(pre_device_share_sibling_sum)
        if share_total > 120.0:
            return "device_share_family_over_100"
        if share_total < 80.0:
            return "device_share_family_under_100"
    ratio = None if abs(raw_stocks) <= 1e-12 else float(pre_effective_stock / raw_stocks)
    if ratio is not None and ratio > 10:
        return "pre_stock_much_higher_than_raw"
    if ratio is not None and ratio < 0.1:
        return "pre_stock_much_lower_than_raw"
    return "within_10x"


def build_comparison_df(
    economy: str,
    scenario: str,
    *,
    pre_stock_multiplier: float = 1.0,
) -> pd.DataFrame:
    economy, scenario, cfg = load_transport_run_config(economy, scenario)
    base_year = int(cfg.transport_base_year)
    final_year = int(cfg.transport_final_year)

    raw_df = pd.read_csv(cfg.transport_model_path, low_memory=False)
    raw_df = raw_df[
        (raw_df["Economy"] == economy)
        & (raw_df["Scenario"].astype(str).str.strip().str.lower() == scenario.lower())
        & (pd.to_numeric(raw_df["Date"], errors="coerce") >= base_year)
        & (pd.to_numeric(raw_df["Date"], errors="coerce") <= final_year)
    ].copy()
    raw_with_fuel = add_fuel_column(raw_df)
    raw_grouped = _prepare_raw_grouped(raw_with_fuel)

    pre_long_path = resolve_path(f"intermediate_data/export_df_checkpoint_{economy}_{scenario}.pkl")
    assert pre_long_path is not None
    pre_lookup: dict[tuple[str, str, int], float]
    years: list[int]
    if pre_long_path.exists():
        pre_long_df = pd.read_pickle(pre_long_path)
        pre_lookup = _build_pre_lookup_from_long(pre_long_df)
        stock_from_absolute_device_share = True
        years = sorted(
            pd.to_numeric(pre_long_df["Date"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
    else:
        pre_path = resolve_path(f"intermediate_data/export_df_for_viewing_checkpoint2_{economy}_{scenario}.pkl")
        assert pre_path is not None
        if not pre_path.exists():
            raise FileNotFoundError(
                "Missing pre-reconciliation checkpoints. Expected one of:\n"
                f"- {pre_long_path}\n- {pre_path}"
            )
        pre_df = pd.read_pickle(pre_path)
        pre_lookup = _build_pre_lookup(pre_df, scenario)
        stock_from_absolute_device_share = False
        years = sorted([int(c) for c in pre_df.columns if str(c).isdigit() and len(str(c)) == 4])

    device_share_parent_sum_lookup, device_share_parent_count_lookup = _build_parent_share_sums(
        pre_lookup,
        "Device Share",
    )
    pre_norm_device_share_parent_sum_lookup, pre_norm_device_share_parent_count_lookup = (
        _build_pre_normalization_device_share_sums(economy, scenario)
    )

    years = [y for y in years if base_year <= y <= final_year]

    @lru_cache(maxsize=None)
    def raw_agg_for_source_tuple(source_tuple: tuple[str, ...], date: int) -> RawAgg:
        sub = raw_grouped[raw_grouped["Date"] == date].copy()
        if sub.empty:
            return RawAgg()
        for i, val in enumerate(source_tuple):
            if i >= len(SOURCE_DIMS):
                break
            sub = sub[sub[SOURCE_DIMS[i]] == val]
            if sub.empty:
                return RawAgg()

        stocks = float(sub["stocks_v"].sum())
        activity = float(sub["activity_v"].sum())
        mileage = None if abs(stocks) <= 1e-12 else float(sub["mileage_num"].sum() / stocks)
        efficiency = None if abs(stocks) <= 1e-12 else float(sub["efficiency_num"].sum() / stocks)
        intensity = None if abs(activity) <= 1e-12 else float(sub["intensity_num"].sum() / activity)
        return RawAgg(stocks=stocks, mileage=mileage, efficiency=efficiency, activity=activity, intensity=intensity)

    rows: list[dict[str, Any]] = []
    for leap_branch_tuple, source_tuple in LEAP_BRANCH_TO_SOURCE_MAP.items():
        if not isinstance(source_tuple, tuple):
            continue
        analysis_type = LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP.get(leap_branch_tuple)
        if analysis_type not in {"Stock", "Intensity"}:
            continue
        leap_path = build_transport_branch_path(leap_branch_tuple, root="Demand")
        for date in years:
            pre_m = _pre_metrics_for_branch(
                lookup=pre_lookup,
                device_share_parent_sum_lookup=device_share_parent_sum_lookup,
                device_share_parent_count_lookup=device_share_parent_count_lookup,
                leap_branch_tuple=leap_branch_tuple,
                analysis_type=analysis_type,
                date=date,
                root="Demand",
                device_share_is_absolute_million_stock=stock_from_absolute_device_share,
            )
            raw_m = raw_agg_for_source_tuple(source_tuple, date)
            pre_norm_sibling_key = (build_transport_branch_path(leap_branch_tuple[:-1], root="Demand"), int(date))
            pre_norm_device_share_sibling_sum = pre_norm_device_share_parent_sum_lookup.get(pre_norm_sibling_key)
            pre_norm_device_share_sibling_count = pre_norm_device_share_parent_count_lookup.get(pre_norm_sibling_key)

            stock_pct = _safe_pct_diff(pre_m["pre_effective_stock"], raw_m.stocks)
            pre_effective_stock_scaled = (
                None
                if pre_m["pre_effective_stock"] is None
                else float(pre_m["pre_effective_stock"] * pre_stock_multiplier)
            )
            stock_pct_scaled = _safe_pct_diff(pre_effective_stock_scaled, raw_m.stocks)
            mileage_pct = _safe_pct_diff(pre_m["pre_mileage"], raw_m.mileage)
            eff_pct = _safe_pct_diff(pre_m["pre_efficiency"], raw_m.efficiency)
            activity_pct = _safe_pct_diff(pre_m["pre_effective_activity"], raw_m.activity)
            intensity_pct = _safe_pct_diff(pre_m["pre_intensity"], raw_m.intensity)
            any_over_5pct = any(
                (v is not None and pd.notna(v) and abs(float(v)) > 0.05)
                for v in (stock_pct_scaled, mileage_pct, eff_pct, activity_pct, intensity_pct)
            )
            stock_cause_flag = _stock_cause_flag(
                pre_effective_stock=pre_effective_stock_scaled,
                raw_stocks=raw_m.stocks,
                pre_device_share_sibling_sum=(
                    pre_norm_device_share_sibling_sum
                    if pre_norm_device_share_sibling_sum is not None
                    else pre_m["pre_device_share_sibling_sum"]
                ),
            )

            rows.append(
                {
                    "Economy": economy,
                    "Scenario": scenario,
                    "Date": date,
                    "LEAP Branch Tuple": str(leap_branch_tuple),
                    "LEAP Branch Path": leap_path,
                    "Source Tuple": str(source_tuple),
                    "Analysis Type": analysis_type,
                    "pre_effective_stock": pre_m["pre_effective_stock"],
                    "pre_effective_stock_scaled": pre_effective_stock_scaled,
                    "pre_parent_stock": pre_m["pre_parent_stock"],
                    "pre_mode_stock_share": pre_m["pre_mode_stock_share"],
                    "pre_device_share": pre_m["pre_device_share"],
                    "pre_device_share_sibling_sum": pre_m["pre_device_share_sibling_sum"],
                    "pre_device_share_sibling_count": pre_m["pre_device_share_sibling_count"],
                    "pre_norm_device_share_sibling_sum": pre_norm_device_share_sibling_sum,
                    "pre_norm_device_share_sibling_count": pre_norm_device_share_sibling_count,
                    "raw_stocks": raw_m.stocks,
                    "stock_pct_diff": stock_pct,
                    "stock_pct_diff_scaled": stock_pct_scaled,
                    "stock_cause_flag": stock_cause_flag,
                    "pre_mileage": pre_m["pre_mileage"],
                    "raw_mileage": raw_m.mileage,
                    "mileage_pct_diff": mileage_pct,
                    "pre_efficiency": pre_m["pre_efficiency"],
                    "raw_efficiency": raw_m.efficiency,
                    "efficiency_pct_diff": eff_pct,
                    "pre_effective_activity": pre_m["pre_effective_activity"],
                    "raw_activity": raw_m.activity,
                    "activity_pct_diff": activity_pct,
                    "pre_intensity": pre_m["pre_intensity"],
                    "raw_intensity": raw_m.intensity,
                    "intensity_pct_diff": intensity_pct,
                    "any_metric_over_5pct": any_over_5pct,
                }
            )

    return pd.DataFrame(rows)


def save_comparison_df(
    comparison_df: pd.DataFrame,
    *,
    economy: str,
    scenario: str,
    output_dir: str | Path = "results/checkpoint_audit",
) -> Path:
    resolved_output_dir = resolve_path(output_dir)
    assert resolved_output_dir is not None
    output_dir = resolved_output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"transport_pre_recon_vs_raw_disaggregated_{economy}_{scenario}.csv"
    comparison_df.to_csv(out_path, index=False)
    return out_path


def add_reverted_stock_view_columns(comparison_df: pd.DataFrame) -> pd.DataFrame:
    """Add optional unsplit stock-family columns for easier diagnostics.

    This collapses alternative fuels (biofuels/efuels) back to their fossil
    counterparts for stock comparison only.
    """
    if comparison_df.empty:
        return comparison_df.copy()

    work = comparison_df.copy()
    parsed = work["Source Tuple"].map(_parse_source_tuple)

    work["source_transport_type"] = parsed.map(lambda x: x[0] if isinstance(x, tuple) and len(x) >= 1 else None)
    work["source_medium"] = parsed.map(lambda x: x[1] if isinstance(x, tuple) and len(x) >= 2 else None)
    work["source_vehicle_type"] = parsed.map(lambda x: x[2] if isinstance(x, tuple) and len(x) >= 3 else None)
    work["source_drive"] = parsed.map(lambda x: x[3] if isinstance(x, tuple) and len(x) >= 4 else None)
    work["source_fuel"] = parsed.map(lambda x: x[4] if isinstance(x, tuple) and len(x) >= 5 else None)
    work["reverted_stock_family_fuel"] = work.apply(
        lambda r: _reverted_stock_family_fuel_label(r["source_fuel"], r["source_drive"]),
        axis=1,
    )

    family_keys = [
        "Economy",
        "Scenario",
        "Date",
        "source_transport_type",
        "source_medium",
        "source_vehicle_type",
        "source_drive",
        "reverted_stock_family_fuel",
    ]

    raw_unique = work[
        [
            *family_keys,
            "Source Tuple",
            "raw_stocks",
        ]
    ].drop_duplicates(subset=["Economy", "Scenario", "Date", "Source Tuple"], keep="first")
    raw_family = (
        raw_unique.groupby(family_keys, dropna=False, as_index=False)["raw_stocks"]
        .sum()
        .rename(columns={"raw_stocks": "raw_stocks_reverted"})
    )

    pre_family = (
        work.groupby(family_keys, dropna=False, as_index=False)[
            ["pre_effective_stock", "pre_effective_stock_scaled"]
        ]
        .sum(min_count=1)
        .rename(
            columns={
                "pre_effective_stock": "pre_effective_stock_reverted",
                "pre_effective_stock_scaled": "pre_effective_stock_scaled_reverted",
            }
        )
    )

    work = work.merge(raw_family, on=family_keys, how="left")
    work = work.merge(pre_family, on=family_keys, how="left")

    raw_den = pd.to_numeric(work["raw_stocks_reverted"], errors="coerce")
    pre_unsplit = pd.to_numeric(work["pre_effective_stock_reverted"], errors="coerce")
    pre_unsplit_scaled = pd.to_numeric(work["pre_effective_stock_scaled_reverted"], errors="coerce")
    work["stock_pct_diff_reverted"] = (pre_unsplit - raw_den) / raw_den
    work["stock_pct_diff_scaled_reverted"] = (pre_unsplit_scaled - raw_den) / raw_den
    zero_den_mask = raw_den.isna() | (raw_den.abs() <= 1e-12)
    work.loc[zero_den_mask, "stock_pct_diff_reverted"] = pd.NA
    work.loc[zero_den_mask, "stock_pct_diff_scaled_reverted"] = pd.NA

    work["stock_cause_flag_reverted"] = work.apply(
        lambda r: _stock_cause_flag(
            pre_effective_stock=_to_float_or_none(r.get("pre_effective_stock_scaled_reverted")),
            raw_stocks=_to_float_or_none(r.get("raw_stocks_reverted")),
            pre_device_share_sibling_sum=None,
        ),
        axis=1,
    )
    return work


def run_comparison_notebook(
    economy: str = "20_USA",
    scenario: str = "Reference",
    *,
    pre_stock_multiplier: float = 1.0,
    include_reverted_stock_view: bool = False,
    include_alt_fuels_in_fossil_families_stock_view: bool | None = None,
    save_csv: bool = True,
    output_dir: str | Path = "results/checkpoint_audit",
) -> tuple[pd.DataFrame, Path | None]:
    """Notebook-friendly entrypoint.

    Returns:
        comparison_df, output_path_or_None
    """
    comparison_df = build_comparison_df(
        economy=economy,
        scenario=scenario,
        pre_stock_multiplier=pre_stock_multiplier,
    )
    # Clearer option name for users unfamiliar with the pipeline internals:
    # when True, collapse alt fuels into fossil families for stock comparison
    # (e.g., Biodiesel -> Gas and diesel oil, Biogasoline -> Motor gasoline).
    include_reverted = (
        include_alt_fuels_in_fossil_families_stock_view
        if include_alt_fuels_in_fossil_families_stock_view is not None
        else include_reverted_stock_view
    )
    if include_reverted:
        comparison_df = add_reverted_stock_view_columns(comparison_df)
    out_path: Path | None = None
    if save_csv:
        out_path = save_comparison_df(
            comparison_df,
            economy=economy,
            scenario=scenario,
            output_dir=output_dir,
        )
    return comparison_df, out_path
#%%

if __name__ == "__main__":
    from results_analysis.transport_pre_recon_vs_raw_disaggregated import run_comparison_notebook

    df, out_path = run_comparison_notebook(
        economy="20_USA",
        scenario="Reference",
        include_alt_fuels_in_fossil_families_stock_view=True,
        save_csv=True,
    )
#%%
