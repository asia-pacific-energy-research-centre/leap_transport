#%%
"""Policy-aware wrapper module for transport sales curve estimation.

This module is a staging area for turnover-policy features. It mirrors core
methods from `sales_curve_estimate.py` while keeping legacy behavior available
by default. When no policy is provided, outputs are identical to the legacy
methods.
"""

from __future__ import annotations

from pathlib import Path
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

_MODULE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _MODULE_DIR.parent
DEFAULT_PASSENGER_CHECKPOINT_PATH = (
    _REPO_ROOT / "intermediate_data/transport_data_20_USA_Target_2022_2060.pkl"
).resolve()
DEFAULT_SURVIVAL_PATH = (
    _REPO_ROOT / "data/lifecycle_profiles/vehicle_survival_modified.xlsx"
).resolve()
DEFAULT_VINTAGE_PATH = (
    _REPO_ROOT / "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx"
).resolve()
DEFAULT_ESTO_ENERGY_PATH = (
    _REPO_ROOT / "data/merged_file_energy_ALL_20250814_pretrump.csv"
).resolve()

from functions.sales_curve_estimate import (
    _validate_and_align_age_profiles,
    build_freight_sales_for_economy as _legacy_build_freight_sales_for_economy,
    build_passenger_sales_for_economy as _legacy_build_passenger_sales_for_economy,
    compute_sales_from_stock_targets as _legacy_compute_sales_from_stock_targets,
    convert_result_to_dataframe as _legacy_convert_result_to_dataframe,
    DEFAULT_FREIGHT_VEHICLE_TYPE_MAP,
    DEFAULT_PASSENGER_VEHICLE_TYPE_MAP,
    load_survival_and_vintage_profiles,
    estimate_freight_sales_from_dataframe as _legacy_estimate_freight_sales_from_dataframe,
    estimate_freight_sales_from_files as _legacy_estimate_freight_sales_from_files,
    estimate_passenger_sales_from_dataframe as _legacy_estimate_passenger_sales_from_dataframe,
    estimate_passenger_sales_from_files as _legacy_estimate_passenger_sales_from_files,
    initialise_cohorts,
    plot_freight_sales_result,
    plot_passenger_sales_result,
    run_example_with_real_data as _legacy_run_example_with_real_data,
)

__all__ = [
    "derive_vehicle_turnover_policies_from_drive_policy",
    "derive_vehicle_turnover_policies_from_checkpoint",
    "derive_initial_fleet_age_shift_vintage_profiles",
    "run_passenger_policy_from_checkpoint",
    "compute_sales_from_stock_targets",
    "build_passenger_sales_for_economy",
    "build_freight_sales_for_economy",
    "estimate_passenger_sales_from_dataframe",
    "estimate_freight_sales_from_dataframe",
    "estimate_passenger_sales_from_files",
    "estimate_freight_sales_from_files",
]


DEFAULT_DRIVE_GROUPS: dict[str, tuple[str, ...]] = {
    "ice": ("ice_d", "ice_g"),
    "hybrid": ("hev", "hev_d", "hev_g"),
    "phev": ("phev_d", "phev_g"),
    "ev": ("bev", "fcev", "erev_d", "erev_g", "phev_d", "phev_g"),
}


def _raise_plot_failure(context: str, exc: Exception) -> None:
    """Raise a contextual plotting error instead of failing silently."""
    raise RuntimeError(
        f"{context} failed ({exc.__class__.__name__}): {exc}"
    ) from exc


def _resolve_repo_path(path_like: str | Path) -> Path:
    """
    Resolve paths relative to repository root when a path is not absolute.
    """
    path_obj = Path(path_like).expanduser()
    if path_obj.is_absolute():
        return path_obj.resolve()
    return (_REPO_ROOT / path_obj).resolve()


def _normalise_vehicle_type_map(vehicle_type_map: Mapping[str, str]) -> dict[str, str]:
    return {str(k).lower().strip(): str(v) for k, v in vehicle_type_map.items()}


def _resolve_drive_policy_rates(
    drive_turnover_policy: Mapping[str, Any],
    years: pd.Index,
) -> dict[str, pd.Series]:
    """
    Expand drive policy specifications into per-drive additional-retirement schedules.

    Supported input styles
    ----------------------
    1) Direct drive keys:
       {"ice_g": {2030: 0.02, 2040: 0.04}}
    2) Group key with implicit default drives:
       {"ICE": {2030: 0.02, 2040: 0.04}}
    3) Explicit spec with drives + schedule:
       {"ICE": {"drives": ["ice_d", "ice_g"], "additional_retirement_rate": {2030: 0.02}}}
    """
    rates_by_drive: dict[str, pd.Series] = {}

    for raw_key, spec in (drive_turnover_policy or {}).items():
        policy_key = str(raw_key).lower().strip()
        if spec is None:
            continue

        schedule_input: Any = None
        drive_list: list[str] = []

        if isinstance(spec, Mapping):
            drives_from_spec = spec.get("drives")
            schedule_input = spec.get("additional_retirement_rate", spec.get("rate"))

            if drives_from_spec is None:
                drive_list = list(DEFAULT_DRIVE_GROUPS.get(policy_key, (policy_key,)))
            elif isinstance(drives_from_spec, str):
                drive_list = [drives_from_spec]
            else:
                drive_list = [str(d) for d in drives_from_spec]

            if schedule_input is None:
                # Fallback: treat numeric year keys in the mapping as a schedule.
                schedule_candidate = {
                    k: v for k, v in spec.items() if str(k).strip().isdigit()
                }
                if schedule_candidate:
                    schedule_input = schedule_candidate
        else:
            drive_list = list(DEFAULT_DRIVE_GROUPS.get(policy_key, (policy_key,)))
            schedule_input = spec

        if schedule_input is None:
            continue

        schedule = _coerce_year_schedule(schedule_input, years, default=0.0).clip(lower=0.0, upper=1.0)
        for drive in drive_list:
            drive_key = str(drive).lower().strip()
            if not drive_key:
                continue
            if drive_key in rates_by_drive:
                rates_by_drive[drive_key] = (rates_by_drive[drive_key] + schedule).clip(lower=0.0, upper=1.0)
            else:
                rates_by_drive[drive_key] = schedule.copy()

    return rates_by_drive


def _coerce_year_schedule(
    values: Any,
    years: pd.Index,
    *,
    default: float,
) -> pd.Series:
    """Convert scalar/Series/dict values into a year-indexed float Series."""
    if values is None:
        return pd.Series(float(default), index=years, dtype=float)

    if np.isscalar(values):
        return pd.Series(float(values), index=years, dtype=float)

    if isinstance(values, Mapping):
        schedule = pd.Series(dict(values), dtype=float)
    else:
        schedule = pd.Series(values, dtype=float)

    if schedule.empty:
        return pd.Series(float(default), index=years, dtype=float)

    idx = pd.to_numeric(pd.Index(schedule.index), errors="coerce")
    schedule = schedule.loc[~idx.isna()].copy()
    if schedule.empty:
        return pd.Series(float(default), index=years, dtype=float)

    schedule.index = pd.Index(idx[~idx.isna()].astype(int), dtype=int)
    schedule = schedule.groupby(level=0).mean().sort_index()
    return schedule.reindex(years).ffill().fillna(float(default)).astype(float)


def _coerce_age_profile(
    values: Any,
    ages: pd.Index,
    *,
    default: float,
) -> pd.Series:
    """Convert scalar/Series/dict values into an age-indexed float Series."""
    if values is None:
        return pd.Series(float(default), index=ages, dtype=float)

    if np.isscalar(values):
        return pd.Series(float(values), index=ages, dtype=float)

    if isinstance(values, Mapping):
        profile = pd.Series(dict(values), dtype=float)
    else:
        profile = pd.Series(values, dtype=float)

    if profile.empty:
        return pd.Series(float(default), index=ages, dtype=float)

    idx = pd.to_numeric(pd.Index(profile.index), errors="coerce")
    profile = profile.loc[~idx.isna()].copy()
    if profile.empty:
        return pd.Series(float(default), index=ages, dtype=float)

    profile.index = pd.Index(idx[~idx.isna()].astype(int), dtype=int)
    profile = profile.groupby(level=0).mean().sort_index()
    return profile.reindex(ages).fillna(float(default)).astype(float)


def _resolve_vehicle_level_scalar(
    values: float | Mapping[str, Any] | None,
    vehicle_key: str,
    *,
    default: float,
) -> float:
    """
    Resolve scalar-or-mapping values to a specific vehicle key (case-insensitive).
    """
    if values is None:
        return float(default)
    if np.isscalar(values):
        return float(values)
    if not isinstance(values, Mapping):
        raise TypeError(
            "Expected scalar, mapping, or None for vehicle-level value resolution."
        )

    norm = {str(k).lower().strip(): v for k, v in values.items()}
    candidates = (
        str(vehicle_key).lower().strip(),
        "all",
        "default",
        "*",
    )
    for candidate in candidates:
        if candidate in norm:
            return float(norm[candidate])
    return float(default)


def _shift_vintage_profile_by_age_years(
    vintage_profile: pd.Series,
    *,
    age_shift_years: float,
) -> pd.Series:
    """
    Shift base-year vintage age mix.

    Positive values move stock mass to older ages; negative values move it
    younger. The profile is re-normalized to sum to 1.
    """
    profile = pd.Series(vintage_profile, dtype=float).sort_index()
    if profile.empty:
        return profile

    idx = pd.to_numeric(pd.Index(profile.index), errors="coerce")
    profile = profile.loc[~idx.isna()].copy()
    if profile.empty:
        return profile

    profile.index = pd.Index(idx[~idx.isna()].astype(int), dtype=int)
    profile = profile.groupby(level=0).mean().sort_index().clip(lower=0.0)
    total = float(profile.sum())
    if total <= 0.0:
        return profile
    profile = profile / total

    shift = float(age_shift_years)
    if abs(shift) < 1e-12:
        return profile.astype(float)

    ages = profile.index.to_numpy(dtype=float)
    base_vals = profile.to_numpy(dtype=float)
    shifted_vals = np.interp(
        ages - shift,
        ages,
        base_vals,
        left=0.0,
        right=0.0,
    )
    shifted = pd.Series(np.clip(shifted_vals, 0.0, None), index=profile.index, dtype=float)
    shifted_total = float(shifted.sum())
    if shifted_total <= 0.0:
        return profile.astype(float)
    return (shifted / shifted_total).astype(float)


def _average_age_from_vintage_profile(vintage_profile: pd.Series) -> float:
    """Compute average age from an age-share profile."""
    profile = pd.Series(vintage_profile, dtype=float).sort_index().clip(lower=0.0)
    if profile.empty:
        return 0.0
    total = float(profile.sum())
    if total <= 0.0:
        return 0.0
    ages = pd.to_numeric(pd.Index(profile.index), errors="coerce")
    valid = ~ages.isna()
    if not bool(valid.any()):
        return 0.0
    vals = profile.to_numpy(dtype=float)[valid]
    ages_valid = ages[valid].to_numpy(dtype=float)
    denom = float(vals.sum())
    if denom <= 0.0:
        return 0.0
    return float(np.dot(ages_valid, vals) / denom)


def derive_initial_fleet_age_shift_vintage_profiles(
    vintage_profiles: Mapping[str, Any],
    initial_fleet_age_shift_years: float | Mapping[str, Any] | None,
) -> tuple[dict[str, pd.Series], pd.DataFrame]:
    """
    Build shifted vintage profiles to emulate an older/younger starting fleet.
    """
    diagnostics_columns = [
        "vehicle_bucket",
        "requested_initial_fleet_age_shift_years",
        "baseline_average_age_years",
        "shifted_average_age_years",
        "implied_average_age_delta_years",
    ]
    if initial_fleet_age_shift_years is None:
        return {}, pd.DataFrame(columns=diagnostics_columns)

    shifted_profiles: dict[str, pd.Series] = {}
    diagnostics_rows: list[dict[str, float | str]] = []

    for vehicle_key, profile in vintage_profiles.items():
        vehicle_name = str(vehicle_key)
        requested_shift = _resolve_vehicle_level_scalar(
            initial_fleet_age_shift_years,
            vehicle_name,
            default=0.0,
        )
        base_profile = pd.Series(profile, dtype=float).sort_index()
        if base_profile.empty:
            continue
        shifted_profile = _shift_vintage_profile_by_age_years(
            base_profile,
            age_shift_years=requested_shift,
        )
        baseline_avg_age = _average_age_from_vintage_profile(base_profile)
        shifted_avg_age = _average_age_from_vintage_profile(shifted_profile)

        if abs(float(requested_shift)) > 1e-12:
            shifted_profiles[vehicle_name] = shifted_profile

        diagnostics_rows.append(
            {
                "vehicle_bucket": vehicle_name,
                "requested_initial_fleet_age_shift_years": float(requested_shift),
                "baseline_average_age_years": float(baseline_avg_age),
                "shifted_average_age_years": float(shifted_avg_age),
                "implied_average_age_delta_years": float(shifted_avg_age - baseline_avg_age),
            }
        )

    diagnostics = pd.DataFrame(diagnostics_rows, columns=diagnostics_columns)
    return shifted_profiles, diagnostics


def _derive_analysis_initial_age_shift_payload(
    result: Mapping[str, Any],
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None,
) -> tuple[dict[str, pd.Series], pd.DataFrame]:
    diagnostics_columns = [
        "vehicle_bucket",
        "requested_initial_fleet_age_shift_years",
        "baseline_average_age_years",
        "shifted_average_age_years",
        "implied_average_age_delta_years",
    ]
    if analysis_initial_fleet_age_shift_years is None:
        return {}, pd.DataFrame(columns=diagnostics_columns)

    vintage_profiles = result.get("vintage_profiles")
    if not isinstance(vintage_profiles, Mapping):
        raise ValueError(
            "Result is missing 'vintage_profiles' required to derive initial-age-shift vintage profiles."
        )
    return derive_initial_fleet_age_shift_vintage_profiles(
        vintage_profiles=vintage_profiles,
        initial_fleet_age_shift_years=analysis_initial_fleet_age_shift_years,
    )


def _apply_shifted_vintage_profiles(
    result: dict[str, Any],
    shifted_profiles: Mapping[str, Any],
) -> None:
    """Apply per-vehicle shifted vintage profiles to a result dict in-place."""
    if not shifted_profiles:
        return
    base_profiles = result.get("vintage_profiles")
    if not isinstance(base_profiles, Mapping):
        raise ValueError(
            "Result is missing 'vintage_profiles' required to apply initial-age-shift profiles."
        )

    updated = {str(k): pd.Series(v).copy() for k, v in base_profiles.items()}
    for vehicle_key, shifted in shifted_profiles.items():
        updated[str(vehicle_key)] = pd.Series(shifted, dtype=float).copy()
    result["vintage_profiles"] = updated


def _extract_integer_index(values: Any) -> pd.Index:
    """Extract integer index labels from scalar/Series/dict-like values."""
    if values is None or np.isscalar(values):
        return pd.Index([], dtype=int)

    if isinstance(values, Mapping):
        idx = pd.to_numeric(pd.Index(values.keys()), errors="coerce")
    else:
        series = pd.Series(values)
        idx = pd.to_numeric(pd.Index(series.index), errors="coerce")

    idx = idx[~idx.isna()]
    if idx.size == 0:
        return pd.Index([], dtype=int)
    return pd.Index(np.unique(idx.astype(int)), dtype=int)


def _multiply_age_profile_values(
    base_values: Any,
    extra_values: Any,
) -> float | pd.Series:
    """
    Multiply two age-based profile specifications (scalar/Series/dict).
    """
    if base_values is None and extra_values is None:
        return 1.0

    if np.isscalar(base_values) and np.isscalar(extra_values):
        return max(0.0, float(base_values) * float(extra_values))

    ages = _extract_integer_index(base_values).union(_extract_integer_index(extra_values))
    if ages.empty:
        base_scalar = 1.0 if base_values is None else float(base_values)
        extra_scalar = 1.0 if extra_values is None else float(extra_values)
        return max(0.0, base_scalar * extra_scalar)

    base_profile = _coerce_age_profile(base_values, ages, default=1.0)
    extra_profile = _coerce_age_profile(extra_values, ages, default=1.0)
    return (base_profile * extra_profile).clip(lower=0.0).astype(float)


def derive_vehicle_turnover_policies_from_drive_policy(
    df: pd.DataFrame,
    years: pd.Index,
    *,
    drive_turnover_policy: Mapping[str, Any],
    vehicle_type_map: Mapping[str, str] | None = None,
    transport_type: str = "passenger",
    medium: str = "road",
    economy: str | None = None,
    scenario: str | None = None,
    stocks_col: str = "Stocks",
) -> tuple[dict[str, dict[str, pd.Series]], dict[str, Any]]:
    """
    Derive vehicle-bucket turnover policies from drive-level policy definitions.

    The method uses drive-level stock shares within each vehicle bucket and year.
    Example: if LPV is 70% ICE in 2035 and ICE additional retirement is 4% in 2035,
    LPV receives an effective additional retirement rate of 2.8% in 2035.
    """
    years = pd.Index(years, dtype=int)
    if years.empty:
        return {}, {
            "effective_rates": pd.DataFrame(),
            "drive_rates": pd.DataFrame(),
            "contributions_long": pd.DataFrame(),
            "all_drive_stock_shares_long": pd.DataFrame(),
            "unused_policy_drives": [],
        }

    if vehicle_type_map is None:
        if str(transport_type).lower() == "freight":
            vehicle_type_map = DEFAULT_FREIGHT_VEHICLE_TYPE_MAP
        else:
            vehicle_type_map = DEFAULT_PASSENGER_VEHICLE_TYPE_MAP
    vehicle_type_map_norm = _normalise_vehicle_type_map(vehicle_type_map)

    required_cols = {"Date", "Vehicle Type", "Drive", stocks_col}
    missing = sorted(required_cols - set(df.columns))
    if missing:
        raise KeyError(f"Missing required columns for drive policy derivation: {missing}")

    df_use = df.copy()
    if economy is not None and "Economy" in df_use.columns:
        df_use = df_use[df_use["Economy"].astype(str) == str(economy)]
    if scenario is not None and "Scenario" in df_use.columns:
        df_use = df_use[df_use["Scenario"].astype(str) == str(scenario)]
    if "Transport Type" in df_use.columns:
        df_use = df_use[df_use["Transport Type"].astype(str).str.lower() == str(transport_type).lower()]
    if "Medium" in df_use.columns:
        df_use = df_use[df_use["Medium"].astype(str).str.lower() == str(medium).lower()]

    if df_use.empty:
        return {}, {
            "effective_rates": pd.DataFrame(index=years),
            "drive_rates": pd.DataFrame(index=years),
            "contributions_long": pd.DataFrame(),
            "all_drive_stock_shares_long": pd.DataFrame(),
            "unused_policy_drives": sorted(_resolve_drive_policy_rates(drive_turnover_policy, years).keys()),
        }

    df_use["_year"] = pd.to_numeric(df_use["Date"], errors="coerce")
    df_use = df_use[df_use["_year"].notna()].copy()
    df_use["_year"] = df_use["_year"].astype(int)
    df_use = df_use[df_use["_year"].isin(set(years))].copy()

    df_use["_vehicle_bucket"] = (
        df_use["Vehicle Type"].astype(str).str.lower().str.strip().map(vehicle_type_map_norm)
    )
    df_use["_drive_key"] = df_use["Drive"].astype(str).str.lower().str.strip()
    df_use["_stocks"] = pd.to_numeric(df_use[stocks_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    df_use = df_use.dropna(subset=["_vehicle_bucket"])

    if df_use.empty:
        return {}, {
            "effective_rates": pd.DataFrame(index=years),
            "drive_rates": pd.DataFrame(index=years),
            "contributions_long": pd.DataFrame(),
            "all_drive_stock_shares_long": pd.DataFrame(),
            "unused_policy_drives": sorted(_resolve_drive_policy_rates(drive_turnover_policy, years).keys()),
        }

    grouped = (
        df_use.groupby(["_year", "_vehicle_bucket", "_drive_key"], as_index=False)["_stocks"]
        .sum()
    )
    all_drive_stock_shares_long = grouped.rename(
        columns={
            "_year": "Date",
            "_vehicle_bucket": "vehicle_bucket",
            "_drive_key": "drive",
            "_stocks": "drive_stock",
        }
    ).copy()
    if all_drive_stock_shares_long.empty:
        all_drive_stock_shares_long = pd.DataFrame(
            columns=[
                "Date",
                "vehicle_bucket",
                "drive",
                "drive_stock",
                "vehicle_type_total_stock",
                "drive_stock_share",
            ]
        )
    else:
        vehicle_totals = (
            all_drive_stock_shares_long.groupby(["Date", "vehicle_bucket"], as_index=False)["drive_stock"]
            .sum()
            .rename(columns={"drive_stock": "vehicle_type_total_stock"})
        )
        all_drive_stock_shares_long = all_drive_stock_shares_long.merge(
            vehicle_totals,
            on=["Date", "vehicle_bucket"],
            how="left",
        )
        denom = all_drive_stock_shares_long["vehicle_type_total_stock"].replace(0.0, np.nan)
        all_drive_stock_shares_long["drive_stock_share"] = (
            all_drive_stock_shares_long["drive_stock"] / denom
        ).fillna(0.0)

    buckets = sorted(grouped["_vehicle_bucket"].dropna().astype(str).unique().tolist())
    full_index = pd.MultiIndex.from_product(
        [years, buckets],
        names=["Date", "vehicle_bucket"],
    )
    stocks_panel = grouped.pivot_table(
        index=["_year", "_vehicle_bucket"],
        columns="_drive_key",
        values="_stocks",
        aggfunc="sum",
        fill_value=0.0,
    ).reindex(full_index, fill_value=0.0)

    drive_rates = _resolve_drive_policy_rates(drive_turnover_policy, years)
    drive_rates_df = (
        pd.DataFrame({k: v.reindex(years).astype(float) for k, v in drive_rates.items()}, index=years)
        if drive_rates
        else pd.DataFrame(index=years)
    )

    effective_rates_df = pd.DataFrame(0.0, index=years, columns=buckets, dtype=float)
    contribution_frames: list[pd.DataFrame] = []

    for bucket in buckets:
        bucket_panel = stocks_panel.xs(bucket, level="vehicle_bucket")
        total_stock = bucket_panel.sum(axis=1).astype(float)
        total_safe = total_stock.replace(0.0, np.nan)

        effective = pd.Series(0.0, index=years, dtype=float)
        for drive_key, rate_series in drive_rates.items():
            drive_stock = (
                bucket_panel[drive_key].astype(float)
                if drive_key in bucket_panel.columns
                else pd.Series(0.0, index=years, dtype=float)
            )
            stock_share = (drive_stock / total_safe).fillna(0.0)
            contribution = (stock_share * rate_series.reindex(years).fillna(0.0)).astype(float)
            effective = effective.add(contribution, fill_value=0.0)

            contribution_frames.append(
                pd.DataFrame(
                    {
                        "Date": years,
                        "vehicle_bucket": bucket,
                        "drive": drive_key,
                        "drive_stock": drive_stock.to_numpy(dtype=float),
                        "bucket_total_stock": total_stock.to_numpy(dtype=float),
                        "drive_stock_share": stock_share.to_numpy(dtype=float),
                        "drive_policy_rate": rate_series.reindex(years).to_numpy(dtype=float),
                        "rate_contribution": contribution.to_numpy(dtype=float),
                    }
                )
            )

        effective_rates_df[bucket] = effective.clip(lower=0.0, upper=1.0)

    turnover_policies: dict[str, dict[str, pd.Series]] = {}
    for bucket in effective_rates_df.columns:
        rate = effective_rates_df[bucket].astype(float)
        if float(rate.max()) > 0.0:
            turnover_policies[str(bucket)] = {"additional_retirement_rate": rate}

    contributions_long = (
        pd.concat(contribution_frames, ignore_index=True)
        if contribution_frames
        else pd.DataFrame(
            columns=[
                "Date",
                "vehicle_bucket",
                "drive",
                "drive_stock",
                "bucket_total_stock",
                "drive_stock_share",
                "drive_policy_rate",
                "rate_contribution",
            ]
        )
    )
    if not contributions_long.empty:
        bucket_rates = (
            contributions_long.groupby(["Date", "vehicle_bucket"], as_index=False)["rate_contribution"]
            .sum()
            .rename(columns={"rate_contribution": "effective_bucket_rate"})
        )
        contributions_long = contributions_long.merge(
            bucket_rates,
            on=["Date", "vehicle_bucket"],
            how="left",
        )

    diagnostics = {
        "effective_rates": effective_rates_df,
        "drive_rates": drive_rates_df,
        "contributions_long": contributions_long,
        "all_drive_stock_shares_long": all_drive_stock_shares_long,
        "unused_policy_drives": sorted(set(drive_rates.keys()) - set(stocks_panel.columns.astype(str))),
    }
    return turnover_policies, diagnostics


def derive_vehicle_turnover_policies_from_checkpoint(
    checkpoint_path: str | Path,
    years: pd.Index,
    *,
    drive_turnover_policy: Mapping[str, Any],
    vehicle_type_map: Mapping[str, str] | None = None,
    transport_type: str = "passenger",
    medium: str = "road",
    economy: str | None = None,
    scenario: str | None = None,
    stocks_col: str = "Stocks",
) -> tuple[dict[str, dict[str, pd.Series]], dict[str, Any]]:
    """
    Convenience wrapper around `derive_vehicle_turnover_policies_from_drive_policy`
    that reads from a preprocessed transport checkpoint (*.pkl).
    """
    path = _resolve_repo_path(checkpoint_path)
    if not path.exists():
        raise FileNotFoundError(f"Checkpoint not found: {path}")
    df = pd.read_pickle(path)
    return derive_vehicle_turnover_policies_from_drive_policy(
        df=df,
        years=years,
        drive_turnover_policy=drive_turnover_policy,
        vehicle_type_map=vehicle_type_map,
        transport_type=transport_type,
        medium=medium,
        economy=economy,
        scenario=scenario,
        stocks_col=stocks_col,
    )


def _merge_turnover_policies(
    base_policies: Mapping[str, Mapping[str, Any]] | None,
    extra_policies: Mapping[str, Mapping[str, Any]] | None,
    years: pd.Index,
) -> dict[str, dict[str, Any]]:
    """
    Merge two turnover-policy dicts.

    `additional_retirement_rate` is additive and capped to [0, 1].
    Multipliers are merged multiplicatively (>=0). Other keys keep base values
    unless missing, then take extra values.
    """
    years = pd.Index(years, dtype=int)
    base = {
        str(k): dict(v) for k, v in (base_policies or {}).items()
    }
    extra = {
        str(k): dict(v) for k, v in (extra_policies or {}).items()
    }

    merged: dict[str, dict[str, Any]] = {k: dict(v) for k, v in base.items()}
    for vehicle_key, extra_policy in extra.items():
        current = dict(merged.get(vehicle_key, {}))

        base_rate = current.get("additional_retirement_rate")
        extra_rate = extra_policy.get("additional_retirement_rate")
        if base_rate is not None or extra_rate is not None:
            rate_base = _coerce_year_schedule(base_rate, years, default=0.0)
            rate_extra = _coerce_year_schedule(extra_rate, years, default=0.0)
            current["additional_retirement_rate"] = (rate_base + rate_extra).clip(lower=0.0, upper=1.0)

        base_survival_mult = current.get("survival_multiplier")
        extra_survival_mult = extra_policy.get("survival_multiplier")
        if base_survival_mult is not None or extra_survival_mult is not None:
            mult_base = _coerce_year_schedule(base_survival_mult, years, default=1.0)
            mult_extra = _coerce_year_schedule(extra_survival_mult, years, default=1.0)
            current["survival_multiplier"] = (mult_base * mult_extra).clip(lower=0.0)

        for key in ("age_multipliers", "survival_multipliers_by_age"):
            base_age_profile = current.get(key)
            extra_age_profile = extra_policy.get(key)
            if base_age_profile is None and extra_age_profile is None:
                continue
            current[key] = _multiply_age_profile_values(base_age_profile, extra_age_profile)

        for key, value in extra_policy.items():
            if key in {
                "additional_retirement_rate",
                "survival_multiplier",
                "age_multipliers",
                "survival_multipliers_by_age",
            }:
                continue
            if key not in current:
                current[key] = value

        merged[vehicle_key] = current

    return merged


def _subtract_turnover_policies(
    total_policies: Mapping[str, Mapping[str, Any]] | None,
    subtract_policies: Mapping[str, Mapping[str, Any]] | None,
    years: pd.Index,
) -> dict[str, dict[str, Any]]:
    """
    Subtract turnover-policy components (currently additional retirement rates).

    This is used to build drive-policy counterfactuals that keep non-drive
    turnover assumptions (e.g. age-shift survival multipliers).
    """
    years = pd.Index(years, dtype=int)
    out: dict[str, dict[str, Any]] = {
        str(k): dict(v) for k, v in (total_policies or {}).items()
    }
    sub = {
        str(k): dict(v) for k, v in (subtract_policies or {}).items()
    }

    for vehicle_key, sub_policy in sub.items():
        current = dict(out.get(vehicle_key, {}))
        total_rate = current.get("additional_retirement_rate")
        sub_rate = sub_policy.get("additional_retirement_rate")
        if total_rate is not None or sub_rate is not None:
            rate_total = _coerce_year_schedule(total_rate, years, default=0.0)
            rate_sub = _coerce_year_schedule(sub_rate, years, default=0.0)
            rate_remaining = (rate_total - rate_sub).clip(lower=0.0, upper=1.0)
            if float(rate_remaining.max()) > 1e-12:
                current["additional_retirement_rate"] = rate_remaining
            else:
                current.pop("additional_retirement_rate", None)

        if current:
            out[vehicle_key] = current
        elif vehicle_key in out:
            out.pop(vehicle_key, None)

    return out


def run_passenger_policy_from_checkpoint(
    *,
    checkpoint_path: str | Path,
    survival_path: str | Path = DEFAULT_SURVIVAL_PATH,
    vintage_path: str | Path = DEFAULT_VINTAGE_PATH,
    esto_energy_path: str | Path = DEFAULT_ESTO_ENERGY_PATH,
    economy: str = "20_USA",
    scenario: str = "Target",
    base_year: int | None = None,
    final_year: int | None = None,
    drive_turnover_policy: Mapping[str, Any] | None = None,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
    drive_policy_stocks_col: str = "Stocks",
    drive_policy_vehicle_type_map: Mapping[str, str] | None = None,
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    saturated: bool = False,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    survival_is_cumulative: bool = True,
    plot: bool = True,
) -> dict:
    """
    One-call runner for passenger policy experiments from a preprocessed checkpoint.

    This keeps policy testing separate from the transport workflow pipeline.
    """
    checkpoint_path = _resolve_repo_path(checkpoint_path)
    if not checkpoint_path.exists():
        raise FileNotFoundError(f"checkpoint_path not found: {checkpoint_path}")
    survival_path = _resolve_repo_path(survival_path)
    vintage_path = _resolve_repo_path(vintage_path)
    esto_energy_path = _resolve_repo_path(esto_energy_path)

    for path_obj, label in (
        (survival_path, "survival_path"),
        (vintage_path, "vintage_path"),
        (esto_energy_path, "esto_energy_path"),
    ):
        if not path_obj.exists():
            raise FileNotFoundError(f"{label} not found: {path_obj}")

    df = pd.read_pickle(checkpoint_path)
    if "Date" not in df.columns:
        raise KeyError("Checkpoint dataframe must include a 'Date' column.")

    df_use = df.copy()
    if "Economy" in df_use.columns:
        df_use = df_use[df_use["Economy"].astype(str) == str(economy)]
    if "Scenario" in df_use.columns:
        df_use = df_use[df_use["Scenario"].astype(str) == str(scenario)]
    if df_use.empty:
        raise ValueError(
            f"No rows in checkpoint for economy='{economy}', scenario='{scenario}'."
        )

    if base_year is None:
        base_year = int(pd.to_numeric(df_use["Date"], errors="coerce").min())
    if final_year is None:
        final_year = int(pd.to_numeric(df_use["Date"], errors="coerce").max())

    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path=survival_path,
        vintage_path=vintage_path,
        vehicle_keys=("LPV", "MC", "Bus"),
        survival_is_cumulative=survival_is_cumulative,
    )

    result = estimate_passenger_sales_from_dataframe(
        df=df,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        esto_energy_path=str(esto_energy_path),
        weights=weights,
        vehicle_shares=vehicle_shares,
        M_sat=M_sat,
        saturated=saturated,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        turnover_policies=turnover_policies,
        drive_turnover_policy=drive_turnover_policy,
        analysis_initial_fleet_age_shift_years=analysis_initial_fleet_age_shift_years,
        drive_policy_dataframe=df,
        drive_policy_stocks_col=drive_policy_stocks_col,
        drive_policy_vehicle_type_map=drive_policy_vehicle_type_map or DEFAULT_PASSENGER_VEHICLE_TYPE_MAP,
        plot=plot,
    )
    result["policy_run_inputs"] = {
        "checkpoint_path": str(checkpoint_path),
        "survival_path": str(survival_path),
        "vintage_path": str(vintage_path),
        "esto_energy_path": str(esto_energy_path),
        "economy": str(economy),
        "scenario": str(scenario),
        "base_year": int(base_year),
        "final_year": int(final_year),
        "analysis_initial_fleet_age_shift_years": analysis_initial_fleet_age_shift_years,
    }
    return result


def compute_sales_from_stock_targets(
    target_stock: pd.Series,
    survival_curve: pd.Series,
    vintage_profile: pd.Series,
    *,
    turnover_policy: Mapping[str, Any] | None = None,
    return_retirements: bool = False,
) -> tuple:
    """
    Policy-aware version of stock-target -> sales turnover.

    Legacy behavior is preserved when `turnover_policy` is None or empty.

    Supported policy keys
    ---------------------
    `additional_retirement_rate`:
      scalar/Series/dict by year in [0, 1]. Applied to post-survival cohorts.
    `age_multipliers`:
      scalar/Series/dict by age (>=0). Scales additional retirement by age.
    `survival_multiplier`:
      scalar/Series/dict by year (>=0). Multiplies annual survival.
    `survival_multipliers_by_age`:
      scalar/Series/dict by age (>=0). Multiplies annual survival by age.
    """
    if not turnover_policy:
        return _legacy_compute_sales_from_stock_targets(
            target_stock,
            survival_curve,
            vintage_profile,
            return_retirements=return_retirements,
        )

    survival_curve, vintage_profile = _validate_and_align_age_profiles(
        survival_curve,
        vintage_profile,
    )
    max_age = len(vintage_profile)
    years = pd.Index(target_stock.index)

    cohorts = initialise_cohorts(target_stock, vintage_profile)
    sales = pd.Series(0.0, index=years, dtype=float)
    retirements = pd.Series(0.0, index=years, dtype=float) if return_retirements else None

    ages = pd.Index(vintage_profile.index, dtype=int)
    base_survival = survival_curve.to_numpy(dtype=float)

    extra_ret_rate = _coerce_year_schedule(
        turnover_policy.get("additional_retirement_rate"),
        years,
        default=0.0,
    ).clip(lower=0.0, upper=1.0)
    extra_ret_age_mult = _coerce_age_profile(
        turnover_policy.get("age_multipliers"),
        ages,
        default=1.0,
    ).clip(lower=0.0)
    survival_year_mult = _coerce_year_schedule(
        turnover_policy.get("survival_multiplier"),
        years,
        default=1.0,
    ).clip(lower=0.0)
    survival_age_mult = _coerce_age_profile(
        turnover_policy.get("survival_multipliers_by_age"),
        ages,
        default=1.0,
    ).clip(lower=0.0)

    extra_ret_age_mult_arr = extra_ret_age_mult.to_numpy(dtype=float)
    survival_age_mult_arr = survival_age_mult.to_numpy(dtype=float)

    for i in range(1, len(years)):
        year_prev = years[i - 1]
        year = years[i]

        prev_cohorts = cohorts.loc[year_prev].to_numpy(dtype=float)
        new_cohorts = np.zeros_like(prev_cohorts, dtype=float)

        # 1) Natural survival + aging (optionally adjusted by policy multipliers)
        year_survival_multiplier = float(survival_year_mult.loc[year])
        for age in range(1, max_age):
            base_prob = float(base_survival[age - 1])
            age_survival_multiplier = float(survival_age_mult_arr[age - 1])
            survive_prob = np.clip(
                base_prob * year_survival_multiplier * age_survival_multiplier,
                0.0,
                1.0,
            )
            new_cohorts[age] = prev_cohorts[age - 1] * survive_prob

        natural_survivors = float(new_cohorts.sum())
        natural_retirements = max(0.0, float(prev_cohorts.sum()) - natural_survivors)

        # 2) Extra policy retirements (e.g., scrappage) from surviving cohorts
        extra_retirements = 0.0
        extra_rate_year = float(extra_ret_rate.loc[year])
        if extra_rate_year > 0.0:
            extra_rate_by_age = np.clip(extra_rate_year * extra_ret_age_mult_arr, 0.0, 1.0)
            retired_by_policy = new_cohorts * extra_rate_by_age
            new_cohorts = np.clip(new_cohorts - retired_by_policy, 0.0, None)
            extra_retirements = float(retired_by_policy.sum())

        survivors_total = float(new_cohorts.sum())
        target_total = float(target_stock.loc[year])

        # 3) Close stock balance with sales (or downscale if survivors exceed target)
        if survivors_total <= target_total:
            required_sales = target_total - survivors_total
            new_cohorts[0] = required_sales
        else:
            scale = target_total / survivors_total if survivors_total > 0 else 0.0
            new_cohorts *= scale
            required_sales = 0.0

        if retirements is not None:
            retirements.loc[year] = natural_retirements + extra_retirements

        cohorts.loc[year, :] = new_cohorts
        sales.loc[year] = required_sales

    if return_retirements:
        return sales, cohorts, retirements
    return sales, cohorts


def _update_result_with_policy_sales(
    result: dict,
    *,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None,
    transport_type: str,
) -> dict:
    """Recompute sales/retirements on an existing result dict with policies."""
    target_stocks = result.get("target_stocks")
    survival_curves = result.get("survival_curves")
    vintage_profiles = result.get("vintage_profiles")

    if not isinstance(target_stocks, dict) or not isinstance(survival_curves, dict) or not isinstance(vintage_profiles, dict):
        raise ValueError("Result is missing target_stocks/survival_curves/vintage_profiles required for policy turnover.")

    turnover_policies = turnover_policies or {}
    sales_by_type: dict[str, pd.Series] = {}
    retirements_by_type: dict[str, pd.Series] = {}

    for vehicle_key, stock_series in target_stocks.items():
        vehicle_policy = turnover_policies.get(vehicle_key)
        sales_v, _, retirements_v = compute_sales_from_stock_targets(
            target_stock=pd.Series(stock_series),
            survival_curve=pd.Series(survival_curves[vehicle_key]),
            vintage_profile=pd.Series(vintage_profiles[vehicle_key]),
            turnover_policy=vehicle_policy,
            return_retirements=True,
        )
        sales_by_type[vehicle_key] = sales_v
        retirements_by_type[vehicle_key] = retirements_v

    vehicle_keys = list(target_stocks.keys())
    total_sales = sum(sales_by_type[k] for k in vehicle_keys)
    total_retirements = sum(retirements_by_type[k] for k in vehicle_keys)

    shares = {}
    denom = total_sales.replace(0, np.nan)
    for vehicle_key in vehicle_keys:
        shares[vehicle_key] = (sales_by_type[vehicle_key] / denom).fillna(0.0)

    result["sales"] = sales_by_type
    result["retirements"] = retirements_by_type

    if transport_type == "freight":
        result["freight_total_sales"] = total_sales
        result["freight_total_retirements"] = total_retirements
        result["freight_shares"] = shares
    else:
        result["passenger_total_sales"] = total_sales
        result["passenger_total_retirements"] = total_retirements
        result["passenger_shares"] = shares

    sales_table = pd.DataFrame(sales_by_type)
    sales_table.index.name = "Date"
    result["sales_table"] = sales_table.reset_index()

    result["turnover_policies"] = dict(turnover_policies)
    result["policy_enabled"] = bool(turnover_policies)
    result["unused_turnover_policy_keys"] = sorted(set(turnover_policies.keys()) - set(vehicle_keys))
    return _legacy_convert_result_to_dataframe(result)


def build_passenger_sales_for_economy(
    years: pd.Index,
    population: pd.Series,
    energy_use_passenger: pd.Series,
    base_stocks: dict,
    survival_curves: dict,
    vintage_profiles: dict,
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    saturated: bool = False,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
    use_9th_vehicle_type_sales_shares: bool = True,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
) -> dict:
    """
    Policy-aware wrapper for passenger workflow.

    When `turnover_policies` is empty, this behaves like the legacy function.
    """
    result = _legacy_build_passenger_sales_for_economy(
        years=years,
        population=population,
        energy_use_passenger=energy_use_passenger,
        base_stocks=base_stocks,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        weights=weights,
        vehicle_shares=vehicle_shares,
        M_sat=M_sat,
        saturated=saturated,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=False,
        use_9th_vehicle_type_sales_shares=use_9th_vehicle_type_sales_shares,
    )
    combined_policies = _merge_turnover_policies(
        turnover_policies,
        None,
        pd.Index(years, dtype=int),
    )
    shifted_vintage_profiles, initial_age_shift_diagnostics = _derive_analysis_initial_age_shift_payload(
        result,
        analysis_initial_fleet_age_shift_years,
    )
    _apply_shifted_vintage_profiles(result, shifted_vintage_profiles)
    result = _update_result_with_policy_sales(
        result,
        turnover_policies=combined_policies,
        transport_type="passenger",
    )
    if analysis_initial_fleet_age_shift_years is not None:
        result["analysis_initial_fleet_age_shift_years_input"] = analysis_initial_fleet_age_shift_years
        result["shifted_vintage_profiles_from_initial_age_shift"] = shifted_vintage_profiles
        result["analysis_initial_fleet_age_shift_diagnostics"] = initial_age_shift_diagnostics
        result["policy_enabled"] = bool(result.get("policy_enabled")) or bool(shifted_vintage_profiles)

    if plot:
        try:
            result["figures"] = plot_passenger_sales_result(result, show=True)
        except ImportError as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Passenger dashboard plotting (policy wrapper)", exc)
        except Exception as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Passenger dashboard plotting (policy wrapper)", exc)
    else:
        result["figures"] = None

    return result


def build_freight_sales_for_economy(
    years: pd.Index,
    population: pd.Series,
    energy_use_freight: pd.Series,
    base_stocks: dict,
    survival_curves: dict,
    vintage_profiles: dict,
    *,
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    economy: str | None = None,
    scenario: str | None = None,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
) -> dict:
    """Policy-aware wrapper for freight workflow."""
    result = _legacy_build_freight_sales_for_economy(
        years=years,
        population=population,
        energy_use_freight=energy_use_freight,
        base_stocks=base_stocks,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        weights=weights,
        vehicle_shares=vehicle_shares,
        M_sat=M_sat,
        economy=economy,
        scenario=scenario,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=False,
    )
    combined_policies = _merge_turnover_policies(
        turnover_policies,
        None,
        pd.Index(years, dtype=int),
    )
    shifted_vintage_profiles, initial_age_shift_diagnostics = _derive_analysis_initial_age_shift_payload(
        result,
        analysis_initial_fleet_age_shift_years,
    )
    _apply_shifted_vintage_profiles(result, shifted_vintage_profiles)
    result = _update_result_with_policy_sales(
        result,
        turnover_policies=combined_policies,
        transport_type="freight",
    )
    if analysis_initial_fleet_age_shift_years is not None:
        result["analysis_initial_fleet_age_shift_years_input"] = analysis_initial_fleet_age_shift_years
        result["shifted_vintage_profiles_from_initial_age_shift"] = shifted_vintage_profiles
        result["analysis_initial_fleet_age_shift_diagnostics"] = initial_age_shift_diagnostics
        result["policy_enabled"] = bool(result.get("policy_enabled")) or bool(shifted_vintage_profiles)

    if plot:
        try:
            result["figures"] = plot_freight_sales_result(result, show=True)
        except ImportError as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Freight dashboard plotting (policy wrapper)", exc)
        except Exception as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Freight dashboard plotting (policy wrapper)", exc)
    else:
        result["figures"] = None

    return result


def estimate_passenger_sales_from_dataframe(
    *args,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    drive_turnover_policy: Mapping[str, Any] | None = None,
    drive_policy_dataframe: pd.DataFrame | None = None,
    drive_policy_stocks_col: str = "Stocks",
    drive_policy_vehicle_type_map: Mapping[str, str] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
    plot: bool = True,
    **kwargs,
) -> dict:
    """
    Policy-aware wrapper for `estimate_passenger_sales_from_dataframe`.

    Accepts all legacy arguments and adds `turnover_policies`.
    """
    kwargs = dict(kwargs)
    kwargs["plot"] = False

    df_for_drive_policy = drive_policy_dataframe
    if df_for_drive_policy is None:
        if "df" in kwargs and isinstance(kwargs["df"], pd.DataFrame):
            df_for_drive_policy = kwargs["df"]
        elif args and isinstance(args[0], pd.DataFrame):
            df_for_drive_policy = args[0]

    result = _legacy_estimate_passenger_sales_from_dataframe(*args, **kwargs)

    years = pd.Index(result.get("years", result.get("M_envelope", pd.Series(dtype=float)).index), dtype=int)
    derived_policies: dict[str, dict[str, pd.Series]] = {}
    drive_policy_diagnostics: dict[str, Any] | None = None
    if drive_turnover_policy:
        if df_for_drive_policy is None:
            raise ValueError(
                "drive_turnover_policy was provided, but no dataframe is available. "
                "Pass `drive_policy_dataframe` (or provide `df` in args/kwargs)."
            )
        derived_policies, drive_policy_diagnostics = derive_vehicle_turnover_policies_from_drive_policy(
            df=df_for_drive_policy,
            years=years,
            drive_turnover_policy=drive_turnover_policy,
            vehicle_type_map=drive_policy_vehicle_type_map or kwargs.get("vehicle_type_map") or DEFAULT_PASSENGER_VEHICLE_TYPE_MAP,
            transport_type="passenger",
            medium="road",
            economy=kwargs.get("economy"),
            scenario=kwargs.get("scenario"),
            stocks_col=drive_policy_stocks_col,
        )

    combined_policies = _merge_turnover_policies(turnover_policies, derived_policies, years)
    shifted_vintage_profiles, initial_age_shift_diagnostics = _derive_analysis_initial_age_shift_payload(
        result,
        analysis_initial_fleet_age_shift_years,
    )
    _apply_shifted_vintage_profiles(result, shifted_vintage_profiles)
    drive_counterfactual_policies = _subtract_turnover_policies(
        combined_policies,
        derived_policies,
        years,
    )
    result = _update_result_with_policy_sales(
        result,
        turnover_policies=combined_policies,
        transport_type="passenger",
    )
    if drive_turnover_policy:
        result["drive_turnover_policy_input"] = dict(drive_turnover_policy)
        result["derived_turnover_policies_from_drive"] = derived_policies
        result["drive_policy_diagnostics"] = drive_policy_diagnostics
        result["drive_policy_counterfactual_turnover_policies"] = drive_counterfactual_policies
    if analysis_initial_fleet_age_shift_years is not None:
        result["analysis_initial_fleet_age_shift_years_input"] = analysis_initial_fleet_age_shift_years
        result["shifted_vintage_profiles_from_initial_age_shift"] = shifted_vintage_profiles
        result["analysis_initial_fleet_age_shift_diagnostics"] = initial_age_shift_diagnostics
        result["policy_enabled"] = bool(result.get("policy_enabled")) or bool(shifted_vintage_profiles)

    if plot:
        try:
            result["figures"] = plot_passenger_sales_result(result, show=True)
        except ImportError as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Passenger dashboard plotting (policy dataframe wrapper)", exc)
        except Exception as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Passenger dashboard plotting (policy dataframe wrapper)", exc)
    else:
        result["figures"] = None
    return result


def estimate_freight_sales_from_dataframe(
    *args,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    drive_turnover_policy: Mapping[str, Any] | None = None,
    drive_policy_dataframe: pd.DataFrame | None = None,
    drive_policy_stocks_col: str = "Stocks",
    drive_policy_vehicle_type_map: Mapping[str, str] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
    plot: bool = True,
    **kwargs,
) -> dict:
    """
    Policy-aware wrapper for `estimate_freight_sales_from_dataframe`.

    Accepts all legacy arguments and adds `turnover_policies`.
    """
    kwargs = dict(kwargs)
    kwargs["plot"] = False

    df_for_drive_policy = drive_policy_dataframe
    if df_for_drive_policy is None:
        if "df" in kwargs and isinstance(kwargs["df"], pd.DataFrame):
            df_for_drive_policy = kwargs["df"]
        elif args and isinstance(args[0], pd.DataFrame):
            df_for_drive_policy = args[0]

    result = _legacy_estimate_freight_sales_from_dataframe(*args, **kwargs)

    years = pd.Index(result.get("years", result.get("M_envelope", pd.Series(dtype=float)).index), dtype=int)
    derived_policies: dict[str, dict[str, pd.Series]] = {}
    drive_policy_diagnostics: dict[str, Any] | None = None
    if drive_turnover_policy:
        if df_for_drive_policy is None:
            raise ValueError(
                "drive_turnover_policy was provided, but no dataframe is available. "
                "Pass `drive_policy_dataframe` (or provide `df` in args/kwargs)."
            )
        derived_policies, drive_policy_diagnostics = derive_vehicle_turnover_policies_from_drive_policy(
            df=df_for_drive_policy,
            years=years,
            drive_turnover_policy=drive_turnover_policy,
            vehicle_type_map=drive_policy_vehicle_type_map or kwargs.get("vehicle_type_map") or DEFAULT_FREIGHT_VEHICLE_TYPE_MAP,
            transport_type="freight",
            medium="road",
            economy=kwargs.get("economy"),
            scenario=kwargs.get("scenario"),
            stocks_col=drive_policy_stocks_col,
        )

    combined_policies = _merge_turnover_policies(turnover_policies, derived_policies, years)
    shifted_vintage_profiles, initial_age_shift_diagnostics = _derive_analysis_initial_age_shift_payload(
        result,
        analysis_initial_fleet_age_shift_years,
    )
    _apply_shifted_vintage_profiles(result, shifted_vintage_profiles)
    drive_counterfactual_policies = _subtract_turnover_policies(
        combined_policies,
        derived_policies,
        years,
    )
    result = _update_result_with_policy_sales(
        result,
        turnover_policies=combined_policies,
        transport_type="freight",
    )
    if drive_turnover_policy:
        result["drive_turnover_policy_input"] = dict(drive_turnover_policy)
        result["derived_turnover_policies_from_drive"] = derived_policies
        result["drive_policy_diagnostics"] = drive_policy_diagnostics
        result["drive_policy_counterfactual_turnover_policies"] = drive_counterfactual_policies
    if analysis_initial_fleet_age_shift_years is not None:
        result["analysis_initial_fleet_age_shift_years_input"] = analysis_initial_fleet_age_shift_years
        result["shifted_vintage_profiles_from_initial_age_shift"] = shifted_vintage_profiles
        result["analysis_initial_fleet_age_shift_diagnostics"] = initial_age_shift_diagnostics
        result["policy_enabled"] = bool(result.get("policy_enabled")) or bool(shifted_vintage_profiles)

    if plot:
        try:
            result["figures"] = plot_freight_sales_result(result, show=True)
        except ImportError as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Freight dashboard plotting (policy dataframe wrapper)", exc)
        except Exception as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Freight dashboard plotting (policy dataframe wrapper)", exc)
    else:
        result["figures"] = None
    return result


def estimate_passenger_sales_from_files(
    *args,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    drive_turnover_policy: Mapping[str, Any] | None = None,
    drive_policy_dataframe: pd.DataFrame | None = None,
    drive_policy_checkpoint_path: str | Path | None = None,
    drive_policy_stocks_col: str = "Stocks",
    drive_policy_vehicle_type_map: Mapping[str, str] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
    plot: bool = True,
    **kwargs,
) -> dict:
    """
    Policy-aware wrapper for `estimate_passenger_sales_from_files`.

    Accepts all legacy arguments and adds `turnover_policies`.
    """
    kwargs = dict(kwargs)
    kwargs["plot"] = False
    result = _legacy_estimate_passenger_sales_from_files(*args, **kwargs)

    years = pd.Index(result.get("years", result.get("M_envelope", pd.Series(dtype=float)).index), dtype=int)
    derived_policies: dict[str, dict[str, pd.Series]] = {}
    drive_policy_diagnostics: dict[str, Any] | None = None
    if drive_turnover_policy:
        if drive_policy_dataframe is not None:
            derived_policies, drive_policy_diagnostics = derive_vehicle_turnover_policies_from_drive_policy(
                df=drive_policy_dataframe,
                years=years,
                drive_turnover_policy=drive_turnover_policy,
                vehicle_type_map=drive_policy_vehicle_type_map or DEFAULT_PASSENGER_VEHICLE_TYPE_MAP,
                transport_type="passenger",
                medium="road",
                economy=kwargs.get("economy"),
                scenario=kwargs.get("scenario"),
                stocks_col=drive_policy_stocks_col,
            )
        elif drive_policy_checkpoint_path is not None:
            derived_policies, drive_policy_diagnostics = derive_vehicle_turnover_policies_from_checkpoint(
                checkpoint_path=drive_policy_checkpoint_path,
                years=years,
                drive_turnover_policy=drive_turnover_policy,
                vehicle_type_map=drive_policy_vehicle_type_map or DEFAULT_PASSENGER_VEHICLE_TYPE_MAP,
                transport_type="passenger",
                medium="road",
                economy=kwargs.get("economy"),
                scenario=kwargs.get("scenario"),
                stocks_col=drive_policy_stocks_col,
            )
        else:
            raise ValueError(
                "drive_turnover_policy was provided for file-based workflow, but no "
                "`drive_policy_dataframe` or `drive_policy_checkpoint_path` was supplied."
            )

    combined_policies = _merge_turnover_policies(turnover_policies, derived_policies, years)
    shifted_vintage_profiles, initial_age_shift_diagnostics = _derive_analysis_initial_age_shift_payload(
        result,
        analysis_initial_fleet_age_shift_years,
    )
    _apply_shifted_vintage_profiles(result, shifted_vintage_profiles)
    drive_counterfactual_policies = _subtract_turnover_policies(
        combined_policies,
        derived_policies,
        years,
    )
    result = _update_result_with_policy_sales(
        result,
        turnover_policies=combined_policies,
        transport_type="passenger",
    )
    if drive_turnover_policy:
        result["drive_turnover_policy_input"] = dict(drive_turnover_policy)
        result["derived_turnover_policies_from_drive"] = derived_policies
        result["drive_policy_diagnostics"] = drive_policy_diagnostics
        result["drive_policy_counterfactual_turnover_policies"] = drive_counterfactual_policies
    if analysis_initial_fleet_age_shift_years is not None:
        result["analysis_initial_fleet_age_shift_years_input"] = analysis_initial_fleet_age_shift_years
        result["shifted_vintage_profiles_from_initial_age_shift"] = shifted_vintage_profiles
        result["analysis_initial_fleet_age_shift_diagnostics"] = initial_age_shift_diagnostics
        result["policy_enabled"] = bool(result.get("policy_enabled")) or bool(shifted_vintage_profiles)

    if plot:
        try:
            result["figures"] = plot_passenger_sales_result(result, show=True)
        except ImportError as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Passenger dashboard plotting (policy file wrapper)", exc)
        except Exception as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Passenger dashboard plotting (policy file wrapper)", exc)
    else:
        result["figures"] = None
    return result


def estimate_freight_sales_from_files(
    *args,
    turnover_policies: Mapping[str, Mapping[str, Any]] | None = None,
    drive_turnover_policy: Mapping[str, Any] | None = None,
    drive_policy_dataframe: pd.DataFrame | None = None,
    drive_policy_checkpoint_path: str | Path | None = None,
    drive_policy_stocks_col: str = "Stocks",
    drive_policy_vehicle_type_map: Mapping[str, str] | None = None,
    analysis_initial_fleet_age_shift_years: float | Mapping[str, Any] | None = None,
    plot: bool = True,
    **kwargs,
) -> dict:
    """
    Policy-aware wrapper for `estimate_freight_sales_from_files`.

    Accepts all legacy arguments and adds `turnover_policies`.
    """
    kwargs = dict(kwargs)
    kwargs["plot"] = False
    result = _legacy_estimate_freight_sales_from_files(*args, **kwargs)

    years = pd.Index(result.get("years", result.get("M_envelope", pd.Series(dtype=float)).index), dtype=int)
    derived_policies: dict[str, dict[str, pd.Series]] = {}
    drive_policy_diagnostics: dict[str, Any] | None = None
    if drive_turnover_policy:
        if drive_policy_dataframe is not None:
            derived_policies, drive_policy_diagnostics = derive_vehicle_turnover_policies_from_drive_policy(
                df=drive_policy_dataframe,
                years=years,
                drive_turnover_policy=drive_turnover_policy,
                vehicle_type_map=drive_policy_vehicle_type_map or DEFAULT_FREIGHT_VEHICLE_TYPE_MAP,
                transport_type="freight",
                medium="road",
                economy=kwargs.get("economy"),
                scenario=kwargs.get("scenario"),
                stocks_col=drive_policy_stocks_col,
            )
        elif drive_policy_checkpoint_path is not None:
            derived_policies, drive_policy_diagnostics = derive_vehicle_turnover_policies_from_checkpoint(
                checkpoint_path=drive_policy_checkpoint_path,
                years=years,
                drive_turnover_policy=drive_turnover_policy,
                vehicle_type_map=drive_policy_vehicle_type_map or DEFAULT_FREIGHT_VEHICLE_TYPE_MAP,
                transport_type="freight",
                medium="road",
                economy=kwargs.get("economy"),
                scenario=kwargs.get("scenario"),
                stocks_col=drive_policy_stocks_col,
            )
        else:
            raise ValueError(
                "drive_turnover_policy was provided for file-based workflow, but no "
                "`drive_policy_dataframe` or `drive_policy_checkpoint_path` was supplied."
            )

    combined_policies = _merge_turnover_policies(turnover_policies, derived_policies, years)
    shifted_vintage_profiles, initial_age_shift_diagnostics = _derive_analysis_initial_age_shift_payload(
        result,
        analysis_initial_fleet_age_shift_years,
    )
    _apply_shifted_vintage_profiles(result, shifted_vintage_profiles)
    drive_counterfactual_policies = _subtract_turnover_policies(
        combined_policies,
        derived_policies,
        years,
    )
    result = _update_result_with_policy_sales(
        result,
        turnover_policies=combined_policies,
        transport_type="freight",
    )
    if drive_turnover_policy:
        result["drive_turnover_policy_input"] = dict(drive_turnover_policy)
        result["derived_turnover_policies_from_drive"] = derived_policies
        result["drive_policy_diagnostics"] = drive_policy_diagnostics
        result["drive_policy_counterfactual_turnover_policies"] = drive_counterfactual_policies
    if analysis_initial_fleet_age_shift_years is not None:
        result["analysis_initial_fleet_age_shift_years_input"] = analysis_initial_fleet_age_shift_years
        result["shifted_vintage_profiles_from_initial_age_shift"] = shifted_vintage_profiles
        result["analysis_initial_fleet_age_shift_diagnostics"] = initial_age_shift_diagnostics
        result["policy_enabled"] = bool(result.get("policy_enabled")) or bool(shifted_vintage_profiles)

    if plot:
        try:
            result["figures"] = plot_freight_sales_result(result, show=True)
        except ImportError as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Freight dashboard plotting (policy file wrapper)", exc)
        except Exception as exc:
            result["figures"] = {"error": exc}
            _raise_plot_failure("Freight dashboard plotting (policy file wrapper)", exc)
    else:
        result["figures"] = None
    return result


#%% example workflow (requires checkpoint to produce drive-level diagnostics)
if __name__ == "__main__":
    # Yearly bump policy ramp (e.g. +0.1 percentage-point each year from 2030).
    policy_start_year = 2040
    policy_end_year = 2060
    policy_start_rate = 0.02
    policy_annual_bump = 0.1
    policy_max_rate = 0.9
    ice_retirement_schedule = {
        year: min(
            round(policy_start_rate + policy_annual_bump * (year - policy_start_year), 4),
            policy_max_rate,
        )
        for year in range(policy_start_year, policy_end_year + 1)
    }
    drive_policy = None# {
    #     "ICE": {
    #         "drives": ["ice_d", "ice_g"],
    #         "additional_retirement_rate": ice_retirement_schedule,
    #     }
    # }

    checkpoint_candidate = DEFAULT_PASSENGER_CHECKPOINT_PATH
    if not checkpoint_candidate.exists():
        raise FileNotFoundError(
            "Drive-level dashboard output requires a checkpoint with drive stocks.\n"
            f"Looked for:\n- {checkpoint_candidate}"
        )

    result = run_passenger_policy_from_checkpoint(
        checkpoint_path=checkpoint_candidate,
        economy="20_USA",
        scenario="Target",
        drive_turnover_policy=drive_policy,
        # Analysis-only knob: +5 shifts the starting fleet older.
        # Keep this as None/omitted for production-default behavior.
        analysis_initial_fleet_age_shift_years=10,  # 20.0,
        plot=True,  # plots passenger dashboard
    )

    drive_diag = result.get("drive_policy_diagnostics")
    drive_rates = pd.DataFrame(drive_diag.get("drive_rates")) if isinstance(drive_diag, dict) else pd.DataFrame()
    if drive_rates.empty:
        raise RuntimeError(
            "Drive policy run completed, but no drive-level diagnostics were produced. "
            "Check drive keys and filtering inputs."
        )

    M_envelope = result["M_envelope"]
    target_stocks = result["target_stocks"]
    sales_by_type = result["sales"]
    passenger_total_sales = result["passenger_total_sales"]
    passenger_shares = result["passenger_shares"]
    k_used = result["k_used"]

#%%
