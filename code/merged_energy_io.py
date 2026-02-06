"""Helpers for loading and filtering merged energy transport datasets."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

from path_utils import resolve_str


TRANSPORT_SECTOR = "15_transport_sector"
DEFAULT_MERGED_ENERGY_ALL_PRETRUMP = "data/merged_file_energy_ALL_20250814_pretrump.csv"
DEFAULT_MERGED_ENERGY_APEC_PRETRUMP = "data/merged_file_energy_00_APEC_20250814_pretrump.csv"


def _normalise_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename year-like columns (e.g., '2022') to integer labels."""
    rename_map = {}
    for col in df.columns:
        if isinstance(col, str) and col.isdigit():
            rename_map[col] = int(col)
        elif isinstance(col, float) and col.is_integer():
            rename_map[col] = int(col)
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return value != 0
    value_str = str(value).strip().lower()
    return value_str in {"true", "1", "y", "yes"}


def _normalise_subtotal_flags(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("subtotal_layout", "subtotal_results"):
        if col in df.columns:
            df[col] = df[col].map(_to_bool)
    return df


@lru_cache(maxsize=16)
def _load_energy_dataset_cached(path: str, sheet_name: str = "all econs") -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path, sheet_name=sheet_name)
    df = _normalise_year_columns(df)
    df = _normalise_subtotal_flags(df)
    return df


def load_transport_energy_dataset(
    path: str,
    *,
    economy: str | None = None,
    sector: str = TRANSPORT_SECTOR,
    sheet_name: str = "all econs",
) -> pd.DataFrame:
    """Load merged energy data (CSV/XLSX), normalise years/flags, and filter to transport sector."""
    resolved_path = resolve_str(path)
    if resolved_path is None:
        raise ValueError("Energy dataset path cannot be None.")
    if economy == "00_APEC":
        resolved_path_obj = Path(resolved_path)
        if "merged_file_energy_ALL_" in resolved_path_obj.name:
            candidate = resolved_path_obj.with_name(
                resolved_path_obj.name.replace(
                    "merged_file_energy_ALL_",
                    "merged_file_energy_00_APEC_",
                    1,
                )
            )
            if candidate.exists():
                resolved_path = str(candidate)
        all_default = resolve_str(DEFAULT_MERGED_ENERGY_ALL_PRETRUMP)
        apec_default = resolve_str(DEFAULT_MERGED_ENERGY_APEC_PRETRUMP)
        if (
            all_default is not None
            and apec_default is not None
            and Path(apec_default).exists()
            and resolved_path == all_default
        ):
            resolved_path = apec_default

    df = _load_energy_dataset_cached(resolved_path, sheet_name).copy()
    if sector and "sectors" in df.columns:
        df = df[df["sectors"] == sector]
    return df


def filter_energy_for_economy_scenario(
    df: pd.DataFrame,
    *,
    economy: str,
    scenario: str,
) -> pd.DataFrame:
    """Filter loaded energy rows for an economy/scenario pair."""
    scenario_lc = scenario.lower()
    filtered = df[
        (df["economy"] == economy)
        & (df["scenarios"].astype(str).str.lower() == scenario_lc)
    ].copy()
    return filtered


def apply_relevant_subtotal_filters(
    df: pd.DataFrame,
    *,
    base_year: int,
    final_year: int,
) -> pd.DataFrame:
    """
    Keep only years in [base_year, final_year] and apply subtotal filters by year type:
    - historical/layout years (<= base_year): subtotal_layout == False
    - projected/results years (> base_year): subtotal_results == False
    """
    year_cols = sorted(col for col in df.columns if isinstance(col, int))
    selected_years = [year for year in year_cols if base_year <= year <= final_year]
    if not selected_years:
        return df.copy()

    static_cols = [col for col in df.columns if not isinstance(col, int)]
    merge_keys = [col for col in static_cols if col not in {"subtotal_layout", "subtotal_results"}]
    historical_years = [year for year in selected_years if year <= base_year]
    future_years = [year for year in selected_years if year > base_year]

    frames = []
    if historical_years:
        if "subtotal_layout" in df.columns:
            historical = df[df["subtotal_layout"] == False][merge_keys + historical_years]
        else:
            historical = df[merge_keys + historical_years]
        frames.append(historical)

    if future_years:
        if "subtotal_results" in df.columns:
            future = df[df["subtotal_results"] == False][merge_keys + future_years]
        else:
            future = df[merge_keys + future_years]
        frames.append(future)

    if not frames:
        return df.copy()
    if len(frames) == 1:
        return frames[0].copy()

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=merge_keys, how="outer")
    return merged.fillna(0.0)
