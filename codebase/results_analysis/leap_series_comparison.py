#%%
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sys
import ast

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCENARIOS: tuple[str, ...] = ("Reference", "Target")
DEFAULT_ECONOMIES: tuple[str, ...] | None = None
DEFAULT_OUTPUT_DIR = REPO_ROOT / "plotting_output"
DEFAULT_STOCK_PROXY_DIR = REPO_ROOT / "plotting_output/stock_projection_exploration"


@dataclass
class TransportResultsComparisonConfig:
    scenario: str | None = None
    scenarios: tuple[str, ...] | None = DEFAULT_SCENARIOS
    input_dir: str | Path = "results/checkpoint_audit"
    output_dir: str | Path = DEFAULT_OUTPUT_DIR
    include_economies: tuple[str, ...] | None = DEFAULT_ECONOMIES
    metrics: tuple[str, ...] = (
        "activity",
        "stock",
        "mileage",
        "efficiency",
        "intensity",
        "energy",
    )
    chart_mode: str = "line"  # "line" or "bar"
    chart_backend: str = "static"  # "static" or "plotly"
    bar_year_step: int = 5
    bar_include_base_year: bool = True
    include_international: bool = True
    international_input_dir: str | Path = "results/international"
    international_medium_summary_path: str | Path | None = None
    include_stock_proxies: bool = False
    stock_proxy_dir: str | Path = DEFAULT_STOCK_PROXY_DIR
    include_apec_aggregate: bool = False
    # Allowed values:
    # input, pre, reconciled, reconciled_plus_alt, checkpoint_direct_proxy, sales_flow_projected_proxy
    series_categories: tuple[str, ...] | None = None


@dataclass
class ComparisonArtifacts:
    comparison_long_csv: Path
    comparison_summary_csv: Path
    charts_dir: Path


_METRIC_TO_COLUMNS = {
    "activity": ("pre_effective_activity", "raw_activity"),
    "stock": ("pre_effective_stock_scaled", "raw_stocks"),
    "mileage": ("pre_mileage", "raw_mileage"),
    "efficiency": ("pre_efficiency", "raw_efficiency"),
    "intensity": ("pre_intensity", "raw_intensity"),
    "energy": ("pre_energy", "raw_energy"),
}

_ALT_FUEL_TO_BASE_FUEL = {
    "Biodiesel": "Gas and diesel oil",
    "Biogasoline": "Motor gasoline",
    "Bio jet kerosene": "Kerosene type jet fuel",
    "Biogas": "Natural gas",
}

_VALID_SERIES_CATEGORIES = {
    "input",
    "pre",
    "reconciled",
    "reconciled_plus_alt",
    "checkpoint_direct_proxy",
    "sales_flow_projected_proxy",
}


def _normalize_series_categories(series_categories: tuple[str, ...] | None) -> set[str]:
    if not series_categories:
        return set()
    out = {str(x).strip().lower() for x in series_categories if str(x).strip()}
    unknown = sorted(out - _VALID_SERIES_CATEGORIES)
    if unknown:
        print(f"[WARN] Ignoring unknown series_categories: {unknown}")
    return out & _VALID_SERIES_CATEGORIES


def _safe_filename_token(value: object) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        return "series"
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text)
    return safe.strip("_") or "series"


def _scenario_display_label(value: object) -> str:
    token = str(value).strip()
    key = token.lower()
    if key == "reference":
        return "REF"
    if key == "target":
        return "TGT"
    return token


def _resolve_repo_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    repo_candidate = (REPO_ROOT / path).resolve()
    if repo_candidate.exists():
        return repo_candidate
    cwd_candidate = (Path.cwd() / path).resolve()
    if cwd_candidate.exists():
        return cwd_candidate
    return repo_candidate


def _major_transport_type_label(source_transport_type: object, source_medium: object) -> str:
    t = str(source_transport_type).strip().lower()
    m = str(source_medium).strip().lower()

    if "pipeline" in t or "pipeline" in m:
        return "Pipelines"
    if "passenger" in t and m == "road":
        return "Passenger road"
    if "freight" in t and m == "road":
        return "Freight road"
    if "passenger" in t:
        if m and m != "non road":
            return f"Passenger {m.title()}"
        return "Passenger non-road"
    if "freight" in t:
        if m and m != "non road":
            return f"Freight {m.title()}"
        return "Freight non-road"
    if t:
        return t.title()
    return "Other"


def _is_road_major_transport_type(label: object) -> bool:
    t = str(label).strip().lower()
    if "non-road" in t or "non road" in t:
        return False
    return t.endswith("road")


def _is_pipeline_major_transport_type(label: object) -> bool:
    t = str(label).strip().lower()
    return "pipeline" in t


def _normalize_international_medium_label(value: object) -> str | None:
    token = str(value).strip().lower()
    if not token:
        return None
    if token in {"air", "international air"} or "air" in token:
        return "International Air"
    if token in {"ship", "shipping", "international shipping"} or "ship" in token:
        return "International Shipping"
    return None


def _discover_international_medium_summary_path(
    *,
    international_input_dir: str | Path,
    international_medium_summary_path: str | Path | None,
) -> Path | None:
    required_cols = {"Scenario", "Economy", "Date", "Medium", "Energy", "Activity", "Intensity"}

    if international_medium_summary_path:
        explicit_path = _resolve_repo_path(international_medium_summary_path)
        if explicit_path.exists():
            print(f"[INFO] Using explicit international medium summary: {explicit_path}")
            return explicit_path
        print(f"[WARN] Explicit international medium summary not found: {explicit_path}")

    base_dir = _resolve_repo_path(international_input_dir)
    if not base_dir.exists():
        print(f"[INFO] International input directory not found; skipping international rows: {base_dir}")
        return None

    def _valid_candidate(path: Path, *, require_multi_economy: bool) -> bool:
        try:
            sample = pd.read_csv(path, usecols=list(required_cols))
        except Exception:
            return False
        if not required_cols.issubset(sample.columns):
            return False
        if require_multi_economy and sample["Economy"].astype(str).str.strip().nunique() <= 1:
            return False
        return True

    direct_candidates = sorted(
        base_dir.glob("international_transport_medium_summary_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for candidate in direct_candidates:
        if _valid_candidate(candidate, require_multi_economy=True):
            print(f"[INFO] Selected economy-level international medium summary: {candidate}")
            return candidate

    fallback_candidates = sorted(
        base_dir.glob("*_international_transport_medium_summary_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for candidate in fallback_candidates:
        if _valid_candidate(candidate, require_multi_economy=False):
            print(f"[INFO] Selected fallback international medium summary: {candidate}")
            return candidate

    print(
        "[INFO] No valid international medium summary file found; "
        "skipping international rows."
    )
    return None


def _build_international_comparison_rows(
    *,
    config: TransportResultsComparisonConfig,
    requested_scenarios: list[str],
) -> pd.DataFrame:
    if not config.include_international:
        print("[INFO] International append disabled by config.")
        return pd.DataFrame()

    requested_metrics = {
        str(metric).strip().lower() for metric in config.metrics if str(metric).strip()
    }
    metrics_to_add = [m for m in ("activity", "energy", "intensity") if m in requested_metrics]
    if not metrics_to_add:
        print("[INFO] No requested metrics overlap with international metrics; skipping append.")
        return pd.DataFrame()

    summary_path = _discover_international_medium_summary_path(
        international_input_dir=config.international_input_dir,
        international_medium_summary_path=config.international_medium_summary_path,
    )
    if summary_path is None:
        return pd.DataFrame()

    required_cols = ["Scenario", "Economy", "Date", "Medium", "Energy", "Activity", "Intensity"]
    try:
        intl_df = pd.read_csv(summary_path, usecols=required_cols)
    except Exception as exc:
        print(f"[WARN] Failed to read international medium summary {summary_path}: {exc}")
        return pd.DataFrame()

    if intl_df.empty:
        print(f"[INFO] International medium summary is empty: {summary_path}")
        return pd.DataFrame()

    scenario_map: dict[str, str] = {}
    for scenario in requested_scenarios:
        key = str(scenario).strip().lower()
        if key and key not in scenario_map:
            scenario_map[key] = str(scenario).strip()

    if scenario_map:
        intl_df["scenario_key"] = intl_df["Scenario"].astype(str).str.strip().str.lower()
        intl_df = intl_df[intl_df["scenario_key"].isin(set(scenario_map))].copy()
        intl_df["scenario"] = intl_df["scenario_key"].map(scenario_map)
    else:
        intl_df["scenario"] = intl_df["Scenario"].astype(str).str.strip()
    if intl_df.empty:
        print("[INFO] International medium summary has no rows for requested scenarios.")
        return pd.DataFrame()

    intl_df["economy"] = intl_df["Economy"].astype(str).str.strip()
    if config.include_economies:
        allowed_norm = {_normalize_economy_code(e) for e in config.include_economies}
        intl_df = intl_df[intl_df["economy"].map(_normalize_economy_code).isin(allowed_norm)].copy()
        if intl_df.empty:
            print("[INFO] International rows filtered out by include_economies.")
            return pd.DataFrame()

    intl_df["major_transport_type"] = intl_df["Medium"].map(_normalize_international_medium_label)
    unknown_mediums = intl_df["major_transport_type"].isna().sum()
    if unknown_mediums:
        print(
            "[INFO] Skipping international rows with unsupported medium labels: "
            f"{unknown_mediums}"
        )
    intl_df = intl_df[intl_df["major_transport_type"].notna()].copy()
    if intl_df.empty:
        print("[INFO] No mappable international medium rows remain after filtering.")
        return pd.DataFrame()

    intl_df["year"] = pd.to_numeric(intl_df["Date"], errors="coerce")
    intl_df = intl_df[intl_df["year"].notna()].copy()
    intl_df["year"] = intl_df["year"].astype(int)
    if intl_df.empty:
        print("[INFO] International medium summary has no valid year rows.")
        return pd.DataFrame()

    metric_to_source_col = {
        "activity": "Activity",
        "energy": "Energy",
        "intensity": "Intensity",
    }
    out_frames: list[pd.DataFrame] = []

    for metric in metrics_to_add:
        value_col = metric_to_source_col[metric]
        metric_df = intl_df[
            ["economy", "scenario", "year", "major_transport_type", value_col]
        ].copy()
        metric_df[value_col] = pd.to_numeric(metric_df[value_col], errors="coerce")
        metric_df = metric_df.dropna(subset=[value_col])
        if metric_df.empty:
            continue

        grouped = (
            metric_df.groupby(
                ["economy", "scenario", "year", "major_transport_type"],
                dropna=False,
            )[value_col]
            .sum(min_count=1)
            .reset_index()
        )
        scale_factor, unit_label = _metric_unit_adjustment(metric, grouped[value_col])
        values = grouped[value_col].astype(float)
        if scale_factor != 1.0:
            values = values / float(scale_factor)

        out_metric = grouped[
            ["economy", "scenario", "year", "major_transport_type"]
        ].copy()
        out_metric["metric"] = metric
        out_metric["unit_label"] = unit_label
        out_metric["scale_factor"] = float(scale_factor)
        out_metric["fuel_label"] = "Total"
        out_metric["pre_value"] = values
        out_metric["reconciled_value"] = values
        out_metric["input_value"] = values
        out_metric["input_plus_alt_value"] = pd.NA
        out_metric["pre_plus_alt_value"] = pd.NA
        out_metric["reconciled_plus_alt_value"] = pd.NA
        out_metric["alt_contribution_input"] = pd.NA
        out_metric["alt_fuels_used"] = ""
        out_metric["delta_pre_vs_input"] = 0.0
        out_metric["pct_delta_pre_vs_input"] = 0.0
        out_metric["delta_recon_vs_input"] = 0.0
        out_metric["pct_delta_recon_vs_input"] = 0.0
        out_frames.append(out_metric)

    if not out_frames:
        print("[INFO] International medium summary had no usable metric rows.")
        return pd.DataFrame()

    out_df = pd.concat(out_frames, ignore_index=True)
    print(
        "[INFO] Prepared international comparison rows: "
        f"{len(out_df)} (metrics={sorted(out_df['metric'].astype(str).unique().tolist())})"
    )
    return out_df


def _add_derived_energy_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    num_cols = [
        "pre_effective_activity",
        "raw_activity",
        "pre_intensity",
        "raw_intensity",
        "pre_effective_stock_scaled",
        "raw_stocks",
        "pre_mileage",
        "raw_mileage",
        "pre_efficiency",
        "raw_efficiency",
    ]
    for col in num_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["pre_energy"] = pd.NA
    out["raw_energy"] = pd.NA
    out["recon_energy"] = pd.NA

    # Road rows should use stock*mileage*efficiency; non-road rows use activity*intensity.
    if "source_medium" in out.columns:
        road_mask = out["source_medium"].astype(str).str.strip().str.lower().eq("road")
    else:
        road_mask = pd.Series(False, index=out.index)

    act_int_pre_mask = (~road_mask) & out["pre_effective_activity"].notna() & out["pre_intensity"].notna()
    act_int_raw_mask = (~road_mask) & out["raw_activity"].notna() & out["raw_intensity"].notna()
    out.loc[act_int_pre_mask, "pre_energy"] = (
        out.loc[act_int_pre_mask, "pre_effective_activity"] * out.loc[act_int_pre_mask, "pre_intensity"]
    )
    out.loc[act_int_raw_mask, "raw_energy"] = (
        out.loc[act_int_raw_mask, "raw_activity"] * out.loc[act_int_raw_mask, "raw_intensity"]
    )
    if "recon_effective_activity" in out.columns and "recon_intensity" in out.columns:
        act_int_recon_mask = (~road_mask) & out["recon_effective_activity"].notna() & out["recon_intensity"].notna()
        out.loc[act_int_recon_mask, "recon_energy"] = (
            out.loc[act_int_recon_mask, "recon_effective_activity"]
            * out.loc[act_int_recon_mask, "recon_intensity"]
        )

    road_pre_mask = (
        out["pre_energy"].isna()
        & out["pre_effective_stock_scaled"].notna()
        & out["pre_mileage"].notna()
        & out["pre_efficiency"].notna()
    )
    road_raw_mask = (
        out["raw_energy"].isna()
        & out["raw_stocks"].notna()
        & out["raw_mileage"].notna()
        & out["raw_efficiency"].notna()
    )
    out.loc[road_pre_mask, "pre_energy"] = (
        out.loc[road_pre_mask, "pre_effective_stock_scaled"]
        * out.loc[road_pre_mask, "pre_mileage"]
        * out.loc[road_pre_mask, "pre_efficiency"]
        / 100.0
    )
    out.loc[road_raw_mask, "raw_energy"] = (
        out.loc[road_raw_mask, "raw_stocks"]
        * out.loc[road_raw_mask, "raw_mileage"]
        * out.loc[road_raw_mask, "raw_efficiency"]
        / 100.0
    )
    if {
        "recon_effective_stock_scaled",
        "recon_mileage",
        "recon_efficiency",
    }.issubset(out.columns):
        road_recon_mask = (
            out["recon_energy"].isna()
            & out["recon_effective_stock_scaled"].notna()
            & out["recon_mileage"].notna()
            & out["recon_efficiency"].notna()
        )
        out.loc[road_recon_mask, "recon_energy"] = (
            out.loc[road_recon_mask, "recon_effective_stock_scaled"]
            * out.loc[road_recon_mask, "recon_mileage"]
            * out.loc[road_recon_mask, "recon_efficiency"]
            / 100.0
        )
    return out


def _safe_parse_leap_tuple(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        return tuple(str(x) for x in value)
    try:
        parsed = ast.literal_eval(str(value))
        if isinstance(parsed, tuple):
            return tuple(str(x) for x in parsed)
    except Exception:
        return None
    return None


def _load_for_viewing_export(economy: str, scenario: str) -> pd.DataFrame:
    adjusted_path = _resolve_repo_path(
        f"results/reconciliation/transport_reconciliation_adjusted_for_viewing_{economy}_{scenario}.csv"
    )
    if not adjusted_path.exists():
        print(
            "[WARN] Missing adjusted reconciliation snapshot for comparison: "
            f"{adjusted_path}"
        )
        return pd.DataFrame()
    try:
        df = pd.read_csv(adjusted_path, low_memory=False)
        required_cols = {"Branch Path", "Variable", "Scenario"}
        if not required_cols.issubset(df.columns):
            print(
                "[WARN] Adjusted reconciliation snapshot missing required columns "
                f"{sorted(required_cols)}: {adjusted_path}"
            )
            return pd.DataFrame()
        print(f"[INFO] Using adjusted reconciliation snapshot: {adjusted_path}")
        return df
    except Exception as exc:
        print(f"[WARN] Failed reading adjusted reconciliation snapshot {adjusted_path}: {exc}")
        return pd.DataFrame()


def _lookup_year_coverage(lookup: dict[tuple[str, str, int], float], variable: str) -> int:
    years = {int(date) for (_, var, date), _ in lookup.items() if str(var) == variable}
    return len(years)


def _add_reconciled_columns(df: pd.DataFrame, *, economy: str, scenario: str) -> pd.DataFrame:
    out = df.copy()
    export_df = _load_for_viewing_export(economy, scenario)
    if export_df.empty:
        print(
            f"[WARN] Reconciled columns unavailable for {economy} | {scenario}; "
            "reconciled series will be empty for this file."
        )
        out["recon_effective_stock_scaled"] = pd.NA
        out["recon_mileage"] = pd.NA
        out["recon_efficiency"] = pd.NA
        out["recon_effective_activity"] = pd.NA
        out["recon_intensity"] = pd.NA
        return out

    try:
        import results_analysis.transport_pre_recon_vs_raw_disaggregated as precomp
    except Exception as exc:
        print(f"[WARN] Could not import reconciliation module: {exc}")
        out["recon_effective_stock_scaled"] = pd.NA
        out["recon_mileage"] = pd.NA
        out["recon_efficiency"] = pd.NA
        out["recon_effective_activity"] = pd.NA
        out["recon_intensity"] = pd.NA
        return out

    lookup = precomp._build_pre_lookup(export_df, scenario)  # noqa: SLF001
    stock_years = _lookup_year_coverage(lookup, "Stock")
    stock_share_years = _lookup_year_coverage(lookup, "Stock Share")
    device_share_years = _lookup_year_coverage(lookup, "Device Share")
    if max(stock_years, stock_share_years) <= 1 and device_share_years > 1:
        print(
            f"[WARN] Sparse Stock/Stock Share in FOR_VIEWING for {economy} | {scenario}: "
            f"Stock years={stock_years}, Stock Share years={stock_share_years}, "
            f"Device Share years={device_share_years}. "
            "Road reconciled stock/energy series may appear sparse in comparison charts."
        )
    share_sums, share_counts = precomp._build_parent_share_sums(lookup, "Device Share")  # noqa: SLF001

    key_cols = ["LEAP Branch Tuple", "Analysis Type", "Date"]
    out["Date"] = pd.to_numeric(out["Date"], errors="coerce").astype("Int64")
    work = out[key_cols].dropna(subset=["Date"]).drop_duplicates().copy()
    recon_rows: list[dict] = []
    for _, row in work.iterrows():
        leap_tuple = _safe_parse_leap_tuple(row["LEAP Branch Tuple"])
        if not leap_tuple:
            continue
        analysis_type = str(row["Analysis Type"]).strip()
        date = int(row["Date"])
        metrics = precomp._pre_metrics_for_branch(  # noqa: SLF001
            lookup,
            share_sums,
            share_counts,
            leap_tuple,
            analysis_type,
            date,
            root="Demand",
        )
        recon_rows.append(
            {
                "LEAP Branch Tuple": row["LEAP Branch Tuple"],
                "Analysis Type": analysis_type,
                "Date": date,
                "recon_effective_stock_scaled": metrics.get("pre_effective_stock"),
                "recon_mileage": metrics.get("pre_mileage"),
                "recon_efficiency": metrics.get("pre_efficiency"),
                "recon_effective_activity": metrics.get("pre_effective_activity"),
                "recon_intensity": metrics.get("pre_intensity"),
            }
        )

    if recon_rows:
        recon_df = pd.DataFrame(recon_rows)
        out = out.merge(recon_df, on=key_cols, how="left")
    else:
        out["recon_effective_stock_scaled"] = pd.NA
        out["recon_mileage"] = pd.NA
        out["recon_efficiency"] = pd.NA
        out["recon_effective_activity"] = pd.NA
        out["recon_intensity"] = pd.NA
    return out


def _fill_sparse_reconciled_projections(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill missing reconciled factors with pre series to avoid base-year-only traces."""
    out = df.copy()
    fallback_map = {
        "recon_effective_stock_scaled": "pre_effective_stock_scaled",
        "recon_mileage": "pre_mileage",
        "recon_efficiency": "pre_efficiency",
        "recon_effective_activity": "pre_effective_activity",
        "recon_intensity": "pre_intensity",
    }
    for recon_col, pre_col in fallback_map.items():
        if recon_col not in out.columns or pre_col not in out.columns:
            continue
        out[recon_col] = pd.to_numeric(out[recon_col], errors="coerce").combine_first(
            pd.to_numeric(out[pre_col], errors="coerce")
        )
    return out


def _metric_unit_adjustment(metric: str, values: pd.Series) -> tuple[float, str]:
    metric = str(metric).lower()
    if metric == "activity":
        return 1e9, "billion pkm/tkm"
    if metric == "efficiency":
        vmax = float(pd.to_numeric(values, errors="coerce").abs().max(skipna=True) or 0.0)
        if vmax >= 1000:
            return 1000.0, "thousand MJ/100km"
        return 1.0, "MJ/100km"
    if metric == "stock":
        vmax = float(pd.to_numeric(values, errors="coerce").abs().max(skipna=True) or 0.0)
        if vmax >= 1e6:
            return 1e6, "million vehicles"
        return 1.0, "vehicles"
    if metric == "energy":
        # Keep all energy charts in PJ for consistency.
        return 1e9, "PJ"
    if metric == "mileage":
        return 1.0, "km"
    if metric == "intensity":
        return 1.0, "MJ/pkm or MJ/tkm"
    return 1.0, "raw units"


def _discover_input_files(input_dir: Path, scenario: str) -> list[tuple[str, Path]]:
    pattern = re.compile(
        rf"^transport_pre_recon_vs_raw_disaggregated_(.+)_{re.escape(scenario)}\.csv$",
        flags=re.IGNORECASE,
    )
    files: list[tuple[str, Path]] = []
    for path in sorted(input_dir.glob(f"transport_pre_recon_vs_raw_disaggregated_*_{scenario}.csv")):
        match = pattern.match(path.name)
        if not match:
            continue
        economy = match.group(1)
        files.append((economy, path))
    return files


def _available_scenarios(input_dir: Path) -> list[str]:
    pattern = re.compile(r"^transport_pre_recon_vs_raw_disaggregated_(.+)_(.+)\.csv$", flags=re.IGNORECASE)
    found: set[str] = set()
    for path in input_dir.glob("transport_pre_recon_vs_raw_disaggregated_*.csv"):
        match = pattern.match(path.name)
        if match:
            found.add(match.group(2))
    return sorted(found)


def _normalize_economy_code(value: object) -> str:
    return str(value).strip().upper()


def _coerce_source_columns_from_tuple(df: pd.DataFrame) -> pd.DataFrame:
    required = {"source_transport_type", "source_medium", "source_fuel"}
    if required.issubset(df.columns):
        return df
    if "Source Tuple" not in df.columns:
        return df

    work = df.copy()
    parsed = work["Source Tuple"].map(
        lambda value: ast.literal_eval(value) if isinstance(value, str) and value.strip().startswith("(") else None
    )
    work["source_transport_type"] = parsed.map(
        lambda tup: tup[0] if isinstance(tup, tuple) and len(tup) >= 1 else pd.NA
    )
    work["source_medium"] = parsed.map(
        lambda tup: tup[1] if isinstance(tup, tuple) and len(tup) >= 2 else pd.NA
    )
    work["source_fuel"] = parsed.map(
        lambda tup: tup[4] if isinstance(tup, tuple) and len(tup) >= 5 else pd.NA
    )
    return work


def _aggregate_rows(
    df: pd.DataFrame,
    *,
    economy: str,
    scenario: str,
    metric: str,
    transport_col: str,
    fuel_col: str,
    pre_col: str,
    raw_col: str,
    recon_col: str,
) -> pd.DataFrame:
    metric_key = str(metric).strip().lower()
    factor_weight_cols = {
        "mileage": {
            "input_value": "raw_stocks",
            "pre_value": "pre_effective_stock_scaled",
            "reconciled_value": "recon_effective_stock_scaled",
        },
        "efficiency": {
            "input_value": "raw_stocks",
            "pre_value": "pre_effective_stock_scaled",
            "reconciled_value": "recon_effective_stock_scaled",
        },
        "intensity": {
            "input_value": "raw_activity",
            "pre_value": "pre_effective_activity",
            "reconciled_value": "recon_effective_activity",
        },
    }.get(metric_key)
    required_cols = {"Date", transport_col, fuel_col, pre_col, raw_col, recon_col}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame()

    select_cols = ["Date", transport_col, fuel_col, pre_col, raw_col, recon_col]
    if factor_weight_cols:
        for weight_col in factor_weight_cols.values():
            if weight_col in df.columns:
                select_cols.append(weight_col)
    select_cols = list(dict.fromkeys(select_cols))
    work = df[select_cols].copy()
    work = work.dropna(subset=["Date", transport_col, fuel_col])
    work[transport_col] = work[transport_col].astype(str).str.strip()
    work[fuel_col] = work[fuel_col].astype(str).str.strip()
    work = work[(work[transport_col] != "") & (work[fuel_col] != "")]
    work["Date"] = pd.to_numeric(work["Date"], errors="coerce")
    work[pre_col] = pd.to_numeric(work[pre_col], errors="coerce")
    work[raw_col] = pd.to_numeric(work[raw_col], errors="coerce")
    work[recon_col] = pd.to_numeric(work[recon_col], errors="coerce")
    if factor_weight_cols:
        for weight_col in set(factor_weight_cols.values()):
            if weight_col in work.columns:
                work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce")
    # Pipeline disaggregated exports can have empty raw fields; use pre values as
    # a conservative fallback so input traces remain visible in comparison charts.
    pipeline_mask = work[transport_col].astype(str).str.strip().str.lower().eq("pipelines")
    pipeline_raw_missing_mask = pipeline_mask & work[raw_col].isna() & work[pre_col].notna()
    if pipeline_raw_missing_mask.any():
        work.loc[pipeline_raw_missing_mask, raw_col] = work.loc[pipeline_raw_missing_mask, pre_col]
    work = work.dropna(subset=["Date"])
    if work.empty:
        return pd.DataFrame()
    international_mask = (
        work[transport_col].astype(str).str.contains("international", case=False, na=False)
        | work[fuel_col].astype(str).str.contains("international", case=False, na=False)
    )
    if international_mask.any():
        work.loc[international_mask, [pre_col, raw_col, recon_col]] = (
            work.loc[international_mask, [pre_col, raw_col, recon_col]].abs()
        )

    group_keys = ["Date", transport_col, fuel_col]
    factor_weights_present = bool(
        factor_weight_cols
        and all(
            weight_col in work.columns
            for weight_col in factor_weight_cols.values()
        )
    )
    if factor_weights_present:
        grouped = work[group_keys].drop_duplicates().copy()
        for src_col, out_col in [
            (pre_col, "pre_value"),
            (raw_col, "input_value"),
            (recon_col, "reconciled_value"),
        ]:
            weight_col = factor_weight_cols[out_col]
            tmp = work[group_keys].copy()
            tmp["_factor"] = pd.to_numeric(work[src_col], errors="coerce")
            tmp["_weight"] = pd.to_numeric(work[weight_col], errors="coerce")
            tmp["_num"] = tmp["_factor"] * tmp["_weight"]
            tmp["_den"] = tmp["_weight"].where(tmp["_factor"].notna())
            sums = (
                tmp.groupby(group_keys, dropna=False)[["_num", "_den"]]
                .sum(min_count=1)
                .reset_index()
            )
            sums[out_col] = sums["_num"] / sums["_den"].replace(0, pd.NA)
            grouped = grouped.merge(sums[group_keys + [out_col]], on=group_keys, how="left")
    else:
        grouped = (
            work.groupby(group_keys, dropna=False)[[pre_col, raw_col, recon_col]]
            .sum(min_count=1)
            .reset_index()
            .rename(
                columns={
                    pre_col: "pre_value",
                    raw_col: "input_value",
                    recon_col: "reconciled_value",
                }
            )
        )
    grouped = grouped.rename(
        columns={
            "Date": "year",
            transport_col: "major_transport_type",
            fuel_col: "fuel_label",
        }
    )
    grouped["year"] = grouped["year"].astype(int)
    scale_factor, unit_label = _metric_unit_adjustment(
        metric,
        pd.concat([grouped["pre_value"], grouped["reconciled_value"], grouped["input_value"]], ignore_index=True),
    )
    if scale_factor != 1.0:
        grouped["pre_value"] = grouped["pre_value"] / scale_factor
        grouped["reconciled_value"] = grouped["reconciled_value"] / scale_factor
        grouped["input_value"] = grouped["input_value"] / scale_factor
    grouped["delta_pre_vs_input"] = grouped["pre_value"] - grouped["input_value"]
    grouped["pct_delta_pre_vs_input"] = grouped["delta_pre_vs_input"] / grouped["input_value"].replace(0, pd.NA)
    grouped["delta_recon_vs_input"] = grouped["reconciled_value"] - grouped["input_value"]
    grouped["pct_delta_recon_vs_input"] = grouped["delta_recon_vs_input"] / grouped["input_value"].replace(0, pd.NA)

    grouped["input_plus_alt_value"] = pd.NA
    grouped["pre_plus_alt_value"] = pd.NA
    grouped["reconciled_plus_alt_value"] = pd.NA
    grouped["alt_contribution_input"] = pd.NA
    grouped["alt_fuels_used"] = ""

    base_to_alts: dict[str, list[str]] = {}
    for alt, base in _ALT_FUEL_TO_BASE_FUEL.items():
        base_to_alts.setdefault(base, []).append(alt)

    key_cols = ["major_transport_type", "year"]
    weight_lookup: pd.DataFrame | None = None
    if factor_weights_present and factor_weight_cols:
        weight_cols = sorted(set(factor_weight_cols.values()))
        rename_map = {
            factor_weight_cols["input_value"]: "input_weight_value",
            factor_weight_cols["pre_value"]: "pre_weight_value",
            factor_weight_cols["reconciled_value"]: "reconciled_weight_value",
        }
        weight_lookup = (
            work.rename(columns={"Date": "year", transport_col: "major_transport_type", fuel_col: "fuel_label"})[
                ["year", "major_transport_type", "fuel_label"] + weight_cols
            ]
            .copy()
        )
        weight_lookup["year"] = pd.to_numeric(weight_lookup["year"], errors="coerce").astype("Int64")
        weight_lookup = weight_lookup.dropna(subset=["year"])
        weight_lookup["year"] = weight_lookup["year"].astype(int)
        for weight_col in weight_cols:
            weight_lookup[weight_col] = pd.to_numeric(weight_lookup[weight_col], errors="coerce")
        weight_lookup = (
            weight_lookup.groupby(["year", "major_transport_type", "fuel_label"], dropna=False)[weight_cols]
            .sum(min_count=1)
            .reset_index()
            .rename(columns=rename_map)
        )
    for base_fuel, alt_fuels in base_to_alts.items():
        base_mask = grouped["fuel_label"].astype(str) == base_fuel
        if not base_mask.any():
            continue
        alt_df = grouped[grouped["fuel_label"].isin(alt_fuels)]
        if alt_df.empty:
            continue
        if factor_weights_present and weight_lookup is not None:
            calc_df = grouped[
                grouped["fuel_label"].astype(str).isin([base_fuel] + alt_fuels)
            ][key_cols + ["fuel_label", "input_value", "pre_value", "reconciled_value"]].copy()
            calc_df = calc_df.merge(
                weight_lookup,
                on=["year", "major_transport_type", "fuel_label"],
                how="left",
            )

            base_rows = grouped.loc[base_mask, key_cols].copy()
            weighted_stats = base_rows.copy()

            for series_col, out_col in [
                ("input_value", "input_plus_alt_value"),
                ("pre_value", "pre_plus_alt_value"),
                ("reconciled_value", "reconciled_plus_alt_value"),
            ]:
                weight_col = {
                    "input_value": "input_weight_value",
                    "pre_value": "pre_weight_value",
                    "reconciled_value": "reconciled_weight_value",
                }[series_col]
                tmp = calc_df.copy()
                tmp["_factor"] = pd.to_numeric(tmp[series_col], errors="coerce")
                tmp["_weight"] = pd.to_numeric(tmp[weight_col], errors="coerce")
                tmp["_num"] = tmp["_factor"] * tmp["_weight"]
                tmp["_den"] = tmp["_weight"].where(tmp["_factor"].notna())
                sums = (
                    tmp.groupby(key_cols, dropna=False)[["_num", "_den"]]
                    .sum(min_count=1)
                    .reset_index()
                )
                sums[out_col] = sums["_num"] / sums["_den"].replace(0, pd.NA)
                weighted_stats = weighted_stats.merge(sums[key_cols + [out_col]], on=key_cols, how="left")

            alt_only = calc_df[calc_df["fuel_label"].astype(str).isin(alt_fuels)].copy()
            alt_input = (
                alt_only.groupby(key_cols, dropna=False)["input_weight_value"]
                .sum(min_count=1)
                .reset_index()
                .rename(columns={"input_weight_value": "alt_contribution_input"})
            )
            weighted_stats = weighted_stats.merge(alt_input, on=key_cols, how="left", suffixes=("", "_alt"))

            base_rows_with_idx = grouped.loc[base_mask, key_cols].copy()
            base_rows_with_idx["_idx"] = grouped.index[base_mask]
            weighted_stats = base_rows_with_idx.merge(weighted_stats, on=key_cols, how="left")
            grouped.loc[weighted_stats["_idx"], "input_plus_alt_value"] = pd.to_numeric(
                weighted_stats["input_plus_alt_value"], errors="coerce"
            ).to_numpy()
            grouped.loc[weighted_stats["_idx"], "pre_plus_alt_value"] = pd.to_numeric(
                weighted_stats["pre_plus_alt_value"], errors="coerce"
            ).to_numpy()
            grouped.loc[weighted_stats["_idx"], "reconciled_plus_alt_value"] = pd.to_numeric(
                weighted_stats["reconciled_plus_alt_value"], errors="coerce"
            ).to_numpy()
            grouped.loc[weighted_stats["_idx"], "alt_contribution_input"] = pd.to_numeric(
                weighted_stats["alt_contribution_input"], errors="coerce"
            ).to_numpy()
        else:
            alt_sums = (
                alt_df.groupby(key_cols, dropna=False)[["input_value", "pre_value", "reconciled_value"]]
                .sum(min_count=1)
                .reset_index()
                .rename(
                    columns={
                        "input_value": "alt_input_sum",
                        "pre_value": "alt_pre_sum",
                        "reconciled_value": "alt_recon_sum",
                    }
                )
            )
            base_rows = grouped.loc[base_mask, key_cols + ["input_value", "pre_value", "reconciled_value"]].copy()
            merged = base_rows.merge(alt_sums, on=key_cols, how="left")
            for col in ["alt_input_sum", "alt_pre_sum", "alt_recon_sum"]:
                merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)

            grouped.loc[base_mask, "input_plus_alt_value"] = (
                pd.to_numeric(grouped.loc[base_mask, "input_value"], errors="coerce").to_numpy()
                + merged["alt_input_sum"].to_numpy()
            )
            grouped.loc[base_mask, "pre_plus_alt_value"] = (
                pd.to_numeric(grouped.loc[base_mask, "pre_value"], errors="coerce").to_numpy()
                + merged["alt_pre_sum"].to_numpy()
            )
            grouped.loc[base_mask, "reconciled_plus_alt_value"] = (
                pd.to_numeric(grouped.loc[base_mask, "reconciled_value"], errors="coerce").to_numpy()
                + merged["alt_recon_sum"].to_numpy()
            )
            grouped.loc[base_mask, "alt_contribution_input"] = merged["alt_input_sum"].to_numpy()
        grouped.loc[base_mask, "alt_fuels_used"] = ", ".join(alt_fuels)
    grouped["economy"] = economy
    grouped["scenario"] = scenario
    grouped["metric"] = metric
    grouped["unit_label"] = unit_label
    grouped["scale_factor"] = scale_factor
    cols = [
        "economy",
        "scenario",
        "metric",
        "unit_label",
        "scale_factor",
        "major_transport_type",
        "fuel_label",
        "year",
        "pre_value",
        "reconciled_value",
        "input_value",
        "input_plus_alt_value",
        "pre_plus_alt_value",
        "reconciled_plus_alt_value",
        "alt_contribution_input",
        "alt_fuels_used",
        "delta_pre_vs_input",
        "pct_delta_pre_vs_input",
        "delta_recon_vs_input",
        "pct_delta_recon_vs_input",
    ]
    return grouped[cols]


def _append_total_rows(comparison_long: pd.DataFrame) -> pd.DataFrame:
    if comparison_long.empty:
        return comparison_long
    total_group_cols = ["economy", "scenario", "metric", "major_transport_type", "year"]
    value_cols = [
        "pre_value",
        "reconciled_value",
        "input_value",
        "input_plus_alt_value",
        "pre_plus_alt_value",
        "reconciled_plus_alt_value",
        "alt_contribution_input",
    ]
    existing_value_cols = [col for col in value_cols if col in comparison_long.columns]
    total_df = (
        comparison_long.groupby(total_group_cols, dropna=False)[existing_value_cols]
        .sum(min_count=1)
        .reset_index()
    )
    first_labels = (
        comparison_long.groupby(["economy", "scenario", "metric", "major_transport_type"], dropna=False)[
            ["unit_label", "scale_factor"]
        ]
        .first()
        .reset_index()
    )
    total_df = total_df.merge(
        first_labels,
        on=["economy", "scenario", "metric", "major_transport_type"],
        how="left",
    )

    # For factor metrics, totals are weighted averages rather than sums:
    # - mileage, efficiency weighted by stock
    # - intensity weighted by activity
    factor_to_weight_metric = {
        "mileage": "stock",
        "efficiency": "stock",
        "intensity": "activity",
    }
    value_series_cols = [
        col
        for col in [
            "pre_value",
            "reconciled_value",
            "input_value",
            "input_plus_alt_value",
            "pre_plus_alt_value",
            "reconciled_plus_alt_value",
        ]
        if col in comparison_long.columns
    ]
    factor_rows = comparison_long.copy()
    join_keys = ["economy", "scenario", "major_transport_type", "fuel_label", "year"]
    for factor_metric, weight_metric in factor_to_weight_metric.items():
        factor_metric_rows = factor_rows[factor_rows["metric"].astype(str) == factor_metric].copy()
        weight_metric_rows = factor_rows[factor_rows["metric"].astype(str) == weight_metric].copy()
        if factor_metric_rows.empty or weight_metric_rows.empty:
            continue
        # Totals for factor series should always be weighted by INPUT weights.
        # This prevents reconciled/reconciled+alt factor totals from becoming plain sums.
        if "input_value" not in weight_metric_rows.columns:
            continue
        weight_lookup = weight_metric_rows[join_keys + ["input_value"]].copy().rename(
            columns={"input_value": "weight_input_value"}
        )
        merged = factor_metric_rows[join_keys + value_series_cols].merge(
            weight_lookup,
            on=join_keys,
            how="left",
        )
        if merged.empty:
            continue

        agg_keys = ["economy", "scenario", "major_transport_type", "year"]
        weighted_out = merged[agg_keys].drop_duplicates().copy()
        for col in value_series_cols:
            num_col = pd.to_numeric(merged[col], errors="coerce") * pd.to_numeric(
                merged["weight_input_value"], errors="coerce"
            )
            den_col = pd.to_numeric(merged["weight_input_value"], errors="coerce")
            tmp = merged[agg_keys].copy()
            tmp["_num"] = num_col
            tmp["_den"] = den_col
            sums = tmp.groupby(agg_keys, dropna=False)[["_num", "_den"]].sum(min_count=1).reset_index()
            sums[col] = sums["_num"] / sums["_den"].replace(0, pd.NA)
            weighted_out = weighted_out.merge(sums[agg_keys + [col]], on=agg_keys, how="left")

        total_mask = total_df["metric"].astype(str).eq(factor_metric)
        if total_mask.any():
            total_df = total_df.merge(
                weighted_out,
                on=agg_keys,
                how="left",
                suffixes=("", "_weighted"),
            )
            for col in value_series_cols:
                wcol = f"{col}_weighted"
                if wcol in total_df.columns:
                    total_df.loc[total_mask, col] = pd.to_numeric(
                        total_df.loc[total_mask, wcol], errors="coerce"
                    )
            total_df = total_df.drop(
                columns=[f"{col}_weighted" for col in value_series_cols if f"{col}_weighted" in total_df.columns],
                errors="ignore",
            )

    total_df["delta_pre_vs_input"] = total_df["pre_value"] - total_df["input_value"]
    total_df["pct_delta_pre_vs_input"] = (
        total_df["delta_pre_vs_input"] / pd.to_numeric(total_df["input_value"], errors="coerce").replace(0, pd.NA)
    )
    total_df["delta_recon_vs_input"] = total_df["reconciled_value"] - total_df["input_value"]
    total_df["pct_delta_recon_vs_input"] = (
        total_df["delta_recon_vs_input"] / pd.to_numeric(total_df["input_value"], errors="coerce").replace(0, pd.NA)
    )
    total_df["fuel_label"] = "Total"
    total_df["alt_fuels_used"] = ""
    cols = comparison_long.columns.tolist()
    for col in cols:
        if col not in total_df.columns:
            total_df[col] = pd.NA
    total_df = total_df[cols]
    return pd.concat([comparison_long, total_df], ignore_index=True)


def _append_apec_aggregate_rows(
    comparison_long: pd.DataFrame,
    *,
    aggregate_economy: str = "00_APEC",
) -> pd.DataFrame:
    if comparison_long.empty:
        return comparison_long
    if "economy" not in comparison_long.columns:
        return comparison_long

    base = comparison_long[comparison_long["economy"].astype(str) != str(aggregate_economy)].copy()
    if base.empty:
        return comparison_long

    value_cols = [
        "pre_value",
        "reconciled_value",
        "input_value",
        "input_plus_alt_value",
        "pre_plus_alt_value",
        "reconciled_plus_alt_value",
        "alt_contribution_input",
    ]
    existing_value_cols = [col for col in value_cols if col in base.columns]
    if not existing_value_cols:
        return comparison_long

    group_cols = ["scenario", "metric", "major_transport_type", "fuel_label", "year"]
    agg_df = (
        base.groupby(group_cols, dropna=False)[existing_value_cols]
        .sum(min_count=1)
        .reset_index()
    )
    first_labels = (
        base.groupby(["metric", "major_transport_type"], dropna=False)[["unit_label", "scale_factor"]]
        .first()
        .reset_index()
    )
    agg_df = agg_df.merge(first_labels, on=["metric", "major_transport_type"], how="left")
    agg_df["economy"] = str(aggregate_economy)
    agg_df["alt_fuels_used"] = ""

    # Weighted average override for factor metrics at aggregated-economy level.
    factor_to_weight_metric = {
        "mileage": "stock",
        "efficiency": "stock",
        "intensity": "activity",
    }
    value_series_cols = [
        col
        for col in [
            "pre_value",
            "reconciled_value",
            "input_value",
            "input_plus_alt_value",
            "pre_plus_alt_value",
            "reconciled_plus_alt_value",
        ]
        if col in base.columns
    ]
    join_keys = ["scenario", "major_transport_type", "fuel_label", "year"]
    for factor_metric, weight_metric in factor_to_weight_metric.items():
        factor_metric_rows = base[base["metric"].astype(str) == factor_metric].copy()
        weight_metric_rows = base[base["metric"].astype(str) == weight_metric].copy()
        if factor_metric_rows.empty or weight_metric_rows.empty:
            continue
        if "input_value" not in weight_metric_rows.columns:
            continue
        weight_lookup = weight_metric_rows[join_keys + ["input_value"]].copy().rename(
            columns={"input_value": "weight_input_value"}
        )
        merged = factor_metric_rows[join_keys + value_series_cols].merge(
            weight_lookup,
            on=join_keys,
            how="left",
        )
        if merged.empty:
            continue
        agg_keys = ["scenario", "major_transport_type", "fuel_label", "year"]
        weighted_out = merged[agg_keys].drop_duplicates().copy()
        for col in value_series_cols:
            num_col = pd.to_numeric(merged[col], errors="coerce") * pd.to_numeric(
                merged["weight_input_value"], errors="coerce"
            )
            den_col = pd.to_numeric(merged["weight_input_value"], errors="coerce")
            tmp = merged[agg_keys].copy()
            tmp["_num"] = num_col
            tmp["_den"] = den_col
            sums = tmp.groupby(agg_keys, dropna=False)[["_num", "_den"]].sum(min_count=1).reset_index()
            sums[col] = sums["_num"] / sums["_den"].replace(0, pd.NA)
            weighted_out = weighted_out.merge(sums[agg_keys + [col]], on=agg_keys, how="left")

        mask = agg_df["metric"].astype(str).eq(factor_metric)
        if mask.any():
            agg_df = agg_df.merge(
                weighted_out,
                on=agg_keys,
                how="left",
                suffixes=("", "_weighted"),
            )
            for col in value_series_cols:
                wcol = f"{col}_weighted"
                if wcol in agg_df.columns:
                    agg_df.loc[mask, col] = pd.to_numeric(agg_df.loc[mask, wcol], errors="coerce")
            agg_df = agg_df.drop(
                columns=[f"{col}_weighted" for col in value_series_cols if f"{col}_weighted" in agg_df.columns],
                errors="ignore",
            )

    agg_df["delta_pre_vs_input"] = agg_df["pre_value"] - agg_df["input_value"]
    agg_df["pct_delta_pre_vs_input"] = (
        agg_df["delta_pre_vs_input"] / pd.to_numeric(agg_df["input_value"], errors="coerce").replace(0, pd.NA)
    )
    agg_df["delta_recon_vs_input"] = agg_df["reconciled_value"] - agg_df["input_value"]
    agg_df["pct_delta_recon_vs_input"] = (
        agg_df["delta_recon_vs_input"] / pd.to_numeric(agg_df["input_value"], errors="coerce").replace(0, pd.NA)
    )

    cols = comparison_long.columns.tolist()
    for col in cols:
        if col not in agg_df.columns:
            agg_df[col] = pd.NA
    agg_df = agg_df[cols]
    print(f"[INFO] Appended aggregate economy rows for {aggregate_economy}: {len(agg_df)}")
    return pd.concat([comparison_long, agg_df], ignore_index=True)


def _attach_stock_proxy_series(
    comparison_long: pd.DataFrame,
    *,
    proxy_dir: str | Path,
) -> pd.DataFrame:
    if comparison_long.empty:
        return comparison_long

    proxy_path = _resolve_repo_path(proxy_dir)
    if not proxy_path.exists():
        print(f"[INFO] Stock proxy directory not found; skipping proxy overlays: {proxy_path}")
        return comparison_long

    proxy_files = sorted(proxy_path.glob("*_road_stock_projection_vs_chart_series.csv"))
    if not proxy_files:
        print(f"[INFO] No stock proxy files found in {proxy_path}; skipping proxy overlays.")
        return comparison_long

    merged = comparison_long.copy()
    merged["checkpoint_direct_proxy_value"] = pd.NA
    merged["sales_flow_projected_proxy_value"] = pd.NA

    joined_count = 0
    for proxy_file in proxy_files:
        # fallback parse from suffix pattern: <economy>_<scenario>_road_stock_projection_vs_chart_series
        # safest approach: split from right by known tail
        tail = "_road_stock_projection_vs_chart_series"
        stem = proxy_file.stem
        if not stem.endswith(tail):
            continue
        core = stem[: -len(tail)]
        if "_" not in core:
            continue
        economy, scenario = core.rsplit("_", 1)

        try:
            proxy_df = pd.read_csv(proxy_file, low_memory=False)
        except Exception as exc:
            print(f"[WARN] Failed reading stock proxy file {proxy_file}: {exc}")
            continue

        required_cols = {
            "Date",
            "scenario",
            "major_transport_type",
            "fuel_label",
            "checkpoint_direct_stock",
            "projected_stock",
        }
        if not required_cols.issubset(proxy_df.columns):
            print(f"[WARN] Proxy file missing required columns; skipping: {proxy_file}")
            continue

        proxy_df = proxy_df.copy()
        proxy_df["economy"] = str(economy)
        proxy_df["scenario"] = proxy_df["scenario"].astype(str).str.strip()
        proxy_df["Date"] = pd.to_numeric(proxy_df["Date"], errors="coerce").astype("Int64")
        proxy_df["major_transport_type"] = proxy_df["major_transport_type"].astype(str).str.strip()
        proxy_df["fuel_label"] = proxy_df["fuel_label"].astype(str).str.strip()
        proxy_df["checkpoint_direct_stock"] = pd.to_numeric(proxy_df["checkpoint_direct_stock"], errors="coerce")
        proxy_df["projected_stock"] = pd.to_numeric(proxy_df["projected_stock"], errors="coerce")
        proxy_df = proxy_df.dropna(subset=["Date"])
        if proxy_df.empty:
            continue

        # Also provide road "Total" proxy overlays so total stock charts can show proxy lines.
        total_proxy = (
            proxy_df.groupby(
                ["economy", "scenario", "major_transport_type", "Date"],
                dropna=False,
            )[["checkpoint_direct_stock", "projected_stock"]]
            .sum(min_count=1)
            .reset_index()
        )
        total_proxy["fuel_label"] = "Total"
        proxy_df = pd.concat([proxy_df, total_proxy], ignore_index=True)

        key_cols = ["economy", "scenario", "major_transport_type", "fuel_label", "year"]
        attach = (
            proxy_df.rename(columns={"Date": "year"})[
                ["economy", "scenario", "major_transport_type", "fuel_label", "year", "checkpoint_direct_stock", "projected_stock"]
            ]
            .drop_duplicates(subset=key_cols, keep="last")
        )

        prior_nonnull = int(merged["checkpoint_direct_proxy_value"].notna().sum())
        merged = merged.merge(attach, on=key_cols, how="left")
        stock_mask = merged["metric"].astype(str).str.lower().eq("stock")
        merged.loc[stock_mask, "checkpoint_direct_proxy_value"] = (
            pd.to_numeric(merged.loc[stock_mask, "checkpoint_direct_proxy_value"], errors="coerce")
            .combine_first(pd.to_numeric(merged.loc[stock_mask, "checkpoint_direct_stock"], errors="coerce"))
        )
        merged.loc[stock_mask, "sales_flow_projected_proxy_value"] = (
            pd.to_numeric(merged.loc[stock_mask, "sales_flow_projected_proxy_value"], errors="coerce")
            .combine_first(pd.to_numeric(merged.loc[stock_mask, "projected_stock"], errors="coerce"))
        )
        merged = merged.drop(columns=["checkpoint_direct_stock", "projected_stock"], errors="ignore")
        joined_count += int(merged["checkpoint_direct_proxy_value"].notna().sum()) - prior_nonnull

    # Fallback for sparse FOR_VIEWING road reconciliation years:
    # use proxy stock as a reconciled/reconciled+alt substitute when those series are missing.
    stock_mask = merged["metric"].astype(str).str.lower().eq("stock")
    road_mask = merged["major_transport_type"].map(_is_road_major_transport_type)
    stock_road_mask = stock_mask & road_mask
    if "reconciled_value" in merged.columns:
        recon_fill_mask = (
            stock_road_mask
            & pd.to_numeric(merged["reconciled_value"], errors="coerce").isna()
            & pd.to_numeric(merged["checkpoint_direct_proxy_value"], errors="coerce").notna()
        )
        merged.loc[recon_fill_mask, "reconciled_value"] = pd.to_numeric(
            merged.loc[recon_fill_mask, "checkpoint_direct_proxy_value"], errors="coerce"
        )
    if "reconciled_plus_alt_value" in merged.columns:
        recon_alt_fill_mask = (
            stock_road_mask
            & pd.to_numeric(merged["reconciled_plus_alt_value"], errors="coerce").isna()
            & pd.to_numeric(merged["sales_flow_projected_proxy_value"], errors="coerce").notna()
        )
        merged.loc[recon_alt_fill_mask, "reconciled_plus_alt_value"] = pd.to_numeric(
            merged.loc[recon_alt_fill_mask, "sales_flow_projected_proxy_value"], errors="coerce"
        )
    if {
        "delta_recon_vs_input",
        "pct_delta_recon_vs_input",
        "reconciled_value",
        "input_value",
    }.issubset(merged.columns):
        merged["delta_recon_vs_input"] = (
            pd.to_numeric(merged["reconciled_value"], errors="coerce")
            - pd.to_numeric(merged["input_value"], errors="coerce")
        )
        merged["pct_delta_recon_vs_input"] = (
            pd.to_numeric(merged["delta_recon_vs_input"], errors="coerce")
            / pd.to_numeric(merged["input_value"], errors="coerce").replace(0, pd.NA)
        )

    # Populate road energy proxy/reconciled series by converting stock proxies with
    # an energy-per-stock factor from the same key-year.
    energy_mask = merged["metric"].astype(str).str.lower().eq("energy")
    road_energy_mask = energy_mask & road_mask
    if road_energy_mask.any():
        key_cols = ["economy", "scenario", "major_transport_type", "fuel_label", "year"]
        stock_lookup = (
            merged.loc[
                stock_road_mask,
                key_cols
                + [
                    "pre_value",
                    "input_value",
                    "reconciled_value",
                    "checkpoint_direct_proxy_value",
                    "sales_flow_projected_proxy_value",
                ],
            ]
            .copy()
            .drop_duplicates(subset=key_cols, keep="last")
            .rename(
                columns={
                    "pre_value": "stock_pre_value",
                    "input_value": "stock_input_value",
                    "reconciled_value": "stock_reconciled_value",
                    "checkpoint_direct_proxy_value": "stock_checkpoint_proxy_value",
                    "sales_flow_projected_proxy_value": "stock_sales_proxy_value",
                }
            )
        )
        if not stock_lookup.empty:
            merged = merged.merge(stock_lookup, on=key_cols, how="left")
            energy_pre = pd.to_numeric(merged["pre_value"], errors="coerce")
            energy_input = pd.to_numeric(merged["input_value"], errors="coerce")
            energy_reconciled = pd.to_numeric(merged["reconciled_value"], errors="coerce")
            stock_pre = pd.to_numeric(merged.get("stock_pre_value"), errors="coerce")
            stock_input = pd.to_numeric(merged.get("stock_input_value"), errors="coerce")
            stock_reconciled = pd.to_numeric(merged.get("stock_reconciled_value"), errors="coerce")
            ratio_pre = energy_pre / stock_pre.replace(0, pd.NA)
            ratio_recon = energy_reconciled / stock_reconciled.replace(0, pd.NA)
            ratio_input = energy_input / stock_input.replace(0, pd.NA)
            energy_per_stock = ratio_pre.combine_first(ratio_recon).combine_first(ratio_input)

            energy_checkpoint_proxy = (
                pd.to_numeric(merged.get("stock_checkpoint_proxy_value"), errors="coerce")
                * energy_per_stock
            )
            energy_sales_proxy = (
                pd.to_numeric(merged.get("stock_sales_proxy_value"), errors="coerce")
                * energy_per_stock
            )

            chk_fill_mask = road_energy_mask & pd.to_numeric(merged["checkpoint_direct_proxy_value"], errors="coerce").isna()
            merged.loc[chk_fill_mask, "checkpoint_direct_proxy_value"] = energy_checkpoint_proxy.loc[chk_fill_mask]
            sales_fill_mask = road_energy_mask & pd.to_numeric(merged["sales_flow_projected_proxy_value"], errors="coerce").isna()
            merged.loc[sales_fill_mask, "sales_flow_projected_proxy_value"] = energy_sales_proxy.loc[sales_fill_mask]

            if "reconciled_value" in merged.columns:
                recon_energy_fill_mask = (
                    road_energy_mask
                    & pd.to_numeric(merged["reconciled_value"], errors="coerce").isna()
                    & pd.to_numeric(merged["checkpoint_direct_proxy_value"], errors="coerce").notna()
                )
                merged.loc[recon_energy_fill_mask, "reconciled_value"] = pd.to_numeric(
                    merged.loc[recon_energy_fill_mask, "checkpoint_direct_proxy_value"],
                    errors="coerce",
                )
            if "reconciled_plus_alt_value" in merged.columns:
                recon_alt_energy_fill_mask = (
                    road_energy_mask
                    & pd.to_numeric(merged["reconciled_plus_alt_value"], errors="coerce").isna()
                    & pd.to_numeric(merged["sales_flow_projected_proxy_value"], errors="coerce").notna()
                )
                merged.loc[recon_alt_energy_fill_mask, "reconciled_plus_alt_value"] = pd.to_numeric(
                    merged.loc[recon_alt_energy_fill_mask, "sales_flow_projected_proxy_value"],
                    errors="coerce",
                )

            merged = merged.drop(
                columns=[
                    "stock_pre_value",
                    "stock_input_value",
                    "stock_reconciled_value",
                    "stock_checkpoint_proxy_value",
                    "stock_sales_proxy_value",
                ],
                errors="ignore",
            )

    if {
        "delta_recon_vs_input",
        "pct_delta_recon_vs_input",
        "reconciled_value",
        "input_value",
    }.issubset(merged.columns):
        merged["delta_recon_vs_input"] = (
            pd.to_numeric(merged["reconciled_value"], errors="coerce")
            - pd.to_numeric(merged["input_value"], errors="coerce")
        )
        merged["pct_delta_recon_vs_input"] = (
            pd.to_numeric(merged["delta_recon_vs_input"], errors="coerce")
            / pd.to_numeric(merged["input_value"], errors="coerce").replace(0, pd.NA)
        )

    print(f"[INFO] Attached stock proxy overlays. non-null proxy points added: {joined_count}")
    return merged


def _select_bar_years(years: list[float], *, step: int, include_base_year: bool) -> set[int]:
    if not years:
        return set()
    year_ints = sorted({int(y) for y in years})
    base_year = min(year_ints)
    selected: set[int] = {y for y in year_ints if (y % max(step, 1) == 0)}
    if include_base_year:
        selected.add(base_year)
    return selected


def _write_transport_comparison_charts_plotly(
    comparison_long: pd.DataFrame,
    charts_dir: Path,
    *,
    chart_mode: str,
    bar_year_step: int,
    bar_include_base_year: bool,
    series_categories: tuple[str, ...] | None,
) -> bool:
    try:
        import plotly.graph_objects as go
    except Exception as exc:
        print(f"[WARN] Plotly backend requested but unavailable ({exc}); falling back to static charts.")
        return False

    groups = comparison_long.groupby(
        ["economy", "metric", "major_transport_type", "fuel_label"], dropna=False
    )
    selected_categories = _normalize_series_categories(series_categories)
    if selected_categories:
        print(f"[INFO] Applying series category filter: {sorted(selected_categories)}")
    total_groups = int(groups.ngroups)
    print(f"[INFO] Plotly chart series groups: {total_groups}")

    def _resolve_unit_label(sub: pd.DataFrame) -> str:
        if "unit_label" not in sub.columns:
            return "scaled value"
        labels = sub["unit_label"].dropna().astype(str).str.strip()
        labels = labels[labels.str.lower() != "nan"]
        return labels.iloc[0] if not labels.empty else "scaled value"

    def _numeric_series(frame: pd.DataFrame, col: str) -> pd.Series:
        if col in frame.columns:
            return pd.to_numeric(frame[col], errors="coerce")
        return pd.Series(pd.NA, index=frame.index, dtype="float64")

    for idx, ((economy, metric, major_transport_type, fuel_label), sub) in enumerate(groups, start=1):
        if idx % 100 == 0 or idx == 1 or idx == total_groups:
            print(
                f"[INFO] Plotly chart progress: {idx}/{total_groups} "
                f"({economy} | {metric} | {major_transport_type} | {fuel_label})"
            )

        allow_alt_overlay = str(fuel_label).strip().lower() != "total"
        scenarios = sorted(sub["scenario"].dropna().astype(str).unique().tolist())
        scenario_alpha: dict[str, float] = {}
        for j, scenario_name in enumerate(scenarios):
            scenario_alpha[scenario_name] = 1.0 if j == 0 else 0.75

        fig = go.Figure()
        trace_count = 0
        for scenario_name in scenarios:
            scenario_label = _scenario_display_label(scenario_name)
            ss = sub[sub["scenario"].astype(str) == scenario_name].sort_values("year")
            years = pd.to_numeric(ss["year"], errors="coerce")
            pre_vals = _numeric_series(ss, "pre_value")
            input_vals = _numeric_series(ss, "input_value")
            recon_vals = _numeric_series(ss, "reconciled_value")
            recon_plus_alt_vals = _numeric_series(ss, "reconciled_plus_alt_value")
            checkpoint_direct_proxy_vals = _numeric_series(ss, "checkpoint_direct_proxy_value")
            sales_flow_projected_proxy_vals = _numeric_series(ss, "sales_flow_projected_proxy_value")

            has_alt = allow_alt_overlay and recon_plus_alt_vals.notna().any()
            alpha = scenario_alpha.get(scenario_name, 0.45)
            show_input = (not selected_categories) or ("input" in selected_categories)
            show_pre = (not selected_categories) or ("pre" in selected_categories)
            show_recon = (not selected_categories) or ("reconciled" in selected_categories)
            show_recon_alt = (not selected_categories) or ("reconciled_plus_alt" in selected_categories)
            show_checkpoint_proxy = (not selected_categories) or ("checkpoint_direct_proxy" in selected_categories)
            show_sales_proxy = (not selected_categories) or ("sales_flow_projected_proxy" in selected_categories)

            series_specs: list[tuple[pd.Series, str, str, str]] = []
            if show_input:
                series_specs.append((input_vals, f"{scenario_label}, Input", "#d62728", "solid"))
            if show_pre:
                series_specs.append((pre_vals, f"{scenario_label}, Pre-reconciled", "#1b66b3", "dash"))
            if has_alt and show_recon_alt:
                series_specs.append((recon_plus_alt_vals, f"{scenario_label}, Reconciled + alternatives", "#9467bd", "dot"))
            elif show_recon:
                series_specs.append((recon_vals, f"{scenario_label}, Reconciled", "#2ca02c", "dashdot"))
            if show_checkpoint_proxy:
                series_specs.append((checkpoint_direct_proxy_vals, f"{scenario_label}, CP proxy", "#8c564b", "dot"))
            if show_sales_proxy:
                series_specs.append((sales_flow_projected_proxy_vals, f"{scenario_label}, SF proxy", "#e377c2", "dot"))

            if str(chart_mode).strip().lower() == "bar":
                all_years = pd.to_numeric(ss["year"], errors="coerce").dropna().astype(float).tolist()
                selected_years = sorted(
                    _select_bar_years(
                        all_years,
                        step=bar_year_step,
                        include_base_year=bar_include_base_year,
                    )
                )
                if not selected_years:
                    selected_years = sorted({int(y) for y in all_years})
                year_idx = pd.to_numeric(ss["year"], errors="coerce").astype("Int64")
                for yvals, label, color, _dash in series_specs:
                    if not yvals.notna().any():
                        continue
                    vals_by_year = pd.Series(yvals.to_numpy(), index=year_idx).to_dict()
                    y = [float(vals_by_year.get(int(yr), 0.0) or 0.0) for yr in selected_years]
                    fig.add_trace(
                        go.Bar(
                            x=selected_years,
                            y=y,
                            name=label,
                            marker_color=color,
                            opacity=alpha,
                        )
                    )
                    trace_count += 1
            else:
                for yvals, label, color, dash in series_specs:
                    valid = years.notna() & yvals.notna()
                    if not valid.any():
                        continue
                    fig.add_trace(
                        go.Scatter(
                            x=years[valid],
                            y=yvals[valid],
                            mode="lines+markers",
                            name=label,
                            line={"color": color, "width": 2.6, "dash": dash},
                            marker={"size": 5},
                            opacity=alpha,
                        )
                    )
                    trace_count += 1

        if trace_count == 0:
            continue

        unit_label = _resolve_unit_label(sub)
        fig.update_layout(
            title=None,
            template="plotly_white",
            hovermode="x unified",
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0, "font": {"size": 10}},
            xaxis_title="Year",
            yaxis_title=unit_label,
            barmode="group",
            margin={"l": 50, "r": 20, "t": 24, "b": 60},
        )
        out_name = (
            f"{_safe_filename_token(economy)}__"
            f"{_safe_filename_token(metric)}__"
            f"{_safe_filename_token(major_transport_type)}__"
            f"{_safe_filename_token(fuel_label)}.html"
        )
        fig.write_html(
            charts_dir / out_name,
            include_plotlyjs="cdn",
            full_html=True,
        )
    return True


def _write_transport_comparison_charts(
    comparison_long: pd.DataFrame,
    charts_dir: Path,
    *,
    chart_backend: str = "static",
    chart_mode: str = "line",
    bar_year_step: int = 5,
    bar_include_base_year: bool = True,
    series_categories: tuple[str, ...] | None = None,
) -> None:
    if comparison_long.empty:
        print("[INFO] No rows available for chart generation.")
        return
    charts_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Generating charts in: {charts_dir}")
    backend = str(chart_backend).strip().lower()
    if backend == "plotly":
        ok = _write_transport_comparison_charts_plotly(
            comparison_long,
            charts_dir,
            chart_mode=chart_mode,
            bar_year_step=bar_year_step,
            bar_include_base_year=bar_include_base_year,
            series_categories=series_categories,
        )
        if ok:
            return
    elif backend not in {"", "static"}:
        print(f"[WARN] Unknown chart_backend='{chart_backend}', defaulting to static.")
    groups = comparison_long.groupby(
        ["economy", "metric", "major_transport_type", "fuel_label"], dropna=False
    )
    selected_categories = _normalize_series_categories(series_categories)
    if selected_categories:
        print(f"[INFO] Applying series category filter: {sorted(selected_categories)}")
    total_groups = int(groups.ngroups)
    print(f"[INFO] Chart series groups: {total_groups}")
    def _resolve_unit_label(sub: pd.DataFrame) -> str:
        if "unit_label" not in sub.columns:
            return "scaled value"
        labels = sub["unit_label"].dropna().astype(str).str.strip()
        labels = labels[labels.str.lower() != "nan"]
        return labels.iloc[0] if not labels.empty else "scaled value"
    plot_value_columns = [
        "pre_value",
        "reconciled_value",
        "input_value",
        "reconciled_plus_alt_value",
        "checkpoint_direct_proxy_value",
        "sales_flow_projected_proxy_value",
    ]
    existing_plot_value_columns = [col for col in plot_value_columns if col in comparison_long.columns]
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.patheffects as pe
        import matplotlib.pyplot as plt
        import numpy as np

        for idx, ((economy, metric, major_transport_type, fuel_label), sub) in enumerate(groups, start=1):
            if idx % 100 == 0 or idx == 1 or idx == total_groups:
                print(
                    f"[INFO] Matplotlib chart progress: {idx}/{total_groups} "
                    f"({economy} | {metric} | {major_transport_type} | {fuel_label})"
                )
            fig, ax = plt.subplots(figsize=(14.0, 7.0), constrained_layout=True)
            allow_alt_overlay = str(fuel_label).strip().lower() != "total"
            scenarios = sorted(sub["scenario"].dropna().astype(str).unique().tolist())
            scenario_alpha: dict[str, float] = {}
            for idx, scenario_name in enumerate(scenarios):
                scenario_alpha[scenario_name] = 1.0 if idx == 0 else 0.75
            numeric_subset = pd.to_numeric(
                sub[existing_plot_value_columns].stack(),
                errors="coerce",
            ).fillna(0.0)
            if numeric_subset.abs().max() == 0:
                plt.close(fig)
                continue

            if str(chart_mode).strip().lower() == "bar":
                all_years = pd.to_numeric(sub["year"], errors="coerce").dropna().astype(float).tolist()
                selected_years = sorted(
                    _select_bar_years(
                        all_years,
                        step=bar_year_step,
                        include_base_year=bar_include_base_year,
                    )
                )
                if not selected_years:
                    selected_years = sorted({int(y) for y in all_years})
                x = np.arange(len(selected_years))
                series_defs: list[tuple[str, str, str, str]] = [
                    ("pre_value", "Pre-reconciled", "#1b66b3", "pre"),
                    ("reconciled_value", "Reconciled", "#2ca02c", "reconciled"),
                    ("input_value", "Input", "#d62728", "input"),
                    ("reconciled_plus_alt_value", "Reconciled + alternatives", "#9467bd", "reconciled_plus_alt"),
                    ("checkpoint_direct_proxy_value", "CP proxy", "#8c564b", "checkpoint_direct_proxy"),
                    ("sales_flow_projected_proxy_value", "SF proxy", "#e377c2", "sales_flow_projected_proxy"),
                ]
                bar_slots: list[tuple[str, str, str, float]] = []
                for scenario_name in scenarios:
                    alpha = scenario_alpha.get(scenario_name, 0.45)
                    has_alt = allow_alt_overlay and pd.to_numeric(
                        sub.loc[sub["scenario"].astype(str) == scenario_name, "reconciled_plus_alt_value"],
                        errors="coerce",
                    ).notna().any()
                    for col, label, color, category in series_defs:
                        if selected_categories and category not in selected_categories:
                            continue
                        if col not in sub.columns:
                            continue
                        if col == "reconciled_value" and has_alt:
                            continue
                        if col == "reconciled_plus_alt_value" and not pd.to_numeric(
                            sub.loc[sub["scenario"].astype(str) == scenario_name, col], errors="coerce"
                        ).notna().any():
                            continue
                        if col in {"checkpoint_direct_proxy_value", "sales_flow_projected_proxy_value"} and not pd.to_numeric(
                            sub.loc[sub["scenario"].astype(str) == scenario_name, col], errors="coerce"
                        ).notna().any():
                            continue
                        bar_slots.append((scenario_name, col, label, alpha))
                if not bar_slots:
                    plt.close(fig)
                    continue
                width = 0.82 / max(len(bar_slots), 1)
                for slot_idx, (scenario_name, col, label, alpha) in enumerate(bar_slots):
                    scenario_label = _scenario_display_label(scenario_name)
                    ss = sub[sub["scenario"].astype(str) == scenario_name].copy()
                    ss["year"] = pd.to_numeric(ss["year"], errors="coerce").astype("Int64")
                    ss[col] = pd.to_numeric(ss[col], errors="coerce")
                    vals_by_year = ss.set_index("year")[col].to_dict()
                    yvals = [float(vals_by_year.get(int(yr), 0.0) or 0.0) for yr in selected_years]
                    offset = (slot_idx - (len(bar_slots) - 1) / 2) * width
                    color = next(c for c_col, _, c, _ in series_defs if c_col == col)
                    ax.bar(
                        x + offset,
                        yvals,
                        width=width * 0.98,
                        label=f"{scenario_label} {label}",
                        color=color,
                        alpha=alpha,
                    )
                ax.set_xticks(x)
                ax.set_xticklabels([str(y) for y in selected_years], fontsize=24)
            else:
                for scenario_name in scenarios:
                    scenario_label = _scenario_display_label(scenario_name)
                    ss = sub[sub["scenario"].astype(str) == scenario_name].sort_values("year")
                    years = pd.to_numeric(ss["year"], errors="coerce")
                    pre_vals = pd.to_numeric(ss["pre_value"], errors="coerce")
                    input_vals = pd.to_numeric(ss["input_value"], errors="coerce")
                    recon_vals = pd.to_numeric(ss["reconciled_value"], errors="coerce")
                    recon_plus_alt_vals = pd.to_numeric(ss.get("reconciled_plus_alt_value"), errors="coerce")
                    has_alt = allow_alt_overlay and recon_plus_alt_vals.notna().any()
                    alpha = scenario_alpha.get(scenario_name, 0.45)

                    def _plot_with_halo(
                        yvals: pd.Series,
                        *,
                        label: str,
                        color: str,
                        linestyle: str,
                        marker: str,
                        zorder: int,
                    ) -> bool:
                        valid = years.notna() & yvals.notna()
                        if not valid.any():
                            return False
                        line, = ax.plot(
                            years[valid],
                            yvals[valid],
                            label=label,
                            linewidth=3.0,
                            marker=marker,
                            markersize=5.2,
                            linestyle=linestyle,
                            color=color,
                            alpha=alpha,
                            zorder=zorder,
                        )
                        line.set_path_effects(
                            [
                                pe.Stroke(linewidth=5.2, foreground="#ffffff"),
                                pe.Normal(),
                            ]
                        )
                        return True

                    def _label_line_end(
                        yvals: pd.Series,
                        *,
                        text: str,
                        color: str,
                        y_offset: float = 0.0,
                    ) -> None:
                        valid = years.notna() & yvals.notna()
                        if not valid.any():
                            return
                        x_last = float(years[valid].iloc[-1])
                        y_last = float(yvals[valid].iloc[-1])
                        ax.annotate(
                            text,
                            xy=(x_last, y_last),
                            xytext=(7, y_offset),
                            textcoords="offset points",
                            fontsize=10.5,
                            color=color,
                            alpha=alpha,
                            va="center",
                            ha="left",
                            clip_on=False,
                            zorder=10,
                            bbox={
                                "boxstyle": "round,pad=0.15",
                                "facecolor": "#ffffff",
                                "edgecolor": "none",
                                "alpha": 0.88,
                            },
                        )

                    checkpoint_direct_proxy_vals = pd.to_numeric(
                        ss.get("checkpoint_direct_proxy_value"), errors="coerce"
                    )
                    sales_flow_projected_proxy_vals = pd.to_numeric(
                        ss.get("sales_flow_projected_proxy_value"), errors="coerce"
                    )

                    show_input = (not selected_categories) or ("input" in selected_categories)
                    show_pre = (not selected_categories) or ("pre" in selected_categories)
                    show_recon = (not selected_categories) or ("reconciled" in selected_categories)
                    show_recon_alt = (not selected_categories) or ("reconciled_plus_alt" in selected_categories)
                    show_checkpoint_proxy = (not selected_categories) or ("checkpoint_direct_proxy" in selected_categories)
                    show_sales_proxy = (not selected_categories) or ("sales_flow_projected_proxy" in selected_categories)

                    input_plotted = False
                    if show_input:
                        input_plotted = _plot_with_halo(
                            input_vals,
                            label=f"{scenario_label}, Input",
                            color="#d62728",
                            linestyle="-",
                            marker="o",
                            zorder=3,
                        )
                    if input_plotted:
                        _label_line_end(
                            input_vals,
                            text=f"{scenario_label} Input",
                            color="#d62728",
                            y_offset=-12,
                        )
                    pre_plotted = False
                    if show_pre:
                        pre_plotted = _plot_with_halo(
                            pre_vals,
                            label=f"{scenario_label}, Pre-reconciled",
                            color="#1b66b3",
                            linestyle="--",
                            marker="s",
                            zorder=4,
                        )
                    if pre_plotted:
                        _label_line_end(
                            pre_vals,
                            text=f"{scenario_label} Pre",
                            color="#1b66b3",
                            y_offset=0,
                        )
                    if has_alt and show_recon_alt:
                        alt_plotted = _plot_with_halo(
                            recon_plus_alt_vals,
                            label=f"{scenario_label}, Reconciled + alternatives",
                            color="#9467bd",
                            linestyle=":",
                            marker="D",
                            zorder=6,
                        )
                        if alt_plotted:
                            _label_line_end(
                                recon_plus_alt_vals,
                                text=f"{scenario_label} Rec+alt",
                                color="#9467bd",
                                y_offset=12,
                            )
                    elif show_recon:
                        recon_plotted = _plot_with_halo(
                            recon_vals,
                            label=f"{scenario_label}, Reconciled",
                            color="#2ca02c",
                            linestyle="-.",
                            marker="^",
                            zorder=5,
                        )
                        if recon_plotted:
                            _label_line_end(
                                recon_vals,
                                text=f"{scenario_label} Reconciled",
                                color="#2ca02c",
                                y_offset=12,
                            )
                    if show_checkpoint_proxy:
                        checkpoint_proxy_plotted = _plot_with_halo(
                            checkpoint_direct_proxy_vals,
                            label=f"{scenario_label}, CP proxy",
                            color="#8c564b",
                            linestyle=":",
                            marker="P",
                            zorder=7,
                        )
                        if checkpoint_proxy_plotted:
                            _label_line_end(
                                checkpoint_direct_proxy_vals,
                                text=f"{scenario_label} CP proxy",
                                color="#8c564b",
                                y_offset=22,
                            )
                    if show_sales_proxy:
                        sales_proxy_plotted = _plot_with_halo(
                            sales_flow_projected_proxy_vals,
                            label=f"{scenario_label}, SF proxy",
                            color="#e377c2",
                            linestyle=":",
                            marker="X",
                            zorder=8,
                        )
                        if sales_proxy_plotted:
                            _label_line_end(
                                sales_flow_projected_proxy_vals,
                                text=f"{scenario_label} SF proxy",
                                color="#e377c2",
                                y_offset=30,
                            )
            ax.set_title(
                f"{economy} | {metric} | {major_transport_type} | {fuel_label}",
                fontsize=24,
            )
            ax.set_xlabel("Year", fontsize=22)
            unit_label = _resolve_unit_label(sub)
            ax.set_ylabel(unit_label, fontsize=22)
            yvals = pd.to_numeric(
                sub[existing_plot_value_columns].stack(),
                errors="coerce",
            ).dropna()
            if not yvals.empty:
                y_min = float(yvals.min())
                y_max = float(yvals.max())
                y_span = y_max - y_min
                y_pad = (y_max * 0.05) if y_max > 0 else max(abs(y_span) * 0.05, 1e-9)
                if y_span == 0:
                    y_pad = max(abs(y_max) * 0.05, 1e-6)
                ax.set_ylim(y_min - y_pad, y_max + y_pad)
            ax.tick_params(axis="both", labelsize=20)
            ax.grid(True, alpha=0.25)
            if str(chart_mode).strip().lower() == "line":
                ax.margins(x=0.08)
            ax.legend(loc="best", fontsize=13)

            out_name = (
                f"{_safe_filename_token(economy)}__"
                f"{_safe_filename_token(metric)}__"
                f"{_safe_filename_token(major_transport_type)}__"
                f"{_safe_filename_token(fuel_label)}.png"
            )
            fig.savefig(charts_dir / out_name, dpi=170)
            plt.close(fig)
        return
    except Exception as exc:
        print(f"[WARN] Matplotlib unavailable; using Pillow fallback charts ({exc}).")

    width, height = 1600, 840
    left, right, top, bottom = 80, 30, 50, 70
    plot_w = width - left - right
    plot_h = height - top - bottom

    for idx, ((economy, metric, major_transport_type, fuel_label), sub) in enumerate(groups, start=1):
        if idx % 100 == 0 or idx == 1 or idx == total_groups:
            print(
                f"[INFO] SVG chart progress: {idx}/{total_groups} "
                f"({economy} | {metric} | {major_transport_type} | {fuel_label})"
            )
        scenarios = sorted(sub["scenario"].dropna().astype(str).unique().tolist())
        allow_alt_overlay = str(fuel_label).strip().lower() != "total"
        scenario_alpha: dict[str, float] = {}
        for idx, scenario_name in enumerate(scenarios):
            scenario_alpha[scenario_name] = 1.0 if idx == 0 else 0.75

        scenario_rows: list[tuple[str, list[tuple[float, float, float, float, float, float, float]]]] = []
        for scenario_name in scenarios:
            ss = sub[sub["scenario"].astype(str) == scenario_name].sort_values("year")
            years = pd.to_numeric(ss["year"], errors="coerce").astype(float).tolist()
            pre_vals = pd.to_numeric(ss["pre_value"], errors="coerce").astype(float).tolist()
            input_vals = pd.to_numeric(ss["input_value"], errors="coerce").astype(float).tolist()
            recon_vals = pd.to_numeric(ss["reconciled_value"], errors="coerce").astype(float).tolist()
            recon_plus_alt_vals = pd.to_numeric(
                ss.get("reconciled_plus_alt_value"), errors="coerce"
            ).astype(float).tolist()
            checkpoint_direct_proxy_vals = pd.to_numeric(
                ss.get("checkpoint_direct_proxy_value"), errors="coerce"
            ).astype(float).tolist()
            sales_flow_projected_proxy_vals = pd.to_numeric(
                ss.get("sales_flow_projected_proxy_value"), errors="coerce"
            ).astype(float).tolist()
            pairs = [
                (y, p, rc, i, rpa, cdp, sps)
                for y, p, rc, i, rpa, cdp, sps in zip(
                    years,
                    pre_vals,
                    recon_vals,
                    input_vals,
                    recon_plus_alt_vals,
                    checkpoint_direct_proxy_vals,
                    sales_flow_projected_proxy_vals,
                )
                if pd.notna(y) and (pd.notna(p) or pd.notna(rc) or pd.notna(i) or pd.notna(rpa) or pd.notna(cdp) or pd.notna(sps))
            ]
            if pairs:
                scenario_rows.append((scenario_name, pairs))

        if not scenario_rows:
            continue
        years_clean = [p[0] for _, pairs in scenario_rows for p in pairs]
        vals_clean = [v for _, pairs in scenario_rows for _, p, rc, i, rpa, cdp, sps in pairs for v in (p, rc, i, rpa, cdp, sps) if pd.notna(v)]
        if not vals_clean:
            continue
        if max(abs(v) for v in vals_clean if pd.notna(v)) == 0:
            continue

        x_min, x_max = min(years_clean), max(years_clean)
        y_min = min(vals_clean)
        y_max = max(vals_clean)
        if x_max == x_min:
            x_max += 1.0
        if y_max <= y_min:
            y_max += 1.0
        y_padding = y_max * 0.05 if y_max > 0 else max(abs(y_max - y_min) * 0.05, 1e-9)
        y_max = y_max + y_padding

        def _px_x(xv: float) -> float:
            return left + ((xv - x_min) / (x_max - x_min)) * plot_w

        def _px_y(yv: float) -> float:
            return top + (1.0 - ((yv - y_min) / (y_max - y_min))) * plot_h

        def _polyline(
            points: list[tuple[float, float]],
            color: str,
            *,
            opacity: float = 1.0,
            dasharray: str | None = None,
        ) -> str:
            if not points:
                return ""
            point_str = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
            dash_attr = f' stroke-dasharray="{dasharray}"' if dasharray else ""
            circles = "".join(
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="3" fill="{color}" fill-opacity="{opacity:.2f}" />'
                for x, y in points
            )
            return (
                f'<polyline fill="none" stroke="{color}" stroke-width="2.8" stroke-opacity="{opacity:.2f}"'
                f'{dash_attr} points="{point_str}" />{circles}'
            )

        grid_lines = []
        for i in range(1, 5):
            gy = top + (i / 5.0) * plot_h
            grid_lines.append(
                f'<line x1="{left}" y1="{gy:.2f}" x2="{left + plot_w}" y2="{gy:.2f}" stroke="#ebeff4" stroke-width="1" />'
            )

        series_parts: list[str] = []
        legend_parts: list[str] = []
        line_label_parts: list[str] = []
        legend_y_start = height - 16
        if str(chart_mode).strip().lower() == "bar":
            all_years = sorted({int(p[0]) for _, pairs in scenario_rows for p in pairs})
            selected_years = sorted(
                _select_bar_years(
                    [float(y) for y in all_years],
                    step=bar_year_step,
                    include_base_year=bar_include_base_year,
                )
            )
            if not selected_years:
                selected_years = all_years
                series_def = [
                    ("input", "Input", "#d62728", 1),
                    ("pre", "Pre", "#1b66b3", 2),
                    ("recon", "Recon", "#2ca02c", 3),
                    ("recon_plus_alt", "Reconciled + alternatives", "#9467bd", 4),
                    ("checkpoint_direct_proxy", "CP proxy", "#8c564b", 5),
                    ("sales_flow_projected_proxy", "SF proxy", "#e377c2", 6),
                ]
                bar_slots: list[tuple[str, str, str, str]] = []
                scenario_value_maps: dict[str, dict[int, tuple[float | None, float | None, float | None, float | None, float | None, float | None]]] = {}
                for scenario_name, pairs in scenario_rows:
                    scenario_label = _scenario_display_label(scenario_name)
                    alpha = scenario_alpha.get(scenario_name, 0.45)
                    scenario_value_maps[scenario_name] = {
                        int(y): (
                            float(p) if pd.notna(p) else None,
                            float(rc) if pd.notna(rc) else None,
                            float(i) if pd.notna(i) else None,
                            float(rpa) if pd.notna(rpa) else None,
                            float(cdp) if pd.notna(cdp) else None,
                            float(sps) if pd.notna(sps) else None,
                        )
                    for y, p, rc, i, rpa, cdp, sps in pairs
                }
                has_alt = any(values[3] is not None for values in scenario_value_maps[scenario_name].values())
                has_alt = allow_alt_overlay and has_alt
                has_recon = any(values[1] is not None for values in scenario_value_maps[scenario_name].values())
                has_checkpoint_proxy = any(values[4] is not None for values in scenario_value_maps[scenario_name].values())
                has_sales_proxy = any(values[5] is not None for values in scenario_value_maps[scenario_name].values())
                for key, label, color, idx_pos in series_def:
                    if selected_categories:
                        if key == "input" and "input" not in selected_categories:
                            continue
                        if key == "pre" and "pre" not in selected_categories:
                            continue
                        if key == "recon" and "reconciled" not in selected_categories:
                            continue
                        if key == "recon_plus_alt" and "reconciled_plus_alt" not in selected_categories:
                            continue
                        if key == "checkpoint_direct_proxy" and "checkpoint_direct_proxy" not in selected_categories:
                            continue
                        if key == "sales_flow_projected_proxy" and "sales_flow_projected_proxy" not in selected_categories:
                            continue
                    if key == "recon" and has_alt:
                        continue
                    if key == "recon" and not has_recon:
                        continue
                    if key == "recon_plus_alt":
                        if not has_alt:
                            continue
                    if key == "checkpoint_direct_proxy" and not has_checkpoint_proxy:
                        continue
                    if key == "sales_flow_projected_proxy" and not has_sales_proxy:
                        continue
                    bar_slots.append((scenario_name, key, label, color))
                legend_parts.append(
                    f'<text x="{left + 120}" y="{legend_y_start - (len(legend_parts) * 20)}" font-size="22" fill="#505a64" fill-opacity="{alpha:.2f}">'
                    f"{scenario_label}: Red=Input, Blue=Pre, "
                    + ("Purple=Reconciled+alternatives" if has_alt else ("Green=Recon" if has_recon else "Recon=missing"))
                    + "</text>"
                )
            slot_count = max(len(bar_slots), 1)
            year_band = max((plot_w / max(len(selected_years), 1)) * 0.78, 18.0)
            bar_width = max((year_band / slot_count) * 0.9, 1.4)
            for year in selected_years:
                x_center = _px_x(float(year))
                for slot_idx, (scenario_name, key, _label, color) in enumerate(bar_slots):
                    alpha = scenario_alpha.get(scenario_name, 0.45)
                    values = scenario_value_maps.get(scenario_name, {}).get(int(year))
                    if not values:
                        continue
                    key_to_pos = {
                        "pre": 0,
                        "recon": 1,
                        "input": 2,
                        "recon_plus_alt": 3,
                        "checkpoint_direct_proxy": 4,
                        "sales_flow_projected_proxy": 5,
                    }
                    val = values[key_to_pos[key]]
                    if val is None:
                        continue
                    offset = -year_band / 2 + ((slot_idx + 0.5) * (year_band / slot_count))
                    x = x_center + offset - (bar_width / 2)
                    y_zero = _px_y(0.0)
                    y_val = _px_y(float(val))
                    rect_y = min(y_zero, y_val)
                    rect_h = max(abs(y_zero - y_val), 1.0)
                    series_parts.append(
                        f'<rect x="{x:.2f}" y="{rect_y:.2f}" width="{bar_width:.2f}" height="{rect_h:.2f}" '
                        f'fill="{color}" fill-opacity="{alpha:.2f}" />'
                    )
        else:
            for idx, (scenario_name, pairs) in enumerate(scenario_rows):
                scenario_label = _scenario_display_label(scenario_name)
                alpha = scenario_alpha.get(scenario_name, 0.45)
                pre_points = [(_px_x(y), _px_y(p)) for y, p, _, _, _, _, _ in pairs if pd.notna(p)]
                recon_points = [(_px_x(y), _px_y(rc)) for y, _, rc, _, _, _, _ in pairs if pd.notna(rc)]
                input_points = [(_px_x(y), _px_y(i)) for y, _, _, i, _, _, _ in pairs if pd.notna(i)]
                recon_plus_alt_points = [(_px_x(y), _px_y(rpa)) for y, _, _, _, rpa, _, _ in pairs if pd.notna(rpa)]
                checkpoint_proxy_points = [(_px_x(y), _px_y(cdp)) for y, _, _, _, _, cdp, _ in pairs if pd.notna(cdp)]
                sales_proxy_points = [(_px_x(y), _px_y(sps)) for y, _, _, _, _, _, sps in pairs if pd.notna(sps)]
                has_alt = allow_alt_overlay and (len(recon_plus_alt_points) > 0)
                has_recon = len(recon_points) > 0
                show_input = (not selected_categories) or ("input" in selected_categories)
                show_pre = (not selected_categories) or ("pre" in selected_categories)
                show_recon = (not selected_categories) or ("reconciled" in selected_categories)
                show_recon_alt = (not selected_categories) or ("reconciled_plus_alt" in selected_categories)
                show_checkpoint_proxy = (not selected_categories) or ("checkpoint_direct_proxy" in selected_categories)
                show_sales_proxy = (not selected_categories) or ("sales_flow_projected_proxy" in selected_categories)

                if show_input:
                    series_parts.append(_polyline(input_points, "#d62728", opacity=alpha))
                if show_pre:
                    series_parts.append(_polyline(pre_points, "#1b66b3", opacity=alpha))
                if has_alt and show_recon_alt:
                    series_parts.append(_polyline(recon_plus_alt_points, "#9467bd", opacity=alpha, dasharray="7,4"))
                elif has_recon and show_recon:
                    series_parts.append(_polyline(recon_points, "#2ca02c", opacity=alpha))
                if show_checkpoint_proxy:
                    series_parts.append(_polyline(checkpoint_proxy_points, "#8c564b", opacity=alpha, dasharray="3,3"))
                if show_sales_proxy:
                    series_parts.append(_polyline(sales_proxy_points, "#e377c2", opacity=alpha, dasharray="2,3"))

                label_specs: list[tuple[list[tuple[float, float]], str, str, int]] = []
                if show_input:
                    label_specs.append((input_points, f"{scenario_label} Input", "#d62728", -10))
                if show_pre:
                    label_specs.append((pre_points, f"{scenario_label} Pre", "#1b66b3", 0))
                if has_alt and show_recon_alt:
                    label_specs.append((recon_plus_alt_points, f"{scenario_label} Rec+alt", "#9467bd", 10))
                elif has_recon and show_recon:
                    label_specs.append((recon_points, f"{scenario_label} Reconciled", "#2ca02c", 10))
                if show_checkpoint_proxy:
                    label_specs.append((checkpoint_proxy_points, f"{scenario_label} CP proxy", "#8c564b", 20))
                if show_sales_proxy:
                    label_specs.append((sales_proxy_points, f"{scenario_label} SF proxy", "#e377c2", 30))
                for points, text, color, y_offset in label_specs:
                    if not points:
                        continue
                    x_last, y_last = points[-1]
                    line_label_parts.append(
                        f'<text x="{x_last + 7:.2f}" y="{y_last + y_offset:.2f}" font-size="18" '
                        f'fill="{color}" fill-opacity="{alpha:.2f}">{text}</text>'
                    )

                legend_parts.append(
                    f'<text x="{left + 120}" y="{legend_y_start - (idx * 20)}" font-size="22" fill="#505a64" fill-opacity="{alpha:.2f}">'
                    f"{scenario_label}: Red=Input, Blue=Pre, "
                    + ("Purple=Reconciled+alternatives" if has_alt else ("Green=Recon" if has_recon else "Recon=missing"))
                    + "</text>"
                )

        unit_label = _resolve_unit_label(sub)
        title = f"{economy} | {metric} | {major_transport_type} | {fuel_label} ({unit_label})"
        out_stem = (
            f"{_safe_filename_token(economy)}__"
            f"{_safe_filename_token(metric)}__"
            f"{_safe_filename_token(major_transport_type)}__"
            f"{_safe_filename_token(fuel_label)}"
        )
        svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" />
  <text x="{left}" y="36" font-size="42" fill="#28323c">{title}</text>
  <rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#ffffff" stroke="#aab4be" stroke-width="1" />
  {''.join(grid_lines)}
  {''.join(series_parts)}
  {''.join(line_label_parts)}
  <text x="{left}" y="{height - 16}" font-size="34" fill="#505a64">X min: {int(x_min)}</text>
  <text x="{left + plot_w - 260}" y="{height - 16}" font-size="34" fill="#505a64">X max: {int(x_max)}</text>
  <text x="{left - 74}" y="{top + 28}" font-size="30" fill="#505a64">Y max: {y_max:.3g}</text>
  <text x="{left - 74}" y="{top + plot_h}" font-size="30" fill="#505a64">Y min: {y_min:.3g}</text>
  {''.join(legend_parts)}
</svg>
"""
        (charts_dir / f"{out_stem}.svg").write_text(svg, encoding="utf-8")


def run_transport_results_table_comparison(
    config: TransportResultsComparisonConfig,
) -> ComparisonArtifacts:
    input_dir = _resolve_repo_path(config.input_dir)
    output_dir = _resolve_repo_path(config.output_dir)
    print("[INFO] Starting transport results comparison.")
    print(f"[INFO] input_dir={input_dir}")
    print(f"[INFO] output_dir={output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    charts_dir = output_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    requested_scenarios: list[str] = []
    if config.scenarios:
        requested_scenarios = [str(s).strip() for s in config.scenarios if str(s).strip()]
    elif config.scenario:
        requested_scenarios = [str(config.scenario).strip()]
    else:
        requested_scenarios = _available_scenarios(input_dir)

    print(f"[INFO] Requested scenarios: {requested_scenarios}")
    if config.include_economies:
        print(f"[INFO] Economy filter (requested): {list(config.include_economies)}")
    else:
        print("[INFO] Economy filter: all discovered economies")

    files: list[tuple[str, str, Path]] = []
    for scenario_name in requested_scenarios:
        discovered = _discover_input_files(input_dir, scenario_name)
        print(f"[INFO] Discovered {len(discovered)} file(s) for scenario '{scenario_name}'.")
        for economy, path in discovered:
            files.append((scenario_name, economy, path))

    if config.include_economies:
        allowed_norm = {_normalize_economy_code(e) for e in config.include_economies}
        files = [
            (scenario_name, economy, path)
            for scenario_name, economy, path in files
            if _normalize_economy_code(economy) in allowed_norm
        ]
        selected_economies = sorted({_normalize_economy_code(economy) for _, economy, _ in files})
        print(f"[INFO] Economy filter (applied): {selected_economies}")
        missing_economies = sorted(allowed_norm - set(selected_economies))
        if missing_economies:
            print(f"[WARN] Requested economies not found for selected scenarios: {missing_economies}")
    if not files:
        available = _available_scenarios(input_dir) if input_dir.exists() else []
        sample_files = sorted(input_dir.glob("transport_pre_recon_vs_raw_disaggregated_*.csv"))[:5] if input_dir.exists() else []
        sample_text = ", ".join(p.name for p in sample_files) if sample_files else "none"
        raise FileNotFoundError(
            "No comparison input files found.\n"
            f"- resolved input_dir: {input_dir}\n"
            f"- requested scenarios: {requested_scenarios or 'none'}\n"
            f"- available scenarios in dir: {available or 'none'}\n"
            f"- sample files: {sample_text}"
        )

    long_frames: list[pd.DataFrame] = []
    pipeline_fallback_rows = 0
    print(f"[INFO] Processing {len(files)} economy/scenario input files.")
    for i, (scenario_name, economy, path) in enumerate(files, start=1):
        print(f"[INFO] [{i}/{len(files)}] Loading {path.name} ({economy} | {scenario_name})")
        df = pd.read_csv(path)
        df = _coerce_source_columns_from_tuple(df)
        if not {"source_transport_type", "source_medium", "source_fuel"}.issubset(df.columns):
            print(f"[WARN] Skipping {path.name}: required source columns missing.")
            continue
        df = _add_reconciled_columns(df, economy=economy, scenario=scenario_name)
        df = _fill_sparse_reconciled_projections(df)
        df = _add_derived_energy_columns(df)
        df["major_transport_type"] = df.apply(
            lambda r: _major_transport_type_label(
                r.get("source_transport_type"),
                r.get("source_medium"),
            ),
            axis=1,
        )
        df["source_fuel_effective"] = df["source_fuel"]
        pipeline_mask = df["major_transport_type"].map(_is_pipeline_major_transport_type)
        fuel_empty_mask = df["source_fuel_effective"].isna() | (
            df["source_fuel_effective"].astype(str).str.strip() == ""
        )
        medium_nonempty_mask = df["source_medium"].notna() & (
            df["source_medium"].astype(str).str.strip() != ""
        )
        pipeline_fuel_fallback_mask = pipeline_mask & fuel_empty_mask & medium_nonempty_mask
        if pipeline_fuel_fallback_mask.any():
            df.loc[pipeline_fuel_fallback_mask, "source_fuel_effective"] = df.loc[
                pipeline_fuel_fallback_mask, "source_medium"
            ]
            pipeline_fallback_rows += int(pipeline_fuel_fallback_mask.sum())
        for metric in config.metrics:
            metric_key = str(metric).strip().lower()
            if metric_key not in _METRIC_TO_COLUMNS:
                print(f"[WARN] Unknown metric '{metric_key}', skipping.")
                continue
            pre_col, raw_col = _METRIC_TO_COLUMNS[metric_key]
            recon_col = {
                "activity": "recon_effective_activity",
                "stock": "recon_effective_stock_scaled",
                "mileage": "recon_mileage",
                "efficiency": "recon_efficiency",
                "intensity": "recon_intensity",
                "energy": "recon_energy",
            }.get(metric_key, "")
            if not recon_col:
                continue

            if metric_key in {"activity", "intensity"}:
                metric_df = df[~df["major_transport_type"].map(_is_road_major_transport_type)].copy()
            elif metric_key in {"stock", "mileage", "efficiency"}:
                metric_df = df[df["major_transport_type"].map(_is_road_major_transport_type)].copy()
            else:
                metric_df = df.copy()

            if metric_df.empty:
                continue
            print(f"[INFO]   Aggregating metric={metric_key} rows={len(metric_df)}")
            long_frames.append(
                _aggregate_rows(
                    metric_df,
                    economy=economy,
                    scenario=scenario_name,
                    metric=metric_key,
                    transport_col="major_transport_type",
                    fuel_col="source_fuel_effective",
                    pre_col=pre_col,
                    raw_col=raw_col,
                    recon_col=recon_col,
                )
            )

    comparison_long = pd.concat([f for f in long_frames if not f.empty], ignore_index=True)
    if comparison_long.empty:
        raise ValueError("Comparison aggregation produced no rows.")
    print(
        "[INFO] Pipeline fuel fallback rows populated from source_medium: "
        f"{pipeline_fallback_rows}"
    )

    international_rows = _build_international_comparison_rows(
        config=config,
        requested_scenarios=requested_scenarios,
    )
    if not international_rows.empty:
        comparison_long = pd.concat([comparison_long, international_rows], ignore_index=True)
        print(f"[INFO] Appended international rows: {len(international_rows)}")

    if config.include_apec_aggregate:
        comparison_long = _append_apec_aggregate_rows(
            comparison_long,
            aggregate_economy="00_APEC",
        )

    if config.include_economies:
        allowed_norm = {_normalize_economy_code(e) for e in config.include_economies}
        if config.include_apec_aggregate:
            allowed_norm.add("00_APEC")
        comparison_long = comparison_long[
            comparison_long["economy"].map(_normalize_economy_code).isin(allowed_norm)
        ].copy()
        print(
            f"[INFO] comparison_long hard-filtered to requested economies. "
            f"rows={len(comparison_long)} economies={sorted(comparison_long['economy'].astype(str).unique().tolist())}"
        )
        if comparison_long.empty:
            raise ValueError("No rows remain after applying include_economies filter.")
    print(f"[INFO] Aggregation complete. comparison_long rows={len(comparison_long)}")

    comparison_long = _append_total_rows(comparison_long)
    print(f"[INFO] Added total rows. comparison_long rows={len(comparison_long)}")
    if config.include_stock_proxies:
        comparison_long = _attach_stock_proxy_series(
            comparison_long,
            proxy_dir=config.stock_proxy_dir,
        )

    comparison_summary = (
        comparison_long.groupby(
            ["economy", "scenario", "metric", "major_transport_type", "fuel_label"],
            dropna=False,
        )
        .agg(
            mean_abs_pct_delta_pre_vs_input=(
                "pct_delta_pre_vs_input",
                lambda s: float(pd.to_numeric(s, errors="coerce").abs().mean(skipna=True)),
            ),
            p95_abs_pct_delta_pre_vs_input=(
                "pct_delta_pre_vs_input",
                lambda s: float(pd.to_numeric(s, errors="coerce").abs().quantile(0.95)),
            ),
            mean_abs_pct_delta_recon_vs_input=(
                "pct_delta_recon_vs_input",
                lambda s: float(pd.to_numeric(s, errors="coerce").abs().mean(skipna=True)),
            ),
            p95_abs_pct_delta_recon_vs_input=(
                "pct_delta_recon_vs_input",
                lambda s: float(pd.to_numeric(s, errors="coerce").abs().quantile(0.95)),
            ),
            max_abs_delta_pre_vs_input=(
                "delta_pre_vs_input",
                lambda s: float(pd.to_numeric(s, errors="coerce").abs().max(skipna=True)),
            ),
            max_abs_delta_recon_vs_input=(
                "delta_recon_vs_input",
                lambda s: float(pd.to_numeric(s, errors="coerce").abs().max(skipna=True)),
            ),
            year_count=("year", "nunique"),
        )
        .reset_index()
        .sort_values(
            ["metric", "major_transport_type", "mean_abs_pct_delta_pre_vs_input"],
            ascending=[True, True, False],
        )
    )

    comparison_long_csv = output_dir / "comparison_long.csv"
    comparison_summary_csv = output_dir / "comparison_summary.csv"
    comparison_long.to_csv(comparison_long_csv, index=False)
    comparison_summary.to_csv(comparison_summary_csv, index=False)
    print(f"[INFO] Wrote comparison_long_csv: {comparison_long_csv}")
    print(f"[INFO] Wrote comparison_summary_csv: {comparison_summary_csv}")

    _write_transport_comparison_charts(
        comparison_long,
        charts_dir,
        chart_backend=config.chart_backend,
        chart_mode=config.chart_mode,
        bar_year_step=config.bar_year_step,
        bar_include_base_year=config.bar_include_base_year,
        series_categories=config.series_categories,
    )
    print("[INFO] Chart generation complete.")
    return ComparisonArtifacts(
        comparison_long_csv=comparison_long_csv,
        comparison_summary_csv=comparison_summary_csv,
        charts_dir=charts_dir,
    )

#%%
if __name__ == "__main__":
    # Notebook/script-editable defaults:
    # - set ECONOMIES to e.g. ("20_USA", "12_NZ")
    # - set SCENARIOS to e.g. ("Reference", "Target")
    ECONOMIES = DEFAULT_ECONOMIES
    SCENARIOS = DEFAULT_SCENARIOS
    cfg = TransportResultsComparisonConfig(
        include_economies=ECONOMIES,
        scenarios=SCENARIOS,
    )
    artifacts = run_transport_results_table_comparison(cfg)
    print("[OK] Transport results comparison complete.")
    print(f"- comparison_long_csv: {artifacts.comparison_long_csv}")
    print(f"- comparison_summary_csv: {artifacts.comparison_summary_csv}")
    print(f"- charts_dir: {artifacts.charts_dir}")
#%%
