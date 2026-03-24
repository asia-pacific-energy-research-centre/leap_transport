"""Standalone international transport export workflow.

This module builds a LEAP-style workbook output for international bunker data
and can optionally write that output to LEAP via COM.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sys

import pandas as pd

from functions.path_utils import resolve_str
from functions.transport_branch_paths import (
    TRANSPORT_ROOT,
    build_transport_branch_path,
    extract_transport_branch_tuple,
)


# Allow sibling leap_utilities package without pip install.
BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_FUNCTIONS_DIR = BASE_DIR / "functions"
UTILS_ROOT_CANDIDATES = [
    (BASE_DIR / "leap_utilities").resolve(),
    (BASE_DIR.parent / "leap_utilities").resolve(),
    (BASE_DIR.parent.parent / "leap_utilities").resolve(),
]

paths_to_add = [BASE_DIR, LOCAL_FUNCTIONS_DIR]
for utils_root in UTILS_ROOT_CANDIDATES:
    legacy_pkg = utils_root / "leap_utils"
    code_pkg = utils_root / "code"
    codebase_pkg = utils_root / "codebase"
    if legacy_pkg.exists():
        paths_to_add.extend([legacy_pkg, utils_root])
    if code_pkg.exists():
        paths_to_add.extend([code_pkg, utils_root])
    if codebase_pkg.exists():
        paths_to_add.extend([codebase_pkg, codebase_pkg / "functions", utils_root])

for path in paths_to_add:
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))
# Keep this repo's local modules ahead of similarly named packages from leap_utilities.
for keep_first in (str(BASE_DIR), str(LOCAL_FUNCTIONS_DIR)):
    if keep_first in sys.path:
        sys.path.remove(keep_first)
for keep_first in (str(LOCAL_FUNCTIONS_DIR), str(BASE_DIR)):
    sys.path.insert(0, keep_first)

try:
    from leap_utils.leap_core import (  # noqa: E402
        connect_to_leap,
        diagnose_measures_in_leap_branch,
        ensure_branch_exists,
        ensure_fuel_exists,
        safe_set_variable,
    )
    from leap_utils.leap_excel_io import finalise_export_df, save_export_files  # noqa: E402
    from leap_utils.config import BRANCH_DEMAND_CATEGORY, BRANCH_DEMAND_TECHNOLOGY  # noqa: E402
except ModuleNotFoundError:
    try:
        from code.leap_core import (  # noqa: E402
            connect_to_leap,
            diagnose_measures_in_leap_branch,
            ensure_branch_exists,
            ensure_fuel_exists,
            safe_set_variable,
        )
        from code.leap_excel_io import finalise_export_df, save_export_files  # noqa: E402
        from code.config import BRANCH_DEMAND_CATEGORY, BRANCH_DEMAND_TECHNOLOGY  # noqa: E402
    except ModuleNotFoundError:
        try:
            from leap_core import (  # noqa: E402
                connect_to_leap,
                diagnose_measures_in_leap_branch,
                ensure_branch_exists,
                ensure_fuel_exists,
                safe_set_variable,
            )
            from leap_excel_io import finalise_export_df, save_export_files  # noqa: E402
            from configuration.config import BRANCH_DEMAND_CATEGORY, BRANCH_DEMAND_TECHNOLOGY  # noqa: E402
        except ModuleNotFoundError:
            from leap_core import (  # noqa: E402
                connect_to_leap,
                diagnose_measures_in_leap_branch,
                ensure_branch_exists,
                ensure_fuel_exists,
                safe_set_variable,
            )
            from leap_excel_io import finalise_export_df, save_export_files  # noqa: E402
            from configuration.config import BRANCH_DEMAND_CATEGORY, BRANCH_DEMAND_TECHNOLOGY  # noqa: E402


REQUIRED_INPUT_COLUMNS: tuple[str, ...] = (
    "Scenario",
    "Economy",
    "Date",
    "Drive",
    "Fuel",
    "Medium",
    "Energy",
    "Activity",
)

CURRENT_ACCOUNTS_LABEL = "Current Accounts"
ROOT_BRANCH = build_transport_branch_path(("International transport",), root=TRANSPORT_ROOT)

MEDIUM_NAME_MAP: dict[str, str] = {
    "air": "Air",
    "ship": "Shipping",
}

DRIVE_TO_FUEL_LEAF: dict[str, str] = {
    "air_gasoline": "Motor gasoline",
    "air_av_gas": "Aviation gasoline",
    "air_kerosene": "Kerosene",
    "air_diesel": "Gas and diesel oil",
    "air_fuel_oil": "Fuel oil",
    "air_lpg": "LPG",
    "air_jet_fuel": "Kerosene type jet fuel",
    "air_hydrogen": "Hydrogen",
    "ship_ammonia": "Ammonia",
    "ship_diesel": "Gas and diesel oil",
    "ship_fuel_oil": "Fuel oil",
    "ship_gasoline": "Motor gasoline",
    "ship_hydrogen": "Hydrogen",
    "ship_kerosene": "Kerosene",
    "ship_lng": "LNG",
    "ship_lpg": "LPG",
    "ship_natural_gas": "Natural gas",
    "ship_other_petroleum_products": "Other products",
}

FUEL_CODE_TO_FUEL_LEAF: dict[str, str] = {
    "07_01_motor_gasoline": "Motor gasoline",
    "07_02_aviation_gasoline": "Aviation gasoline",
    "07_06_kerosene": "Kerosene",
    "07_07_gas_diesel_oil": "Gas and diesel oil",
    "07_08_fuel_oil": "Fuel oil",
    "07_09_lpg": "LPG",
    "07_x_jet_fuel": "Kerosene type jet fuel",
    "07_x_other_petroleum_products": "Other products",
    "08_01_natural_gas": "Natural gas",
    "08_02_lng": "LNG",
    "16_x_ammonia": "Ammonia",
    "16_x_hydrogen": "Hydrogen",
}

CANONICAL_FUELS_BY_MEDIUM: dict[str, list[str]] = {
    "Air": [
        "Aviation gasoline",
        "Gas and diesel oil",
        "Fuel oil",
        "Motor gasoline",
        "Hydrogen",
        "Kerosene type jet fuel",
        "Kerosene",
        "LPG",
    ],
    "Shipping": [
        "Ammonia",
        "Gas and diesel oil",
        "Fuel oil",
        "Motor gasoline",
        "Hydrogen",
        "Kerosene",
        "LNG",
        "LPG",
        "Natural gas",
        "Other products",
    ],
}


@dataclass(frozen=True)
class InternationalExportConfig:
    input_path: str
    output_dir: str
    scenario: str | list[str] = "Reference"
    scope: str = "00_APEC"
    base_year: int = 2022
    final_year: int = 2060
    emit_quality_report: bool = True
    emit_medium_summary: bool = True
    check_branches_in_leap_using_com: bool = False
    set_vars_in_leap_using_com: bool = False
    auto_set_missing_branches: bool = False
    ensure_fuels_in_leap: bool = True


def _resolve_input_and_output(config: InternationalExportConfig) -> tuple[Path, Path]:
    resolved_input = resolve_str(config.input_path)
    resolved_output = resolve_str(config.output_dir)
    if resolved_input is None:
        raise ValueError("input_path must be provided.")
    if resolved_output is None:
        raise ValueError("output_dir must be provided.")

    input_path = Path(resolved_input)
    output_dir = Path(resolved_output)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_path, output_dir


def _validate_scope(scope: str) -> None:
    scope_text = str(scope).strip()
    if not scope_text:
        raise ValueError(
            "scope must be a non-empty string such as '00_APEC' or an economy code like '20_USA'."
        )


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_INPUT_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Input is missing required columns: " + ", ".join(missing)
        )


def _coerce_and_validate_dates(df: pd.DataFrame) -> pd.Series:
    date_numeric = pd.to_numeric(df["Date"], errors="coerce")
    if date_numeric.isna().any():
        bad_count = int(date_numeric.isna().sum())
        raise ValueError(f"'Date' contains {bad_count} non-numeric value(s).")
    if not (date_numeric % 1 == 0).all():
        raise ValueError("'Date' must contain integer year values only.")
    return date_numeric.astype(int)


def _validate_input_uniqueness(df: pd.DataFrame) -> None:
    key_cols = ["Scenario", "Economy", "Date", "Medium", "Drive", "Fuel"]
    duplicates = df.duplicated(subset=key_cols, keep=False)
    if duplicates.any():
        dup_count = int(duplicates.sum())
        raise ValueError(
            "Duplicate key rows found for "
            f"{key_cols}: {dup_count} duplicate row(s)."
        )


def _validate_year_window(df: pd.DataFrame, base_year: int, final_year: int) -> None:
    detected_min = int(df["Date"].min())
    detected_max = int(df["Date"].max())

    if base_year < detected_min:
        raise ValueError(
            f"base_year={base_year} is below input range start {detected_min}."
        )
    if final_year > detected_max:
        raise ValueError(
            f"final_year={final_year} is above input range end {detected_max}."
        )
    if base_year > final_year:
        raise ValueError("base_year must be <= final_year.")


def _resolve_scenario_labels(df: pd.DataFrame, scenario: str | list[str]) -> list[str]:
    available = df["Scenario"].dropna().astype(str).unique().tolist()
    requested = [scenario] if isinstance(scenario, str) else list(scenario)
    requested_clean = [str(item).strip() for item in requested if str(item).strip()]
    if not requested_clean:
        raise ValueError("At least one scenario must be provided.")

    resolved: list[str] = []
    for requested_name in requested_clean:
        matches = [name for name in available if name.lower() == requested_name.lower()]
        if not matches:
            raise ValueError(
                f"Scenario '{requested_name}' not found in input. Available scenarios: {sorted(available)}"
            )
        if len(matches) > 1:
            raise ValueError(
                "Scenario "
                f"'{requested_name}' matched multiple labels with differing case: {sorted(matches)}"
            )
        resolved.append(matches[0])

    ordered_unique: list[str] = []
    seen: set[str] = set()
    for label in resolved:
        key = label.lower()
        if key not in seen:
            ordered_unique.append(label)
            seen.add(key)

    return ordered_unique


def _map_medium_branch(raw_medium: str) -> str:
    key = str(raw_medium).strip().lower()
    if key in MEDIUM_NAME_MAP:
        return MEDIUM_NAME_MAP[key]
    token = str(raw_medium).strip()
    return token.capitalize() if token else "Unknown"


def _map_fuel_leaf(drive: str, fuel_code: str) -> str:
    drive_key = str(drive)
    fuel_key = str(fuel_code)
    if drive_key in DRIVE_TO_FUEL_LEAF:
        return DRIVE_TO_FUEL_LEAF[drive_key]
    if fuel_key in FUEL_CODE_TO_FUEL_LEAF:
        return FUEL_CODE_TO_FUEL_LEAF[fuel_key]
    return fuel_key


def _prepare_clean_input(source_df: pd.DataFrame) -> pd.DataFrame:
    _validate_required_columns(source_df)

    df = source_df.copy()
    df["Date"] = _coerce_and_validate_dates(df)
    _validate_input_uniqueness(df)

    df["Energy_raw"] = pd.to_numeric(df["Energy"], errors="coerce")
    df["Activity_raw"] = pd.to_numeric(df["Activity"], errors="coerce").fillna(0.0)

    df["Energy_clean"] = df["Energy_raw"].fillna(0.0)
    df["flag_energy_missing"] = df["Energy_raw"].isna()
    df["flag_energy_positive_activity_zero"] = (
        (df["Energy_clean"] > 0.0) & (df["Activity_raw"] == 0.0)
    )

    return df


def _build_scoped_leaf_table(
    clean_df: pd.DataFrame,
    *,
    scope: str,
    scenario_label: str,
    base_year: int,
    final_year: int,
) -> pd.DataFrame:
    scope_text = str(scope).strip()
    scoped = clean_df[
        (clean_df["Scenario"].astype(str).str.lower() == scenario_label.lower())
        & (clean_df["Date"] >= int(base_year))
        & (clean_df["Date"] <= int(final_year))
    ].copy()
    if scope_text.lower() != "00_apec":
        scoped = scoped[
            scoped["Economy"].astype(str).str.lower() == scope_text.lower()
        ].copy()
    if scoped.empty:
        raise ValueError(
            "No rows found after scenario/year/scope filtering for "
            f"scenario='{scenario_label}', scope='{scope_text}', and years {base_year}-{final_year}."
        )

    # Aggregate across rows in scope (all economies for 00_APEC, one economy otherwise).
    scoped = (
        scoped.groupby(["Scenario", "Date", "Medium", "Drive", "Fuel"], dropna=False)[
            ["Energy_clean", "Activity_raw"]
        ]
        .sum(min_count=1)
        .reset_index()
    )

    scoped["Medium_branch"] = scoped["Medium"].astype(str).map(_map_medium_branch)
    scoped["Fuel_leaf"] = scoped.apply(
        lambda row: _map_fuel_leaf(row["Drive"], row["Fuel"]), axis=1
    )

    leaf_df = (
        scoped.groupby(["Scenario", "Date", "Medium_branch", "Fuel_leaf"], dropna=False)[
            ["Energy_clean", "Activity_raw"]
        ]
        .sum(min_count=1)
        .reset_index()
    )

    # Fixed fuel branch set across years: use canonical sets when available.
    observed_fuels = (
        leaf_df[["Medium_branch", "Fuel_leaf"]]
        .drop_duplicates()
        .groupby("Medium_branch", dropna=False)["Fuel_leaf"]
        .apply(list)
        .to_dict()
    )

    fuels_by_medium: dict[str, list[str]] = {}
    for medium, fuels in observed_fuels.items():
        if medium in CANONICAL_FUELS_BY_MEDIUM:
            allowed = CANONICAL_FUELS_BY_MEDIUM[medium]
            medium_set = set(fuels)
            ordered = [fuel for fuel in allowed if fuel in medium_set]
            extras = sorted([fuel for fuel in fuels if fuel not in set(allowed)])
            fuels_by_medium[medium] = ordered + extras
        else:
            fuels_by_medium[medium] = sorted(set(fuels))

    medium_order = [m for m in ["Air", "Shipping"] if m in fuels_by_medium]
    medium_order += sorted([m for m in fuels_by_medium if m not in medium_order])

    years = list(range(int(base_year), int(final_year) + 1))
    records: list[dict[str, object]] = []
    for year in years:
        for medium in medium_order:
            fuels = fuels_by_medium.get(medium, [])
            for fuel_leaf in fuels:
                records.append(
                    {
                        "Scenario": scenario_label,
                        "Date": int(year),
                        "Medium_branch": medium,
                        "Fuel_leaf": fuel_leaf,
                    }
                )

    full_index_df = pd.DataFrame(records)
    if full_index_df.empty:
        raise ValueError(
            "No medium/fuel branches available after applying scope and year window."
        )

    leaf_df = full_index_df.merge(
        leaf_df,
        on=["Scenario", "Date", "Medium_branch", "Fuel_leaf"],
        how="left",
    )
    leaf_df["Energy_clean"] = leaf_df["Energy_clean"].fillna(0.0)
    leaf_df["Activity_raw"] = leaf_df["Activity_raw"].fillna(0.0)

    return leaf_df


def _compute_share_or_equal(series: pd.Series) -> pd.Series:
    total = float(series.sum())
    if total > 0.0:
        return series.astype(float) * (100.0 / total)
    if len(series) == 0:
        return series.astype(float)
    equal_share = 100.0 / float(len(series))
    return pd.Series(equal_share, index=series.index, dtype=float)


def _compute_hierarchy_values(
    leaf_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = leaf_df.copy()

    df["I_src"] = 0.0
    has_activity = df["Activity_raw"] > 0.0
    df.loc[has_activity, "I_src"] = (
        df.loc[has_activity, "Energy_clean"] / df.loc[has_activity, "Activity_raw"]
    )

    df["A_leaf"] = df["Activity_raw"].astype(float)

    medium_df = (
        df.groupby(["Scenario", "Date", "Medium_branch"], dropna=False)["A_leaf"]
        .sum(min_count=1)
        .reset_index(name="A_medium")
    )

    top_df = (
        medium_df.groupby(["Scenario", "Date"], dropna=False)["A_medium"]
        .sum(min_count=1)
        .reset_index(name="A_total")
    )

    medium_df["Activity_share"] = (
        medium_df.groupby(["Scenario", "Date"], dropna=False)["A_medium"]
        .transform(_compute_share_or_equal)
        .astype(float)
    )

    df["Leaf_activity_share"] = (
        df.groupby(["Scenario", "Date", "Medium_branch"], dropna=False)["A_leaf"]
        .transform(_compute_share_or_equal)
        .astype(float)
    )

    top_df["Top_activity_value"] = top_df["A_total"].astype(float) * 1e9
    df["Leaf_intensity_value"] = df["I_src"].astype(float) * 1e-3

    return top_df, medium_df, df


def _validate_activity_share_sums(
    medium_df: pd.DataFrame,
    leaf_df: pd.DataFrame,
    *,
    tol: float = 1e-6,
    max_examples: int = 5,
) -> None:
    """Validate that activity share siblings sum to 100 within tolerance."""
    med_totals = (
        medium_df.groupby(["Scenario", "Date"], dropna=False)["Activity_share"]
        .sum(min_count=1)
    )
    bad_medium = med_totals[(med_totals - 100.0).abs() > tol]

    leaf_totals = (
        leaf_df.groupby(["Scenario", "Date", "Medium_branch"], dropna=False)["Leaf_activity_share"]
        .sum(min_count=1)
    )
    bad_leaf = leaf_totals[(leaf_totals - 100.0).abs() > tol]

    if bad_medium.empty and bad_leaf.empty:
        return

    lines: list[str] = []
    for (scenario, year), total in bad_medium.head(max_examples).items():
        lines.append(
            "medium shares | "
            f"scenario={scenario}, year={int(year)}, total={float(total):.6f}"
        )
    for (scenario, year, medium), total in bad_leaf.head(max_examples).items():
        lines.append(
            "leaf shares | "
            f"scenario={scenario}, year={int(year)}, medium={medium}, total={float(total):.6f}"
        )

    raise ValueError(
        "Activity share validation failed: expected sibling totals of 100."
        + ("\n" + "\n".join(lines) if lines else "")
    )


def _build_export_long(
    *,
    top_df: pd.DataFrame,
    medium_df: pd.DataFrame,
    leaf_df: pd.DataFrame,
) -> pd.DataFrame:
    top_rows = pd.DataFrame(
        {
            "Date": top_df["Date"].astype(int),
            "Branch_Path": ROOT_BRANCH,
            "Measure": "Activity Level",
            "Value": top_df["Top_activity_value"].astype(float),
            "Scenario": top_df["Scenario"].astype(str),
            "Units": "Unspecified Unit",
            "Scale": "",
            "Per...": "",
        }
    )

    medium_rows = pd.DataFrame(
        {
            "Date": medium_df["Date"].astype(int),
            "Branch_Path": ROOT_BRANCH + "\\" + medium_df["Medium_branch"].astype(str),
            "Measure": "Activity Level",
            "Value": medium_df["Activity_share"].astype(float),
            "Scenario": medium_df["Scenario"].astype(str),
            "Units": "Share",
            "Scale": "%",
            "Per...": "",
        }
    )

    leaf_activity_rows = pd.DataFrame(
        {
            "Date": leaf_df["Date"].astype(int),
            "Branch_Path": (
                ROOT_BRANCH
                + "\\"
                + leaf_df["Medium_branch"].astype(str)
                + "\\"
                + leaf_df["Fuel_leaf"].astype(str)
            ),
            "Measure": "Activity Level",
            "Value": leaf_df["Leaf_activity_share"].astype(float),
            "Scenario": leaf_df["Scenario"].astype(str),
            "Units": "Share",
            "Scale": "%",
            "Per...": "",
        }
    )

    leaf_intensity_rows = pd.DataFrame(
        {
            "Date": leaf_df["Date"].astype(int),
            "Branch_Path": (
                ROOT_BRANCH
                + "\\"
                + leaf_df["Medium_branch"].astype(str)
                + "\\"
                + leaf_df["Fuel_leaf"].astype(str)
            ),
            "Measure": "Final Energy Intensity",
            "Value": leaf_df["Leaf_intensity_value"].astype(float),
            "Scenario": leaf_df["Scenario"].astype(str),
            "Units": "Gigajoule",
            "Scale": "",
            "Per...": "",
        }
    )

    export_long = pd.concat(
        [top_rows, medium_rows, leaf_activity_rows, leaf_intensity_rows],
        ignore_index=True,
    )

    # Guard against accidental negative values from transforms.
    export_long["Value"] = export_long["Value"].clip(lower=0.0)

    dedup_cols = ["Date", "Branch_Path", "Measure", "Scenario"]
    duplicate_mask = export_long.duplicated(subset=dedup_cols, keep=False)
    if duplicate_mask.any():
        raise ValueError(
            "Duplicate export rows detected for key "
            f"{dedup_cols}: {int(duplicate_mask.sum())} row(s)."
        )

    return export_long


def _format_data_expression(points: list[tuple[int, float]]) -> str:
    if len(points) == 1:
        return str(points[0][1])
    body = ", ".join(f"{year}, {value:.6g}" for year, value in points)
    return f"Data({body})"


def _year_columns(df: pd.DataFrame) -> list[int | str]:
    cols: list[int | str] = []
    for col in df.columns:
        if isinstance(col, int):
            cols.append(col)
        elif isinstance(col, str) and col.isdigit() and len(col) == 4:
            cols.append(col)
    return sorted(cols, key=lambda x: int(x))


def _convert_values_to_expressions(
    for_viewing_df: pd.DataFrame,
    *,
    current_accounts_label: str = CURRENT_ACCOUNTS_LABEL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    viewing_df = for_viewing_df.copy()
    year_cols = _year_columns(viewing_df)

    expressions: list[str | None] = []
    methods: list[str | None] = []

    for _, row in viewing_df.iterrows():
        points: list[tuple[int, float]] = []
        for year_col in year_cols:
            value = row[year_col]
            if pd.notna(value):
                points.append((int(year_col), float(value)))

        if not points:
            expressions.append(None)
            methods.append(None)
            continue

        is_current_accounts = str(row.get("Scenario", "")) == current_accounts_label
        if is_current_accounts or len(points) == 1:
            expressions.append(str(points[0][1]))
            methods.append("SingleValue")
        else:
            expressions.append(_format_data_expression(points))
            methods.append("Data")

    viewing_df["Method"] = methods

    leap_df = viewing_df.drop(columns=year_cols + ["Method"]).copy()
    level_cols = [col for col in leap_df.columns if str(col).startswith("Level ")]
    base_cols = [
        col
        for col in [
            "Branch Path",
            "Variable",
            "Scenario",
            "Region",
            "Scale",
            "Units",
            "Per...",
        ]
        if col in leap_df.columns
    ]
    other_cols = [
        col
        for col in leap_df.columns
        if col not in set(base_cols + level_cols)
    ]
    leap_df = leap_df[base_cols + other_cols + level_cols].copy()
    leap_df.insert(7 if len(base_cols) >= 7 else len(base_cols), "Expression", expressions)

    return leap_df, viewing_df


def _attach_blank_id_columns(df: pd.DataFrame, *, leap_sheet: bool) -> pd.DataFrame:
    out = df.copy()
    id_cols = ["RegionID", "BranchID", "VariableID", "ScenarioID"]
    for col in id_cols:
        out[col] = pd.Series([pd.NA] * len(out), dtype="Int64")

    if leap_sheet:
        front = ["BranchID", "VariableID", "ScenarioID", "RegionID"]
    else:
        front = ["RegionID", "BranchID", "VariableID", "ScenarioID"]

    ordered = front + [col for col in out.columns if col not in front]
    return out.loc[:, ordered].copy()


def _collect_leaf_fuels_for_leap(leap_df: pd.DataFrame) -> list[str]:
    fuels: set[str] = set()
    if "Branch Path" not in leap_df.columns:
        return []
    for branch_path in leap_df["Branch Path"].dropna().astype(str).unique():
        logical_tuple = extract_transport_branch_tuple(branch_path)
        if len(logical_tuple) >= 3 and logical_tuple[0] == "International transport":
            fuels.add(logical_tuple[-1])
    return sorted(fuels)


def ensure_international_fuels_in_leap(L, leap_df: pd.DataFrame) -> None:
    fuels = _collect_leaf_fuels_for_leap(leap_df)
    if not fuels:
        print("[INFO] No international transport fuels detected to ensure in LEAP.")
        return
    print(f"[INFO] Ensuring {len(fuels)} international fuel(s) exist in LEAP.")
    for fuel in fuels:
        ensure_fuel_exists(L, fuel)


def _expected_measures_by_branch(leap_df: pd.DataFrame) -> dict[str, set[str]]:
    required_cols = {"Branch Path", "Variable"}
    missing = sorted(required_cols.difference(leap_df.columns))
    if missing:
        raise ValueError(
            "LEAP export dataframe is missing required columns for branch diagnostics: "
            + ", ".join(missing)
        )
    grouped = (
        leap_df[["Branch Path", "Variable"]]
        .dropna(subset=["Branch Path", "Variable"])
        .groupby("Branch Path", dropna=False)["Variable"]
        .apply(lambda values: {str(v) for v in values if str(v).strip()})
    )
    return grouped.to_dict()


def diagnose_international_branches_in_leap(
    L,
    leap_df: pd.DataFrame,
    *,
    auto_set_missing_branches: bool,
) -> None:
    print("\n=== Checking international transport branches in LEAP via COM ===")
    expected_by_branch = _expected_measures_by_branch(leap_df)
    ordered_paths = sorted(
        expected_by_branch.keys(),
        key=lambda path: (len([part for part in str(path).split("\\") if part]), str(path)),
    )
    category_hint = {"branch_type": BRANCH_DEMAND_CATEGORY}
    technology_hint = {"branch_type": BRANCH_DEMAND_TECHNOLOGY}

    for branch_path in ordered_paths:
        parts = [part for part in str(branch_path).split("\\") if part]
        if len(parts) < 2:
            raise ValueError(f"Unexpected branch path format: '{branch_path}'.")
        logical_tuple = extract_transport_branch_tuple(branch_path)
        if not logical_tuple or logical_tuple[0] != "International transport":
            raise ValueError(
                "Unexpected branch root for international export: "
                f"'{branch_path}'. Expected prefix '{ROOT_BRANCH}'."
            )

        if len(logical_tuple) >= 3:
            parent_path = "\\".join(parts[:-1])
            ensure_branch_exists(
                L,
                parent_path,
                category_hint,
                AUTO_SET_MISSING_BRANCHES=auto_set_missing_branches,
            )
            branch_hint = technology_hint
        else:
            branch_hint = category_hint

        ensure_branch_exists(
            L,
            branch_path,
            branch_hint,
            AUTO_SET_MISSING_BRANCHES=auto_set_missing_branches,
        )

        expected_measures = expected_by_branch.get(branch_path, set())
        diagnose_measures_in_leap_branch(
            L,
            branch_path,
            logical_tuple,
            expected_measures,
        )


def _scenario_name_from_com(active_scenario) -> str | None:
    """Extract a scenario name from a LEAP COM scenario object or value."""
    if active_scenario is None:
        return None
    try:
        name = getattr(active_scenario, "Name", None)
        if name is not None and str(name).strip():
            return str(name).strip()
    except Exception:
        pass
    text = str(active_scenario).strip()
    return text or None


def _list_available_scenarios(L) -> list[str]:
    names: list[str] = []
    scenarios = getattr(L, "Scenarios", None)
    if scenarios is None:
        return names
    try:
        count = int(scenarios.Count)
    except Exception:
        return names
    for idx in range(1, count + 1):
        try:
            item = scenarios.Item(idx)
        except Exception:
            continue
        item_name = _scenario_name_from_com(item)
        if item_name:
            names.append(item_name)
    return names


def _activate_leap_scenario(L, scenario_name: str) -> str:
    """
    Set LEAP active scenario robustly and verify it actually changed.

    LEAP can ignore direct string assignment silently. We try multiple lookup
    paths and require an exact scenario-name match before writing variables.
    """
    target = str(scenario_name).strip()
    if not target:
        raise ValueError("Cannot activate an empty LEAP scenario name.")

    def _matches_target() -> tuple[bool, str | None]:
        active_name = _scenario_name_from_com(getattr(L, "ActiveScenario", None))
        if active_name is None:
            return False, None
        return active_name.casefold() == target.casefold(), active_name

    # 1) Direct assignment by name.
    try:
        L.ActiveScenario = target
    except Exception:
        pass
    ok, active_name = _matches_target()
    if ok:
        return active_name or target

    # 2) Lookup via L.Scenario(name), then assign object.
    try:
        scenario_obj = L.Scenario(target)
        if scenario_obj is not None:
            L.ActiveScenario = scenario_obj
    except Exception:
        pass
    ok, active_name = _matches_target()
    if ok:
        return active_name or target

    # 3) Case-insensitive lookup from L.Scenarios collection.
    scenarios = getattr(L, "Scenarios", None)
    if scenarios is not None:
        try:
            count = int(scenarios.Count)
        except Exception:
            count = 0
        for idx in range(1, count + 1):
            try:
                item = scenarios.Item(idx)
            except Exception:
                continue
            item_name = _scenario_name_from_com(item)
            if item_name and item_name.casefold() == target.casefold():
                try:
                    L.ActiveScenario = item
                except Exception:
                    break
                ok, active_name = _matches_target()
                if ok:
                    return active_name or target
                break

    available = _list_available_scenarios(L)
    available_text = ", ".join(available) if available else "unavailable"
    active_text = active_name or "unknown"
    raise RuntimeError(
        f"[ERROR] Failed to activate LEAP scenario '{target}'. "
        f"Active scenario remains '{active_text}'. Available: {available_text}"
    )


def write_export_df_to_leap(L, leap_df: pd.DataFrame) -> None:
    print(
        "\n=== Setting international transport variables in LEAP via COM. "
        "Do not use the LEAP UI while this runs. ==="
    )
    required_cols = {"Scenario", "Branch Path", "Variable", "Expression"}
    missing = sorted(required_cols.difference(leap_df.columns))
    if missing:
        raise ValueError(
            "LEAP export dataframe is missing required columns for COM writes: "
            + ", ".join(missing)
        )

    total_written = 0
    for scenario in leap_df["Scenario"].dropna().astype(str).unique():
        try:
            active_name = _activate_leap_scenario(L, scenario)
        except Exception as exc:
            raise RuntimeError(f"[ERROR] Failed to set active scenario '{scenario}' in LEAP: {exc}") from exc

        scenario_rows = leap_df[leap_df["Scenario"].astype(str) == scenario]
        missing_expr = 0
        missing_vars = 0
        scenario_written = 0

        for _, row in scenario_rows.iterrows():
            branch_path = str(row["Branch Path"])
            variable = str(row["Variable"])
            expression = row["Expression"]
            expression_text = "" if pd.isna(expression) else str(expression).strip()
            if not expression_text:
                print(f"[WARN] Skipping empty expression for '{variable}' on {branch_path}")
                missing_expr += 1
                continue

            unit_name = row.get("Units")
            branch = ensure_branch_exists(
                L,
                branch_path,
                tuple(part for part in branch_path.split("\\")[1:] if part),
                AUTO_SET_MISSING_BRANCHES=False,
            )
            if branch is None:
                missing_vars += 1
                continue
            success = safe_set_variable(
                L,
                branch,
                variable,
                expression_text,
                unit_name=unit_name,
                context=branch_path,
            )
            if success:
                total_written += 1
                scenario_written += 1
            else:
                missing_vars += 1

        print(
            f"[INFO] Finished scenario '{scenario}' | "
            f"active='{active_name}', written={scenario_written}, "
            f"missing_variables={missing_vars}, empty_expressions={missing_expr}, "
            f"running_total_written={total_written}"
        )


def _sync_international_export_to_leap(config: InternationalExportConfig, leap_df: pd.DataFrame) -> None:
    require_leap_connection = (
        config.check_branches_in_leap_using_com
        or config.set_vars_in_leap_using_com
    )
    if not require_leap_connection:
        return

    L = connect_to_leap()
    if L is None:
        raise RuntimeError(
            "LEAP COM connection failed. Disable COM flags or run from a Windows environment "
            "with LEAP open and pywin32 available."
        )

    if config.ensure_fuels_in_leap:
        ensure_international_fuels_in_leap(L, leap_df)

    if config.check_branches_in_leap_using_com:
        diagnose_international_branches_in_leap(
            L,
            leap_df,
            auto_set_missing_branches=config.auto_set_missing_branches,
        )

    if config.set_vars_in_leap_using_com:
        write_export_df_to_leap(L, leap_df)


def _build_quality_report(clean_df: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    scope_text = str(scope).strip()
    report_df = clean_df.copy()
    if scope_text.lower() != "00_apec":
        report_df = report_df[
            report_df["Economy"].astype(str).str.lower() == scope_text.lower()
        ].copy()

    row_event_cols = [
        "Scenario",
        "Economy",
        "Date",
        "Medium",
        "Drive",
        "Fuel",
        "Energy_raw",
        "Energy_clean",
        "Activity_raw",
    ]

    missing_energy_rows = report_df[report_df["flag_energy_missing"]][row_event_cols].copy()
    missing_energy_rows["record_type"] = "row"
    missing_energy_rows["quality_flag"] = "flag_energy_missing"
    missing_energy_rows["count"] = 1

    anomaly_rows = report_df[report_df["flag_energy_positive_activity_zero"]][row_event_cols].copy()
    anomaly_rows["record_type"] = "row"
    anomaly_rows["quality_flag"] = "flag_energy_positive_activity_zero"
    anomaly_rows["count"] = 1

    row_events = pd.concat([missing_energy_rows, anomaly_rows], ignore_index=True)

    grouped_counts = (
        row_events.groupby(["quality_flag", "Scenario", "Economy", "Drive"], dropna=False)["count"]
        .sum()
        .reset_index()
    )
    grouped_counts["record_type"] = "count_by_scenario_economy_drive"
    grouped_counts["Date"] = pd.NA
    grouped_counts["Medium"] = pd.NA
    grouped_counts["Fuel"] = pd.NA
    grouped_counts["Energy_raw"] = pd.NA
    grouped_counts["Energy_clean"] = pd.NA
    grouped_counts["Activity_raw"] = pd.NA

    total_counts = (
        row_events.groupby(["quality_flag"], dropna=False)["count"]
        .sum()
        .reset_index()
    )
    total_counts["record_type"] = "count_total"
    total_counts["Scenario"] = pd.NA
    total_counts["Economy"] = pd.NA
    total_counts["Date"] = pd.NA
    total_counts["Medium"] = pd.NA
    total_counts["Drive"] = pd.NA
    total_counts["Fuel"] = pd.NA
    total_counts["Energy_raw"] = pd.NA
    total_counts["Energy_clean"] = pd.NA
    total_counts["Activity_raw"] = pd.NA

    quality_df = pd.concat([row_events, grouped_counts, total_counts], ignore_index=True)
    quality_cols = [
        "record_type",
        "quality_flag",
        "Scenario",
        "Economy",
        "Date",
        "Medium",
        "Drive",
        "Fuel",
        "Energy_raw",
        "Energy_clean",
        "Activity_raw",
        "count",
    ]
    return quality_df.loc[:, quality_cols].copy()


def _build_medium_summary(
    leaf_df: pd.DataFrame,
    *,
    scope: str,
) -> pd.DataFrame:
    summary = (
        leaf_df.groupby(["Scenario", "Date", "Medium_branch"], dropna=False)[
            ["Energy_clean", "Activity_raw"]
        ]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"Medium_branch": "Medium", "Energy_clean": "Energy", "Activity_raw": "Activity"})
    )

    summary["Intensity"] = 0.0
    has_activity = summary["Activity"] > 0.0
    summary.loc[has_activity, "Intensity"] = (
        summary.loc[has_activity, "Energy"] / summary.loc[has_activity, "Activity"]
    )
    summary.insert(1, "Economy", str(scope))

    return summary[["Scenario", "Economy", "Date", "Medium", "Energy", "Activity", "Intensity"]]


def _sanitize_filename_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value).strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or "scenario"


def _drop_all_zero_rows_across_scenarios(export_long_df: pd.DataFrame) -> pd.DataFrame:
    if export_long_df.empty:
        return export_long_df.copy()

    out = export_long_df.copy()
    out["Value"] = pd.to_numeric(out["Value"], errors="coerce").fillna(0.0).astype(float)
    group_cols = ["Branch_Path", "Measure", "Units", "Scale", "Per..."]
    keep_mask = out.groupby(group_cols, dropna=False)["Value"].transform(
        lambda values: bool((values.abs() > 0.0).any())
    )
    return out.loc[keep_mask].copy()


def _append_current_accounts_rows(
    export_long_df: pd.DataFrame,
    *,
    base_year: int,
    source_scenario: str,
    current_accounts_label: str = CURRENT_ACCOUNTS_LABEL,
) -> pd.DataFrame:
    source_mask = export_long_df["Scenario"].astype(str).str.lower() == source_scenario.lower()
    source_df = export_long_df.loc[source_mask].copy()
    if source_df.empty:
        raise ValueError(
            f"Could not build Current Accounts because source scenario '{source_scenario}' has no rows."
        )

    current_accounts_df = source_df[source_df["Date"] == int(base_year)].copy()
    current_accounts_df["Scenario"] = current_accounts_label

    combined = pd.concat([export_long_df, current_accounts_df], ignore_index=True)
    dedup_cols = ["Date", "Branch_Path", "Measure", "Scenario"]
    return combined.drop_duplicates(subset=dedup_cols, keep="first")


def run_international_export_workflow(
    config: InternationalExportConfig,
) -> dict[str, str]:
    input_path, output_dir = _resolve_input_and_output(config)
    _validate_scope(config.scope)

    source_df = pd.read_csv(input_path, low_memory=False)
    clean_df = _prepare_clean_input(source_df)

    scenario_labels = _resolve_scenario_labels(clean_df, config.scenario)
    scenario_label_set = {label.lower() for label in scenario_labels}
    clean_df = clean_df[
        clean_df["Scenario"].astype(str).str.lower().isin(scenario_label_set)
    ].copy()
    _validate_year_window(clean_df, config.base_year, config.final_year)

    scoped_leaf_parts: list[pd.DataFrame] = []
    for scenario_label in scenario_labels:
        scoped_leaf_parts.append(
            _build_scoped_leaf_table(
                clean_df,
                scope=config.scope,
                scenario_label=scenario_label,
                base_year=config.base_year,
                final_year=config.final_year,
            )
        )
    scoped_leaf_df = pd.concat(scoped_leaf_parts, ignore_index=True)

    top_df, medium_df, leaf_df = _compute_hierarchy_values(scoped_leaf_df)
    _validate_activity_share_sums(medium_df, leaf_df)
    export_long_df = _build_export_long(top_df=top_df, medium_df=medium_df, leaf_df=leaf_df)
    export_long_df = _drop_all_zero_rows_across_scenarios(export_long_df)
    if export_long_df.empty:
        raise ValueError("All export rows were zero across the selected scenarios.")

    current_accounts_source = scenario_labels[0]
    export_long_df = _append_current_accounts_rows(
        export_long_df,
        base_year=config.base_year,
        source_scenario=current_accounts_source,
        current_accounts_label=CURRENT_ACCOUNTS_LABEL,
    )

    viewing_df = finalise_export_df(
        export_long_df,
        scenario=current_accounts_source,
        region=str(config.scope),
        base_year=config.base_year,
        final_year=config.final_year,
    )
    if viewing_df is None or viewing_df.empty:
        raise ValueError("No rows were produced for international export workbook.")

    leap_df, viewing_df = _convert_values_to_expressions(
        viewing_df,
        current_accounts_label=CURRENT_ACCOUNTS_LABEL,
    )

    viewing_df = _attach_blank_id_columns(viewing_df, leap_sheet=False)
    leap_df = _attach_blank_id_columns(leap_df, leap_sheet=True)

    scenario_token = _sanitize_filename_token("_".join(scenario_labels))
    workbook_path = output_dir / f"{config.scope}_international_transport_leap_export_{scenario_token}.xlsx"

    save_export_files(
        leap_df,
        viewing_df,
        str(workbook_path),
        config.base_year,
        config.final_year,
        model_name="APEC International Transport",
    )

    _sync_international_export_to_leap(config, leap_df)

    output_paths: dict[str, str] = {"workbook": str(workbook_path)}

    if config.emit_medium_summary:
        medium_summary_df = _build_medium_summary(scoped_leaf_df, scope=config.scope)
        medium_summary_path = (
            output_dir
            / f"{config.scope}_international_transport_medium_summary_{scenario_token}.csv"
        )
        medium_summary_df.to_csv(medium_summary_path, index=False)
        output_paths["medium_summary"] = str(medium_summary_path)

    if config.emit_quality_report:
        quality_df = _build_quality_report(clean_df, scope=config.scope)
        quality_path = (
            output_dir
            / f"{config.scope}_international_transport_quality_{scenario_token}.csv"
        )
        quality_df.to_csv(quality_path, index=False)
        output_paths["quality"] = str(quality_path)

    print(
        "[INFO] International transport export complete | "
        f"scenarios={scenario_labels} + {CURRENT_ACCOUNTS_LABEL} | "
        f"years={config.base_year}-{config.final_year} | "
        f"rows(for_viewing)={len(viewing_df)}"
    )
    for key, value in output_paths.items():
        print(f"[INFO] Wrote {key}: {value}")

    return output_paths


__all__ = [
    "InternationalExportConfig",
    "run_international_export_workflow",
]
