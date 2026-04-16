#%%
# ============================================================
# transport_workflow_pipeline.py
# ============================================================
# Main logic for processing and loading transport data into LEAP.
# Depends on LEAP_core.py and mapping/config modules.
# ============================================================

import sys
from pathlib import Path
import pandas as pd
import shutil
from datetime import datetime
from enum import Enum
from collections.abc import Mapping
from typing import Any

# Allow sibling leap_utilities package without pip install
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
#%%
from functions.path_utils import resolve_str
from functions.transport_branch_paths import (
    build_transport_branch_path,
    extract_transport_branch_tuple,
    is_non_road_branch_tuple,
)
try:
    from leap_utils.leap_core import (
        connect_to_leap,
        diagnose_measures_in_leap_branch,
        ensure_branch_exists,
        ensure_fuel_exists,
        safe_set_variable,
        # diagnose_leap_branch,
        create_transport_export_df,
        write_row_to_leap_export_df,
        build_expression_from_mapping,
        define_value_based_on_src_tuple,
    )
except ModuleNotFoundError:
    try:
        from code.leap_core import (
            connect_to_leap,
            diagnose_measures_in_leap_branch,
            ensure_branch_exists,
            ensure_fuel_exists,
            safe_set_variable,
            # diagnose_leap_branch,
            create_transport_export_df,
            write_row_to_leap_export_df,
            build_expression_from_mapping,
            define_value_based_on_src_tuple,
        )
    except ModuleNotFoundError:
        try:
            from leap_core import (
                connect_to_leap,
                diagnose_measures_in_leap_branch,
                ensure_branch_exists,
                ensure_fuel_exists,
                safe_set_variable,
                # diagnose_leap_branch,
                create_transport_export_df,
                write_row_to_leap_export_df,
                build_expression_from_mapping,
                define_value_based_on_src_tuple,
            )
        except ModuleNotFoundError:
            from leap_core import (
                connect_to_leap,
                diagnose_measures_in_leap_branch,
                ensure_branch_exists,
                ensure_fuel_exists,
                safe_set_variable,
                # diagnose_leap_branch,
                create_transport_export_df,
                write_row_to_leap_export_df,
                build_expression_from_mapping,
                define_value_based_on_src_tuple,
            )
from config.branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    create_new_source_rows_based_on_combinations,
    create_new_source_rows_based_on_proxies_with_no_activity,
)
from config.measure_catalog import list_all_measures, LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP
from config.measure_metadata import SOURCE_WEIGHT_PRIORITY
from functions.measure_processing import process_measures_for_leap
from functions.preprocessing import (
    allocate_fuel_alternatives_energy_and_activity,
    calculate_sales,
    normalize_and_calculate_shares)
try:
    from leap_utils.leap_excel_io import finalise_export_df, save_export_files, join_and_check_import_structure_matches_export_structure, separate_current_accounts_from_scenario
except ModuleNotFoundError:
    try:
        from code.leap_excel_io import finalise_export_df, save_export_files, join_and_check_import_structure_matches_export_structure, separate_current_accounts_from_scenario
    except ModuleNotFoundError:
        try:
            from leap_excel_io import finalise_export_df, save_export_files, join_and_check_import_structure_matches_export_structure, separate_current_accounts_from_scenario
        except ModuleNotFoundError:
            from leap_excel_io import finalise_export_df, save_export_files, join_and_check_import_structure_matches_export_structure, separate_current_accounts_from_scenario
from config.branch_expression_mapping import LEAP_BRANCH_TO_EXPRESSION_MAPPING, ALL_YEARS
from functions.esto_data import (
    extract_other_type_rows_from_esto_and_insert_into_transport_df,
)

from config.basic_mappings import (
    ESTO_TRANSPORT_SECTOR_TUPLES,
    LEAP_STRUCTURE,
    add_fuel_column,
    EXPECTED_COLS_IN_SOURCE,
)

from functions.mappings_validation import (
    validate_all_mappings_with_measures,
    validate_and_fix_shares_normalise_to_one,
    normalize_share_columns_wide,
    validate_final_energy_use_for_base_year_equals_esto_totals,
)
from functions.sales_curve_estimate import (
    load_survival_and_vintage_profiles,
)
from sales_workflow import (
    estimate_passenger_sales_from_dataframe,
    estimate_freight_sales_from_dataframe,
)
import os

LEAP_API_DISABLED_ERROR = (
    "[ERROR] LEAP API usage is disabled because the LEAP API is currently buggy. "
    "Disable LEAP COM flags to continue (CHECK_BRANCHES_IN_LEAP_USING_COM=False, "
    "SET_VARS_IN_LEAP_USING_COM=False). "
    "Attempted operation: {operation}"
)


def _raise_leap_api_disabled(operation: str) -> None:
    raise RuntimeError(LEAP_API_DISABLED_ERROR.format(operation=operation))

##########
#for reconciliation:

# imports and data loading
import pandas as pd
try:
    from leap_utils.energy_use_reconciliation import (
        build_branch_rules_from_mapping,
        reconcile_energy_use,
        build_adjustment_change_tables,
        get_adjustment_year_columns,
    )
except ModuleNotFoundError:
    try:
        from code.energy_use_reconciliation import (
            build_branch_rules_from_mapping,
            reconcile_energy_use,
            build_adjustment_change_tables,
            get_adjustment_year_columns,
        )
    except ModuleNotFoundError:
        try:
            from energy_use_reconciliation import (
                build_branch_rules_from_mapping,
                reconcile_energy_use,
                build_adjustment_change_tables,
                get_adjustment_year_columns,
            )
        except ModuleNotFoundError:
            from energy_use_reconciliation import (
                build_branch_rules_from_mapping,
                reconcile_energy_use,
                build_adjustment_change_tables,
                get_adjustment_year_columns,
            )
from config.branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    ALL_LEAP_BRANCHES_TRANSPORT,
)

from config.measure_catalog import LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP

from functions.energy_use_reconciliation_road import (
    transport_energy_fn,
    transport_adjustment_fn,
    build_transport_esto_energy_totals,
)
from functions.merged_energy_io import load_transport_energy_dataset
from config.transport_economy_config import (
    COMMON_CONFIG,
    DOMESTIC_EXPORT_DIR,
    ECONOMY_METADATA,
    FREIGHT_SALES_DIR,
    PASSENGER_SALES_DIR,
    load_transport_run_config,
    list_transport_run_configs,
    resolve_lifecycle_profile_path_for_economy,
)

#%%
# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------

def prepare_input_data(transport_model_excel_path, economy, scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = 'data/merged_file_energy_ALL_20250814_pretrump.csv', LOAD_CHECKPOINT=False, TRANSPORT_FUELS_DATA_FILE_PATH = None):
    """Load and preprocess transport data for a specific economy."""    
    print(f"\n=== Loading Transport Data for {economy} ===")
    transport_model_excel_path = resolve_str(transport_model_excel_path)
    TRANSPORT_ESTO_BALANCES_PATH = resolve_str(TRANSPORT_ESTO_BALANCES_PATH)
    if TRANSPORT_FUELS_DATA_FILE_PATH is not None:
        TRANSPORT_FUELS_DATA_FILE_PATH = resolve_str(TRANSPORT_FUELS_DATA_FILE_PATH)
    
    # Check for checkpoint file
    checkpoint_filename = resolve_str(
        f"intermediate_data/transport_data_{economy}_{scenario}_{base_year}_{final_year}.pkl"
    )
    if LOAD_CHECKPOINT and os.path.exists(checkpoint_filename):
        print(f"Loading data from checkpoint: {checkpoint_filename}")
        df = pd.read_pickle(checkpoint_filename)
        return df
    if transport_model_excel_path.endswith('.csv'):
        df = pd.read_csv(transport_model_excel_path, low_memory=False)
    else:
        df = pd.read_excel(transport_model_excel_path)
    df = df[(df["Economy"] == economy) & (df["Scenario"] == scenario)]
    df = df[(df["Date"] >= base_year) & (df["Date"] <= final_year)]
    #check EXPECTED_COLS_IN_SOURCE are all in df
    missing_cols = [col for col in EXPECTED_COLS_IN_SOURCE if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in source data: {missing_cols}")
    unnecessary_cols = ['Unit', 'Data_available', 'Measure']
    df = df.drop(columns=unnecessary_cols, errors='ignore')
    
    df = add_fuel_column(df)
    df.loc[df["Medium"] != "road", ["Stocks", 'Vehicle_sales_share']] = 0
    
    df = allocate_fuel_alternatives_energy_and_activity(df, economy, scenario, TRANSPORT_FUELS_DATA_FILE_PATH)
    
    new_rows1 = create_new_source_rows_based_on_combinations(df)
    df = pd.concat([df, new_rows1], ignore_index=True)
    new_rows2 = create_new_source_rows_based_on_proxies_with_no_activity(df, strict_missing=False)
    df = pd.concat([df, new_rows2], ignore_index=True)
    if new_rows1.empty:
        raise ValueError("No new source rows were created from combinations; check the mapping and source data just in case.")
    if new_rows2.empty:
        print("[WARN] No proxy-based source rows were created for this run.")
    
    #check for duplicates
    duplicates = df.duplicated(subset=['Date', 'Economy', 'Scenario', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'])
    if duplicates.any():
        errors_path = resolve_str("data/errors/duplicate_source_rows.csv")
        os.makedirs(Path(errors_path).parent, exist_ok=True)
        df[duplicates].to_csv(errors_path, index=False)
        raise ValueError(
            "Duplicates found in source data after adding new rows based on combinations and proxies; "
            f"see {errors_path} for details."
        )
     
    df = calculate_sales(df)
    df = normalize_and_calculate_shares(df)
    
    df = extract_other_type_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, scenario, TRANSPORT_ESTO_BALANCES_PATH)
    
    # Save checkpoint file
    os.makedirs(Path(checkpoint_filename).parent, exist_ok=True)
    df.to_pickle(checkpoint_filename)
    print(f"Saved checkpoint: {checkpoint_filename}")
    return df


def _first_non_null(series: pd.Series):
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    return non_null.iloc[0]


def _aggregate_weighted_column(
    df: pd.DataFrame,
    group_cols: list[str],
    value_col: str,
    weight_candidates: list[str | None],
) -> pd.DataFrame:
    value_series = pd.to_numeric(df[value_col], errors="coerce")

    selected_weight = None
    for candidate in weight_candidates:
        if candidate and candidate in df.columns:
            selected_weight = candidate
            break

    if selected_weight is None:
        return (
            df.assign(_value=value_series)
            .groupby(group_cols, dropna=False)["_value"]
            .mean()
            .reset_index(name=value_col)
        )

    tmp = df[group_cols].copy()
    tmp["_value"] = value_series
    tmp["_weight"] = pd.to_numeric(df[selected_weight], errors="coerce").fillna(0.0)
    tmp["_weighted"] = tmp["_value"].fillna(0.0) * tmp["_weight"]

    grouped = (
        tmp.groupby(group_cols, dropna=False)
        .agg(
            weighted_sum=("_weighted", "sum"),
            weight_sum=("_weight", "sum"),
            mean_value=("_value", "mean"),
        )
        .reset_index()
    )

    weighted = grouped["weighted_sum"] / grouped["weight_sum"].replace(0, pd.NA)
    grouped[value_col] = weighted.fillna(grouped["mean_value"])
    return grouped[group_cols + [value_col]]


def _collect_fuels_from_tree(tree: dict) -> list[str]:
    fuels: set[str] = set()

    def walk(node):
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                fuels.add(item)

    walk(tree)
    return sorted(fuels)


def ensure_transport_fuels_in_leap(L) -> None:
    fuels = _collect_fuels_from_tree(LEAP_STRUCTURE)
    if not fuels:
        print("[INFO] No transport fuels detected to ensure in LEAP.")
        return
    print(f"[INFO] Ensuring {len(fuels)} transport fuel(s) exist in LEAP.")
    for fuel in fuels:
        ensure_fuel_exists(L, fuel)


def aggregate_economies_to_apec(
    source_df: pd.DataFrame,
    *,
    scenario: str,
    economy_code: str = "00_APEC",
) -> pd.DataFrame:
    """Aggregate preprocessed economy-level transport inputs into a single 00_APEC input dataframe."""
    df = source_df[source_df["Scenario"].astype(str).str.lower() == scenario.lower()].copy()
    if df.empty:
        raise ValueError(f"No rows found while aggregating economies for scenario '{scenario}'.")

    group_cols = ["Date", "Scenario", "Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"]
    grouped_index = (
        df.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="_rows")
        .drop(columns="_rows")
    )

    non_group_cols = [col for col in df.columns if col not in group_cols + ["Economy"]]

    sum_cols = [
        "Energy",
        "Stocks",
        "Activity",
        "Travel_km",
        "Gdp",
        "Population",
        "Stocks_old",
        "Surplus_stocks",
        "Stock_turnover",
        "New_stocks_needed",
        "Sales",
    ]
    sum_cols = [col for col in sum_cols if col in non_group_cols]

    weighted_cols = {
        "Efficiency": SOURCE_WEIGHT_PRIORITY.get("Efficiency", ["Activity", "Stocks", None]),
        "Mileage": SOURCE_WEIGHT_PRIORITY.get("Mileage", ["Stocks", "Activity", None]),
        "Intensity": SOURCE_WEIGHT_PRIORITY.get("Intensity", ["Activity", "Stocks", None]),
        "Occupancy_or_load": ["Activity", "Stocks", None],
        "New_vehicle_efficiency": ["New_stocks_needed", "Stocks", "Activity", None],
        "Turnover_rate": ["Stocks", "Activity", None],
        "Activity_per_Stock": ["Stocks", "Activity", None],
        "Average_age": ["Stocks", None],
        "Activity_efficiency_improvement": ["Activity", None],
        "Non_road_intensity_improvement": ["Activity", None],
        "Activity_growth": ["Activity", None],
        "Gdp_per_capita": ["Population", None],
        "Stocks_per_thousand_capita": ["Population", "Stocks", None],
        "Vehicle_sales_share": ["Sales", "Stocks", None],
    }
    weighted_cols = {col: weights for col, weights in weighted_cols.items() if col in non_group_cols}

    derived_cols_to_recalculate = {"Sales", "Vehicle_sales_share", "Stock Share"}
    first_cols = [
        col
        for col in non_group_cols
        if col not in set(sum_cols) and col not in set(weighted_cols) and col not in derived_cols_to_recalculate
    ]

    aggregated = grouped_index.copy()
    if sum_cols:
        aggregated = aggregated.merge(
            df.groupby(group_cols, dropna=False)[sum_cols].sum(min_count=1).reset_index(),
            on=group_cols,
            how="left",
        )

    for col, weights in weighted_cols.items():
        weighted_df = _aggregate_weighted_column(df, group_cols, col, weights)
        aggregated = aggregated.merge(weighted_df, on=group_cols, how="left")

    if first_cols:
        first_df = (
            df.groupby(group_cols, dropna=False)[first_cols]
            .agg(_first_non_null)
            .reset_index()
        )
        aggregated = aggregated.merge(first_df, on=group_cols, how="left")

    aggregated["Economy"] = economy_code

    if "Stocks" in aggregated.columns:
        aggregated = calculate_sales(aggregated)
    if "Scenario" in aggregated.columns and "Date" in aggregated.columns:
        aggregated = normalize_and_calculate_shares(aggregated)

    duplicates = aggregated.duplicated(subset=["Date", "Economy", "Scenario", "Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"])
    if duplicates.any():
        raise ValueError("Duplicates detected after 00_APEC aggregation.")

    ordered_cols = [col for col in source_df.columns if col in aggregated.columns]
    extra_cols = [col for col in aggregated.columns if col not in ordered_cols]
    return aggregated[ordered_cols + extra_cols].reset_index(drop=True)


def prepare_apec_input_data(
    *,
    scenario: str,
    base_year: int,
    final_year: int,
    load_checkpoint: bool,
) -> pd.DataFrame:
    """Build (or load) a synthetic 00_APEC input dataframe by aggregating all configured economies."""
    checkpoint_filename = resolve_str(
        f"intermediate_data/transport_data_00_APEC_{scenario}_{base_year}_{final_year}.pkl"
    )
    if load_checkpoint and checkpoint_filename and os.path.exists(checkpoint_filename):
        print(f"Loading APEC data from checkpoint: {checkpoint_filename}")
        return pd.read_pickle(checkpoint_filename)

    economy_frames = []
    for economy_code, economy_scenario in list_transport_run_configs(scenario):
        _, _, cfg = load_transport_run_config(economy_code, economy_scenario)
        economy_base_year = min(base_year, cfg.transport_base_year)
        economy_final_year = max(final_year, cfg.transport_base_year)
        df_i = prepare_input_data(
            transport_model_excel_path=cfg.transport_model_path,
            economy=economy_code,
            scenario=economy_scenario,
            base_year=economy_base_year,
            final_year=economy_final_year,
            TRANSPORT_ESTO_BALANCES_PATH=cfg.transport_esto_balances_path,
            LOAD_CHECKPOINT=load_checkpoint,
            TRANSPORT_FUELS_DATA_FILE_PATH=cfg.transport_fuels_path,
        )
        economy_frames.append(df_i)

    combined = pd.concat(economy_frames, ignore_index=True)
    combined = combined[(combined["Date"] >= base_year) & (combined["Date"] <= final_year)].copy()
    apec_df = aggregate_economies_to_apec(combined, scenario=scenario, economy_code="00_APEC")

    if checkpoint_filename:
        os.makedirs(Path(checkpoint_filename).parent, exist_ok=True)
        apec_df.to_pickle(checkpoint_filename)
        print(f"Saved APEC checkpoint: {checkpoint_filename}")
    return apec_df


_ALLOWED_SALES_POLICY_SETTING_KEYS = {
    "turnover_policies",
    "drive_turnover_policy",
    "drive_policy_dataframe",
    "drive_policy_checkpoint_path",
    "drive_policy_stocks_col",
    "drive_policy_vehicle_type_map",
    "analysis_initial_fleet_age_shift_years",
}


def _normalise_sales_policy_settings(
    policy_settings: Mapping[str, Any] | None,
    *,
    context: str,
) -> dict[str, Any]:
    """Validate and compact optional policy settings for sales wrappers."""
    if policy_settings is None:
        return {}
    if not isinstance(policy_settings, Mapping):
        raise TypeError(
            f"{context} must be a mapping of policy kwargs, got {type(policy_settings).__name__}."
        )

    settings = dict(policy_settings)
    unknown_keys = sorted(set(settings.keys()) - _ALLOWED_SALES_POLICY_SETTING_KEYS)
    if unknown_keys:
        raise KeyError(
            f"{context} includes unsupported keys: {unknown_keys}. "
            f"Allowed keys: {sorted(_ALLOWED_SALES_POLICY_SETTING_KEYS)}"
        )

    compact_settings = {
        str(key): value
        for key, value in settings.items()
        if value is not None
    }
    if compact_settings:
        print(
            f"[INFO] {context} enabled with keys: "
            f"{', '.join(sorted(compact_settings.keys()))}"
        )
    return compact_settings


def _build_unique_archive_path(path: str) -> str:
    """Return a non-existing path by suffixing an increment when needed."""
    if not os.path.exists(path):
        return path

    stem, ext = os.path.splitext(path)
    counter = 1
    while True:
        candidate = f"{stem}_{counter:02d}{ext}"
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def _archive_existing_output_file(
    output_path: str | None,
    *,
    date_id: str | None = None,
) -> str | None:
    """
    Move an existing output file into a sibling `archive/` folder.

    Returns the archived file path when a move occurred, otherwise None.
    """
    if not output_path:
        return None

    resolved_output = resolve_str(output_path)
    if not resolved_output or not os.path.exists(resolved_output):
        return None

    archive_dir = os.path.join(os.path.dirname(resolved_output), "archive")
    os.makedirs(archive_dir, exist_ok=True)

    base_name = os.path.basename(resolved_output)
    stem, ext = os.path.splitext(base_name)
    stamp = (
        str(date_id).strip()
        if date_id is not None and str(date_id).strip()
        else datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    )
    archive_path = os.path.join(archive_dir, f"{stem}_{stamp}{ext}")
    archive_path = _build_unique_archive_path(archive_path)

    shutil.move(resolved_output, archive_path)
    return archive_path


def run_passenger_sales_workflow(
    df: pd.DataFrame,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    survival_path: str = "data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_path: str = "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    esto_energy_path: str = "data/merged_file_energy_ALL_20250814_pretrump.csv",
    output_path: str | None = None,
    plot: bool = False,
    policy_settings: Mapping[str, Any] | None = None,
    archive_existing_output: bool = True,
    **kwargs,
) -> dict:
    """
    Run passenger sales estimation using survival/vintage lifecycle profiles.

    Returns the result dict from estimate_passenger_sales_from_dataframe and
    writes sales_table to CSV if output_path is provided.
    """
    
    survival_path = resolve_str(survival_path)
    vintage_path = resolve_str(vintage_path)
    esto_energy_path = resolve_str(esto_energy_path)
    if output_path:
        output_path = resolve_str(output_path)

    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path=survival_path,
        vintage_path=vintage_path,
        vehicle_keys=("LPV", "MC", "Bus"),
    )
    estimate_kwargs = dict(kwargs)
    estimate_kwargs.update(
        _normalise_sales_policy_settings(
            policy_settings,
            context=f"Passenger sales policy ({economy} | {scenario})",
        )
    )
    result = estimate_passenger_sales_from_dataframe(
        df=df,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        plot=plot,
        esto_energy_path=esto_energy_path,
        **estimate_kwargs,
    )

    sales_table = result.get("sales_table")
    if output_path and sales_table is not None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if archive_existing_output:
            archived_output = _archive_existing_output_file(output_path, date_id=DATE_ID)
            if archived_output:
                print(f"[INFO] Archived previous passenger sales table to {archived_output}")
        sales_table.to_csv(output_path, index=False)
        print(f"[INFO] Saved passenger sales table to {output_path}")

    return result


def run_freight_sales_workflow(
    df: pd.DataFrame,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    survival_path: str = "data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_path: str = "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    esto_energy_path: str = "data/merged_file_energy_ALL_20250814_pretrump.csv",
    output_path: str | None = None,
    plot: bool = False,
    policy_settings: Mapping[str, Any] | None = None,
    archive_existing_output: bool = True,
    **kwargs,
) -> dict:
    """
    Run freight sales estimation using survival/vintage lifecycle profiles.

    Returns the result dict from estimate_freight_sales_from_dataframe and
    writes sales_table to CSV if output_path is provided.
    """
    survival_path = resolve_str(survival_path)
    vintage_path = resolve_str(vintage_path)
    esto_energy_path = resolve_str(esto_energy_path)
    if output_path:
        output_path = resolve_str(output_path)

    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path=survival_path,
        vintage_path=vintage_path,
        vehicle_keys=("Trucks", "LCVs"),
    )
    estimate_kwargs = dict(kwargs)
    estimate_kwargs.update(
        _normalise_sales_policy_settings(
            policy_settings,
            context=f"Freight sales policy ({economy} | {scenario})",
        )
    )
    result = estimate_freight_sales_from_dataframe(
        df=df,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        plot=plot,
        esto_energy_path=esto_energy_path,
        **estimate_kwargs,
    )

    sales_table = result.get("sales_table")
    if output_path and sales_table is not None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if archive_existing_output:
            archived_output = _archive_existing_output_file(output_path, date_id=DATE_ID)
            if archived_output:
                print(f"[INFO] Archived previous freight sales table to {archived_output}")
        sales_table.to_csv(output_path, index=False)
        print(f"[INFO] Saved freight sales table to {output_path}")

    return result


def _extract_template_alignment_keys(df: pd.DataFrame) -> pd.DataFrame:
    # Region is allowed to differ between the exported workbook and the template.
    # Keep the alignment gate focused on the structural keys that must match.
    key_cols = ["Branch Path", "Variable", "Scenario"]
    missing = [col for col in key_cols if col not in df.columns]
    if missing:
        return pd.DataFrame(columns=key_cols)
    keys = df[key_cols].copy().fillna("<NA>")
    return keys.drop_duplicates()


def _report_template_alignment_changes(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    *,
    dataset_label: str,
    report_tag: str | None = None,
    max_examples: int = 10,
) -> None:
    """Log and save rows removed during template alignment."""
    key_cols = ["Branch Path", "Variable", "Scenario"]
    before_keys = _extract_template_alignment_keys(before_df)
    after_keys = _extract_template_alignment_keys(after_df)

    if before_keys.empty:
        print(f"[INFO] Template alignment ({dataset_label}): no comparable rows before merge.")
        return

    merged = before_keys.merge(after_keys, on=key_cols, how="left", indicator=True)
    dropped = merged.loc[merged["_merge"] == "left_only", key_cols].copy()
    kept = len(before_keys) - len(dropped)
    kept_pct = (kept / len(before_keys) * 100.0) if len(before_keys) else 100.0
    print(
        f"[INFO] Template alignment ({dataset_label}): kept {kept}/{len(before_keys)} "
        f"unique rows ({kept_pct:.1f}%), dropped {len(dropped)} rows not found in template."
    )

    if dropped.empty:
        return

    top_branches = dropped["Branch Path"].value_counts().head(10)
    print(f"[INFO] Top dropped branches ({dataset_label}):")
    print(top_branches.to_string())
    print(f"[INFO] Example dropped rows ({dataset_label}):")
    print(dropped.head(max_examples).to_string(index=False))

    safe_label = dataset_label.lower().replace(" ", "_").replace("/", "_")
    if report_tag:
        report_name = f"template_alignment_dropped_{report_tag}_{safe_label}.csv"
    else:
        report_name = f"template_alignment_dropped_{safe_label}.csv"
    report_path = resolve_str(f"results/checkpoint_audit/{report_name}")
    os.makedirs(Path(report_path).parent, exist_ok=True)
    dropped.sort_values(key_cols).to_csv(report_path, index=False)
    print(f"[INFO] Wrote template-drop report ({dataset_label}) to: {report_path}")


def _enforce_exact_template_alignment_keys(
    *,
    import_filename: str,
    leap_export_df: pd.DataFrame,
    export_df_for_viewing: pd.DataFrame,
    scenario: str,
    region: str,
    current_accounts_label: str = "Current Accounts",
    max_examples: int = 15,
) -> None:
    """Fail fast if export rows are not exact key matches to template rows."""
    key_cols = ["Branch Path", "Variable", "Scenario"]
    try:
        template_df = pd.read_excel(import_filename, sheet_name="Export", header=2, usecols=key_cols)
    except Exception as exc:
        raise ValueError(
            "Could not load import template for strict alignment checks "
            f"('{import_filename}'): {exc}"
        ) from exc

    def _missing_rows(dataset: pd.DataFrame, label: str) -> list[str]:
        lines: list[str] = []
        for scenario_name in [current_accounts_label, scenario]:
            template_slice = template_df[
                template_df["Scenario"] == scenario_name
            ][key_cols].drop_duplicates()
            export_slice = dataset[
                dataset["Scenario"] == scenario_name
            ][key_cols].drop_duplicates()
            if export_slice.empty:
                continue

            comparison = export_slice.merge(
                template_slice,
                on=key_cols,
                how="left",
                indicator=True,
            )
            missing = comparison.loc[comparison["_merge"] == "left_only", key_cols]
            if missing.empty:
                continue

            lines.append(
                f"{label} | scenario={scenario_name}, region={region}, "
                f"rows_not_in_template={len(missing)}"
            )
            for row in missing.head(max_examples).itertuples(index=False):
                lines.append(
                    f"  - Branch Path='{row[0]}', Variable='{row[1]}', "
                    f"Scenario='{row[2]}'"
                )
        return lines

    issues: list[str] = []
    issues.extend(_missing_rows(leap_export_df, "LEAP sheet"))
    issues.extend(_missing_rows(export_df_for_viewing, "FOR_VIEWING sheet"))

    if issues:
        raise ValueError(
            "Strict template alignment failed: export rows must exactly match template keys "
            "(case-sensitive; no auto-correction is applied).\n"
            + "\n".join(issues)
        )


def _register_transport_regions_for_leap_excel_helpers() -> None:
    """Add transport economy regions to leap_excel_io's structure-check region lookup."""
    helper_globals = getattr(
        join_and_check_import_structure_matches_export_structure,
        "__globals__",
        {},
    )
    region_lookup = helper_globals.get("region_id_name_dict")
    if not isinstance(region_lookup, dict):
        return

    existing_names = {
        str(payload.get("region_name", "")).strip()
        for payload in region_lookup.values()
        if isinstance(payload, dict)
    }
    existing_ids = [
        int(payload.get("region_id"))
        for payload in region_lookup.values()
        if isinstance(payload, dict) and str(payload.get("region_id", "")).isdigit()
    ]
    next_region_id = max(existing_ids, default=0) + 1

    added: list[str] = []
    for economy_code, metadata in sorted(ECONOMY_METADATA.items()):
        region_name = str(metadata.get("region", "")).strip()
        if not region_name or region_name in existing_names:
            continue
        region_lookup[economy_code] = {
            "region_id": next_region_id,
            "region_name": region_name,
            "region_code": economy_code,
        }
        existing_names.add(region_name)
        added.append(region_name)
        next_region_id += 1

    if added:
        print(
            "[INFO] Registered transport regions for LEAP Excel structure checks: "
            + ", ".join(added)
        )


def _patch_leap_excel_region_handling() -> None:
    """Allow structure checks to reuse template rows from any known region."""
    helper_globals = getattr(
        join_and_check_import_structure_matches_export_structure,
        "__globals__",
        {},
    )
    if helper_globals.get("_transport_region_patch_active"):
        return

    region_lookup = helper_globals.get("region_id_name_dict")
    scenario_lookup = helper_globals.get("scenario_dict")
    if not isinstance(region_lookup, dict) or not isinstance(scenario_lookup, dict):
        return

    def _transport_check_scenario_and_region_ids(import_df, scenario, region):
        dict_regions = [
            str(payload.get("region_name", "")).strip()
            for payload in region_lookup.values()
            if isinstance(payload, dict)
        ]
        if region not in dict_regions:
            raise ValueError(
                f"[ERROR] The region {region} specified for structure checking is not found "
                f"in the region_id_name_dict: {dict_regions}. Make sure to load the correct "
                "region data for structure checking."
            )

        import_df = import_df.copy()
        template_regions = [
            str(value).strip()
            for value in import_df.get("Region", pd.Series(dtype=object)).dropna().unique()
            if str(value).strip()
        ]
        if region in template_regions:
            import_df = import_df[import_df["Region"] == region].copy()
        elif template_regions:
            source_region = template_regions[0]
            import_df = import_df[import_df["Region"].astype(str).str.strip() == source_region].copy()
            print(
                "[INFO] Structure-check template region fallback: "
                f"using template region='{source_region}' rows for requested region='{region}'."
            )
        else:
            raise ValueError("[ERROR] No regions found in import_df during structure checks.")

        region_ids = [
            payload.get("region_id")
            for payload in region_lookup.values()
            if isinstance(payload, dict) and str(payload.get("region_name", "")).strip() == region
        ]
        if len(region_ids) != 1:
            raise ValueError(f"[ERROR] Multiple or no region ids found for region {region} in region_id_name_dict.")
        import_df["Region"] = region
        import_df["RegionID"] = region_ids[0]

        dict_scenarios = [
            scenario_lookup[key]["scenario_name"]
            for key in scenario_lookup
        ]
        if scenario not in dict_scenarios:
            raise ValueError(
                f"[ERROR] The scenario {scenario} specified for structure checking is not found "
                f"in the scenario_dict: {dict_scenarios}. Make sure to load the correct scenario "
                "data for structure checking."
            )
        import_df = import_df[
            (import_df["Scenario"] == scenario) | (import_df["Scenario"] == "Current Accounts")
        ].copy()

        import_scenarios = [
            value for value in import_df["Scenario"].dropna().unique()
            if value != "Current Accounts"
        ]
        if len(import_scenarios) != 1:
            raise ValueError(
                "[ERROR] More or less than one scenario found in import_df during structure checks: "
                f"{import_scenarios}. There should be a Current Accounts scenario and the projected scenario."
            )
        if scenario not in import_scenarios:
            scenario_ids = [
                scenario_lookup[key]["scenario_id"]
                for key in scenario_lookup
                if scenario_lookup[key]["scenario_name"] == scenario
            ]
            if len(scenario_ids) != 1:
                raise ValueError(f"[ERROR] Multiple scenario ids found for scenario {scenario} in scenario_dict.")
            import_df.loc[import_df["Scenario"] != "Current Accounts", "Scenario"] = scenario
            import_df.loc[import_df["Scenario"] != "Current Accounts", "ScenarioID"] = scenario_ids[0]

        return import_df

    helper_globals["check_scenario_and_region_ids"] = _transport_check_scenario_and_region_ids
    helper_globals["_transport_region_patch_active"] = True


def process_transport_branch_mapping(leap_tuple, src_tuple, TRANSPORT_ROOT=r"Demand"):
    """Construct LEAP branch path and identify column groupings."""
    ttype = medium = vtype = drive = fuel = None
    if len(src_tuple) == 5:
        ttype, medium, vtype, drive, fuel = src_tuple
        source_cols_for_grouping = ['Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel']
    elif len(src_tuple) == 4:
        ttype, medium, vtype, drive = src_tuple
        source_cols_for_grouping = ['Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive']
    elif len(src_tuple) == 3:
        ttype, medium, vtype = src_tuple
        source_cols_for_grouping = ['Date', 'Transport Type', 'Medium', 'Vehicle Type']
    elif len(src_tuple) == 2:
        ttype, medium = src_tuple
        source_cols_for_grouping = ['Date', 'Transport Type', 'Medium']
    else:
        ttype = src_tuple[0]
        source_cols_for_grouping = ['Date', 'Transport Type']
        # breakpoint()
        # ttype, medium = src_tuple
        # source_cols_for_grouping = ['Date', 'Transport Type', 'Medium']

    branch_path = build_transport_branch_path(leap_tuple, root=TRANSPORT_ROOT)
    
    return ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping

def write_measures_to_transport_export_df_for_current_branch(
    df_copy, leap_tuple, src_tuple, branch_path, filtered_measure_config,
    shortname, source_cols_for_grouping, leap_export_df,
    passenger_sales_result=None,
    freight_sales_result=None,
):
    """Process measures for a branch and write them into LEAP."""
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    
    processed_measures = process_measures_for_leap(
        df_copy,
        filtered_measure_config,
        shortname,
        source_cols_for_grouping,
        ttype,
        medium,
        vtype,
        drive,
        fuel,
        src_tuple,
        leap_tuple=leap_tuple,
        passenger_sales_result=passenger_sales_result,
        freight_sales_result=freight_sales_result,
    )
    
    for measure, df_m in processed_measures.items():
        #record prepared data into leap_export_df
        before_len = len(leap_export_df) if (leap_export_df is not None) else 0
        leap_export_df = write_row_to_leap_export_df(leap_export_df, leap_tuple, src_tuple, branch_path, measure, df_m)
        # attach LEAP metadata (units/scale/per) from the measure config to the newly appended rows
        try:
            if leap_export_df is not None:
                after_len = len(leap_export_df)
                if after_len > before_len:
                    # breakpoint()#thisneeds to consider the shortname variable for the current iteration since the measurs are in a sub dict within each shortname key
                    # get values from measure config (keys expected: LEAP_units, LEAP_Scale, LEAP_Per)
                    cfg = filtered_measure_config.get(measure, {}) if filtered_measure_config else {}
                    meta_values = {
                        'LEAP_units': cfg.get('LEAP_units'),#if there is a dollar sign in any of the units we need to define waht unit it is based on teh values in the src_tuple
                        'LEAP_Scale': cfg.get('LEAP_Scale'),
                        'LEAP_Per': cfg.get('LEAP_Per'),
                    }
                    meta_values = define_value_based_on_src_tuple(meta_values, src_tuple)
                    leap_export_df.loc[leap_export_df.index[before_len:after_len], 'Units'] = meta_values['LEAP_units']
                    leap_export_df.loc[leap_export_df.index[before_len:after_len], 'Scale'] = meta_values['LEAP_Scale']
                    leap_export_df.loc[leap_export_df.index[before_len:after_len], 'Per...'] = meta_values['LEAP_Per']
                else:
                    print(f"[WARN] No new rows added to leap_export_df for {measure} on {branch_path}")
        except Exception as e:
            raise RuntimeError(f"[ERROR] Failed to attach LEAP metadata for {measure} on {branch_path}: {e}")
            
    return leap_export_df

def convert_values_to_expressions(leap_export_df):
    
    print("\n=== Building LEAP expressions from export rows ===")
    total_written = 0
    total_missing_variables = 0
    new_leap_export_df = leap_export_df.copy()
    new_leap_export_df['Expression'] = None
    #drop the year columns since we dont need them anymore
    year_cols = [col for col in new_leap_export_df.columns if str(col).isdigit() and len(str(col)) == 4]
    new_leap_export_df = new_leap_export_df.drop(columns=year_cols)
    
    for idx, row in leap_export_df.iterrows():
        branch_path = row['Branch Path']
        measure = row['Variable']
        # Create a mini df from this row by putting 4-digit dates into one column:
        mini_df = row.to_frame().T
        mini_df = mini_df.melt(
            id_vars=[col for col in mini_df.columns if col not in year_cols],
            value_vars=year_cols,
            var_name='Date',
            value_name='Value'
        )
        
        expr, method = build_expression_from_mapping(
            extract_transport_branch_tuple(branch_path),
            mini_df,
            measure,
            mapping=LEAP_BRANCH_TO_EXPRESSION_MAPPING,
            all_years=ALL_YEARS,
        )
        
        if not expr:
            raise ValueError(f"[ERROR] Failed to build expression for {measure} on {branch_path}")
        
        new_leap_export_df.at[idx, 'Expression'] = expr
        leap_export_df.at[idx, 'Method'] = method#when we convert to expr the method is no longer relevant. but it should be recorded in this spreadsheet for reference as well as just in case we want to use the spreadsheet to set variables in leap without expressions later on.
        
    return new_leap_export_df, leap_export_df 


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

    LEAP can sometimes ignore direct string assignment silently. We try a few
    API paths and require a verified match before writing any variables.
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
    
def write_export_df_to_leap(
    L, leap_export_df
    ):
    print("\n=== Setting variables in LEAP via COM interface. Make sure not to use the LEAP window while this is running or it might cause problems! ===")
    total_written = 0
    scenario_series = leap_export_df['Scenario'].astype(str).str.strip()
    current_accounts_label = "Current Accounts"
    key_cols = ["Branch Path", "Variable"]
    ca_keys = set(
        leap_export_df.loc[
            scenario_series.str.lower() == current_accounts_label.lower(),
            key_cols,
        ]
        .dropna()
        .itertuples(index=False, name=None)
    )
    
    for scenario in leap_export_df['Scenario'].dropna().astype(str).unique():
        
        try:
            active_name = _activate_leap_scenario(L, scenario)
        except Exception as e:
            raise RuntimeError(f"[ERROR] Failed to set active scenario to '{scenario}' in LEAP: {e}")
        rows = leap_export_df[leap_export_df['Scenario'].astype(str) == scenario]
        scenario_keys = set(rows[key_cols].dropna().itertuples(index=False, name=None))

        # Explicitly clear branch/variable pairs that exist in Current Accounts
        # but are omitted in this scenario export. Without this, stale scenario
        # expressions from prior runs can persist and inflate totals.
        scenario_cleared = 0
        if scenario.strip().lower() != current_accounts_label.lower() and ca_keys:
            missing_keys = sorted(ca_keys - scenario_keys)
            for branch_path, measure in missing_keys:
                branch = ensure_branch_exists(
                    L,
                    branch_path,
                    tuple(part for part in str(branch_path).split("\\")[1:] if part),
                    AUTO_SET_MISSING_BRANCHES=False,
                    shortname_to_leap_branches=SHORTNAME_TO_LEAP_BRANCHES,
                )
                if branch is None:
                    continue
                success = safe_set_variable(
                    L,
                    branch,
                    measure,
                    "",
                    unit_name=None,
                    context=f"{branch_path} [clear for scenario {scenario}]",
                )
                if success:
                    scenario_cleared += 1
            
        scenario_written = 0
        scenario_missing_variables = 0
        for idx, row in rows.iterrows():
            branch_path = row['Branch Path']
            measure = row['Variable']
            expr = row['Expression']
            unit = row['Units']

            branch = ensure_branch_exists(
                L,
                branch_path,
                tuple(part for part in str(branch_path).split("\\")[1:] if part),
                AUTO_SET_MISSING_BRANCHES=False,
                shortname_to_leap_branches=SHORTNAME_TO_LEAP_BRANCHES,
            )
            if branch is None:
                scenario_missing_variables += 1
                continue

            success = safe_set_variable(L, branch, measure, expr, unit_name=unit, context=branch_path)
            if success:
                total_written += 1
                scenario_written += 1
            else:
                scenario_missing_variables += 1
        print(
            f"\n=== Finished setting variables in LEAP for scenario '{scenario}' "
            f"(active: '{active_name}'). "
            f"Written: {scenario_written}, Missing variables: {scenario_missing_variables}, "
            f"Cleared stale expressions: {scenario_cleared}, "
            f"Running total written: {total_written} ===\n"
        )

def process_single_leap_transport_mapping(
    *,
    L,
    df,
    leap_tuple,
    src_tuple,
    diagnose_method,
    first_branch_diagnosed,
    first_of_each_length_diagnosed,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
    leap_export_df,
    TRANSPORT_ROOT,
    CHECK_BRANCHES_IN_LEAP_USING_COM,
    AUTO_SET_MISSING_BRANCHES,
    passenger_sales_result=None,
    freight_sales_result=None,
):
    """Process one (leap_tuple, src_tuple) mapping and return updated state.
    Returns updated leap_export_df.
    """
    df_copy = df.copy()
    ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping = process_transport_branch_mapping(
        leap_tuple, src_tuple, TRANSPORT_ROOT=TRANSPORT_ROOT
    )

    expected_shortname = {k for k, v in SHORTNAME_TO_LEAP_BRANCHES.items() if leap_tuple in v}
    if len(expected_shortname) != 1:
        raise ValueError(f"[ERROR] Expected exactly one shortname for LEAP branch {leap_tuple}, found: {expected_shortname}")

    shortname = expected_shortname.pop()
    filtered_measure_config = LEAP_MEASURE_CONFIG[shortname]
    expected_measures = set(filtered_measure_config.keys())
    leap_path_tuple_for_com = tuple(part for part in branch_path.split("\\")[1:] if part)
    #if we are setting vars in leap using com we want to diagnose the branches first 
    if CHECK_BRANCHES_IN_LEAP_USING_COM:#not sure if necessary
        # if diagnose_method == 'first_branch_diagnosed' and not first_branch_diagnosed:
        #     diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures, AUTO_SET_MISSING_BRANCHES=True)
        #     first_branch_diagnosed = True
        # elif diagnose_method == 'first_of_each_length' and len(leap_tuple) not in first_of_each_length_diagnosed:
        #     diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures, AUTO_SET_MISSING_BRANCHES=True)
        #     first_of_each_length_diagnosed.add(len(leap_tuple))
        # elif diagnose_method == 'all':
        ensure_branch_exists(
            L,
            branch_path,
            leap_path_tuple_for_com,
            AUTO_SET_MISSING_BRANCHES=True,
            shortname_to_leap_branches=SHORTNAME_TO_LEAP_BRANCHES,
        )
        diagnose_measures_in_leap_branch(L, branch_path, leap_path_tuple_for_com, expected_measures)

    leap_export_df = write_measures_to_transport_export_df_for_current_branch(
        df_copy,
        leap_tuple,
        src_tuple,
        branch_path,
        filtered_measure_config,
        shortname,
        source_cols_for_grouping,
        leap_export_df,
        passenger_sales_result=passenger_sales_result,
        freight_sales_result=freight_sales_result,
    )
    return leap_export_df

#------------------------------------------------------------
# Transport Reconciliation
#------------------------------------------------------------

def run_transport_reconciliation(
    apply_adjustments_to_future_years,
    report_adjustment_changes,
    date_id,
    transport_esto_balances_path,
    transport_export_path,
    reconciliation_input_path,
    economy,
    base_year,
    final_year,
    scenario,
    model_name,
    unmappable_branches,
    analysis_type_lookup,
    all_leap_branches,
    esto_to_leap_mapping,
    set_vars_in_leap_using_com,
    subtotal_column='subtotal_layout',
    scale_factor_tolerance: float = 1e-4,
    raise_on_non_convergence: bool = False,
):
    if set_vars_in_leap_using_com:
        _raise_leap_api_disabled(
            "reconciliation COM write (set_vars_in_leap_using_com=True)"
        )

    def _esto_key_to_str(esto_key: object) -> str:
        if isinstance(esto_key, tuple):
            return " | ".join(str(part) for part in esto_key)
        if isinstance(esto_key, list):
            return " | ".join(str(part) for part in esto_key)
        return str(esto_key)

    def _normalize_transport_branch_rules(branch_rules: dict) -> dict:
        normalized: dict = {}
        for esto_key, rules in branch_rules.items():
            normalized_rules: list[dict] = []
            for rule in rules:
                updated_rule = dict(rule)
                branch_tuple = tuple(updated_rule.get("branch_tuple", ()))
                root = str(updated_rule.get("root", "Demand")).strip() or "Demand"
                if is_non_road_branch_tuple(branch_tuple):
                    root = r"Demand\Transport non road"
                updated_rule["root"] = root
                updated_rule["branch_path"] = build_transport_branch_path(branch_tuple, root=root)
                normalized_rules.append(updated_rule)
            normalized[esto_key] = normalized_rules
        return normalized

    def _safe_total_energy_for_key(
        df: pd.DataFrame,
        *,
        rules: list[dict],
        base_year: int | str,
    ) -> float:
        total_energy = 0.0
        for rule in rules:
            energy_value = transport_energy_fn(
                export_df=df,
                base_year=base_year,
                rule=rule,
                strategies={},
                combination_fn=None,
            )
            total_energy += float(energy_value)
        return float(total_energy)

    def _safe_energy_for_rule(
        df: pd.DataFrame,
        *,
        rule: dict,
        base_year: int | str,
    ) -> float:
        energy_value = transport_energy_fn(
            export_df=df,
            base_year=base_year,
            rule=rule,
            strategies={},
            combination_fn=None,
        )
        return float(energy_value)

    def _build_reconciliation_energy_metadata(
        *,
        original_df: pd.DataFrame,
        adjusted_df: pd.DataFrame,
        branch_rules: dict,
        esto_energy_totals: dict,
        base_year: int | str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str], dict[str, str]]:
        energy_rows: list[dict[str, object]] = []
        branch_energy_rows: list[dict[str, object]] = []
        path_to_key_candidates: dict[str, set[str]] = {}

        for esto_key, rules in branch_rules.items():
            key_string = _esto_key_to_str(esto_key)
            original_energy = _safe_total_energy_for_key(
                original_df,
                rules=rules,
                base_year=base_year,
            )
            adjusted_energy = _safe_total_energy_for_key(
                adjusted_df,
                rules=rules,
                base_year=base_year,
            )
            abs_change = adjusted_energy - original_energy
            pct_change = pd.NA
            if abs(original_energy) > 1e-12:
                pct_change = abs_change / original_energy

            esto_total = esto_energy_totals.get(esto_key, pd.NA)
            if pd.notna(esto_total):
                esto_total = float(esto_total)

            energy_rows.append(
                {
                    "ESTO Key": key_string,
                    "LEAP Energy Original": original_energy,
                    "LEAP Energy Adjusted": adjusted_energy,
                    "LEAP Energy Abs Change": abs_change,
                    "LEAP Energy Pct Change": pct_change,
                    "ESTO Energy Use": esto_total,
                }
            )

            for rule in rules:
                branch_path = build_transport_branch_path(rule["branch_tuple"], root=rule.get("root", "Demand"))
                path_to_key_candidates.setdefault(branch_path, set()).add(key_string)
                branch_original_energy = _safe_energy_for_rule(
                    original_df,
                    rule=rule,
                    base_year=base_year,
                )
                branch_adjusted_energy = _safe_energy_for_rule(
                    adjusted_df,
                    rule=rule,
                    base_year=base_year,
                )
                branch_abs_change = branch_adjusted_energy - branch_original_energy
                branch_pct_change = pd.NA
                if abs(branch_original_energy) > 1e-12:
                    branch_pct_change = branch_abs_change / branch_original_energy
                branch_share_original = pd.NA
                if abs(original_energy) > 1e-12:
                    branch_share_original = branch_original_energy / original_energy
                branch_share_adjusted = pd.NA
                if abs(adjusted_energy) > 1e-12:
                    branch_share_adjusted = branch_adjusted_energy / adjusted_energy

                branch_energy_rows.append(
                    {
                        "Branch Path": branch_path,
                        "ESTO Key": key_string,
                        "Branch LEAP Energy Original": branch_original_energy,
                        "Branch LEAP Energy Adjusted": branch_adjusted_energy,
                        "Branch LEAP Energy Abs Change": branch_abs_change,
                        "Branch LEAP Energy Pct Change": branch_pct_change,
                        "Branch Share of ESTO Key Original": branch_share_original,
                        "Branch Share of ESTO Key Adjusted": branch_share_adjusted,
                    }
                )

        energy_change_df = pd.DataFrame(energy_rows)
        path_to_key = {
            path: sorted(keys)[0]
            for path, keys in path_to_key_candidates.items()
            if len(keys) == 1
        }
        path_to_candidate_str = {
            path: "; ".join(sorted(keys))
            for path, keys in path_to_key_candidates.items()
        }
        branch_energy_change_df = pd.DataFrame(branch_energy_rows)
        if not branch_energy_change_df.empty and path_to_key:
            branch_energy_change_df = branch_energy_change_df[
                branch_energy_change_df["Branch Path"].isin(path_to_key)
            ].drop_duplicates(subset=["Branch Path"], keep="first")
        return energy_change_df, branch_energy_change_df, path_to_key, path_to_candidate_str

    def _annotate_change_table_with_energy_change(
        change_df: pd.DataFrame | None,
        *,
        energy_change_df: pd.DataFrame,
        branch_energy_change_df: pd.DataFrame,
        path_to_key: dict[str, str],
        path_to_candidate_str: dict[str, str],
    ) -> pd.DataFrame | None:
        if change_df is None:
            return None
        if change_df.empty:
            return change_df

        annotated = change_df.copy()
        if "Branch Path" not in annotated.columns:
            return annotated

        annotated["ESTO Key"] = annotated["Branch Path"].map(path_to_key)
        annotated["ESTO Key Candidates"] = annotated["Branch Path"].map(path_to_candidate_str)

        if not energy_change_df.empty:
            energy_cols = [
                "ESTO Key",
                "LEAP Energy Original",
                "LEAP Energy Adjusted",
                "LEAP Energy Abs Change",
                "LEAP Energy Pct Change",
                "ESTO Energy Use",
            ]
            energy_lookup = energy_change_df[energy_cols].drop_duplicates(subset=["ESTO Key"])
            annotated = annotated.merge(energy_lookup, on="ESTO Key", how="left")
        if not branch_energy_change_df.empty:
            branch_cols = [
                "Branch Path",
                "Branch LEAP Energy Original",
                "Branch LEAP Energy Adjusted",
                "Branch LEAP Energy Abs Change",
                "Branch LEAP Energy Pct Change",
                "Branch Share of ESTO Key Original",
                "Branch Share of ESTO Key Adjusted",
            ]
            branch_lookup = branch_energy_change_df[branch_cols].drop_duplicates(subset=["Branch Path"])
            annotated = annotated.merge(branch_lookup, on="Branch Path", how="left")
        return annotated

    def _build_before_after_year_long(
        original_df: pd.DataFrame,
        adjusted_df: pd.DataFrame,
    ) -> pd.DataFrame:
        year_cols = sorted(
            [
                col
                for col in original_df.columns
                if str(col).isdigit() and len(str(col)) == 4 and col in adjusted_df.columns
            ],
            key=lambda x: int(str(x)),
        )
        if not year_cols:
            return pd.DataFrame()

        key_cols = [col for col in original_df.columns if col not in year_cols]

        original_keyed = original_df.copy()
        adjusted_keyed = adjusted_df.copy()
        original_keyed["_row_ordinal"] = original_keyed.groupby(key_cols, dropna=False).cumcount()
        adjusted_keyed["_row_ordinal"] = adjusted_keyed.groupby(key_cols, dropna=False).cumcount()
        id_cols = [*key_cols, "_row_ordinal"]

        before_long = original_keyed.melt(
            id_vars=id_cols,
            value_vars=year_cols,
            var_name="Year",
            value_name="Before Value",
        )
        after_long = adjusted_keyed.melt(
            id_vars=id_cols,
            value_vars=year_cols,
            var_name="Year",
            value_name="After Value",
        )
        merged = before_long.merge(after_long, on=[*id_cols, "Year"], how="outer")
        merged["Year"] = pd.to_numeric(merged["Year"], errors="coerce").astype("Int64")
        merged["Before Value Numeric"] = pd.to_numeric(merged["Before Value"], errors="coerce")
        merged["After Value Numeric"] = pd.to_numeric(merged["After Value"], errors="coerce")
        merged["Delta (After-Before)"] = merged["After Value Numeric"] - merged["Before Value Numeric"]
        denom = merged["Before Value Numeric"].replace(0, pd.NA)
        merged["Pct Delta (After-Before)"] = merged["Delta (After-Before)"] / denom
        return merged.drop(columns=["_row_ordinal"], errors="ignore")

    def _assert_base_year_consistency_across_scenarios(
        export_df: pd.DataFrame,
        *,
        base_year: int | str,
        current_accounts_label: str = "Current Accounts",
        ignore_variables: list[str] | None = None,
        tol: float = 1e-6,
    ) -> None:
        """Ensure base-year values match across all scenarios for shared rows."""
        if base_year not in export_df.columns:
            raise ValueError(f"Base year column '{base_year}' not found in export data.")

        df = export_df.copy()
        if ignore_variables:
            df = df[~df["Variable"].isin(ignore_variables)]

        scenario_col = "Scenario"
        key_cols = [col for col in ["Branch Path", "Variable", "Region"] if col in df.columns]
        if not key_cols:
            raise ValueError("Expected Branch Path/Variable/Region columns not found in export data.")

        dup_mask = df.duplicated(subset=key_cols + [scenario_col], keep=False)
        if dup_mask.any():
            sample = df.loc[dup_mask, key_cols + [scenario_col]].head(5).to_dict("records")
            raise ValueError(
                "Duplicate base-year rows found for the same scenario/branch/variable. "
                f"Sample: {sample}"
            )

        scenarios = [s for s in df[scenario_col].dropna().unique()]
        if len(scenarios) <= 1:
            return

        non_ca = [s for s in scenarios if s != current_accounts_label]
        ref_scenario = non_ca[0] if non_ca else scenarios[0]
        ref_df = df[df[scenario_col] == ref_scenario].set_index(key_cols)

        for scenario in scenarios:
            if scenario == ref_scenario:
                continue
            cur_df = df[df[scenario_col] == scenario].set_index(key_cols)
            missing = ref_df.index.difference(cur_df.index)
            if len(missing) > 0:
                sample = list(missing)[:5]
                raise ValueError(
                    f"Scenario '{scenario}' is missing {len(missing)} base-year rows found in "
                    f"'{ref_scenario}'. Sample: {sample}"
                )
            extra = cur_df.index.difference(ref_df.index)
            if len(extra) > 0 and scenario != current_accounts_label:
                sample = list(extra)[:5]
                raise ValueError(
                    f"Scenario '{scenario}' has {len(extra)} extra base-year rows not in "
                    f"'{ref_scenario}'. Sample: {sample}"
                )

            common = ref_df.index.intersection(cur_df.index)
            if common.empty:
                continue
            ref_vals = pd.to_numeric(ref_df.loc[common, base_year], errors="coerce")
            cur_vals = pd.to_numeric(cur_df.loc[common, base_year], errors="coerce")
            diff = (ref_vals - cur_vals).abs()
            bad = diff[diff > tol]
            if not bad.empty:
                sample = bad.head(5).to_dict()
                raise ValueError(
                    f"Base-year values differ between '{ref_scenario}' and '{scenario}'. "
                    f"Sample diffs: {sample}"
                )

    def _enforce_current_accounts_base_year_only(
        df: pd.DataFrame,
        *,
        base_year: int | str,
        current_accounts_label: str = "Current Accounts",
    ) -> pd.DataFrame:
        """Keep Current Accounts populated only in base year; clear all other year values."""
        if "Scenario" not in df.columns:
            return df

        ca_mask = (
            df["Scenario"].astype(str).str.strip().str.lower()
            == current_accounts_label.lower()
        )
        if not ca_mask.any():
            return df

        base_year_int = int(base_year)
        year_cols_to_clear = [
            col
            for col in df.columns
            if str(col).isdigit() and len(str(col)) == 4 and int(col) != base_year_int
        ]
        if not year_cols_to_clear:
            return df

        df.loc[ca_mask, year_cols_to_clear] = pd.NA
        return df

    def _align_scenario_base_year_to_current_accounts(
        df: pd.DataFrame,
        *,
        base_year: int | str,
        current_accounts_label: str = "Current Accounts",
    ) -> pd.DataFrame:
        """Copy Current Accounts base-year values into other scenarios for shared rows."""
        if "Scenario" not in df.columns or base_year not in df.columns:
            return df

        scenario_series = df["Scenario"].astype(str).str.strip()
        ca_mask = scenario_series.str.lower() == current_accounts_label.lower()
        if not ca_mask.any():
            return df

        scenario_names = [
            s for s in scenario_series.dropna().unique()
            if s.lower() != current_accounts_label.lower()
        ]
        if not scenario_names:
            return df

        key_cols = [col for col in ["Branch Path", "Variable", "Region"] if col in df.columns]
        if not key_cols:
            return df

        current_accounts_df = df[ca_mask]
        if current_accounts_df.empty:
            return df

        ca_source = current_accounts_df[key_cols + [base_year]].copy()
        duplicate_ca = ca_source.duplicated(subset=key_cols, keep=False)
        if duplicate_ca.any():
            grouped = (
                ca_source.groupby(key_cols, dropna=False)[base_year]
                .agg(["first", "nunique"])
                .reset_index()
            )
            inconsistent = grouped["nunique"] > 1
            if inconsistent.any():
                sample = grouped.loc[inconsistent, key_cols].head(5).to_dict("records")
                raise ValueError(
                    "Cannot align scenario base-year values from Current Accounts because duplicate keys "
                    f"have different base-year values. Sample: {sample}"
                )
            ca_source = grouped[key_cols + ["first"]].rename(columns={"first": base_year})

        ca_lookup = ca_source.set_index(key_cols)[base_year]

        for scenario_name in scenario_names:
            scenario_mask = scenario_series == scenario_name
            scenario_df = df[scenario_mask]
            if scenario_df.empty:
                continue
            scenario_keys = pd.MultiIndex.from_frame(scenario_df[key_cols])
            matched_ca = ca_lookup.reindex(scenario_keys)
            match_mask = matched_ca.notna().to_numpy()
            if not match_mask.any():
                continue
            target_index = scenario_df.index[match_mask]
            df.loc[target_index, base_year] = matched_ca[match_mask].to_numpy()
        return df

    esto_df = load_transport_energy_dataset(
        transport_esto_balances_path,
        economy=economy,
    )
    if not os.path.exists(reconciliation_input_path):
        raise FileNotFoundError(
            "Reconciliation input checkpoint was not found. Reconciliation now reads input from "
            f"intermediate_data, not from the export workbook.\nMissing file: {reconciliation_input_path}\n"
            "Run input creation first (RUN_PROFILE='input_only' or 'full')."
        )
    export_df_all = pd.read_pickle(reconciliation_input_path)
    _assert_base_year_consistency_across_scenarios(
        export_df_all,
        base_year=base_year,
        current_accounts_label="Current Accounts",
        ignore_variables=["Stock Share", "Stock"],
    )
    export_df = export_df_all[export_df_all['Scenario'] == 'Current Accounts'].copy()

    esto_energy_totals = build_transport_esto_energy_totals(
        esto_df=esto_df,
        economy=economy,
        original_scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        SUBTOTAL_COLUMN=subtotal_column,
    )
    missing_esto_keys = [key for key, value in esto_energy_totals.items() if pd.isna(value)]
    if missing_esto_keys:
        preview = "\n".join(
            f"  - {' | '.join(key)}" for key in missing_esto_keys[:10]
        )
        raise ValueError(
            "Reconciliation aborted because ESTO totals are missing for mapped keys.\n"
            f"Missing keys: {len(missing_esto_keys)}\n"
            f"First keys:\n{preview}"
        )
    branch_rules = build_branch_rules_from_mapping(
        esto_to_leap_mapping=esto_to_leap_mapping,
        unmappable_branches=unmappable_branches,
        all_leap_branches=all_leap_branches,
        analysis_type_lookup=analysis_type_lookup,
        root='Demand',
    )
    branch_rules = _normalize_transport_branch_rules(branch_rules)
    # pd.Series(esto_energy_totals).to_pickle('../data/temp/transport_esto_energy_totals.pkl')
    # pd.Series(branch_rules).to_pickle('../data/temp/transport_branch_rules.pkl')
    # else:
    # esto_energy_totals = pd.read_pickle('../data/temp/transport_esto_energy_totals.pkl').to_dict()
    # branch_rules = pd.read_pickle('../data/temp/transport_branch_rules.pkl').to_dict()

    max_reconcile_iterations = 8
    energy_abs_tolerance = 1e-3
    working_df = export_df.copy()
    summary_df = pd.DataFrame()
    summary_df_check = pd.DataFrame()
    reconciliation_converged = False
    cumulative_scale_factors: dict[tuple[str, ...], float] = {}

    for iteration in range(1, max_reconcile_iterations + 1):
        working_df, summary_df = reconcile_energy_use(
            export_df=working_df,
            base_year=base_year,
            branch_mapping_rules=branch_rules,
            esto_energy_totals=esto_energy_totals,
            energy_fn=transport_energy_fn,
            adjustment_fn=transport_adjustment_fn,
            apply_adjustments_to_future_years=apply_adjustments_to_future_years,
        )
        if {"ESTO Key", "Scale Factor"}.issubset(summary_df.columns):
            for key_text, sf_raw in zip(
                summary_df["ESTO Key"].astype(str),
                pd.to_numeric(summary_df["Scale Factor"], errors="coerce"),
            ):
                if pd.isna(sf_raw):
                    continue
                sf = float(sf_raw)
                if sf in (float("inf"), float("-inf")):
                    continue
                key_tuple = tuple(part.strip() for part in key_text.split(" | "))
                if not key_tuple:
                    continue
                cumulative_scale_factors[key_tuple] = (
                    cumulative_scale_factors.get(key_tuple, 1.0) * sf
                )

        # Run a check pass on the updated dataframe.
        _, summary_df_check = reconcile_energy_use(
            export_df=working_df,
            base_year=base_year,
            branch_mapping_rules=branch_rules,
            esto_energy_totals=esto_energy_totals,
            energy_fn=transport_energy_fn,
            adjustment_fn=transport_adjustment_fn,
            apply_adjustments_to_future_years=apply_adjustments_to_future_years,
        )

        scale_check_series = pd.to_numeric(summary_df_check["Scale Factor"], errors="coerce")
        non_finite_scale_mask = scale_check_series.isna() | scale_check_series.isin([float("inf"), float("-inf")])
        scale_check_off_tol = scale_check_series.notna() & (
            scale_check_series.sub(1.0).abs() >= scale_factor_tolerance
        )

        if "LEAP Energy Use" in summary_df_check.columns:
            leap_check = pd.to_numeric(summary_df_check["LEAP Energy Use"], errors="coerce")
        else:
            leap_check = pd.Series(pd.NA, index=summary_df_check.index, dtype="float64")
        if "ESTO Energy Use" in summary_df_check.columns:
            esto_check = pd.to_numeric(summary_df_check["ESTO Energy Use"], errors="coerce")
        else:
            esto_check = pd.Series(pd.NA, index=summary_df_check.index, dtype="float64")
        both_near_zero_mask = (
            leap_check.abs().fillna(0.0) <= energy_abs_tolerance
        ) & (
            esto_check.abs().fillna(0.0) <= energy_abs_tolerance
        )
        non_finite_scale_mask = non_finite_scale_mask & ~both_near_zero_mask
        scale_check_off_tol = scale_check_off_tol & ~both_near_zero_mask
        energy_abs_diff = (leap_check - esto_check).abs()
        esto_abs = esto_check.abs()
        energy_rel_diff = energy_abs_diff / esto_abs.replace(0, pd.NA)
        energy_mismatch_mask = (
            (esto_abs <= energy_abs_tolerance) & (energy_abs_diff > energy_abs_tolerance)
        ) | (
            (esto_abs > energy_abs_tolerance) & (energy_rel_diff > scale_factor_tolerance)
        )
        energy_mismatch_mask = energy_mismatch_mask.fillna(False)

        working_df, fallback_injection_count = _apply_zero_energy_fallbacks_from_summary(
            working_df,
            summary_df_check=summary_df_check,
            base_year=base_year,
            economy=economy,
            scenario=scenario,
            energy_abs_tolerance=energy_abs_tolerance,
        )
        if fallback_injection_count:
            if iteration < max_reconcile_iterations:
                print(
                    f"[WARN] Applied {fallback_injection_count} fallback injection(s) "
                    f"for zero-energy mismatch keys on pass {iteration}; running another pass."
                )
                continue
            print(
                f"[WARN] Applied {fallback_injection_count} fallback injection(s) on the final pass "
                f"({iteration}/{max_reconcile_iterations}); no additional pass remains."
            )

        if not non_finite_scale_mask.any() and not scale_check_off_tol.any() and not energy_mismatch_mask.any():
            reconciliation_converged = True
            break

        ignored_near_zero_count = int(both_near_zero_mask.sum())
        if ignored_near_zero_count:
            print(
                "[INFO] Ignoring scale-factor checks for "
                f"{ignored_near_zero_count} key(s) with both LEAP and ESTO near zero "
                f"(abs <= {energy_abs_tolerance:g})."
            )

        if iteration < max_reconcile_iterations:
            print(
                f"[WARN] Reconciliation not converged after pass {iteration}/{max_reconcile_iterations}; "
                "running another pass."
            )

    non_convergence_warning = ""
    if not reconciliation_converged:
        error_parts = []
        if non_finite_scale_mask.any():
            failed_non_finite = summary_df_check.loc[non_finite_scale_mask]
            key_col = "ESTO Key" if "ESTO Key" in failed_non_finite.columns else failed_non_finite.columns[0]
            preview = "\n".join(f"  - {key}" for key in failed_non_finite[key_col].astype(str).head(10))
            error_parts.append(
                "Non-finite scale factors were produced.\n"
                f"Affected keys: {int(non_finite_scale_mask.sum())}\n"
                f"First keys:\n{preview}"
            )
        if scale_check_off_tol.any():
            failed = summary_df_check.loc[scale_check_off_tol]
            error_parts.append(
                "Some adjustment multipliers remain outside tolerance "
                f"({scale_factor_tolerance:g}).\n{failed.to_string()}"
            )
        if energy_mismatch_mask.any():
            mismatch = summary_df_check.loc[energy_mismatch_mask].copy()
            if "LEAP Energy Use" in mismatch.columns and "ESTO Energy Use" in mismatch.columns:
                mismatch["Abs Diff"] = (
                    pd.to_numeric(mismatch["LEAP Energy Use"], errors="coerce")
                    - pd.to_numeric(mismatch["ESTO Energy Use"], errors="coerce")
                ).abs()
                mismatch["Rel Diff"] = mismatch["Abs Diff"] / pd.to_numeric(
                    mismatch["ESTO Energy Use"], errors="coerce"
                ).abs().replace(0, pd.NA)
            mismatch_preview = mismatch.head(20).to_string(index=False)
            error_parts.append(
                "LEAP vs ESTO energy mismatches remain above tolerance after reconciliation.\n"
                f"Relative tolerance={scale_factor_tolerance:g}, absolute tolerance={energy_abs_tolerance:g}\n"
                f"First mismatches:\n{mismatch_preview}"
            )
        non_convergence_warning = (
            "Reconciliation failed to converge after "
            f"{max_reconcile_iterations} passes.\n" + "\n\n".join(error_parts)
        )
        if raise_on_non_convergence:
            raise ValueError(non_convergence_warning)
        print(f"[WARN] {non_convergence_warning}")

    # Quick run-down of what changed
    scale_series = pd.Series(cumulative_scale_factors, dtype="float64")
    adjusted_mask = scale_series.notna() & (
        scale_series.sub(1.0).abs() > scale_factor_tolerance
    )
    adjusted_count = int(adjusted_mask.sum())
    if adjusted_count:
        adjusted_scales = scale_series.loc[adjusted_mask]
        abs_dev = (adjusted_scales - 1.0).abs()
        max_dev_pct = float(abs_dev.max() * 100)
        mean_dev_pct = float(abs_dev.mean() * 100)
        median_dev_pct = float(abs_dev.median() * 100)
        min_sf = float(adjusted_scales.min())
        max_sf = float(adjusted_scales.max())
        down = int((adjusted_scales < (1.0 - scale_factor_tolerance)).sum())
        up = int((adjusted_scales > (1.0 + scale_factor_tolerance)).sum())
        total_keys = len(scale_series)
        change_msg = (
            f"Adjusted {adjusted_count}/{total_keys} ESTO keys (tol={scale_factor_tolerance:g}).\n"
            f"Adjustment multipliers ranged from {min_sf:.4f} to {max_sf:.4f}; decreased {down}, increased {up}.\n"
            f"Mean abs adjustment {mean_dev_pct:.2f}% (median {median_dev_pct:.2f}%, max {max_dev_pct:.2f}%)."
        )
        extreme_scale_mask = (adjusted_scales < 0.1) | (adjusted_scales > 10.0)
        if extreme_scale_mask.any():
            extreme_series = adjusted_scales.loc[extreme_scale_mask].copy()
            if isinstance(extreme_series.index, pd.MultiIndex):
                # Series index comes from tuple ESTO keys; flatten to a single label
                # before formatting preview output to avoid MultiIndex axis-name errors.
                extreme_series.index = extreme_series.index.map(
                    lambda key: " | ".join(str(part) for part in key)
                )
            extreme_preview = (
                extreme_series
                .rename_axis("ESTO Key Tuple")
                .reset_index(name="Scale Factor")
                .head(10)
            )
            print(
                "[WARN] Extreme reconciliation multipliers detected "
                f"(<0.1x or >10x): {int(extreme_scale_mask.sum())} key(s)."
            )
            if not extreme_preview.empty:
                print(extreme_preview.to_string(index=False))
    else:
        change_msg = "No adjustments required; all adjustment multipliers were 1.0."

    reconciliation_energy_change_df, branch_energy_change_df, path_to_esto_key, path_to_esto_key_candidates = (
        _build_reconciliation_energy_metadata(
            original_df=export_df,
            adjusted_df=working_df,
            branch_rules=branch_rules,
            esto_energy_totals=esto_energy_totals,
            base_year=base_year,
        )
    )

    if report_adjustment_changes:
        reconciliation_dir = resolve_str("results/reconciliation")
        recon_archive_dir = os.path.join(reconciliation_dir, "archive")
        os.makedirs(reconciliation_dir, exist_ok=True)
        os.makedirs(recon_archive_dir, exist_ok=True)
        base_changes, future_changes = build_adjustment_change_tables(
            original_df=export_df,
            adjusted_df=working_df,
            base_year=base_year,
            include_future_years=apply_adjustments_to_future_years,
        )
        base_changes = _annotate_change_table_with_energy_change(
            base_changes,
            energy_change_df=reconciliation_energy_change_df,
            branch_energy_change_df=branch_energy_change_df,
            path_to_key=path_to_esto_key,
            path_to_candidate_str=path_to_esto_key_candidates,
        )
        future_changes = _annotate_change_table_with_energy_change(
            future_changes,
            energy_change_df=reconciliation_energy_change_df,
            branch_energy_change_df=branch_energy_change_df,
            path_to_key=path_to_esto_key,
            path_to_candidate_str=path_to_esto_key_candidates,
        )
        suffix = f"{economy}_{scenario}".replace(" ", "_")
        input_snapshot_path = os.path.join(reconciliation_dir, f"transport_reconciliation_input_for_viewing_{suffix}.csv")
        if os.path.exists(input_snapshot_path):
            input_archive_path = os.path.join(
                recon_archive_dir, f"transport_reconciliation_input_for_viewing_{suffix}_{date_id}.csv"
            )
            shutil.move(input_snapshot_path, input_archive_path)
        export_df_all.to_csv(input_snapshot_path, index=False)
        if non_convergence_warning:
            warning_path = os.path.join(
                reconciliation_dir,
                f"transport_reconciliation_warning_{suffix}.txt",
            )
            with open(warning_path, "w", encoding="utf-8") as handle:
                handle.write(non_convergence_warning)
        print(f"Saved pre-reconciliation input snapshot to {input_snapshot_path} ({len(export_df_all)} rows).")

        energy_change_path = os.path.join(reconciliation_dir, f"transport_reconciliation_energy_change_{suffix}.csv")
        if os.path.exists(energy_change_path):
            energy_change_archive_path = os.path.join(
                recon_archive_dir, f"transport_reconciliation_energy_change_{suffix}_{date_id}.csv"
            )
            shutil.move(energy_change_path, energy_change_archive_path)
        reconciliation_energy_change_df.to_csv(energy_change_path, index=False)
        print(
            f"Saved reconciliation energy change summary to {energy_change_path} "
            f"({len(reconciliation_energy_change_df)} ESTO keys)."
        )

        base_changes_path = os.path.join(reconciliation_dir, f"transport_adjustment_changes_base_year_{suffix}.csv")
        if os.path.exists(base_changes_path):
            base_archive_path = os.path.join(
                recon_archive_dir, f"transport_adjustment_changes_base_year_{suffix}_{date_id}.csv"
            )
            shutil.move(base_changes_path, base_archive_path)
        base_changes.to_csv(base_changes_path, index=False)
        print(f"Saved base-year adjustment details to {base_changes_path} ({len(base_changes)} rows).")

        if future_changes is not None and not future_changes.empty:
            future_changes_path = os.path.join(reconciliation_dir, f"transport_adjustment_changes_future_years_{suffix}.csv")
            if os.path.exists(future_changes_path):
                future_archive_path = os.path.join(
                    recon_archive_dir, f"transport_adjustment_changes_future_years_{suffix}_{date_id}.csv"
                )
                shutil.move(future_changes_path, future_archive_path)
            future_changes.to_csv(future_changes_path, index=False)
            print(f"Saved future-year adjustment details to {future_changes_path} ({len(future_changes)} rows).")
        elif future_changes is not None:
            print("No future-year adjustments detected.")

    # Apply the same adjustment factors to every scenario (base-year only unless apply_adjustments_to_future_years=True)
    scale_factors = dict(cumulative_scale_factors)

    adjustment_year_columns = get_adjustment_year_columns(
        export_df_all,
        base_year,
        include_future_years=apply_adjustments_to_future_years,
    )
    adjusted_export_df_all = export_df_all.copy()

    # Apply adjustments within each scenario/region slice to avoid cross-scenario coupling.
    group_cols = [col for col in ("Scenario", "Region") if col in adjusted_export_df_all.columns]
    if group_cols:
        grouped_indexes = adjusted_export_df_all.groupby(group_cols, dropna=False, sort=False).groups
        grouped_items = list(grouped_indexes.items())
    else:
        grouped_items = [(None, adjusted_export_df_all.index)]

    for _, group_index in grouped_items:
        group_df = adjusted_export_df_all.loc[group_index].copy()
        scenario_label = (
            str(group_df["Scenario"].iloc[0]).strip()
            if ("Scenario" in group_df.columns and not group_df.empty)
            else ""
        )
        years_for_group = (
            [base_year]
            if scenario_label.lower() == "current accounts"
            else adjustment_year_columns
        )

        for esto_key, rules in branch_rules.items():
            scale_factor = scale_factors.get(esto_key)
            if scale_factor is None:
                continue
            if abs(scale_factor - 1.0) <= 1e-12:
                continue
            for rule in rules:
                transport_adjustment_fn(
                    group_df,
                    base_year,
                    rule,
                    scale_factor,
                    strategies={},
                    year_columns=years_for_group,
                    apply_to_future_years=apply_adjustments_to_future_years,
                )

        adjusted_export_df_all.loc[group_index, group_df.columns] = group_df

    adjusted_export_df_all = _align_scenario_base_year_to_current_accounts(
        adjusted_export_df_all,
        base_year=base_year,
        current_accounts_label="Current Accounts",
    )
    adjusted_export_df_all = _enforce_current_accounts_base_year_only(
        adjusted_export_df_all,
        base_year=base_year,
        current_accounts_label="Current Accounts",
    )

    if report_adjustment_changes:
        reconciliation_dir = resolve_str("results/reconciliation")
        recon_archive_dir = os.path.join(reconciliation_dir, "archive")
        os.makedirs(reconciliation_dir, exist_ok=True)
        os.makedirs(recon_archive_dir, exist_ok=True)
        suffix = f"{economy}_{scenario}".replace(" ", "_")

        adjusted_snapshot_path = os.path.join(
            reconciliation_dir, f"transport_reconciliation_adjusted_for_viewing_{suffix}.csv"
        )
        if os.path.exists(adjusted_snapshot_path):
            adjusted_archive_path = os.path.join(
                recon_archive_dir, f"transport_reconciliation_adjusted_for_viewing_{suffix}_{date_id}.csv"
            )
            shutil.move(adjusted_snapshot_path, adjusted_archive_path)
        adjusted_export_df_all.to_csv(adjusted_snapshot_path, index=False)
        print(
            f"Saved post-reconciliation adjusted snapshot to {adjusted_snapshot_path} "
            f"({len(adjusted_export_df_all)} rows)."
        )

        before_after_long = _build_before_after_year_long(export_df_all, adjusted_export_df_all)
        before_after_path = os.path.join(
            reconciliation_dir, f"transport_reconciliation_before_after_long_{suffix}.csv"
        )
        if os.path.exists(before_after_path):
            before_after_archive_path = os.path.join(
                recon_archive_dir, f"transport_reconciliation_before_after_long_{suffix}_{date_id}.csv"
            )
            shutil.move(before_after_path, before_after_archive_path)
        before_after_long.to_csv(before_after_path, index=False)
        print(
            f"Saved per-year before/after reconciliation table to {before_after_path} "
            f"({len(before_after_long)} rows)."
        )

    leap_export_df, export_df_for_viewing = convert_values_to_expressions(adjusted_export_df_all)
    # Archive existing export before overwriting
    os.makedirs(os.path.dirname(transport_export_path), exist_ok=True)
    archived_export = _archive_existing_output_file(transport_export_path, date_id=date_id)
    if archived_export:
        print(f"[INFO] Archived previous transport export to {archived_export}")

    save_export_files(
        leap_export_df,
        export_df_for_viewing,
        transport_export_path,
        base_year,
        final_year,
        model_name=model_name,
    )

    print(f"\n=== Transport data reconciliation completed. {change_msg} ===\n")
# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap(
    transport_model_excel_path,
    economy,
    original_scenario,
    new_scenario,
    region,
    diagnose_method='first_of_each_length',
    base_year=2022,
    final_year=2060,
    model_name="Transport Model",
    CHECK_BRANCHES_IN_LEAP_USING_COM=True,
    SET_VARS_IN_LEAP_USING_COM=False,
    AUTO_SET_MISSING_BRANCHES=False,
    export_filename=f"{DOMESTIC_EXPORT_DIR}/leap_export.xlsx",
    import_filename="data/import_files/leap_import.xlsx",
    TRANSPORT_ESTO_BALANCES_PATH = 'data/merged_file_energy_ALL_20250814_pretrump.csv',
    TRANSPORT_FUELS_DATA_FILE_PATH = 'data/transport_data_9th/model_output_with_fuels/20_USA_NON_ROAD_DETAILED_model_output_with_fuels20250225.csv',
    TRANSPORT_ROOT = r"Demand",
    LOAD_INPUT_CHECKPOINT=False,
    LOAD_HALFWAY_CHECKPOINT=False,
    LOAD_THREEQUART_WAY_CHECKPOINT=False,
    LOAD_EXPORT_DF_CHECKPOINT=False,
    CHECKPOINT_TAG=None,
    MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE=False,
    RUN_PASSENGER_SALES=True,
    RUN_FREIGHT_SALES=True,
    SURVIVAL_PROFILE_PATH="data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    VINTAGE_PROFILE_PATH="data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    PASSENGER_SALES_OUTPUT=None,
    FREIGHT_SALES_OUTPUT=None,
    PASSENGER_SALES_POLICY_SETTINGS: Mapping[str, Any] | None = None,
    FREIGHT_SALES_POLICY_SETTINGS: Mapping[str, Any] | None = None,
    PASSENGER_PLOT=True,
    PREPARED_INPUT_DF: pd.DataFrame | None = None,
    ENSURE_FUELS_IN_LEAP=True,
    LEAP_REGION_NAME_OVERRIDE: str | None = None,
):
    """Main orchestrator for LEAP transport data loading."""
    if CHECK_BRANCHES_IN_LEAP_USING_COM or SET_VARS_IN_LEAP_USING_COM:
        _raise_leap_api_disabled(
            "transport workflow LEAP COM access "
            "(CHECK_BRANCHES_IN_LEAP_USING_COM and/or SET_VARS_IN_LEAP_USING_COM enabled)"
        )

    export_filename = resolve_str(export_filename)
    import_filename = resolve_str(import_filename)
    TRANSPORT_ESTO_BALANCES_PATH = resolve_str(TRANSPORT_ESTO_BALANCES_PATH)
    TRANSPORT_FUELS_DATA_FILE_PATH = resolve_str(TRANSPORT_FUELS_DATA_FILE_PATH)
    SURVIVAL_PROFILE_PATH = resolve_str(SURVIVAL_PROFILE_PATH)
    VINTAGE_PROFILE_PATH = resolve_str(VINTAGE_PROFILE_PATH)
    region_for_leap = LEAP_REGION_NAME_OVERRIDE or region
    if LEAP_REGION_NAME_OVERRIDE:
        print(
            f"[INFO] Region override active for LEAP/export: "
            f"source region='{region}' -> leap region='{region_for_leap}'"
        )
    checkpoint_tag = (CHECKPOINT_TAG or f"{economy}_{original_scenario}").replace(" ", "_")
    halfway_checkpoint_path = resolve_str(f"intermediate_data/export_df_checkpoint_{checkpoint_tag}.pkl")
    three_quarter_checkpoint_path = resolve_str(f"intermediate_data/export_df_checkpoint2_{checkpoint_tag}.pkl")
    viewing_checkpoint_path = resolve_str(f"intermediate_data/export_df_for_viewing_checkpoint2_{checkpoint_tag}.pkl")
    
    mappings_validation = validate_all_mappings_with_measures(
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAPPING,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
        ESTO_TRANSPORT_SECTOR_TUPLES,
        UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
        EXAMPLE_SAMPLE_SIZE=1000
    )
    
    if PREPARED_INPUT_DF is not None:
        df = PREPARED_INPUT_DF.copy()
    else:
        df = prepare_input_data(
            transport_model_excel_path,
            economy,
            original_scenario,
            base_year,
            final_year,
            TRANSPORT_ESTO_BALANCES_PATH=TRANSPORT_ESTO_BALANCES_PATH,
            LOAD_CHECKPOINT=LOAD_INPUT_CHECKPOINT,
            TRANSPORT_FUELS_DATA_FILE_PATH=TRANSPORT_FUELS_DATA_FILE_PATH,
        )
    
    passenger_sales_result = None
    freight_sales_result = None
    if RUN_PASSENGER_SALES:
        passenger_output = PASSENGER_SALES_OUTPUT or f"{PASSENGER_SALES_DIR}/passenger_sales_{economy}_{original_scenario}.csv"
        # breakpoint()
        passenger_sales_result = run_passenger_sales_workflow(
            df=df,
            economy=economy,
            scenario=original_scenario,
            base_year=base_year,
            final_year=final_year,
            survival_path=SURVIVAL_PROFILE_PATH,
            vintage_path=VINTAGE_PROFILE_PATH,
            esto_energy_path=TRANSPORT_ESTO_BALANCES_PATH,
            output_path=passenger_output,
            plot=PASSENGER_PLOT,
            policy_settings=PASSENGER_SALES_POLICY_SETTINGS,
        )
    if RUN_FREIGHT_SALES:
        freight_output = FREIGHT_SALES_OUTPUT or f"{FREIGHT_SALES_DIR}/freight_sales_{economy}_{original_scenario}.csv"
        freight_sales_result = run_freight_sales_workflow(
            df=df,
            economy=economy,
            scenario=original_scenario,
            base_year=base_year,
            final_year=final_year,
            survival_path=SURVIVAL_PROFILE_PATH,
            vintage_path=VINTAGE_PROFILE_PATH,
            esto_energy_path=TRANSPORT_ESTO_BALANCES_PATH,
            output_path=freight_output,
            plot=False,
            policy_settings=FREIGHT_SALES_POLICY_SETTINGS,
        )
    
    L = None
    if L is not None and ENSURE_FUELS_IN_LEAP:
        ensure_transport_fuels_in_leap(L)
    leap_export_df = create_transport_export_df()
    
    first_branch_diagnosed = False
    first_of_each_length_diagnosed = set()
    for leap_tuple, src_tuple in LEAP_BRANCH_TO_SOURCE_MAP.items():
        if LOAD_EXPORT_DF_CHECKPOINT or LOAD_HALFWAY_CHECKPOINT:
            break
        leap_export_df = process_single_leap_transport_mapping(
            L=L,
            df=df,
            leap_tuple=leap_tuple,
            src_tuple=src_tuple,
            diagnose_method=diagnose_method,
            first_branch_diagnosed=first_branch_diagnosed,
            first_of_each_length_diagnosed=first_of_each_length_diagnosed,
            SHORTNAME_TO_LEAP_BRANCHES=SHORTNAME_TO_LEAP_BRANCHES,
            LEAP_MEASURE_CONFIG=LEAP_MEASURE_CONFIG,
            leap_export_df=leap_export_df,
            TRANSPORT_ROOT=TRANSPORT_ROOT,
            CHECK_BRANCHES_IN_LEAP_USING_COM=CHECK_BRANCHES_IN_LEAP_USING_COM,
            AUTO_SET_MISSING_BRANCHES=AUTO_SET_MISSING_BRANCHES,
            passenger_sales_result=passenger_sales_result,
            freight_sales_result=freight_sales_result,
        )
        continue
    #save temporary export df checkpoint
    if LOAD_HALFWAY_CHECKPOINT or LOAD_EXPORT_DF_CHECKPOINT or LOAD_THREEQUART_WAY_CHECKPOINT:
        leap_export_df = pd.read_pickle(halfway_checkpoint_path)
    else:
        leap_export_df.to_pickle(halfway_checkpoint_path)
    
    rebuild_expressions_from_viewing = False
    if LOAD_THREEQUART_WAY_CHECKPOINT or LOAD_EXPORT_DF_CHECKPOINT:
        leap_export_df = pd.read_pickle(three_quarter_checkpoint_path)
        export_df_for_viewing = pd.read_pickle(viewing_checkpoint_path)
        export_df_for_viewing, shares_changed = normalize_share_columns_wide(export_df_for_viewing)
        if shares_changed:
            print("[INFO] Normalized share measures in cached export dataframe; rebuilding expressions.")
            leap_export_df, export_df_for_viewing = convert_values_to_expressions(export_df_for_viewing)
            rebuild_expressions_from_viewing = True
    else:
        #do validation and finalisation
        leap_export_df = validate_and_fix_shares_normalise_to_one(leap_export_df,EXAMPLE_SAMPLE_SIZE=5)
        
        #create current accounts scenario
        leap_export_df = separate_current_accounts_from_scenario(leap_export_df, base_year=base_year, scenario=new_scenario)
        
        leap_export_df = finalise_export_df(
            leap_export_df,
            scenario=new_scenario,
            region=region_for_leap,
            base_year=base_year,
            final_year=final_year,
        )
        leap_export_df, shares_changed = normalize_share_columns_wide(leap_export_df)
        if shares_changed:
            print("[INFO] Normalized share measures in export dataframe before expression build.")
        validate_final_energy_use_for_base_year_equals_esto_totals(economy, original_scenario,new_scenario, base_year, final_year, leap_export_df, TRANSPORT_ESTO_BALANCES_PATH, TRANSPORT_ROOT)
        print("\n=== Transport data successfully filled into LEAP. ===\n")
        
        leap_export_df, export_df_for_viewing = convert_values_to_expressions(leap_export_df)
        
        leap_export_df.to_pickle(three_quarter_checkpoint_path)
        export_df_for_viewing.to_pickle(viewing_checkpoint_path)
    
    
    if LOAD_EXPORT_DF_CHECKPOINT and not rebuild_expressions_from_viewing:
        # breakpoint()
        leap_export_df = pd.read_excel(export_filename, sheet_name='LEAP', header=2)
        print(f"Loaded leap_export_df from checkpoint: {export_filename}")
    else:
        # def create_current_accounts_scenario(export_df):
        #     export_df_current = export_df.copy()
        #     export_df_current['Scenario'] = 'Current Accounts'
        #     return export_df_current
        #todo need to change the expressions for rows in scneario current accounts os they dont include any values except the base year
        if MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE:
            _register_transport_regions_for_leap_excel_helpers()
            _patch_leap_excel_region_handling()
            pre_merge_leap_export_df = leap_export_df.copy()
            pre_merge_viewing_df = export_df_for_viewing.copy()
            _enforce_exact_template_alignment_keys(
                import_filename=import_filename,
                leap_export_df=leap_export_df,
                export_df_for_viewing=export_df_for_viewing,
                scenario=new_scenario,
                region=region_for_leap,
            )
            leap_export_df, export_df_for_viewing = join_and_check_import_structure_matches_export_structure(
                import_filename,
                leap_export_df,
                export_df_for_viewing,
                scenario=new_scenario,
                region=region_for_leap,
                STRICT_CHECKS=False,
            )
            _report_template_alignment_changes(
                pre_merge_leap_export_df,
                leap_export_df,
                dataset_label="LEAP sheet",
                report_tag=checkpoint_tag,
            )
            _report_template_alignment_changes(
                pre_merge_viewing_df,
                export_df_for_viewing,
                dataset_label="FOR_VIEWING sheet",
                report_tag=checkpoint_tag,
            )
        
        os.makedirs(os.path.dirname(export_filename), exist_ok=True)
        archived_export = _archive_existing_output_file(export_filename, date_id=DATE_ID)
        if archived_export:
            print(f"[INFO] Archived previous transport export to {archived_export}")
        save_export_files(
            leap_export_df, export_df_for_viewing, export_filename, base_year, final_year, model_name
        )
    
    print("\n=== Transport data loading process completed. ===\n")

#%%
# ------------------------------------------------------------
# Run option helpers
# ------------------------------------------------------------

class CheckpointLoadStage(str, Enum):
    """Canonical export checkpoints to resume the pipeline from."""

    NONE = "none"
    HALFWAY = "halfway"
    THREE_QUARTER = "three_quarter"
    EXPORT = "export"


class InputDataSource(str, Enum):
    """Source for the preprocessed input dataframe."""

    RAW = "raw"
    CHECKPOINT = "checkpoint"


class SalesMode(str, Enum):
    """Which sales workflows to run."""

    NONE = "none"
    PASSENGER = "passenger"
    FREIGHT = "freight"
    BOTH = "both"


class RunProfile(str, Enum):
    """Which top-level stages to execute."""

    INPUT_ONLY = "input_only"
    RECONCILE_ONLY = "reconcile_only"
    FULL = "full"


_CHECKPOINT_STAGE_ALIASES = {
    "none": CheckpointLoadStage.NONE,
    "halfway": CheckpointLoadStage.HALFWAY,
    "half": CheckpointLoadStage.HALFWAY,
    "three_quarter": CheckpointLoadStage.THREE_QUARTER,
    "threequarter": CheckpointLoadStage.THREE_QUARTER,
    "three_quart": CheckpointLoadStage.THREE_QUARTER,
    "threequart": CheckpointLoadStage.THREE_QUARTER,
    "export": CheckpointLoadStage.EXPORT,
}

_INPUT_SOURCE_ALIASES = {
    "raw": InputDataSource.RAW,
    "checkpoint": InputDataSource.CHECKPOINT,
    "ckpt": InputDataSource.CHECKPOINT,
    "pkl": InputDataSource.CHECKPOINT,
}

_SALES_MODE_ALIASES = {
    "none": SalesMode.NONE,
    "passenger": SalesMode.PASSENGER,
    "freight": SalesMode.FREIGHT,
    "both": SalesMode.BOTH,
    "all": SalesMode.BOTH,
}

_RUN_PROFILE_ALIASES = {
    "input_only": RunProfile.INPUT_ONLY,
    "input": RunProfile.INPUT_ONLY,
    "reconcile_only": RunProfile.RECONCILE_ONLY,
    "reconcile": RunProfile.RECONCILE_ONLY,
    "full": RunProfile.FULL,
    "all": RunProfile.FULL,
}

def resolve_export_checkpoint_flags(stage: str | CheckpointLoadStage) -> tuple[bool, bool, bool]:
    """Return (load_halfway, load_three_quarter, load_export)."""
    if isinstance(stage, CheckpointLoadStage):
        normalized = stage
    else:
        key = str(stage).strip().lower()
        normalized = _CHECKPOINT_STAGE_ALIASES.get(key)
    if normalized is None:
        valid = ", ".join(sorted(_CHECKPOINT_STAGE_ALIASES))
        raise ValueError(f"Invalid CHECKPOINT_LOAD_STAGE '{stage}'. Use one of: {valid}.")

    order = [
        CheckpointLoadStage.NONE,
        CheckpointLoadStage.HALFWAY,
        CheckpointLoadStage.THREE_QUARTER,
        CheckpointLoadStage.EXPORT,
    ]
    idx = order.index(normalized)
    return (
        idx >= 1,  # halfway
        idx >= 2,  # three_quarter
        idx >= 3,  # export
    )


def resolve_input_checkpoint(source: str | InputDataSource) -> bool:
    """Return whether to load preprocessed input from checkpoint."""
    if isinstance(source, InputDataSource):
        normalized = source
    else:
        key = str(source).strip().lower()
        normalized = _INPUT_SOURCE_ALIASES.get(key)
    if normalized is None:
        valid = ", ".join(sorted(_INPUT_SOURCE_ALIASES))
        raise ValueError(f"Invalid INPUT_DATA_SOURCE '{source}'. Use one of: {valid}.")
    return normalized is InputDataSource.CHECKPOINT


def resolve_sales_mode(mode: str | SalesMode) -> tuple[bool, bool]:
    """Return (run_passenger_sales, run_freight_sales)."""
    if isinstance(mode, SalesMode):
        normalized = mode
    else:
        key = str(mode).strip().lower()
        normalized = _SALES_MODE_ALIASES.get(key)
    if normalized is None:
        valid = ", ".join(sorted(_SALES_MODE_ALIASES))
        raise ValueError(f"Invalid SALES_MODE '{mode}'. Use one of: {valid}.")

    run_passenger = normalized in {SalesMode.PASSENGER, SalesMode.BOTH}
    run_freight = normalized in {SalesMode.FREIGHT, SalesMode.BOTH}
    return run_passenger, run_freight


def resolve_run_profile(mode: str | RunProfile) -> tuple[bool, bool]:
    """Return (run_input_creation, run_reconciliation)."""
    if isinstance(mode, RunProfile):
        normalized = mode
    else:
        key = str(mode).strip().lower()
        normalized = _RUN_PROFILE_ALIASES.get(key)
    if normalized is None:
        valid = ", ".join(sorted(_RUN_PROFILE_ALIASES))
        raise ValueError(f"Invalid RUN_PROFILE '{mode}'. Use one of: {valid}.")

    run_input = normalized in {RunProfile.INPUT_ONLY, RunProfile.FULL}
    run_reconcile = normalized in {RunProfile.RECONCILE_ONLY, RunProfile.FULL}
    return run_input, run_reconcile


# ------------------------------------------------------------
# Optional: run directly
# ------------------------------------------------------------

# Select economy config by code (e.g. "12_NZ", "20_USA") or "all".
TRANSPORT_ECONOMY_SELECTION = "all"
TRANSPORT_SCENARIO_SELECTION = "Reference"
# Applies only when TRANSPORT_ECONOMY_SELECTION == "all" (ignored otherwise):
# "separate" -> run each configured economy independently (01_AUS ... 21_VN).
# "apec"     -> run one synthetic 00_APEC case (aggregated from all configured economies).
# "both"     -> run all separate economies first, then run synthetic 00_APEC.
# Note: combined passenger/freight CSVs are built from the "separate" runs only.
# Reconciliation scope in all-mode:
# - "separate" or "both": reconciliation can run for all configured economies.
# - "apec": reconciliation runs for 00_APEC only.
ALL_RUN_MODE = "apec"
APEC_REGION = "APEC"
# For testing synthetic 00_APEC runs in a single-region LEAP area, map APEC
# exports/IDs to an existing LEAP region name.
APEC_LEAP_REGION_OVERRIDE = "United States of America"
APEC_BASE_YEAR = 2022
APEC_FINAL_YEAR = 2060

# INPUT CREATION VARS
RUN_PROFILE = "input_only"  # "input_only", "reconcile_only", "full"
RUN_INPUT_CREATION, RUN_RECONCILIATION = resolve_run_profile(RUN_PROFILE)
SALES_MODE = "none"  # "none", "passenger", "freight", "both"
RUN_PASSENGER_SALES, RUN_FREIGHT_SALES = resolve_sales_mode(SALES_MODE)
PASSENGER_PLOT = False
# When running in all+apec mode, prime all separate economy input exports by
# default so later economy-level work can reuse prepared outputs/checkpoints.
PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC = True
# Optional policy payloads passed to sales_workflow wrappers.
PASSENGER_SALES_POLICY_SETTINGS: dict[str, Any] | None = None
FREIGHT_SALES_POLICY_SETTINGS: dict[str, Any] | None = None

# RECONCILIATION VARS
APPLY_ADJUSTMENTS_TO_FUTURE_YEARS = True
REPORT_ADJUSTMENT_CHANGES = True
# Optional convergence-time fallback injections keyed by ESTO energy key.
# Example:
# {
#   "15_02_road | 17_electricity | x": [
#       {"type": "mode_stock_seed", "economy": "all", "parent_path": "...", "mode_path": "...", "min_mode_stock": 1000.0}
#       {"type": "mode_stock_seed", "economy": "20_USA", "parent_path": "...", "mode_path": "...", "min_mode_stock": 2000.0}
#   ]
# }
# If an ESTO key has both "all" and economy-specific rules, the economy-specific
# set is used for that economy.
ESTO_ZERO_ENERGY_FALLBACK_RULES: dict[str, list[dict[str, Any]]] = {}

# COM / validation flags
CHECK_BRANCHES_IN_LEAP_USING_COM = True
SET_VARS_IN_LEAP_USING_COM = True
AUTO_SET_MISSING_BRANCHES = True
ENSURE_FUELS_IN_LEAP = True

# Checkpoint/loading options
# Where to load the preprocessed input dataframe from: "raw" or "checkpoint"
INPUT_DATA_SOURCE = "checkpoint"
LOAD_INPUT_CHECKPOINT = resolve_input_checkpoint(INPUT_DATA_SOURCE)
# Resume export pipeline from a single stage: "none", "halfway", "three_quarter", "export"
CHECKPOINT_LOAD_STAGE = "none"
(
    LOAD_HALFWAY_CHECKPOINT,
    LOAD_THREEQUART_WAY_CHECKPOINT,
    LOAD_EXPORT_DF_CHECKPOINT,
) = resolve_export_checkpoint_flags(CHECKPOINT_LOAD_STAGE)
MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE = True

DATE_ID = datetime.now().strftime("%Y%m%d")


def _normalise_esto_key(value: str) -> str:
    key = " | ".join(part.strip() for part in str(value).split("|"))
    return key.lower().strip()


def _rule_applies_to_run(rule: Mapping[str, Any], *, economy: str) -> bool:
    economy_filter = str(rule.get("economy", "")).strip().lower()
    if economy_filter and economy_filter not in {"all", "*"} and economy_filter != str(economy).strip().lower():
        return False
    return True


def _select_rules_for_economy(
    rules: list[dict[str, Any]],
    *,
    economy: str,
) -> list[dict[str, Any]]:
    economy_norm = str(economy).strip().lower()
    specific_rules: list[dict[str, Any]] = []
    all_scope_rules: list[dict[str, Any]] = []

    for rule in rules:
        economy_filter = str(rule.get("economy", "all")).strip().lower()
        if economy_filter in {"", "all", "*"}:
            all_scope_rules.append(rule)
        elif economy_filter == economy_norm:
            specific_rules.append(rule)

    # Economy-specific rules override "all" defaults for this ESTO key.
    return specific_rules if specific_rules else all_scope_rules


def _resolve_reconciliation_year_col(df: pd.DataFrame, base_year: int) -> int | str | None:
    if base_year in df.columns:
        return base_year
    if str(base_year) in df.columns:
        return str(base_year)
    return None


def _estimate_mode_stock_from_energy_target(
    working: pd.DataFrame,
    *,
    year_col: int | str,
    economy: str,
    scenario: str,
    rule_label: str,
    mode_path: str,
    target_energy: float,
    device_path: str | None = None,
) -> float | None:
    if target_energy <= 0:
        return None

    resolved_device_path = str(device_path or (mode_path + "\\Electricity")).strip()
    if not resolved_device_path:
        return None

    device_share_mask = (
        (working["Branch Path"] == resolved_device_path)
        & (working["Variable"] == "Device Share")
    )
    mileage_mask = (
        (working["Branch Path"] == resolved_device_path)
        & (working["Variable"] == "Mileage")
    )
    efficiency_mask = (
        (working["Branch Path"] == resolved_device_path)
        & (working["Variable"].isin(["Fuel Economy", "Final On-Road Fuel Economy"]))
    )
    if not device_share_mask.any() or not mileage_mask.any() or not efficiency_mask.any():
        print(
            f"[WARN] Could not auto-calculate mode stock for {economy}/{scenario} ({rule_label}): "
            f"missing Device Share/Mileage/Fuel Economy at '{resolved_device_path}'."
        )
        return None

    device_share = float(pd.to_numeric(working.loc[device_share_mask, year_col], errors="coerce").iloc[0])
    mileage = float(pd.to_numeric(working.loc[mileage_mask, year_col], errors="coerce").iloc[0])
    efficiency = float(pd.to_numeric(working.loc[efficiency_mask, year_col], errors="coerce").iloc[0])
    if device_share <= 0 or mileage <= 0 or efficiency <= 0:
        print(
            f"[WARN] Could not auto-calculate mode stock for {economy}/{scenario} ({rule_label}): "
            f"non-positive Device Share ({device_share}), Mileage ({mileage}), or Fuel Economy ({efficiency})."
        )
        return None

    stock_scale = float(LEAP_MEASURE_CONFIG["Vehicle type (road)"]["Stock"]["factor"])
    mileage_scale = float(LEAP_MEASURE_CONFIG["Fuel (road)"]["Mileage"]["factor"])
    efficiency_scale = float(LEAP_MEASURE_CONFIG["Fuel (road)"]["Final On-Road Fuel Economy"]["factor"])
    denom = device_share * mileage * efficiency
    if denom <= 0:
        return None

    # Rearranged from transport_stock_energy_fn:
    # energy = (mode_stock * device_share / 100 / stock_scale) * (mileage / mileage_scale) * (efficiency / efficiency_scale)
    required_mode_stock = target_energy * stock_scale * mileage_scale * efficiency_scale * 100.0 / denom
    if required_mode_stock <= 0 or pd.isna(required_mode_stock):
        return None
    return float(required_mode_stock)


def _apply_mode_stock_seed_rule(
    working: pd.DataFrame,
    *,
    year_col: int | str,
    base_year: int,
    economy: str,
    scenario: str,
    rule: Mapping[str, Any],
    rule_label: str,
    target_energy: float | None = None,
) -> bool:
    parent_path = str(rule.get("parent_path", "")).strip()
    mode_path = str(rule.get("mode_path", "")).strip()
    if not parent_path or not mode_path:
        return False

    min_mode_stock: float | None
    min_mode_stock_raw = rule.get("min_mode_stock", None)
    if min_mode_stock_raw is not None:
        try:
            min_mode_stock = float(min_mode_stock_raw)
        except (TypeError, ValueError):
            print(
                f"[WARN] Invalid min_mode_stock in fallback rule for {economy}/{scenario} "
                f"({rule_label}): {min_mode_stock_raw!r}"
            )
            return False
    else:
        min_mode_stock = None

    parent_mask = (working["Branch Path"] == parent_path) & (working["Variable"] == "Stock")
    mode_mask = (working["Branch Path"] == mode_path) & (working["Variable"] == "Stock Share")
    depth_parent = parent_path.count("\\")
    sibling_mask = (
        (working["Variable"] == "Stock Share")
        & working["Branch Path"].str.startswith(parent_path + "\\")
        & (working["Branch Path"].str.count(r"\\") == (depth_parent + 1))
    )
    if not parent_mask.any() or not mode_mask.any() or not sibling_mask.any():
        print(
            f"[WARN] Fallback rule skipped for {economy}/{scenario} ({rule_label}): "
            "required stock rows were not found."
        )
        return False

    parent_stock = float(pd.to_numeric(working.loc[parent_mask, year_col], errors="coerce").iloc[0])
    sibling_df = working.loc[sibling_mask, ["Branch Path", year_col]].copy()
    sibling_df[year_col] = pd.to_numeric(sibling_df[year_col], errors="coerce").fillna(0.0)
    sibling_tot = float(sibling_df[year_col].sum())
    if parent_stock <= 0 or sibling_tot <= 0:
        print(
            f"[WARN] Fallback rule skipped for {economy}/{scenario} ({rule_label}): "
            f"invalid parent stock ({parent_stock}) or share total ({sibling_tot})."
        )
        return False

    mode_share = float(pd.to_numeric(working.loc[mode_mask, year_col], errors="coerce").iloc[0])
    current_mode_stock = parent_stock * (mode_share / sibling_tot)
    if min_mode_stock is None:
        inferred_mode_stock = _estimate_mode_stock_from_energy_target(
            working,
            year_col=year_col,
            economy=economy,
            scenario=scenario,
            rule_label=rule_label,
            mode_path=mode_path,
            target_energy=float(target_energy or 0.0),
            device_path=str(rule.get("device_path", "")).strip() or None,
        )
        if inferred_mode_stock is None:
            print(
                f"[WARN] Fallback rule skipped for {economy}/{scenario} ({rule_label}): "
                "could not infer required mode stock and no min_mode_stock provided."
            )
            return False
        min_mode_stock = inferred_mode_stock

    if current_mode_stock >= min_mode_stock:
        return False

    delta_stock = min_mode_stock - current_mode_stock
    sibling_df["mode_stock"] = parent_stock * (sibling_df[year_col] / sibling_tot)
    target_row_mask = sibling_df["Branch Path"] == mode_path
    if not target_row_mask.any():
        return False
    sibling_df.loc[target_row_mask, "mode_stock"] = (
        sibling_df.loc[target_row_mask, "mode_stock"] + delta_stock
    )
    new_parent_stock = float(sibling_df["mode_stock"].sum())
    if new_parent_stock <= 0:
        return False
    sibling_df["new_share"] = sibling_tot * (sibling_df["mode_stock"] / new_parent_stock)

    working.loc[parent_mask, year_col] = new_parent_stock
    for _, row in sibling_df.iterrows():
        sibling_path_mask = (
            (working["Branch Path"] == row["Branch Path"])
            & (working["Variable"] == "Stock Share")
        )
        working.loc[sibling_path_mask, year_col] = float(row["new_share"])

    print(
        f"[INFO] Applied fallback mode stock seed for {economy}/{scenario} ({rule_label}): "
        f"{mode_path} stock {current_mode_stock:.3f} -> {min_mode_stock:.3f} (base year {base_year})."
    )
    return True


def _apply_scalar_min_rule(
    working: pd.DataFrame,
    *,
    year_col: int | str,
    economy: str,
    scenario: str,
    rule: Mapping[str, Any],
    rule_label: str,
) -> bool:
    branch_path = str(rule.get("branch_path", "")).strip()
    variable = str(rule.get("variable", "")).strip()
    min_value_raw = rule.get("min_value", 0.0)
    try:
        min_value = float(min_value_raw)
    except (TypeError, ValueError):
        print(
            f"[WARN] Invalid min_value in fallback rule for {economy}/{scenario} "
            f"({rule_label}): {min_value_raw!r}"
        )
        return False
    if min_value <= 0 or not branch_path or not variable:
        return False

    target_mask = (working["Branch Path"] == branch_path) & (working["Variable"] == variable)
    if not target_mask.any():
        print(
            f"[WARN] Fallback scalar rule skipped for {economy}/{scenario} ({rule_label}): "
            f"missing row {branch_path} / {variable}."
        )
        return False

    current = float(pd.to_numeric(working.loc[target_mask, year_col], errors="coerce").iloc[0])
    if current >= min_value:
        return False
    working.loc[target_mask, year_col] = min_value
    print(
        f"[INFO] Applied fallback scalar minimum for {economy}/{scenario} ({rule_label}): "
        f"{branch_path} [{variable}] {current:.3f} -> {min_value:.3f}."
    )
    return True


def _apply_zero_energy_fallbacks_from_summary(
    working_df: pd.DataFrame,
    *,
    summary_df_check: pd.DataFrame,
    base_year: int,
    economy: str,
    scenario: str,
    energy_abs_tolerance: float,
) -> tuple[pd.DataFrame, int]:
    if summary_df_check.empty or not ESTO_ZERO_ENERGY_FALLBACK_RULES:
        return working_df, 0

    year_col = _resolve_reconciliation_year_col(working_df, base_year)
    if year_col is None:
        return working_df, 0
    if "ESTO Key" not in summary_df_check.columns:
        return working_df, 0

    leap_check = pd.to_numeric(summary_df_check.get("LEAP Energy Use"), errors="coerce")
    esto_check = pd.to_numeric(summary_df_check.get("ESTO Energy Use"), errors="coerce")
    if leap_check is None or esto_check is None:
        return working_df, 0

    zero_gap_mask = (
        leap_check.fillna(0.0).abs() <= energy_abs_tolerance
    ) & (esto_check.fillna(0.0) > energy_abs_tolerance)

    candidate_keys = (
        summary_df_check.loc[zero_gap_mask, "ESTO Key"]
        .astype(str)
        .dropna()
        .unique()
        .tolist()
    )
    if not candidate_keys:
        return working_df, 0

    keyed_rules: dict[str, list[dict[str, Any]]] = {
        _normalise_esto_key(key): [dict(rule) for rule in rules if isinstance(rule, Mapping)]
        for key, rules in ESTO_ZERO_ENERGY_FALLBACK_RULES.items()
        if isinstance(rules, list)
    }

    working = working_df.copy()
    applied_count = 0
    for key in candidate_keys:
        norm_key = _normalise_esto_key(key)
        rules_for_key = keyed_rules.get(norm_key, [])
        if not rules_for_key:
            continue
        key_rows = summary_df_check.loc[
            summary_df_check["ESTO Key"].astype(str) == str(key)
        ].copy()
        key_esto_energy = float(
            pd.to_numeric(key_rows.get("ESTO Energy Use"), errors="coerce").dropna().max()
            if not key_rows.empty
            else 0.0
        )
        selected_rules = _select_rules_for_economy(rules_for_key, economy=economy)
        for idx, rule in enumerate(selected_rules, start=1):
            if not _rule_applies_to_run(rule, economy=economy):
                continue
            rule_type = str(rule.get("type", "scalar_min")).strip().lower()
            rule_label = f"{key}#{idx}"
            if rule_type == "mode_stock_seed":
                applied = _apply_mode_stock_seed_rule(
                    working,
                    year_col=year_col,
                    base_year=base_year,
                    economy=economy,
                    scenario=scenario,
                    rule=rule,
                    rule_label=rule_label,
                    target_energy=key_esto_energy,
                )
            else:
                applied = _apply_scalar_min_rule(
                    working,
                    year_col=year_col,
                    economy=economy,
                    scenario=scenario,
                    rule=rule,
                    rule_label=rule_label,
                )
            if applied:
                applied_count += 1

    return working, applied_count


def resolve_transport_run_mode(transport_economy_selection: str, all_run_mode: str):
    """Return normalized all-run settings and execution flags."""
    is_all_mode = transport_economy_selection.strip().lower() == "all"
    normalized_all_mode = all_run_mode.strip().lower()
    valid_all_modes = {"separate", "apec", "both"}
    if is_all_mode and normalized_all_mode not in valid_all_modes:
        raise ValueError(f"Invalid ALL_RUN_MODE '{all_run_mode}'. Use one of {sorted(valid_all_modes)}.")

    run_separate = (not is_all_mode) or (normalized_all_mode in {"separate", "both"})
    run_apec = is_all_mode and (normalized_all_mode in {"apec", "both"})
    return is_all_mode, normalized_all_mode, run_separate, run_apec


def aggregate_batch_sales_outputs(run_records, scenario, date_id):
    """Concatenate per-economy sales CSVs into single files for quick cross-economy analysis."""
    passenger_frames = []
    freight_frames = []
    for record in run_records:
        if record.get("status") != "success":
            continue
        passenger_path = record.get("passenger_sales_output")
        freight_path = record.get("freight_sales_output")
        economy = record["economy"]
        if passenger_path and os.path.exists(passenger_path):
            passenger_df = pd.read_csv(passenger_path)
            passenger_df["Economy"] = economy
            passenger_df["Scenario"] = scenario
            passenger_frames.append(passenger_df)
        if freight_path and os.path.exists(freight_path):
            freight_df = pd.read_csv(freight_path)
            freight_df["Economy"] = economy
            freight_df["Scenario"] = scenario
            freight_frames.append(freight_df)

    if passenger_frames:
        passenger_all = pd.concat(passenger_frames, ignore_index=True)
        passenger_all_path = resolve_str(f"{PASSENGER_SALES_DIR}/passenger_sales_ALL_{scenario}_{date_id}.csv")
        os.makedirs(os.path.dirname(passenger_all_path), exist_ok=True)
        archived_output = _archive_existing_output_file(passenger_all_path, date_id=date_id)
        if archived_output:
            print(f"[INFO] Archived previous combined passenger sales to {archived_output}")
        passenger_all.to_csv(passenger_all_path, index=False)
        print(f"[INFO] Wrote combined passenger sales: {passenger_all_path}")
    if freight_frames:
        freight_all = pd.concat(freight_frames, ignore_index=True)
        freight_all_path = resolve_str(f"{FREIGHT_SALES_DIR}/freight_sales_ALL_{scenario}_{date_id}.csv")
        os.makedirs(os.path.dirname(freight_all_path), exist_ok=True)
        archived_output = _archive_existing_output_file(freight_all_path, date_id=date_id)
        if archived_output:
            print(f"[INFO] Archived previous combined freight sales to {archived_output}")
        freight_all.to_csv(freight_all_path, index=False)
        print(f"[INFO] Wrote combined freight sales: {freight_all_path}")


def build_apec_run_config(scenario: str):
    """Build runtime config for synthetic 00_APEC runs."""
    from types import SimpleNamespace

    seed_targets = list_transport_run_configs(scenario)
    if not seed_targets:
        raise ValueError(f"No configured economies found for scenario '{scenario}'.")
    seed_economy, seed_scenario = seed_targets[0]
    _, _, seed_cfg = load_transport_run_config(seed_economy, seed_scenario)

    all_balances_path = Path(seed_cfg.transport_esto_balances_path)
    apec_balances_path = all_balances_path
    if "_ALL_" in all_balances_path.name:
        candidate = all_balances_path.with_name(all_balances_path.name.replace("_ALL_", "_00_APEC_"))
        if candidate.exists():
            apec_balances_path = candidate
        else:
            print(f"[WARN] Expected 00_APEC balances file not found, using ALL file: {candidate}")

    default_survival_profile = str(
        COMMON_CONFIG.get("survival_profile_path", seed_cfg.survival_profile_path)
    )
    default_vintage_profile = str(
        COMMON_CONFIG.get("vintage_profile_path", seed_cfg.vintage_profile_path)
    )
    apec_survival_profile = resolve_lifecycle_profile_path_for_economy(
        default_survival_profile,
        "00_APEC",
    )
    apec_vintage_profile = resolve_lifecycle_profile_path_for_economy(
        default_vintage_profile,
        "00_APEC",
    )

    return SimpleNamespace(
        transport_model_path=seed_cfg.transport_model_path,
        transport_region=APEC_REGION,
        transport_leap_region_override=APEC_LEAP_REGION_OVERRIDE,
        transport_base_year=APEC_BASE_YEAR,
        transport_final_year=APEC_FINAL_YEAR,
        transport_model_name="APEC transport",
        transport_export_path=resolve_str(f"{DOMESTIC_EXPORT_DIR}/00_APEC_transport_leap_export_{scenario}.xlsx"),
        transport_import_path=seed_cfg.transport_import_path,
        transport_esto_balances_path=str(apec_balances_path),
        transport_fuels_path=seed_cfg.transport_fuels_path,
        survival_profile_path=resolve_str(apec_survival_profile),
        vintage_profile_path=resolve_str(apec_vintage_profile),
        passenger_sales_output=resolve_str(f"{PASSENGER_SALES_DIR}/passenger_sales_00_APEC_{scenario}.csv"),
        freight_sales_output=resolve_str(f"{FREIGHT_SALES_DIR}/freight_sales_00_APEC_{scenario}.csv"),
    )


def run_configured_transport_workflow(
    *,
    transport_economy: str,
    transport_scenario: str,
    transport_cfg,
    run_type: str,
    prepared_input_df: pd.DataFrame | None = None,
):
    """Run input creation + optional reconciliation for one configured workflow target."""
    print(f"\n=== Running {run_type} workflow for {transport_economy} | scenario {transport_scenario} ===")
    passenger_sales_policy_settings = getattr(transport_cfg, "passenger_sales_policy_settings", None)
    if passenger_sales_policy_settings is None:
        passenger_sales_policy_settings = PASSENGER_SALES_POLICY_SETTINGS
    freight_sales_policy_settings = getattr(transport_cfg, "freight_sales_policy_settings", None)
    if freight_sales_policy_settings is None:
        freight_sales_policy_settings = FREIGHT_SALES_POLICY_SETTINGS

    record = {
        "economy": transport_economy,
        "scenario": transport_scenario,
        "run_type": run_type,
        "transport_export_path": transport_cfg.transport_export_path,
        "passenger_sales_output": transport_cfg.passenger_sales_output,
        "freight_sales_output": transport_cfg.freight_sales_output,
        "status": "success",
        "error": "",
    }
    checkpoint_tag = f"{transport_economy}_{transport_scenario}".replace(" ", "_")
    reconciliation_input_path = resolve_str(
        f"intermediate_data/export_df_for_viewing_checkpoint2_{checkpoint_tag}.pkl"
    )
    if RUN_RECONCILIATION and (not RUN_INPUT_CREATION) and not os.path.exists(reconciliation_input_path):
        raise FileNotFoundError(
            "Reconciliation input checkpoint was not found. Reconciliation now reads input from "
            f"intermediate_data, not from the export workbook.\nMissing file: {reconciliation_input_path}\n"
            "Run input creation first (RUN_PROFILE='input_only' or 'full')."
        )

    try:
        if RUN_INPUT_CREATION:
            load_transport_into_leap(
                transport_model_excel_path=transport_cfg.transport_model_path,
                economy=transport_economy,
                original_scenario=transport_scenario,
                new_scenario=transport_scenario,
                region=transport_cfg.transport_region,
                diagnose_method='all',
                base_year=transport_cfg.transport_base_year,
                final_year=transport_cfg.transport_final_year,
                model_name=transport_cfg.transport_model_name,
                CHECK_BRANCHES_IN_LEAP_USING_COM=CHECK_BRANCHES_IN_LEAP_USING_COM,
                SET_VARS_IN_LEAP_USING_COM=SET_VARS_IN_LEAP_USING_COM,
                AUTO_SET_MISSING_BRANCHES=AUTO_SET_MISSING_BRANCHES,
                export_filename=transport_cfg.transport_export_path,
                import_filename=transport_cfg.transport_import_path,
                TRANSPORT_ESTO_BALANCES_PATH=transport_cfg.transport_esto_balances_path,
                TRANSPORT_FUELS_DATA_FILE_PATH=transport_cfg.transport_fuels_path,
                TRANSPORT_ROOT=r"Demand",
                LOAD_INPUT_CHECKPOINT=LOAD_INPUT_CHECKPOINT,
                LOAD_HALFWAY_CHECKPOINT=LOAD_HALFWAY_CHECKPOINT,
                LOAD_THREEQUART_WAY_CHECKPOINT=LOAD_THREEQUART_WAY_CHECKPOINT,
                LOAD_EXPORT_DF_CHECKPOINT=LOAD_EXPORT_DF_CHECKPOINT,
                CHECKPOINT_TAG=checkpoint_tag,
                MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE=MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE,
                RUN_PASSENGER_SALES=RUN_PASSENGER_SALES,
                RUN_FREIGHT_SALES=RUN_FREIGHT_SALES,
                ENSURE_FUELS_IN_LEAP=ENSURE_FUELS_IN_LEAP,
                SURVIVAL_PROFILE_PATH=transport_cfg.survival_profile_path,
                VINTAGE_PROFILE_PATH=transport_cfg.vintage_profile_path,
                PASSENGER_SALES_OUTPUT=transport_cfg.passenger_sales_output,
                FREIGHT_SALES_OUTPUT=transport_cfg.freight_sales_output,
                PASSENGER_SALES_POLICY_SETTINGS=passenger_sales_policy_settings,
                FREIGHT_SALES_POLICY_SETTINGS=freight_sales_policy_settings,
                PASSENGER_PLOT=PASSENGER_PLOT,
                PREPARED_INPUT_DF=prepared_input_df,
                LEAP_REGION_NAME_OVERRIDE=getattr(transport_cfg, "transport_leap_region_override", None),
            )

        if RUN_RECONCILIATION:
            run_transport_reconciliation(
                apply_adjustments_to_future_years=APPLY_ADJUSTMENTS_TO_FUTURE_YEARS,
                report_adjustment_changes=REPORT_ADJUSTMENT_CHANGES,
                date_id=DATE_ID,
                transport_esto_balances_path=transport_cfg.transport_esto_balances_path,
                transport_export_path=transport_cfg.transport_export_path,
                reconciliation_input_path=reconciliation_input_path,
                economy=transport_economy,
                base_year=transport_cfg.transport_base_year,
                final_year=transport_cfg.transport_final_year,
                scenario=transport_scenario,
                model_name=transport_cfg.transport_model_name,
                unmappable_branches=UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
                analysis_type_lookup=LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP.get,
                all_leap_branches=ALL_LEAP_BRANCHES_TRANSPORT,
                esto_to_leap_mapping=ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
                set_vars_in_leap_using_com=SET_VARS_IN_LEAP_USING_COM,
                scale_factor_tolerance=1e-4,
            )
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = str(exc)
        print(f"[ERROR] {transport_economy} failed: {exc}")

    return record


def run_input_only_separate_prep_for_all_economies(
    *,
    scenario: str,
) -> list[dict]:
    """Run per-economy input creation (without reconciliation) for all configured economies."""
    global RUN_RECONCILIATION

    print(
        "[INFO] Priming per-economy input setup before synthetic 00_APEC run "
        f"(scenario={scenario}; reconciliation disabled for this pre-pass)."
    )

    prior_run_reconciliation = RUN_RECONCILIATION
    prepass_records: list[dict] = []
    try:
        RUN_RECONCILIATION = False
        for transport_economy, transport_scenario in list_transport_run_configs(scenario):
            _, _, transport_cfg = load_transport_run_config(transport_economy, transport_scenario)
            record = run_configured_transport_workflow(
                transport_economy=transport_economy,
                transport_scenario=transport_scenario,
                transport_cfg=transport_cfg,
                run_type="separate_input_prep",
            )
            prepass_records.append(record)
    finally:
        RUN_RECONCILIATION = prior_run_reconciliation

    return prepass_records


def run_transport_workflow() -> list[dict]:
    """Run the configured transport workflow using module-level settings."""
    if not (RUN_INPUT_CREATION or RUN_RECONCILIATION):
        print("[INFO] Nothing to run: both RUN_INPUT_CREATION and RUN_RECONCILIATION are False.")
        return []

    pd.options.display.float_format = "{:,.3f}".format
    list_all_measures()
    is_all_mode, all_mode, run_separate, run_apec = resolve_transport_run_mode(
        TRANSPORT_ECONOMY_SELECTION,
        ALL_RUN_MODE,
    )

    if is_all_mode:
        print(
            f"[INFO] all-mode plan | scenario={TRANSPORT_SCENARIO_SELECTION} | "
            f"mode={all_mode} | run_separate={run_separate} | run_apec={run_apec}"
        )
        if RUN_RECONCILIATION:
            if run_separate and run_apec:
                print(
                    "[INFO] Reconciliation scope: all separate economies + 00_APEC "
                    "(ALL_RUN_MODE='both')."
                )
            elif run_separate:
                print(
                    "[INFO] Reconciliation scope: all separate economies only "
                    "(ALL_RUN_MODE='separate')."
                )
            elif run_apec:
                print(
                    "[INFO] Reconciliation scope: 00_APEC only "
                    "(ALL_RUN_MODE='apec'). Use ALL_RUN_MODE='separate' or 'both' "
                    "to reconcile all economies."
                )
    else:
        print(
            f"[INFO] single-economy plan | economy={TRANSPORT_ECONOMY_SELECTION} | "
            f"scenario={TRANSPORT_SCENARIO_SELECTION}"
        )
        if ALL_RUN_MODE.strip():
            print(f"[INFO] ALL_RUN_MODE='{ALL_RUN_MODE}' is ignored when TRANSPORT_ECONOMY_SELECTION != 'all'.")

    if is_all_mode and SET_VARS_IN_LEAP_USING_COM:
        print("[WARN] 'all' mode with COM writes enabled will update LEAP for multiple runs.")

    run_records = []

    if (
        PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC
        and is_all_mode
        and run_apec
        and RUN_INPUT_CREATION
        and not run_separate
    ):
        prepass_records = run_input_only_separate_prep_for_all_economies(
            scenario=TRANSPORT_SCENARIO_SELECTION,
        )
        run_records.extend(prepass_records)

    if run_separate:
        run_targets = (
            list_transport_run_configs(TRANSPORT_SCENARIO_SELECTION)
            if is_all_mode
            else [(TRANSPORT_ECONOMY_SELECTION, TRANSPORT_SCENARIO_SELECTION)]
        )
        for transport_economy, transport_scenario in run_targets:
            _, _, transport_cfg = load_transport_run_config(transport_economy, transport_scenario)
            record = run_configured_transport_workflow(
                transport_economy=transport_economy,
                transport_scenario=transport_scenario,
                transport_cfg=transport_cfg,
                run_type="separate",
            )
            run_records.append(record)

    if run_apec:
        apec_cfg = build_apec_run_config(TRANSPORT_SCENARIO_SELECTION)
        apec_input_df = None
        if RUN_INPUT_CREATION:
            apec_input_df = prepare_apec_input_data(
                scenario=TRANSPORT_SCENARIO_SELECTION,
                base_year=APEC_BASE_YEAR,
                final_year=APEC_FINAL_YEAR,
                load_checkpoint=LOAD_INPUT_CHECKPOINT,
            )
        apec_record = run_configured_transport_workflow(
            transport_economy="00_APEC",
            transport_scenario=TRANSPORT_SCENARIO_SELECTION,
            transport_cfg=apec_cfg,
            run_type="apec",
            prepared_input_df=apec_input_df,
        )
        run_records.append(apec_record)

    if is_all_mode:
        summary_path = resolve_str(
            f"results/run_summaries/transport_all_run_summary_{TRANSPORT_SCENARIO_SELECTION}_{all_mode}_{DATE_ID}.csv"
        )
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        archived_output = _archive_existing_output_file(summary_path, date_id=DATE_ID)
        if archived_output:
            print(f"[INFO] Archived previous all-run summary to {archived_output}")
        pd.DataFrame(run_records).to_csv(summary_path, index=False)
        print(f"[INFO] Wrote run summary: {summary_path}")

        separate_records = [record for record in run_records if record.get("run_type") == "separate"]
        if separate_records:
            aggregate_batch_sales_outputs(separate_records, TRANSPORT_SCENARIO_SELECTION, DATE_ID)

    return run_records


if __name__ == "__main__":
    run_transport_workflow()

#%%

# from config.branch_mappings import ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,     UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT, SHORTNAME_TO_LEAP_BRANCHES, ALL_LEAP_BRANCHES_TRANSPORT
# from config.measure_catalog import LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP
# from energy_use_reconciliation import reconcile_energy_use, build_branch_rules_from_mapping
# from functions.energy_use_reconciliation_road import build_transport_esto_energy_totals
# from functions.esto_data import extract_esto_energy_use_for_leap_branches
# import pandas as pd
# export_df = pd.read_excel("../results/domestic_exports/USA_transport_leap_export_Target.xlsx")
# esto_totals = {("15_02_road", "07_petroleum_products", "07_01_motor_gasoline"): 100.0}
# # ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP, UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT
# LEAP_BRANCHES_LIST = [branch for branches in SHORTNAME_TO_LEAP_BRANCHES.values() for branch in branches]

# # branch_rules = build_branch_rules_from_mapping(
# #     ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
# #     UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
# #     all_leap_branches=LEAP_BRANCHES_LIST,    
# #     analysis_type_lookup=LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP.get,
# #     root="Demand",
# # )
#%%
