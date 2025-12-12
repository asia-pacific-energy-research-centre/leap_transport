#%%
# ============================================================
# transport_leap_import.py
# ============================================================
# Main logic for processing and loading transport data into LEAP.
# Depends on LEAP_core.py and mapping/config modules.
# ============================================================

import sys
from pathlib import Path
import pandas as pd
import shutil
from datetime import datetime

# Allow sibling leap_utilities package without pip install
BASE_DIR = Path(__file__).resolve().parent.parent
UTILS_ROOT = (BASE_DIR / "leap_utilities").resolve()
UTILS_PKG = UTILS_ROOT / "leap_utils"
for path in (UTILS_PKG, UTILS_ROOT):
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

from leap_utils.leap_core import (
    connect_to_leap,
    diagnose_measures_in_leap_branch,
    ensure_branch_exists,
    safe_set_variable,
    # diagnose_leap_branch,
    create_transport_export_df,
    write_row_to_leap_export_df,
    build_expression_from_mapping,
    define_value_based_on_src_tuple
)
from branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    create_new_source_rows_based_on_combinations,
    create_new_source_rows_based_on_proxies_with_no_activity,
)
from measure_catalog import list_all_measures, LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP
from measure_processing import process_measures_for_leap
from preprocessing import (
    allocate_fuel_alternatives_energy_and_activity,
    calculate_sales,
    normalize_and_calculate_shares)
from leap_utils.leap_excel_io import finalise_export_df, save_export_files, join_and_check_import_structure_matches_export_structure, separate_current_accounts_from_scenario
from branch_expression_mapping import LEAP_BRANCH_TO_EXPRESSION_MAPPING
from esto_data import (
    extract_other_type_rows_from_esto_and_insert_into_transport_df,
)

from basic_mappings import (
    ESTO_TRANSPORT_SECTOR_TUPLES,
    add_fuel_column,
    EXPECTED_COLS_IN_SOURCE,
)

from mappings_validation import (
    validate_all_mappings_with_measures,
    validate_and_fix_shares_normalise_to_one,
    validate_final_energy_use_for_base_year_equals_esto_totals,
)
from sales_curve_estimate import (
    load_survival_and_vintage_profiles,
    estimate_passenger_sales_from_dataframe,
    estimate_freight_sales_from_dataframe,
)
import os

##########
#for reconciliation:

# imports and data loading
import pandas as pd
from leap_utils.energy_use_reconciliation import (
    build_branch_rules_from_mapping,
    reconcile_energy_use,
    build_adjustment_change_tables,
)
from branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    ALL_LEAP_BRANCHES_TRANSPORT,
)

from measure_catalog import LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP

from energy_use_reconciliation_road import transport_energy_fn, transport_adjustment_fn, build_transport_esto_energy_totals


# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------

def prepare_input_data(transport_model_excel_path, economy, scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../data/all transport balances data.xlsx', LOAD_CHECKPOINT=False, TRANSPORT_FUELS_DATA_FILE_PATH = '../data/USA fuels model output.csv'):
    """Load and preprocess transport data for a specific economy."""    
    print(f"\n=== Loading Transport Data for {economy} ===")
    
    # Check for checkpoint file
    checkpoint_filename = f"../intermediate_data/transport_data_{economy}_{scenario}_{base_year}_{final_year}.pkl"
    if LOAD_CHECKPOINT and os.path.exists(checkpoint_filename):
        print(f"Loading data from checkpoint: {checkpoint_filename}")
        df = pd.read_pickle(checkpoint_filename)
        return df
    
    df = pd.read_excel(transport_model_excel_path)
    df = df[(df["Economy"] == economy) & (df["Scenario"] == scenario)]
    df = df[(df["Date"] >= base_year) & (df["Date"] <= final_year)]
    #check EXPECTED_COLS_IN_SOURCE are all in df
    missing_cols = [col for col in EXPECTED_COLS_IN_SOURCE if col not in df.columns]
    if missing_cols:
        breakpoint()
        raise ValueError(f"Missing expected columns in source data: {missing_cols}")
    unnecessary_cols = ['Unit', 'Data_available', 'Measure']
    df = df.drop(columns=unnecessary_cols, errors='ignore')
    
    df = add_fuel_column(df)
    df.loc[df["Medium"] != "road", ["Stocks", 'Vehicle_sales_share']] = 0
    
    df = allocate_fuel_alternatives_energy_and_activity(df, economy, scenario, TRANSPORT_FUELS_DATA_FILE_PATH)
    
    new_rows1 = create_new_source_rows_based_on_combinations(df)
    df = pd.concat([df, new_rows1], ignore_index=True)
    new_rows2 = create_new_source_rows_based_on_proxies_with_no_activity(df)
    df = pd.concat([df, new_rows2], ignore_index=True)
    if new_rows1.empty:
        breakpoint()
        raise ValueError("No new source rows were created from combinations; check the mapping and source data just in case.")
    if new_rows2.empty:
        breakpoint()
        raise ValueError("No new source rows were created from proxies; check the mapping and source data just in case.")
    
    #check for duplicates
    duplicates = df.duplicated(subset=['Date', 'Economy', 'Scenario', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'])
    if duplicates.any():
        breakpoint()
        #save to ../data/errors/duplicate_source_rows.csv
        df[duplicates].to_csv('../data/errors/duplicate_source_rows.csv', index=False)
        raise ValueError("Duplicates found in source data after adding new rows based on combinations and proxies; see ../data/errors/duplicate_source_rows.csv for details.")
     
    df = calculate_sales(df)
    df = normalize_and_calculate_shares(df)
    
    df = extract_other_type_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, scenario, TRANSPORT_ESTO_BALANCES_PATH)
    
    # Save checkpoint file
    os.makedirs("../intermediate_data", exist_ok=True)
    df.to_pickle(checkpoint_filename)
    print(f"Saved checkpoint: {checkpoint_filename}")
    return df


def run_passenger_sales_workflow(
    df: pd.DataFrame,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    survival_path: str = "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_path: str = "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    esto_energy_path: str = '../data/all transport balances data.xlsx',
    output_path: str | None = None,
    plot: bool = False,
    **kwargs,
) -> dict:
    """
    Run passenger sales estimation using survival/vintage lifecycle profiles.

    Returns the result dict from estimate_passenger_sales_from_dataframe and
    writes sales_table to CSV if output_path is provided.
    """
    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path=survival_path,
        vintage_path=vintage_path,
        vehicle_keys=("LPV", "MC", "Bus"),
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
        **kwargs,
    )

    sales_table = result.get("sales_table")
    if output_path and sales_table is not None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        sales_table.to_csv(output_path, index=False)
        print(f"[INFO] Saved passenger sales table to {output_path}")

    return result


def run_freight_sales_workflow(
    df: pd.DataFrame,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    survival_path: str = "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_path: str = "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    esto_energy_path: str = '../data/all transport balances data.xlsx',
    output_path: str | None = None,
    plot: bool = False,
    **kwargs,
) -> dict:
    """
    Run freight sales estimation using survival/vintage lifecycle profiles.

    Returns the result dict from estimate_freight_sales_from_dataframe and
    writes sales_table to CSV if output_path is provided.
    """
    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path=survival_path,
        vintage_path=vintage_path,
        vehicle_keys=("Trucks", "LCVs"),
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
        **kwargs,
    )

    sales_table = result.get("sales_table")
    if output_path and sales_table is not None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        sales_table.to_csv(output_path, index=False)
        print(f"[INFO] Saved freight sales table to {output_path}")

    return result


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

    leap_ttype, leap_vtype, leap_drive, leap_fuel = (list(leap_tuple) + [None] * (4 - len(leap_tuple)))[:4]
    branch_path = f"{TRANSPORT_ROOT}\\{leap_ttype}" + "".join(
        f"\\{x}" for x in [leap_vtype, leap_drive, leap_fuel] if x
    )
    
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
                    breakpoint()#no new rows added?
                    print(f"[WARN] No new rows added to leap_export_df for {measure} on {branch_path}")
        except Exception as e:
            breakpoint()
            raise RuntimeError(f"[ERROR] Failed to attach LEAP metadata for {measure} on {branch_path}: {e}")
            
    return leap_export_df

def convert_values_to_expressions(leap_export_df):
    
    print("\n=== Setting variables in LEAP via COM interface ===")
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
            tuple(branch_path.split('\\')[2:]), mini_df, measure
        )
        
        if not expr:
            breakpoint()
            raise ValueError(f"[ERROR] Failed to build expression for {measure} on {branch_path}")
        
        new_leap_export_df.at[idx, 'Expression'] = expr
        leap_export_df.at[idx, 'Method'] = method#when we convert to expr the method is no longer relevant. but it should be recorded in this spreadsheet for reference as well as just in case we want to use the spreadsheet to set variables in leap without expressions later on.
        
    return new_leap_export_df, leap_export_df 
    
def write_export_df_to_leap(
    L, leap_export_df
    ):
    print("\n=== Setting variables in LEAP via COM interface. Make sure not to use the LEAP window while this is running or it might cause problems! ===")
    total_written = 0
    
    for scenario in leap_export_df['Scenario'].unique():
        
        try:
            L.ActiveScenario = scenario #set the active scenario in LEAP
        except Exception as e:
            breakpoint()
            raise RuntimeError(f"[ERROR] Failed to set active scenario to '{scenario}' in LEAP: {e}")
        rows = leap_export_df[leap_export_df['Scenario'] == scenario]
            
        total_missing_variables = 0
        for idx, row in rows.iterrows():
            branch_path = row['Branch Path']
            measure = row['Variable']
            expr = row['Expression']
            unit = row['Units']
            
            success = safe_set_variable(L, L.Branch(branch_path), measure, expr, unit_name=unit, context=branch_path)
            if success:
                total_written += 1
            else:
                total_missing_variables += 1
        print(f"\n=== Finished setting variables in LEAP for scenario '{scenario}'. Total written: {total_written}, Missing variables: {total_missing_variables} ===\n")

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
        breakpoint()
        raise ValueError(f"[ERROR] Expected exactly one shortname for LEAP branch {leap_tuple}, found: {expected_shortname}")

    shortname = expected_shortname.pop()
    filtered_measure_config = LEAP_MEASURE_CONFIG[shortname]
    expected_measures = set(filtered_measure_config.keys())

    #if we are setting vars in leap using com we want to diagnose the branches first 
    if CHECK_BRANCHES_IN_LEAP_USING_COM:#not sure if necessary
        # if diagnose_method == 'first_branch_diagnosed' and not first_branch_diagnosed:
        #     diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures, AUTO_SET_MISSING_BRANCHES=True)
        #     first_branch_diagnosed = True
        # elif diagnose_method == 'first_of_each_length' and len(leap_tuple) not in first_of_each_length_diagnosed:
        #     diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures, AUTO_SET_MISSING_BRANCHES=True)
        #     first_of_each_length_diagnosed.add(len(leap_tuple))
        # elif diagnose_method == 'all':
        ensure_branch_exists(L, branch_path, leap_tuple, AUTO_SET_MISSING_BRANCHES=True)
        diagnose_measures_in_leap_branch(L, branch_path, leap_tuple, expected_measures)

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
):
    esto_df = pd.read_excel(transport_esto_balances_path)
    export_df = pd.read_excel(transport_export_path, header=2, sheet_name='FOR_VIEWING')
    export_df = export_df[export_df['Scenario'] == 'Current Accounts']

    esto_energy_totals = build_transport_esto_energy_totals(
        esto_df=esto_df,
        economy=economy,
        original_scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        SUBTOTAL_COLUMN=subtotal_column,
    )
    branch_rules = build_branch_rules_from_mapping(
        esto_to_leap_mapping=esto_to_leap_mapping,
        unmappable_branches=unmappable_branches,
        all_leap_branches=all_leap_branches,
        analysis_type_lookup=analysis_type_lookup,
        root='Demand',
    )
    # pd.Series(esto_energy_totals).to_pickle('../data/temp/transport_esto_energy_totals.pkl')
    # pd.Series(branch_rules).to_pickle('../data/temp/transport_branch_rules.pkl')
    # else:
    # esto_energy_totals = pd.read_pickle('../data/temp/transport_esto_energy_totals.pkl').to_dict()
    # branch_rules = pd.read_pickle('../data/temp/transport_branch_rules.pkl').to_dict()

    working_df, summary_df = reconcile_energy_use(
        export_df=export_df,
        base_year=base_year,
        branch_mapping_rules=branch_rules,
        esto_energy_totals=esto_energy_totals,
        energy_fn=transport_energy_fn,
        adjustment_fn=transport_adjustment_fn,
        apply_adjustments_to_future_years=apply_adjustments_to_future_years,
    )
    #run again to check that everything is reconciled properly
    _, summary_df_check = reconcile_energy_use(
        export_df=working_df,
        base_year=base_year,
        branch_mapping_rules=branch_rules,
        esto_energy_totals=esto_energy_totals,
        energy_fn=transport_energy_fn,
        adjustment_fn=transport_adjustment_fn,
        apply_adjustments_to_future_years=apply_adjustments_to_future_years,
    )
    if not summary_df_check['Scale Factor'].apply(lambda x: abs(x - 1.0) < 1e-6).all():
        breakpoint()
        failed = summary_df_check[summary_df_check['Scale Factor'].apply(lambda x: abs(x - 1.0) >= 1e-6)]
        raise ValueError('Reconciliation failed: some scale factors are not 1.0:\n' + failed.to_string())

    # Quick run-down of what changed
    adjusted_mask = summary_df['Scale Factor'].apply(lambda x: abs(x - 1.0) > 1e-6)
    adjusted_count = int(adjusted_mask.sum())
    if adjusted_count:
        abs_dev = (summary_df.loc[adjusted_mask, 'Scale Factor'] - 1.0).abs()
        max_dev_pct = float(abs_dev.max() * 100)
        mean_dev_pct = float(abs_dev.mean() * 100)
        change_msg = f"Adjusted {adjusted_count} ESTO keys; max scale deviation {max_dev_pct:.2f}% (avg {mean_dev_pct:.2f}%)."
    else:
        change_msg = "No adjustments required; all scale factors were 1.0."

    if report_adjustment_changes:
        reconciliation_dir = "../results/reconciliation"
        recon_archive_dir = os.path.join(reconciliation_dir, "archive")
        os.makedirs(reconciliation_dir, exist_ok=True)
        os.makedirs(recon_archive_dir, exist_ok=True)
        base_changes, future_changes = build_adjustment_change_tables(
            original_df=export_df,
            adjusted_df=working_df,
            base_year=base_year,
            include_future_years=apply_adjustments_to_future_years,
        )
        suffix = f"{economy}_{scenario}".replace(" ", "_")
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

    leap_export_df, export_df_for_viewing = convert_values_to_expressions(working_df)
    # Archive existing export before overwriting
    export_archive_dir = os.path.join(os.path.dirname(transport_export_path), "archive")
    os.makedirs(export_archive_dir, exist_ok=True)
    if os.path.exists(transport_export_path):
        export_base = os.path.splitext(os.path.basename(transport_export_path))
        archived_export = os.path.join(export_archive_dir, f"{export_base[0]}_{date_id}{export_base[1]}")
        shutil.move(transport_export_path, archived_export)

    save_export_files(
        leap_export_df,
        export_df_for_viewing,
        transport_export_path,
        base_year,
        final_year,
        model_name=model_name,
    )

    if set_vars_in_leap_using_com:
        L = connect_to_leap()
        write_export_df_to_leap(L, leap_export_df)
        #TODO NEED A STEP TO COMMUNICATE THE VALUES THAT HAVE CHANGED AND BY HOW MUCH. ALSO NEED TO HAVE AN OPTION TO ADJUST THE PROJECTED VALUES BY THE SAME FACTOR RATHER THAN JUST THE BASE YEAR.
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
    export_filename="../results/leap_export.xlsx",
    import_filename="../data/import_files/leap_import.xlsx",
    TRANSPORT_ESTO_BALANCES_PATH = '../data/all transport balances data.xlsx',
    TRANSPORT_FUELS_DATA_FILE_PATH = '../data/USA fuels model output.csv',
    TRANSPORT_ROOT = r"Demand",
    LOAD_INPUT_CHECKPOINT=False,
    LOAD_HALFWAY_CHECKPOINT=False,
    LOAD_THREEQUART_WAY_CHECKPOINT=False,
    LOAD_EXPORT_DF_CHECKPOINT=False,
    MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE=False,
    RUN_PASSENGER_SALES=False,
    RUN_FREIGHT_SALES=False,
    SURVIVAL_PROFILE_PATH="../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    VINTAGE_PROFILE_PATH="../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    PASSENGER_SALES_OUTPUT=None,
    FREIGHT_SALES_OUTPUT=None,
    PASSENGER_PLOT=False,
):
    """Main orchestrator for LEAP transport data loading."""
    
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
    
    df = prepare_input_data(transport_model_excel_path, economy, original_scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../data/all transport balances data.xlsx', LOAD_CHECKPOINT=LOAD_INPUT_CHECKPOINT, TRANSPORT_FUELS_DATA_FILE_PATH = TRANSPORT_FUELS_DATA_FILE_PATH)
    
    passenger_sales_result = None
    freight_sales_result = None
    if RUN_PASSENGER_SALES:
        passenger_output = PASSENGER_SALES_OUTPUT or f"../results/passenger_sales_{economy}_{original_scenario}.csv"
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
        )
    if RUN_FREIGHT_SALES:
        freight_output = FREIGHT_SALES_OUTPUT or f"../results/freight_sales_{economy}_{original_scenario}.csv"
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
        )
    
    L = connect_to_leap()
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
    if LOAD_HALFWAY_CHECKPOINT:
        leap_export_df = pd.read_pickle("../intermediate_data/export_df_checkpoint.pkl")
    else:
        leap_export_df.to_pickle("../intermediate_data/export_df_checkpoint.pkl")
    
    if LOAD_THREEQUART_WAY_CHECKPOINT:
        leap_export_df = pd.read_pickle("../intermediate_data/export_df_checkpoint2.pkl")
        export_df_for_viewing = pd.read_pickle("../intermediate_data/export_df_for_viewing_checkpoint2.pkl")
    else:
        #do validation and finalisation
        leap_export_df = validate_and_fix_shares_normalise_to_one(leap_export_df,EXAMPLE_SAMPLE_SIZE=5)
        
        #create current accounts scenario
        leap_export_df = separate_current_accounts_from_scenario(leap_export_df, base_year=base_year, scenario=new_scenario)
        
        leap_export_df = finalise_export_df(
            leap_export_df, scenario=new_scenario, region=region, base_year=base_year, final_year=final_year
        )
        validate_final_energy_use_for_base_year_equals_esto_totals(economy, original_scenario,new_scenario, base_year, final_year, leap_export_df, TRANSPORT_ESTO_BALANCES_PATH, TRANSPORT_ROOT)
        print("\n=== Transport data successfully filled into LEAP. ===\n")
        
        leap_export_df, export_df_for_viewing = convert_values_to_expressions(leap_export_df)
        
        leap_export_df.to_pickle("../intermediate_data/export_df_checkpoint2.pkl")
        export_df_for_viewing.to_pickle("../intermediate_data/export_df_for_viewing_checkpoint2.pkl")
    
    
    if LOAD_EXPORT_DF_CHECKPOINT:
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
            leap_export_df, export_df_for_viewing = join_and_check_import_structure_matches_export_structure(import_filename, leap_export_df, export_df_for_viewing, scenario=new_scenario, region=region, STRICT_CHECKS=False)
        
        save_export_files(
            leap_export_df, export_df_for_viewing, export_filename, base_year, final_year, model_name
        )
    
    if SET_VARS_IN_LEAP_USING_COM:
        write_export_df_to_leap(L, leap_export_df)
    print("\n=== Transport data loading process completed. ===\n")

#%%
# ------------------------------------------------------------
# Optional: run directly
# ------------------------------------------------------------

transport_model_path = r"../data/USA transport file.xlsx"
transport_economy = "20_USA"
transport_scenario = "Target"
transport_region = "Region 1"
transport_base_year = 2022
transport_final_year = 2060
transport_model_name = "USA transport"
transport_export_path = "../results/USA_transport_leap_export_Target.xlsx"
transport_import_path = "../data/import_files/USA_transport_leap_import_Target.xlsx"
transport_esto_balances_path = '../data/all transport balances data.xlsx'
transport_fuels_path = '../data/USA fuels model output.csv'
survival_profile_path = "../data/lifecycle_profiles/vehicle_survival_modified.xlsx"
vintage_profile_path = "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx"
passenger_sales_output = "../results/passenger_sales_USA_Target.csv"
freight_sales_output = "../results/freight_sales_USA_Target.csv"
#INPUT CREATION VARS
RUN_INPUT_CREATION = True
RUN_PASSENGER_SALES = True
RUN_FREIGHT_SALES = True
#RECONCILIATION VARS
RUN_RECONCILIATION = False#this will need to have curret accounts made for it.
APPLY_ADJUSTMENTS_TO_FUTURE_YEARS = True
REPORT_ADJUSTMENT_CHANGES = True
DATE_ID = datetime.now().strftime("%Y%m%d")
if __name__ == "__main__" and (RUN_INPUT_CREATION or RUN_RECONCILIATION):
    pd.options.display.float_format = "{:,.3f}".format
    if RUN_INPUT_CREATION:
        list_all_measures()

        load_transport_into_leap(
            transport_model_excel_path=transport_model_path,
            economy=transport_economy,
            original_scenario=transport_scenario,
            new_scenario=transport_scenario,
            region=transport_region,
            diagnose_method='all',
            base_year=transport_base_year,
            final_year=transport_final_year,
            model_name=transport_model_name,
            CHECK_BRANCHES_IN_LEAP_USING_COM=True,
            SET_VARS_IN_LEAP_USING_COM=True,
            AUTO_SET_MISSING_BRANCHES=False,
            export_filename=transport_export_path,
            import_filename=transport_import_path,
            TRANSPORT_ESTO_BALANCES_PATH=transport_esto_balances_path,
            TRANSPORT_FUELS_DATA_FILE_PATH=transport_fuels_path,
            TRANSPORT_ROOT=r"Demand",
            #checkpoint/loading options
            LOAD_INPUT_CHECKPOINT=False,
            LOAD_HALFWAY_CHECKPOINT=False,
            LOAD_THREEQUART_WAY_CHECKPOINT=False,
            LOAD_EXPORT_DF_CHECKPOINT=False,
            MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE=True,
            #related to passenger sales calculation
            RUN_PASSENGER_SALES=RUN_PASSENGER_SALES,
            RUN_FREIGHT_SALES=RUN_FREIGHT_SALES,
            SURVIVAL_PROFILE_PATH=survival_profile_path,
            VINTAGE_PROFILE_PATH=vintage_profile_path,
            PASSENGER_SALES_OUTPUT=passenger_sales_output,
            FREIGHT_SALES_OUTPUT=freight_sales_output,
            PASSENGER_PLOT=False,
        )
    if RUN_RECONCILIATION:
        esto_to_leap_mapping=ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP
        unmappable_branches=UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT
        all_leap_branches=ALL_LEAP_BRANCHES_TRANSPORT
        analysis_type_lookup=LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP.get
        run_transport_reconciliation(
        apply_adjustments_to_future_years=APPLY_ADJUSTMENTS_TO_FUTURE_YEARS,
        report_adjustment_changes=REPORT_ADJUSTMENT_CHANGES,
        date_id=DATE_ID,
        transport_esto_balances_path=transport_esto_balances_path,
        transport_export_path=transport_export_path,
        economy=transport_economy,
        base_year=transport_base_year,
        final_year=transport_final_year,
        scenario=transport_scenario,
        model_name=transport_model_name,
        unmappable_branches=unmappable_branches,
        analysis_type_lookup=analysis_type_lookup,
        all_leap_branches=all_leap_branches,
        esto_to_leap_mapping=esto_to_leap_mapping,
        set_vars_in_leap_using_com=True,
        )
    
#%%

# from branch_mappings import ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,     UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT, SHORTNAME_TO_LEAP_BRANCHES, ALL_LEAP_BRANCHES_TRANSPORT
# from measure_catalog import LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP
# from energy_use_reconciliation import reconcile_energy_use, build_branch_rules_from_mapping
# from energy_use_reconciliation_road import build_transport_esto_energy_totals
# from esto_data import extract_esto_energy_use_for_leap_branches
# import pandas as pd
# export_df = pd.read_excel("../results/USA_transport_leap_export_Target.xlsx")
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
