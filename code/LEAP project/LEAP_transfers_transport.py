
#%%
# ============================================================
# LEAP_transfers_transport_loader.py
# ============================================================
# Main logic for processing and loading transport data into LEAP.
# Depends on LEAP_transfers_transport_core.py and mappings/config files.
# ============================================================

import pandas as pd
from LEAP_transfers_transport_core import (
    connect_to_leap, safe_set_variable,
    diagnose_leap_branch, normalize_sales_shares, analyze_data_quality,
    ensure_activity_levels, create_leap_data_log, log_leap_data, save_leap_data_log, validate_shares, build_expression_from_mapping
)
from LEAP_transfers_transport_MAPPINGS import LEAP_BRANCH_TO_SOURCE_MAP, SHORTNAME_TO_LEAP_BRANCHES, add_fuel_column, LEAP_MEASURE_CONFIG
from LEAP_tranposrt_measures_config import calculate_sales, process_measures_for_leap, list_all_measures
from LEAP_transfers_transport_excel import create_leap_import_file


# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------
def prepare_input_data(excel_path, economy, base_year, final_year):
    """Load and preprocess transport data for a specific economy."""
    print(f"\n=== Loading Transport Data for {economy} ===")
    df = pd.read_excel(excel_path)
    df = df[df["Economy"] == economy]
    df = df[(df["Date"] >= base_year) & (df["Date"] <= final_year)]
    df = add_fuel_column(df)
    df.loc[df["Medium"] != "road", ["Stocks", 'Vehicle_sales_share']] = 0
    df = calculate_sales(df)
    analyze_data_quality(df)
    df = normalize_sales_shares(df)
    # Validate consistency of shares
    df, share_report = validate_shares(df, tolerance=0.01, auto_correct=True)

    #and create a non road medium which just uses the sum of Activity and ignores the other measures.
    non_road_df = df[df["Medium"] != "road"].copy()
    non_road_df["Medium"] = "non road"
    non_road_df = non_road_df[["Scenario", 'Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Activity']].groupby(["Scenario", 'Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'])['Activity'].sum().reset_index()
    df = pd.concat([df, non_road_df], ignore_index=True)

    # Optionally save or print the report
    share_report.to_csv("../../results/share_validation_report.csv", index=False)
    return df


def setup_leap_environment():
    """Connect to LEAP and ensure Activity Levels are initialized."""
    L = connect_to_leap()
    ensure_activity_levels(L)
    return L


def process_branch_mapping(leap_tuple, src_tuple):
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
    else:
        ttype, medium = src_tuple
        source_cols_for_grouping = ['Date', 'Transport Type', 'Medium']

    leap_ttype, leap_vtype, leap_drive, leap_fuel = (list(leap_tuple) + [None] * (4 - len(leap_tuple)))[:4]
    branch_path = f"Demand\\{leap_ttype}" + "".join(
        f"\\{x}" for x in [leap_vtype, leap_drive, leap_fuel] if x
    )
    return ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping


def write_measures_to_leap(
    L, df_copy, leap_tuple, src_tuple, branch_path, filtered_measure_config,
    shortname, source_cols_for_grouping, save_log, leap_data_log, SET_VARS_IN_LEAP_USING_COM
):
    """Process measures for a branch and write them into LEAP."""
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    processed_measures = process_measures_for_leap(
        df_copy, filtered_measure_config, shortname, source_cols_for_grouping, ttype, medium, vtype, drive, fuel
    )
    written_this_branch = missing_variables = 0
    for measure, df_m in processed_measures.items():
        if save_log:
            leap_data_log = log_leap_data(leap_data_log, leap_tuple, src_tuple, branch_path, measure, df_m)
        
        expr = build_expression_from_mapping(leap_tuple, df_m, measure)
        
        if expr and SET_VARS_IN_LEAP_USING_COM:
            success = safe_set_variable(L.Branch(branch_path), measure, expr, branch_path)
            written_this_branch += 1 if success else 0
            missing_variables += 0 if success else 1
        elif expr:
            print(f"[INFO] Prepared but not applied: {measure} on {branch_path}")
            written_this_branch += 1
    return written_this_branch, missing_variables, leap_data_log


def summarize_and_export(total_written, total_skipped, missing_branches, missing_variables,
                         leap_data_log, save_log, log_filename, create_import_files, import_filename):
    """Print summary and save logs / import files."""
    print("\n=== Summary ===")
    print(f"✅ Variables written: {total_written}")
    print(f"⚠️  Skipped (no data or invalid tuples): {total_skipped}")
    print(f"❌ Missing LEAP branches: {missing_branches}")
    print(f"❌ Missing variables: {missing_variables}")
    print("================\n")
    if save_log and leap_data_log is not None and not leap_data_log.empty:
        print(f"Saving LEAP data log to {log_filename}...")
        save_leap_data_log(leap_data_log, log_filename)
        if create_import_files:
            print("\n=== Exporting LEAP import-compatible files ===")
            create_leap_import_file(leap_data_log, import_filename)


# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap_v3(
    excel_path,
    economy,
    validate=True,
    diagnose_method='first_of_each_length',
    base_year=2022,
    final_year=2060,
    save_log=True,
    log_filename="../../results/leap_data_log.xlsx",
    SET_VARS_IN_LEAP_USING_COM=True,
    create_import_files=False,
    import_filename="../../results/leap_import.xlsx"
):
    """Main orchestrator for LEAP transport data loading."""
    df = prepare_input_data(excel_path, economy, base_year, final_year)
    L = setup_leap_environment()
    leap_data_log = create_leap_data_log() if save_log else None

    total_written = total_skipped = missing_branches = missing_variables = 0
    first_branch_diagnosed = False
    first_of_each_length_diagnosed = set()

    for leap_tuple, src_tuple in LEAP_BRANCH_TO_SOURCE_MAP.items():
        df_copy = df.copy()
        if df_copy.empty:
            total_skipped += 1
            continue

        ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping = process_branch_mapping(leap_tuple, src_tuple)

        try:
            branch = L.Branch(branch_path)
        except Exception:
            print(f"[WARN] Missing LEAP branch: {branch_path}")
            missing_branches += 1
            continue

        expected_shortname = {k for k, v in SHORTNAME_TO_LEAP_BRANCHES.items() if leap_tuple in v}
        if len(expected_shortname) != 1:
            print(f"[ERROR] Could not identify unique measure config for {branch_path}")
            total_skipped += 1
            continue

        shortname = expected_shortname.pop()
        filtered_measure_config = LEAP_MEASURE_CONFIG[shortname]
        expected_measures = set(filtered_measure_config.keys())

        # Optional diagnostics
        if diagnose_method == 'first_branch_diagnosed' and not first_branch_diagnosed:
            diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures)
            first_branch_diagnosed = True
        elif diagnose_method == 'first_of_each_length' and len(leap_tuple) not in first_of_each_length_diagnosed:
            diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures)
            first_of_each_length_diagnosed.add(len(leap_tuple))
        elif diagnose_method == 'all':
            diagnose_leap_branch(L, branch_path, leap_tuple, expected_measures)

        written, missing_vars, leap_data_log = write_measures_to_leap(
            L, df_copy, leap_tuple, src_tuple, branch_path, filtered_measure_config,
            shortname, source_cols_for_grouping, save_log, leap_data_log, SET_VARS_IN_LEAP_USING_COM
        )

        total_written += written
        missing_variables += missing_vars
        if written == 0:
            total_skipped += 1

    summarize_and_export(
        total_written, total_skipped, missing_branches, missing_variables,
        leap_data_log, save_log, log_filename, create_import_files, import_filename
    )

    print("\n=== Transport data successfully filled into LEAP. ===\n")

#%%
# ------------------------------------------------------------
# Optional: run directly
# ------------------------------------------------------------
if __name__ == "__main__":
    pd.options.display.float_format = "{:,.3f}".format
    list_all_measures()
    load_transport_into_leap_v3(
        excel_path=r"../../data/bd dummy transport file - 2100.xlsx",
        economy="02_BD",
        validate=True,
        diagnose_method='all',
        base_year=2022,
        final_year=2060,
        save_log=True,
        log_filename="../../results/BD_transport_leap_data_log.xlsx",
        SET_VARS_IN_LEAP_USING_COM=False,
        create_import_files=True,
        import_filename="../../results/BD_transport_leap_import.xlsx"
    )
#%%