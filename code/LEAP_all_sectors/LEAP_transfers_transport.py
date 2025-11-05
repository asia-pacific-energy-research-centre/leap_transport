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
    ensure_activity_levels, create_leap_data_log, log_leap_data, save_leap_data_log, validate_shares, build_expression_from_mapping, extract_other_type_rows_from_esto_and_insert_into_transport_df
)
from LEAP_transfers_transport_MAPPINGS import LEAP_BRANCH_TO_SOURCE_MAP, SHORTNAME_TO_LEAP_BRANCHES, LEAP_MEASURE_CONFIG
from LEAP_tranposrt_measures_config import calculate_sales, process_measures_for_leap, list_all_measures
from LEAP_transfers_transport_excel import create_leap_import_file

from LEAP_transfers_transport_MAPPINGS import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT
)
from LEAP_BRANCH_TO_EXPRESSION_MAPPING import LEAP_BRANCH_TO_EXPRESSION_MAPPING

from basic_mappings import ESTO_TRANSPORT_SECTOR_TUPLES,add_fuel_column

from LEAP_mappings_validation import validate_all_mappings_with_measures, validate_final_energy_use_for_base_year_equals_esto_totals

# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------
def prepare_input_data(excel_path, economy, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx'):
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
    df, share_report = validate_shares(df, tolerance=0.01, auto_correct=True, road_only=True)

    #and create a non road medium which just uses the sum of Activity and ignores the other measures.
    non_road_df = df[df["Medium"] != "road"].copy()
    non_road_df["Medium"] = "non road"
    non_road_df = non_road_df[["Scenario", 'Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Activity']].groupby(["Scenario", 'Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'])['Activity'].sum().reset_index()
    df = pd.concat([df, non_road_df], ignore_index=True)
    
    df = extract_other_type_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, TRANSPORT_ESTO_BALANCES_PATH)

    # Optionally save or print the report
    share_report.to_csv("../../results/share_validation_report.csv", index=False)
    return df

def setup_leap_environment():
    """Connect to LEAP and ensure Activity Levels are initialized."""
    L = connect_to_leap()
    # ensure_activity_levels(L)#dont kow wat the point of this was
    # breakpoint()
    return L

def process_branch_mapping(leap_tuple, src_tuple, TRANSPORT_ROOT=r"Demand\Transport"):
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


def define_value_based_on_src_tuple(meta_values, src_tuple):
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    for col in ['LEAP_units', 'LEAP_Scale', 'LEAP_Per']:
        val = meta_values.get(col)
        if val is not None and isinstance(val, str) and '$' in val:
            # extract the options. if there are multiple $'s throw an error, code is not designed for that
            parts = val.split('$')
            if len(parts) != 2:
                raise ValueError(f"Unexpected format for metadata value: {val}")
            #now we have special code based on what the pklaceholder is
            if val == 'Passenger-km$Tonne-km':
                if ttype == 'passenger':
                    resolved_value = 'Passenger-km'
                elif ttype == 'freight':
                    resolved_value = 'Tonne-km'
                else:
                    raise ValueError(f"Unexpected ttype for resolving Passenger-km$Tonne-km: {ttype}")
                meta_values[col] = resolved_value
            else:
                raise ValueError(f"Unknown placeholder in metadata value: {val}")
    return meta_values

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
        # if drive == 'BEV' or drive == 'bev':
        #     breakpoint()#looking for non saleshare measures that have % scale
        # --- LOG: record prepared data into leap_data_log (if requested) ---
        if save_log:
            before_len = len(leap_data_log) if (leap_data_log is not None) else 0
            leap_data_log = log_leap_data(leap_data_log, leap_tuple, src_tuple, branch_path, measure, df_m)
            # attach LEAP metadata (units/scale/per) from the measure config to the newly appended rows
            try:
                if leap_data_log is not None:
                    after_len = len(leap_data_log)
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
                        leap_data_log.loc[leap_data_log.index[before_len:after_len], 'Units'] = meta_values['LEAP_units']
                        leap_data_log.loc[leap_data_log.index[before_len:after_len], 'Scale'] = meta_values['LEAP_Scale']
                        leap_data_log.loc[leap_data_log.index[before_len:after_len], 'Per...'] = meta_values['LEAP_Per']
            except Exception as e:
                breakpoint()
                print(f"[WARN] Failed to attach LEAP metadata for {measure} on {branch_path}: {e}")
        
        expr = build_expression_from_mapping(leap_tuple, df_m, measure)
        
        if expr and SET_VARS_IN_LEAP_USING_COM:
            success = safe_set_variable(L.Branch(branch_path), measure, expr, branch_path)
            written_this_branch += 1 if success else 0
            missing_variables += 0 if success else 1
        elif expr:
            print(f"[INFO] Prepared but not applied: {measure} on {branch_path}")
            written_this_branch += 1
    return written_this_branch, missing_variables, leap_data_log


# def write_measures_to_leap(
#     L, df_copy, leap_tuple, src_tuple, branch_path, filtered_measure_config,
#     shortname, source_cols_for_grouping, save_log, leap_data_log, SET_VARS_IN_LEAP_USING_COM
# ):
#     """Process measures for a branch and write them into LEAP."""
#     ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
#     processed_measures = process_measures_for_leap(
#         df_copy, filtered_measure_config, shortname, source_cols_for_grouping, ttype, medium, vtype, drive, fuel
#     )
#     written_this_branch = missing_variables = 0
#     for measure, df_m in processed_measures.items():
#         if save_log:
#             leap_data_log = log_leap_data(leap_data_log, leap_tuple, src_tuple, branch_path, measure, df_m)
        
#         expr = build_expression_from_mapping(leap_tuple, df_m, measure)
        
#         if expr and SET_VARS_IN_LEAP_USING_COM:
#             success = safe_set_variable(L.Branch(branch_path), measure, expr, branch_path)
#             written_this_branch += 1 if success else 0
#             missing_variables += 0 if success else 1
#         elif expr:
#             print(f"[INFO] Prepared but not applied: {measure} on {branch_path}")
#             written_this_branch += 1
#     return written_this_branch, missing_variables, leap_data_log


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
            export_df = create_leap_import_file(leap_data_log, import_filename)
            if export_df is not None:
                print(f"LEAP import file created at: {import_filename}")
                return export_df
    print("No LEAP import file created.")
    return None


# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap_v3(
    excel_path,
    economy,
    scenario,
    diagnose_method='first_of_each_length',
    base_year=2022,
    final_year=2060,
    save_log=True,
    log_filename="../../results/leap_data_log.xlsx",
    SET_VARS_IN_LEAP_USING_COM=True,
    create_import_files=False,
    import_filename="../../results/leap_import.xlsx",
    TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx',
    TRANSPORT_ROOT = r"Demand\Transport"
):
    """Main orchestrator for LEAP transport data loading."""
    results = validate_all_mappings_with_measures(
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAPPING,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
        ESTO_TRANSPORT_SECTOR_TUPLES,
        UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
        EXAMPLE_SAMPLE_SIZE=1000
    )
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
        if src_tuple is None:
            #we are looking at one of the nonsepcified or pipeline mappings. Just ignore them for now
            print(f"[INFO] Skipping LEAP branch {leap_tuple} as it has no source mapping. Creating placeholder entry in the log.")
            # create a lightweight placeholder row so we record the missing mapping and can process later
            if leap_data_log is not None:
                if len(leap_tuple) == 2:
                    ttype, medium = leap_tuple
                elif len(leap_tuple) == 1:
                    ttype = leap_tuple[0]
                    medium = None
                else:
                    raise ValueError(f"Unexpected leap_tuple length for {leap_tuple}")
                vtype, drive, fuel = (None, None, None)
                source_cols_for_grouping = ['Date', 'Transport Type'] + (['Medium'] if medium else [])
                #todo we willa need to create new rows in the transport dataset for these branches. we can set their energy use to be the same as in esto, then have 1-to-1 intensity and activity level measures.

                leap_ttype, leap_vtype, leap_drive, leap_fuel = (list(leap_tuple) + [None] * (4 - len(leap_tuple)))[:4]
                branch_path = f"{TRANSPORT_ROOT}\\{leap_ttype}" + "".join(
                    f"\\{x}" for x in [leap_vtype, leap_drive, leap_fuel] if x
                )
            else:
                total_skipped += 1
                continue
        else:
            ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping = process_branch_mapping(leap_tuple, src_tuple, TRANSPORT_ROOT=TRANSPORT_ROOT)

        # if 'Biodiesel' in branch_path:
        #     breakpoint()
        if SET_VARS_IN_LEAP_USING_COM:
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
        if SET_VARS_IN_LEAP_USING_COM:
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
        # if 'Biodiesel' in branch_path:
        #     breakpoint()
        total_written += written
        missing_variables += missing_vars
        if written == 0:
            total_skipped += 1
    breakpoint()#how does it lookright now? I think we watned to do soemthign with the dataset somehow? maybe it was to isnett all the units and stuff
    export_df = summarize_and_export(
        total_written, total_skipped, missing_branches, missing_variables,
        leap_data_log, save_log, log_filename, create_import_files, import_filename
    )
    breakpoint()    
    validate_final_energy_use_for_base_year_equals_esto_totals(economy, scenario, base_year, final_year, export_df, TRANSPORT_ESTO_BALANCES_PATH)
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
        scenario='Reference',
        diagnose_method='all',
        base_year=2022,
        final_year=2060,
        save_log=True,
        log_filename="../../results/BD_transport_leap_data_log.xlsx",
        SET_VARS_IN_LEAP_USING_COM=True,
        create_import_files=True,
        import_filename="../../results/BD_transport_leap_import.xlsx",
        TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx',
        TRANSPORT_ROOT = r"Demand\Transport"
    )
#%%