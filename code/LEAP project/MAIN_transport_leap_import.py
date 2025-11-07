#%%
# ============================================================
# transport_leap_import.py
# ============================================================
# Main logic for processing and loading transport data into LEAP.
# Depends on transport_leap_core.py and mapping/config modules.
# ============================================================

import pandas as pd

from transport_leap_core import (
    connect_to_leap,
    safe_set_variable,
    diagnose_leap_branch,
    normalize_sales_shares,
    analyze_data_quality,
    ensure_activity_levels,
    create_leap_data_log,
    log_leap_data,
    save_leap_data_log,
    build_expression_from_mapping,
    define_value_based_on_src_tuple
)
from transport_branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    create_new_source_rows_based_on_combinations,
    create_new_source_rows_based_on_proxies_with_no_activity,
)
from transport_measure_catalog import list_all_measures
from transport_measure_processing import process_measures_for_leap
from transport_preprocessing import (
    allocate_fuel_alternatives_energy_and_activity,
    calculate_sales,
)
from transport_excel_io import summarize_and_create_export_df, save_export_file
from branch_expression_mapping import LEAP_BRANCH_TO_EXPRESSION_MAPPING
from esto_transport_data import (
    extract_other_type_rows_from_esto_and_insert_into_transport_df,
)

from basic_mappings import (
    ESTO_TRANSPORT_SECTOR_TUPLES,
    add_fuel_column,
    EXPECTED_COLS_IN_SOURCE,
)

from transport_mappings_validation import (
    validate_shares,
    validate_all_mappings_with_measures,
    validate_and_fix_shares_normalise_to_one,
    validate_final_energy_use_for_base_year_equals_esto_totals,
)
import os
# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------

def prepare_input_data(excel_path, economy, scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx', LOAD_CHECKPOINT=False):
    """Load and preprocess transport data for a specific economy."""    
    print(f"\n=== Loading Transport Data for {economy} ===")
    
    # Check for checkpoint file
    checkpoint_filename = f"../../intermediate_data/transport_data_{economy}_{scenario}_{base_year}_{final_year}.csv"
    if LOAD_CHECKPOINT and os.path.exists(checkpoint_filename):
        print(f"Loading data from checkpoint: {checkpoint_filename}")
        df = pd.read_csv(checkpoint_filename)
        return df
    
    df = pd.read_excel(excel_path)
    df = df[df["Economy"] == economy]
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
    
    df = allocate_fuel_alternatives_energy_and_activity(df, economy)
    
    new_rows1, rows_to_remove = create_new_source_rows_based_on_combinations(df)
    new_rows2 = create_new_source_rows_based_on_proxies_with_no_activity(df)
    if not new_rows1.empty or not new_rows2.empty:
        new_rows = pd.concat([new_rows1, new_rows2], ignore_index=True)
        df = pd.concat([df, new_rows], ignore_index=True)
        
        df = df.merge(rows_to_remove.drop_duplicates(), how='outer', indicator=True)
        df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        breakpoint()
        raise ValueError("No new source rows were created from proxies; check the mapping and source data just in case.")
            
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
    
    # Save checkpoint file
    os.makedirs("../../intermediate_data", exist_ok=True)
    df.to_csv(checkpoint_filename, index=False)
    print(f"Saved checkpoint: {checkpoint_filename}")
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

def write_measures_to_leap(
    L, df_copy, leap_tuple, src_tuple, branch_path, filtered_measure_config,
    shortname, source_cols_for_grouping, save_log, leap_data_log, SET_VARS_IN_LEAP_USING_COM
):
    """Process measures for a branch and write them into LEAP."""
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    processed_measures = process_measures_for_leap(
        df_copy, filtered_measure_config, shortname, source_cols_for_grouping, ttype, medium, vtype, drive, fuel, src_tuple
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


# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap(
    excel_path,
    economy,
    scenario,
    diagnose_method='first_of_each_length',
    base_year=2022,
    final_year=2060,
    save_log=True,
    log_filename="../../results/leap_data_log.xlsx",
    SET_VARS_IN_LEAP_USING_COM=True,
    SAVE_IMPORT_FILE=True,
    import_filename="../../results/leap_import.xlsx",
    TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx',
    TRANSPORT_ROOT = r"Demand\Transport",
    LOAD_CHECKPOINT=False
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
    df = prepare_input_data(excel_path, economy, scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx', LOAD_CHECKPOINT=LOAD_CHECKPOINT)
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

        # if branch_path =='Demand\Transport\Passenger non road\Air\Hydrogen' or 'Air' in branch_path:
        #     breakpoint()#check why intensity is not being set for air transport hydrogen
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
     
    
    if save_log and leap_data_log is not None and not leap_data_log.empty:
        save_leap_data_log(leap_data_log, log_filename, log_tuple=(total_written, total_skipped, missing_branches, missing_variables))
        
    export_df = summarize_and_create_export_df(
        leap_data_log, scenario="Current Accounts", region="Region 1", method="Interp", base_year=2022, final_year=2060
    )
    validate_final_energy_use_for_base_year_equals_esto_totals(economy, scenario, base_year, final_year, export_df, TRANSPORT_ESTO_BALANCES_PATH)
    print("\n=== Transport data successfully filled into LEAP. ===\n")
    # breakpoint()
    #save export df to a pickle for later use
    # export_df.to_pickle('export_df.pkl')
    export_df = validate_and_fix_shares_normalise_to_one(export_df, base_year, LEAP_BRANCH_TO_EXPRESSION_MAPPING, EXAMPLE_SAMPLE_SIZE=5)
    
    # breakpoint()#how does it lookright now? I think we watned to do soemthign with the dataset somehow? maybe it was to isnett all the units and stuff
    if SAVE_IMPORT_FILE:
        save_export_file(
            export_df, leap_data_log, import_filename, base_year, final_year
        )
    
    
#%%
# ------------------------------------------------------------
# Optional: run directly
# ------------------------------------------------------------
RUN = True
if __name__ == "__main__" and RUN:
    pd.options.display.float_format = "{:,.3f}".format
    list_all_measures()
    load_transport_into_leap(
        excel_path=r"../../data/bd dummy transport file - 2100.xlsx",
        economy="02_BD",
        scenario='Reference',
        diagnose_method='all',
        base_year=2022,
        final_year=2060,
        save_log=True,
        log_filename="../../results/BD_transport_leap_data_log.xlsx",
        SET_VARS_IN_LEAP_USING_COM=True,
        SAVE_IMPORT_FILE=True,
        import_filename="../../results/BD_transport_leap_import.xlsx",
        TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx',
        TRANSPORT_ROOT = r"Demand\Transport",
        LOAD_CHECKPOINT=False
    )
#%%

# export_df =  pd.read_pickle('export_df.pkl')
# base_year = 2022
# export_df = validate_and_fix_shares_normalise_to_one(export_df, base_year, LEAP_BRANCH_TO_EXPRESSION_MAPPING, EXAMPLE_SAMPLE_SIZE=5)
