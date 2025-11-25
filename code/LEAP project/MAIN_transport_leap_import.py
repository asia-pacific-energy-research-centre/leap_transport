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
    diagnose_measures_in_leap_branch,
    ensure_branch_exists,
    safe_set_variable,
    # diagnose_leap_branch,
    create_leap_export_df,
    write_row_to_leap_export_df,
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
    normalize_and_calculate_shares)
from transport_excel_io import finalise_export_df, save_export_files,join_and_check_import_structure_matches_export_structure
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
    validate_all_mappings_with_measures,
    validate_and_fix_shares_normalise_to_one,
    validate_final_energy_use_for_base_year_equals_esto_totals,
)
import os
# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------

def prepare_input_data(transport_model_excel_path, economy, scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx', LOAD_CHECKPOINT=False, TRANSPORT_FUELS_DATA_FILE_PATH = '../../data/USA fuels model output.csv'):
    """Load and preprocess transport data for a specific economy."""    
    print(f"\n=== Loading Transport Data for {economy} ===")
    
    # Check for checkpoint file
    checkpoint_filename = f"../../intermediate_data/transport_data_{economy}_{scenario}_{base_year}_{final_year}.pkl"
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
        raise ValueError("No new source rows were created from combinations; check the mapping and source data just in case.")
    if new_rows2.empty:
        raise ValueError("No new source rows were created from proxies; check the mapping and source data just in case.")
    
    #check for duplicates
    duplicates = df.duplicated(subset=['Date', 'Economy', 'Scenario', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'])
    if duplicates.any():
        breakpoint()
        #save to ../../data/errors/duplicate_source_rows.csv
        df[duplicates].to_csv('../../data/errors/duplicate_source_rows.csv', index=False)
        raise ValueError("Duplicates found in source data after adding new rows based on combinations and proxies; see ../../data/errors/duplicate_source_rows.csv for details.")
     
    df = calculate_sales(df)
    df = normalize_and_calculate_shares(df)
    
    df = extract_other_type_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, scenario, TRANSPORT_ESTO_BALANCES_PATH)
    
    # Save checkpoint file
    os.makedirs("../../intermediate_data", exist_ok=True)
    df.to_pickle(checkpoint_filename)
    print(f"Saved checkpoint: {checkpoint_filename}")
    return df

def setup_leap_environment():
    """Connect to LEAP and ensure Activity Levels are initialized."""
    print("\n=== Setting up LEAP Environment ===")
    L = connect_to_leap()
    # ensure_activity_levels(L)#dont kow wat the point of this was
    # breakpoint()
    return L

def process_branch_mapping(leap_tuple, src_tuple, TRANSPORT_ROOT=r"Demand"):
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

def write_measures_to_export_df_for_current_branch(
    df_copy, leap_tuple, src_tuple, branch_path, filtered_measure_config,
    shortname, source_cols_for_grouping, leap_export_df
):
    """Process measures for a branch and write them into LEAP."""
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    
    processed_measures = process_measures_for_leap(
        df_copy, filtered_measure_config, shortname, source_cols_for_grouping, ttype, medium, vtype, drive, fuel, src_tuple
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
    print("\n=== Setting variables in LEAP via COM interface ===")
    total_written = 0
    total_missing_variables = 0
    for idx, row in leap_export_df.iterrows():
        branch_path = row['Branch Path']
        measure = row['Variable']
        expr = row['Expression']
        
        success = safe_set_variable(L.Branch(branch_path), measure, expr, branch_path)
        if success:
            total_written += 1
        else:
            total_missing_variables += 1
    print(f"\n=== Finished setting variables in LEAP. Total written: {total_written}, Missing variables: {total_missing_variables} ===\n")

def process_single_leap_mapping(
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
    AUTO_SET_MISSING_BRANCHES
):
    """Process one (leap_tuple, src_tuple) mapping and return updated state.
    Returns updated leap_export_df.
    """
    df_copy = df.copy()
    ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping = process_branch_mapping(
        leap_tuple, src_tuple, TRANSPORT_ROOT=TRANSPORT_ROOT
    )

    expected_shortname = {k for k, v in SHORTNAME_TO_LEAP_BRANCHES.items() if leap_tuple in v}
    if len(expected_shortname) != 1:
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

    leap_export_df = write_measures_to_export_df_for_current_branch(
        df_copy,
        leap_tuple,
        src_tuple,
        branch_path,
        filtered_measure_config,
        shortname,
        source_cols_for_grouping,
        leap_export_df
    )

    return leap_export_df
                                                                              
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
    export_filename="../../results/leap_export.xlsx",
    import_filename="../../data/import_files/leap_import.xlsx",
    TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx',
    TRANSPORT_FUELS_DATA_FILE_PATH = '../../data/USA fuels model output.csv',
    TRANSPORT_ROOT = r"Demand",
    LOAD_INPUT_CHECKPOINT=False,
    LOAD_HALFWAY_CHECKPOINT=False,
    LOAD_THREEQUART_WAY_CHECKPOINT=False,
    LOAD_EXPORT_DF_CHECKPOINT=False,
    MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE=False
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
    
    df = prepare_input_data(transport_model_excel_path, economy, original_scenario, base_year, final_year, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx', LOAD_CHECKPOINT=LOAD_INPUT_CHECKPOINT, TRANSPORT_FUELS_DATA_FILE_PATH = TRANSPORT_FUELS_DATA_FILE_PATH)
    
    L = setup_leap_environment()
    leap_export_df = create_leap_export_df()
    
    first_branch_diagnosed = False
    first_of_each_length_diagnosed = set()
    for leap_tuple, src_tuple in LEAP_BRANCH_TO_SOURCE_MAP.items():
        if LOAD_EXPORT_DF_CHECKPOINT or LOAD_HALFWAY_CHECKPOINT:
            break
        leap_export_df = process_single_leap_mapping(
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
            AUTO_SET_MISSING_BRANCHES=AUTO_SET_MISSING_BRANCHES
        )
        continue
    #save temporary export df checkpoint
    if LOAD_HALFWAY_CHECKPOINT:
        leap_export_df = pd.read_pickle("../../data/export_df_checkpoint.pkl")
    else:
        leap_export_df.to_pickle("../../data/export_df_checkpoint.pkl")
    
    
    if LOAD_THREEQUART_WAY_CHECKPOINT:
        leap_export_df = pd.read_pickle("../../data/export_df_checkpoint2.pkl")
        export_df_for_viewing = pd.read_pickle("../../data/export_df_for_viewing_checkpoint2.pkl")
    else:
        #do validation and finalisation
        leap_export_df = validate_and_fix_shares_normalise_to_one(leap_export_df,EXAMPLE_SAMPLE_SIZE=5)
        
        leap_export_df = finalise_export_df(
            leap_export_df, scenario=new_scenario, region=region, base_year=2022, final_year=2060
        )
        validate_final_energy_use_for_base_year_equals_esto_totals(economy, original_scenario,new_scenario, base_year, final_year, leap_export_df, TRANSPORT_ESTO_BALANCES_PATH, TRANSPORT_ROOT)
        print("\n=== Transport data successfully filled into LEAP. ===\n")
        
        leap_export_df, export_df_for_viewing = convert_values_to_expressions(leap_export_df)
        
        leap_export_df.to_pickle("../../data/export_df_checkpoint2.pkl")
        export_df_for_viewing.to_pickle("../../data/export_df_for_viewing_checkpoint2.pkl")
    
    
    if LOAD_EXPORT_DF_CHECKPOINT:
        leap_export_df = pd.read_excel(export_filename, sheet_name='LEAP')
        print(f"Loaded leap_export_df from checkpoint: {export_filename}")
    else:
        if MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE:
            leap_export_df, export_df_for_viewing = join_and_check_import_structure_matches_export_structure(import_filename, leap_export_df, export_df_for_viewing, STRICT_CHECKS=False)
        
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
RUN = True
if __name__ == "__main__" and RUN:
    pd.options.display.float_format = "{:,.3f}".format
    list_all_measures()
    load_transport_into_leap(
        transport_model_excel_path=r"../../data/USA transport file.xlsx",
        economy="20_USA",
        original_scenario='Target',
        new_scenario='Target',
        region="Region 1",
        diagnose_method='all',
        base_year=2022,
        final_year=2060,
        model_name="USA transport",
        CHECK_BRANCHES_IN_LEAP_USING_COM=True,
        SET_VARS_IN_LEAP_USING_COM=False,
        AUTO_SET_MISSING_BRANCHES=False,
        export_filename="../../results/USA_transport_leap_export_Target.xlsx",
        import_filename="../../data/import_files/USA_transport_leap_import_Target.xlsx",
        TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx',
        TRANSPORT_FUELS_DATA_FILE_PATH = '../../data/USA fuels model output.csv',
        TRANSPORT_ROOT = r"Demand",
        #make sure that if one of these is true the earlier ones are true too.  i.e. if LOAD_THREEQUART_WAY_CHECKPOINT is true then LOAD_HALFWAY_CHECKPOINT and LOAD_INPUT_CHECKPOINT must also be true.
        LOAD_INPUT_CHECKPOINT=True,
        LOAD_HALFWAY_CHECKPOINT=False,
        LOAD_THREEQUART_WAY_CHECKPOINT=False,
        LOAD_EXPORT_DF_CHECKPOINT=False,
        MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE=True
    )
#%%

# export_df =  pd.read_pickle('export_df.pkl')
# base_year = 2022
# export_df = validate_and_fix_shares_normalise_to_one(export_df, base_year, LEAP_BRANCH_TO_EXPRESSION_MAPPING, EXAMPLE_SAMPLE_SIZE=5)
