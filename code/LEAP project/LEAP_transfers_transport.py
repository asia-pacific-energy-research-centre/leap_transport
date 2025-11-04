
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
from LEAP_transfers_transport_MAPPINGS import LEAP_BRANCH_TO_SOURCE_MAP, SHORTNAME_TO_LEAP_BRANCHES, LEAP_MEASURE_CONFIG, extract_esto_sector_fuels_for_leap_branches
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
    df, share_report = validate_shares(df, tolerance=0.01, auto_correct=True)

    #and create a non road medium which just uses the sum of Activity and ignores the other measures.
    non_road_df = df[df["Medium"] != "road"].copy()
    non_road_df["Medium"] = "non road"
    non_road_df = non_road_df[["Scenario", 'Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Activity']].groupby(["Scenario", 'Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'])['Activity'].sum().reset_index()
    df = pd.concat([df, non_road_df], ignore_index=True)
    
    df = extract_other_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, TRANSPORT_ESTO_BALANCES_PATH)

    # Optionally save or print the report
    share_report.to_csv("../../results/share_validation_report.csv", index=False)
    return df

def extract_other_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx'):
    """Extract 'Other' shortname rows from ESTO and insert them into the transport dataframe."""
    
    #and insert the 'Other' shortname rows. These are those under the Other level 1 and level 2 in SHORTNAME_TO_LEAP_BRANCHES  and are basically rows that arent in this transport dataset because they were modelled separately. However to make it easy to use the same code to load them into LEAP we create rows for them here with activity levels equal to their enertgy use in the ESTO dataset and intensity=1. They will then have energy use = activity level * intensity = activity level = esto energy use. We can access their ESTO energy use from the ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP using extract_esto_sector_fuels_for_leap_branches(leap_branch_list) where leap_branch_list is the list of leap branches for the 'Other' shortnames
    
    #load esto dataset 
    esto_energy_use = pd.read_excel(TRANSPORT_ESTO_BALANCES_PATH) 
    #filter for the given economy, scenario and base year
    
    #filter for the given economy, scenario. We will extract data for the base year and then the data for all projected years. #todo is there any complciation with russia base year? also diod we even project these datas??
    esto_energy_use_filtered_base_year = esto_energy_use[
        (esto_energy_use['economy'] == economy) &
        (esto_energy_use['scenarios'] == 'reference') &
        (esto_energy_use['subtotal_layout'] == False)
    ][['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', base_year]]
    projected_years = [year for year in esto_energy_use.columns if isinstance(year, int) and year >= base_year and year <= final_year]
    breakpoint()#check it went ok. are the str/int types of?
    esto_energy_use_filtered = esto_energy_use[
        (esto_energy_use['economy'] == economy) &
        (esto_energy_use['scenarios'] == 'reference') &
        (esto_energy_use['subtotal_results'] == False)
    ][['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'] + projected_years]
    
    other_shortnames = [sn for sn in SHORTNAME_TO_LEAP_BRANCHES.keys() if sn.startswith('Other')]
    other_leap_branches = []
    for sn in other_shortnames:
        other_leap_branches.extend(SHORTNAME_TO_LEAP_BRANCHES[sn])
    def extract_esto_energy_use_for_leap_branches(leap_branches, esto_energy_use):
        #todo make sure this works with the validation def and this. 
        esto_sector_fuels_for_other = extract_esto_sector_fuels_for_leap_branches(leap_branches, ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP)
        other_rows = []
        for leap_branch, esto_rows in esto_sector_fuels_for_other.items():
            esto_rows_df_base_year = pd.DataFrame()#we will sum up all rows for this leap branch and insert their base year energy use into the transport df as activity level and energy use, with intensity =1
            esto_rows_df = pd.DataFrame()#we will sum up all rows for this leap branch and insert their values for all projected_years energy use into the transport df as activity level and energy use, with intensity =1

            for (subsector, fuel, subfuel) in esto_rows:
                #create new rows for df using the ESTO data, filtered for the (subsector, fuel, subfuel) values, eg. ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline").
                breakpoint()#check this works ok. worried that having only one col will cause issues
                esto_row_base_year = esto_energy_use_filtered_base_year[
                    (esto_energy_use_filtered_base_year['sub1sectors'] == subsector) &
                    (esto_energy_use_filtered_base_year['sub2sectors'] == 'x') &
                    (esto_energy_use_filtered_base_year['fuels'] == fuel) &
                    (esto_energy_use_filtered_base_year['subfuels'] == subfuel)
                ][base_year]
                esto_rows_df_base_year = pd.concat([esto_rows_df_base_year, esto_row_base_year], ignore_index=True)
                esto_row_projected_years = esto_energy_use_filtered[
                    (esto_energy_use_filtered['sub1sectors'] == subsector) &
                    (esto_energy_use_filtered['sub2sectors'] == 'x') &
                    (esto_energy_use_filtered['fuels'] == fuel) &
                    (esto_energy_use_filtered['subfuels'] == subfuel)
                ][projected_years]
                esto_rows_df = pd.concat([esto_rows_df, esto_row_projected_years], ignore_index=True)
            total_activity_level_base_year = esto_rows_df_base_year.sum().values[0]
            breakpoint()#wioll thius worlk ok if we are summing multiple cols? do they sumthe right way?
            total_activity_levels_projected_years = esto_rows_df.sum().values
        
        #create new row in df with this activity level and intensity =1
        df_new_rows = {
            'Economy': economy,
            'Scenario': 'Reference',
            'Date': [base_year] + projected_years,
            'Transport Type': leap_branch[0],
            'Medium': leap_branch[1] if len(leap_branch) > 1 else None,#todo is it ok if we make these None? even if it doesnt amtch the way it is in ther rest of the df?
            'Vehicle Type': leap_branch[2] if len(leap_branch) > 2 else None,
            'Drive': leap_branch[3] if len(leap_branch) > 3 else None,
            'Fuel': leap_branch[4] if len(leap_branch) > 4 else None,
            'Activity': [total_activity_level_base_year] + list(total_activity_levels_projected_years),
            'Intensity': 1,
            'Energy' : [total_activity_level_base_year] + list(total_activity_levels_projected_years)
        }
        breakpoint()#check this works
        other_rows.append(pd.DataFrame(df_new_rows))
    if other_rows:
        other_rows_df = pd.concat(other_rows, ignore_index=True)
        df = pd.concat([df, other_rows_df], ignore_index=True)
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
    TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx'
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
                branch_path = f"Demand\\{leap_ttype}" + "".join(
                    f"\\{x}" for x in [leap_vtype, leap_drive, leap_fuel] if x
                )
            else:
                total_skipped += 1
                continue
        else:
            ttype, medium, vtype, drive, fuel, branch_path, source_cols_for_grouping = process_branch_mapping(leap_tuple, src_tuple)

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

    export_df = summarize_and_export(
        total_written, total_skipped, missing_branches, missing_variables,
        leap_data_log, save_log, log_filename, create_import_files, import_filename
    )

    validate_final_energy_use_for_base_year_equals_esto_totals(economy, scenario, base_year, export_df, TRANSPORT_ESTO_BALANCES_PATH)
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
        TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx'
    )
#%%