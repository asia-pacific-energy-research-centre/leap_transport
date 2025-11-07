#%% ============================================================
# VALIDATION: Transport Mapping Consistency (with Measures)
# ============================================================
from collections import defaultdict, Counter
import pandas as pd
from LEAP_tranposrt_measures_config import (
    get_leap_branch_to_analysis_type_mapping, SHARE_MEASURES
)
from LEAP_transfers_transport_MAPPINGS import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
)

from LEAP_transfers_transport_core import (extract_esto_energy_use_for_leap_branches
)
def get_most_detailed_branches(mapping: dict):
    """
    From a mapping of tuple keys (branch hierarchy) → values,
    return only the keys that are the most detailed (i.e. deepest level)
    for each unique branch group.

    Example:
        ('Passenger road',)
        ('Passenger road','LPVs','ICE small','Gasoline')  ✅ kept
        ('Passenger road','LPVs')                         ❌ removed
    """
    keys = list(mapping.keys())
    most_detailed = set()

    for k in keys:
        # If there exists another tuple that starts with this one (same prefix)
        # and is longer, then this one is NOT most detailed.
        if any(other != k and len(other) > len(k) and other[:len(k)] == k for other in keys):
            continue
        most_detailed.add(k)

    return {k: mapping[k] for k in most_detailed}.keys()


def check_for_duplicate_keys(
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAPPING,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        EXAMPLE_SAMPLE_SIZE=5
):
        #check for duplicate keys in all the mappings:
        # Check for duplicate keys in all mappings
        duplicate_key_checker = {}
        duplicated_keys = {}
        
        # Check ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP for duplicate keys
        for key in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.keys():
            if key in duplicate_key_checker:
                duplicated_keys.setdefault("ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP", []).append(key)
            else:
                duplicate_key_checker[key] = "ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP"
        
        # Check LEAP_BRANCH_TO_EXPRESSION_MAPPING for duplicate keys
        for key in LEAP_BRANCH_TO_EXPRESSION_MAPPING.keys():
            if key in duplicate_key_checker:
                duplicated_keys.setdefault("LEAP_BRANCH_TO_EXPRESSION_MAPPING", []).append(key)
            else:
                duplicate_key_checker[key] = "LEAP_BRANCH_TO_EXPRESSION_MAPPING"
        
        # Check LEAP_BRANCH_TO_SOURCE_MAP for duplicate keys
        for key in LEAP_BRANCH_TO_SOURCE_MAP.keys():
            if key in duplicate_key_checker:
                duplicated_keys.setdefault("LEAP_BRANCH_TO_SOURCE_MAP", []).append(key)
            else:
                duplicate_key_checker[key] = "LEAP_BRANCH_TO_SOURCE_MAP"
        
        # Check SHORTNAME_TO_LEAP_BRANCHES for duplicate keys
        for key in SHORTNAME_TO_LEAP_BRANCHES.keys():
            if key in duplicate_key_checker:
                duplicated_keys.setdefault("SHORTNAME_TO_LEAP_BRANCHES", []).append(key)
            else:
                duplicate_key_checker[key] = "SHORTNAME_TO_LEAP_BRANCHES"
        
        if duplicated_keys:
            print(f"\n⚠️  Found duplicate keys across mapping dictionaries:")
            for mapping_name, keys in duplicated_keys.items():
                print(f"   • {mapping_name}: {len(keys)} duplicate keys")
                for k in keys[:EXAMPLE_SAMPLE_SIZE]:
                    print(f"     - {k}")
        else:
            print("✅ No duplicate keys found across mapping dictionaries.")
            
def validate_all_mappings_with_measures(
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_EXPRESSION_MAPPING,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,ESTO_TRANSPORT_SECTOR_TUPLES,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    EXAMPLE_SAMPLE_SIZE=5
):
    """Validate cross-consistency across all LEAP mapping layers, including measures."""
    print("\n=== Transport Mapping Validation (with Measures) ===")
    
    # ------------------------------------------------------------
    # 0. Keep only most detailed branches for validation
    # ------------------------------------------------------------
    most_detailed_leap_branches = get_most_detailed_branches(LEAP_BRANCH_TO_SOURCE_MAP)
    # ------------------------------------------------------------
    # 1. Check ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP for duplicates
    # ------------------------------------------------------------
    reverse_map = defaultdict(list)
    nonspecified_map = defaultdict(list)
    empty_keys = []
    for key, leap_list in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
        if not leap_list:
            empty_keys.append(key)
            continue
        for leap in leap_list:
            reverse_map[leap].append(key)
            if leap == "NONSPECIFIED":
                nonspecified_map[key].append(leap)

    # --- Duplicate detection ---
    # LEAP branches that appear multiple times across different keys
    value_to_count = Counter()
    for leap, keys in reverse_map.items():
        value_to_count[leap] = len(keys)
    duplicated_values = {leap: c for leap, c in value_to_count.items() if c > 1}
    #drop any in duplicate values that are for the Nonspecified branch since we are doing a many to one mapping on these (so we gather all random fuels [e.g. kerosene use in vehicles] under nonspecified)
    duplicated_values = {k: v for k, v in duplicated_values.items() if 'Nonspecified transport' not in k}

    print(f"→ {len(ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP)} keys checked in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.")
    print(f"→ {len(reverse_map)} unique LEAP branches mapped across all sector-fuel keys.")

    if empty_keys:
        print(f"⚠️  {len(empty_keys)} sector-fuel keys have no LEAP branches assigned in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.")
        for e in empty_keys[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {e}")
    if duplicated_values:
        print(f"⚠️  {len(duplicated_values)} LEAP branches are mapped to by multiple sector-fuel keys in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP:")
        for v, c in list(duplicated_values.items())[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {v} ← {c} sector-fuel keys")

    if nonspecified_map:
        print(f"ℹ️  {len(nonspecified_map)} sector-fuel keys use 'NONSPECIFIED' placeholders.")
        
    check_for_duplicate_keys(
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAPPING,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        EXAMPLE_SAMPLE_SIZE
    )
    # ------------------------------------------------------------
    # 2. Parse measure-level keys
    # ------------------------------------------------------------
    expr_with_measures = set(LEAP_BRANCH_TO_EXPRESSION_MAPPING.keys())
    measure_names = {k[0] for k in expr_with_measures}
    branch_only_keys = {k[1:] for k in expr_with_measures}

    print(f"\n→ Found {len(expr_with_measures)} keys in LEAP_BRANCH_TO_EXPRESSION_MAPPING.")
    print(f"→ {len(measure_names)} distinct measures found.")
    print(f"→ {len(branch_only_keys)} unique branch tuples (ignoring measures).")

    # ------------------------------------------------------------
    # 3. Validate measure names
    # ------------------------------------------------------------
    valid_measures = {m for group in LEAP_MEASURE_CONFIG.values() for m in group.keys()}
    invalid_measures = measure_names - valid_measures
    if invalid_measures:
        print(f"❌ {len(invalid_measures)} invalid measure names found in LEAP_BRANCH_TO_EXPRESSION_MAPPING (not in LEAP_MEASURE_CONFIG):")
        for m in list(invalid_measures)[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {m}")
    else:
        print("✅ All measure names in LEAP_BRANCH_TO_EXPRESSION_MAPPING exist in LEAP_MEASURE_CONFIG.")

    # ------------------------------------------------------------
    # 4. Cross-check between mappings
    # ------------------------------------------------------------
    branches_source = set(LEAP_BRANCH_TO_SOURCE_MAP.keys())
    branches_shortnames = {b for lst in SHORTNAME_TO_LEAP_BRANCHES.values() for b in lst}
    all_expected_branches = branch_only_keys | branches_source | branches_shortnames

    missing_in_source = branch_only_keys - branches_source
    missing_in_expression = branches_source - branch_only_keys
    missing_in_shortnames = branch_only_keys - branches_shortnames
    unmapped_to_sector = branch_only_keys - set(reverse_map.keys())
    # Remove branches that are known to have no ESTO equivalent and are not the most detailed branches
    unmapped_to_sector = {b for b in unmapped_to_sector 
                         if b in most_detailed_leap_branches 
                         and b not in UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT}

    #only include those that are in most_detailed_leap_branches
    missing_in_source = {b for b in missing_in_source if b in most_detailed_leap_branches}
    print("\n--- Cross-dictionary consistency ---")
    print(f"Total unique LEAP branches across all mappings: {len(all_expected_branches)}")
    
    if missing_in_source:
        print(f"⚠️  {len(missing_in_source)} branch tuples exist in LEAP_BRANCH_TO_EXPRESSION_MAPPING "
              f"but are missing from LEAP_BRANCH_TO_SOURCE_MAP:")
        for b in list(missing_in_source)[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {b}")

    if missing_in_expression:
        print(f"⚠️  {len(missing_in_expression)} branch tuples exist in LEAP_BRANCH_TO_SOURCE_MAP "
              f"but are missing from LEAP_BRANCH_TO_EXPRESSION_MAPPING:")
        for b in list(missing_in_expression)[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {b}")
    
    if missing_in_shortnames:
        print(f"⚠️  {len(missing_in_shortnames)} branch tuples exist in LEAP_BRANCH_TO_EXPRESSION_MAPPING "
              f"but are missing from SHORTNAME_TO_LEAP_BRANCHES:")
        for b in list(missing_in_shortnames)[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {b}")

    if unmapped_to_sector:
        print(f"⚠️  {len(unmapped_to_sector)} branch tuples exist in LEAP_BRANCH_TO_EXPRESSION_MAPPING "
              f"but are not referenced in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP:")
        for b in list(unmapped_to_sector)[:EXAMPLE_SAMPLE_SIZE]:
            print(f"   • {b}")
    
    
    # Add info about excluded branches
    excluded_count = len([b for b in branch_only_keys if b in UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT])
    if excluded_count > 0:
        print(f"ℹ️  {excluded_count} branch tuples excluded from validation (no ESTO fuel equivalent).")
    
    if not any([missing_in_source, missing_in_expression, missing_in_shortnames, unmapped_to_sector]):
        print("✅ All LEAP branches consistently represented across all mappings.")

    # ------------------------------------------------------------
    # 5. Check for consistency between ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP keys and ESTO_TRANSPORT_SECTOR_TUPLES
    # ------------------------------------------------------------
    try:
        # Try to import ESTO_TRANSPORT_SECTOR_TUPLES from appropriate module
        
        esto_keys = set(ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.keys())
        transport_tuples = set(ESTO_TRANSPORT_SECTOR_TUPLES)
        
        missing_in_map = transport_tuples - esto_keys
        extra_in_map = esto_keys - transport_tuples
        
        if missing_in_map:
            print(f"\n⚠️  {len(missing_in_map)} transport sector-fuel tuples are in ESTO_TRANSPORT_SECTOR_TUPLES "
                  f"but missing from ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP:")
            for item in list(missing_in_map)[:EXAMPLE_SAMPLE_SIZE]:
                print(f"   • {item}")
                
        if extra_in_map:
            print(f"\n⚠️  {len(extra_in_map)} keys in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP "
                  f"are not present in ESTO_TRANSPORT_SECTOR_TUPLES:")
            for item in list(extra_in_map)[:EXAMPLE_SAMPLE_SIZE]:
                print(f"   • {item}")
                
        if not (missing_in_map or extra_in_map):
            print("\n✅ ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP keys and ESTO_TRANSPORT_SECTOR_TUPLES are fully consistent.")
            
    except ImportError:
        breakpoint()
        print("\nℹ️  Could not import ESTO_TRANSPORT_SECTOR_TUPLES for comparison check.")
    # ------------------------------------------------------------
    # EXAMPLE_SAMPLE_SIZE. Build summary for programmatic use
    # ------------------------------------------------------------
    summary = {
        "empty_keys": empty_keys,
        # "duplicated_keys": duplicated_keys,
        "duplicated_values": duplicated_values,
        "nonspecified_map": dict(nonspecified_map),
        "invalid_measures": invalid_measures,
        "missing_in_source": missing_in_source,
        "missing_in_expression": missing_in_expression,
        "missing_in_shortnames": missing_in_shortnames,
        "unmapped_to_sector": unmapped_to_sector,
    }

    print("\n✅ Validation complete.")
    print("======================================\n")
    return summary


#%%
# Example usage
# if __name__ == "__main__":
#     from LEAP_transfers_transport_MAPPINGS import (
#         ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
#         LEAP_BRANCH_TO_SOURCE_MAP,
#         SHORTNAME_TO_LEAP_BRANCHES,
#         LEAP_MEASURE_CONFIG,
#         UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT
#     )
#     from LEAP_BRANCH_TO_EXPRESSION_MAPPING import LEAP_BRANCH_TO_EXPRESSION_MAPPING
    
#     from basic_mappings import ESTO_TRANSPORT_SECTOR_TUPLES

#     results = validate_all_mappings_with_measures(
#         ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
#         LEAP_BRANCH_TO_EXPRESSION_MAPPING,
#         LEAP_BRANCH_TO_SOURCE_MAP,
#         SHORTNAME_TO_LEAP_BRANCHES,
#         LEAP_MEASURE_CONFIG,
#         ESTO_TRANSPORT_SECTOR_TUPLES,
#         UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
#         EXAMPLE_SAMPLE_SIZE=1000
#     )
#%%

def calculate_energy_use_for_stock_analysis_branch(branch_path, branch_tuple, export_df, BASE_YEAR):
    
    # Placeholder function to calculate energy use for stock-based branches
    # This would involve retrieving stocks, mileage, and efficiency from the excel import sheet for leap

    # Example implementation (to be replaced with actual logic):
    stocks = export_df.loc[(export_df['Branch Path'] == branch_path) & (export_df['Variable'] == 'Stock') , BASE_YEAR].values
    mileage = export_df.loc[(export_df['Branch Path'] == branch_path) & (export_df['Variable'] == 'Mileage') , BASE_YEAR].values
    efficiency = export_df.loc[(export_df['Branch Path'] == branch_path) & (export_df['Variable'] == 'Fuel Economy') , BASE_YEAR].values

    # Calculate energy use (this is a simplified example)
    if (efficiency == 0).all():
        breakpoint()
        raise ValueError(f"Efficiency data missing or zero for branch {branch_path}")
    energy_use = stocks * mileage * (1/efficiency)
    
    return energy_use.sum() if energy_use.size > 0 else 0

def calculate_energy_use_for_intensity_analysis_branch(branch_path, branch_tuple, export_df, BASE_YEAR):
    
    # Placeholder function to calculate energy use for intensity-based branches
    # This would involve retrieving activity level and intensity from the excel import sheet for leap

    # Example implementation (to be replaced with actual logic):
    activity_level = export_df.loc[(export_df['Branch Path'] == branch_path) & (export_df['Variable'] == 'Activity Level'), BASE_YEAR].values
    intensity = export_df.loc[(export_df['Branch Path'] == branch_path) & (export_df['Variable'] == 'Final Energy Intensity'), BASE_YEAR].values

    if (intensity == 0).all() or intensity.size == 0:
        breakpoint()
        raise ValueError(f"intensity data missing or zero for branch {branch_path}")
    # Calculate energy use (this is a simplified example)
    energy_use = activity_level * intensity
    return energy_use.sum() if energy_use.size > 0 else 0

def validate_final_energy_use_for_base_year_equals_esto_totals(ECONOMY, SCENARIO, BASE_YEAR, FINAL_YEAR, export_df, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx', TRANSPORT_ROOT = r"Demand\Transport"):
    """
    Validate that LEAP final energy use for the base year matches ESTO totals.
    this will utilise the ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP to sum up LEAP final energy use by branch, using the msot detailed branch levels and then caculating total energy use for each branch based on what measures are avaialble. There would be two types of calculation: 
    Stock based:  where stocks*mileage*efficiency -> energy use
    Intensity based: where activity level * intensity -> energy use
    We will iterate over each key in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP, sum up the energy use from each of the LEAP branches mapped to that key, and compare it to the ESTO total for that sector-fuel combination.
    """
    esto_energy_use = pd.read_excel(TRANSPORT_ESTO_BALANCES_PATH) 
    #filter for the given economy, scenario and base year
    esto_energy_use_filtered = esto_energy_use[
        (esto_energy_use['economy'] == ECONOMY) &
        (esto_energy_use['scenarios'] == SCENARIO) &
        (esto_energy_use['subtotal_layout'] == False)
    ][['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', BASE_YEAR]]

    leap_energy_use_totals = {}
    esto_energy_totals = {}
    
    for esto_key, leap_branches in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
        total_energy_use = 0
        for leap_branch in leap_branches:
            if 'Nonspecified transport' in leap_branch:
                #todo this. want to make ti so we can calcualte the nonspecified values now in the same way thgat is done in 
                # breakpoint()#is this right if we insert the esto energy?
                continue  # Skip nonspecified branches since they don't have direct ESTO equivalents
            analysis_type = get_leap_branch_to_analysis_type_mapping(leap_branch)
            leap_ttype, leap_vtype, leap_drive, leap_fuel = (list(leap_branch) + [None] * (4 - len(leap_branch)))[:4]
            branch_path = f"{TRANSPORT_ROOT}\\{leap_ttype}" + "".join(
                f"\\{x}" for x in [leap_vtype, leap_drive, leap_fuel] if x
            )
            try:
                # Determine if the branch is stock-based or intensity-based
                if analysis_type == 'Stock':
                    energy_use = calculate_energy_use_for_stock_analysis_branch(branch_path, leap_branch, export_df, BASE_YEAR)
                elif analysis_type == 'Intensity':
                    energy_use = calculate_energy_use_for_intensity_analysis_branch(branch_path, leap_branch, export_df, BASE_YEAR)
                else:
                    energy_use = 0  # Unknown branch type

            except Exception as e:
                breakpoint()
                print(f"Error calculating energy use for branch {leap_branch}: {e}")
                energy_use = 0

            total_energy_use += energy_use

        leap_energy_use_totals[esto_key] = total_energy_use
        #now find the esto_energy_totals:
        if 'Nonspecified transport' in leap_branch:
            esto_energy_total_list = extract_esto_energy_use_for_leap_branches(leap_branches, esto_energy_use, ECONOMY, BASE_YEAR, FINAL_YEAR)#todo
            # breakpoint()#how to get the right df format here. it is currently a dict list
            try:
                esto_energy_total = pd.DataFrame()
                for item in esto_energy_total_list:
                    esto_energy_total_i = pd.DataFrame(item)
                    esto_energy_total = pd.concat([esto_energy_total, esto_energy_total_i], ignore_index=True)
                #and then extract onl the base year then sum energy if we need
                esto_energy_total = esto_energy_total.loc[esto_energy_total['Date'] == BASE_YEAR]
                esto_energy_total[BASE_YEAR] = esto_energy_total['Energy']
                    
            except Exception as e:
                breakpoint()
                print(f"Error converting ESTO nonspecified data to DataFrame for key {esto_key}: {e}")
                esto_energy_totals[esto_key] = 0
                continue
        else:
            esto_energy_total = esto_energy_use_filtered.loc[
                (esto_energy_use_filtered['sub1sectors'] == esto_key[0]) &
                (esto_energy_use_filtered['sub2sectors'] == 'x') &           
                (esto_energy_use_filtered['fuels'] == esto_key[1]) &
                (esto_energy_use_filtered['subfuels'] == esto_key[2]) #&
                # (esto_energy_use_filtered[BASE_YEAR] != 0)
            ]
        #check its just one row otherwise raise error sicne this shoudlnt occur
        if len(esto_energy_total) != 1:
            
            if len(esto_energy_total) > 1:
                breakpoint()
                raise ValueError(f"Multiple or no rows found in ESTO data for key {esto_key}")
            else:
                print(f"⚠️  No ESTO data found for key {esto_key}, setting total to 0.")
                #for now jsut skip it since its not a big deal
                esto_energy_totals[esto_key] = 0
                continue
                # raise ValueError(f"Multiple or no rows found in ESTO data for key {esto_key}")
                
        esto_energy_totals[esto_key] = esto_energy_total[BASE_YEAR].values[0]

    # Compare LEAP energy use totals with ESTO energy use
    for key, leap_total in leap_energy_use_totals.items():
        esto_total = esto_energy_totals.get(key, 0)
        if leap_total != esto_total:
            print(f"Discrepancy found for {key}: LEAP = {leap_total}, ESTO = {esto_total}")
    
    validate_non_specified_energy_use_for_base_year_equals_esto_totals(BASE_YEAR, export_df, esto_energy_use_filtered)
    
def validate_non_specified_energy_use_for_base_year_equals_esto_totals(BASE_YEAR, export_df, esto_energy_use_filtered, TRANSPORT_ROOT = r"Demand\Transport"):
    """
    #handle non specified slightly differently.. we will add up all of the fuel use for nonspecified branches and compare to the use for their corresponding branches in the esto data set:
    #Note, there are many esto totals for each leap nonsepcified branch. So we will create a entry in nonspecified_branches_leap for each leap branch with non specified in it, and have a tuple with the first entry being the total energy use from leap for that branch, and the second entry bein the total energy use from esto for all the corresponding esto keys that map to that leap branch.
    """
    nonspecified_branches_leap = {}
    # completed_nonspecified_branches = {}
    for esto_key, leap_branches in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
        for leap_branch in leap_branches:
            if 'Nonspecified transport' in leap_branch:
                if leap_branch in nonspecified_branches_leap.keys():
                    pass  # Skip already processed nonspecified branches
                else:
                    analysis_type = get_leap_branch_to_analysis_type_mapping(leap_branch)
                    
                    leap_ttype, leap_vtype, leap_drive, leap_fuel = (list(leap_branch) + [None] * (4 - len(leap_branch)))[:4]
                    branch_path = f"{TRANSPORT_ROOT}\\{leap_ttype}" + "".join(
                        f"\\{x}" for x in [leap_vtype, leap_drive, leap_fuel] if x
                    )
                    try:
                        if analysis_type == 'Stock':
                            energy_use = calculate_energy_use_for_stock_analysis_branch(branch_path, leap_branch, export_df, BASE_YEAR)
                        elif analysis_type == 'Intensity':
                            energy_use = calculate_energy_use_for_intensity_analysis_branch(branch_path, leap_branch, export_df, BASE_YEAR)
                        else:
                            energy_use = 0
                    except Exception as e:
                        breakpoint()
                        print(f"Error calculating energy use for branch {leap_branch}: {e}")
                        energy_use = 0
                    nonspecified_branches_leap[leap_branch] = (energy_use, 0)

                # Find corresponding ESTO data that is one of many that maps to this nonspecified branch.
                esto_nonspec_data = esto_energy_use_filtered.loc[
                    (esto_energy_use_filtered['sub1sectors'] == esto_key[0]) &
                    (esto_energy_use_filtered['sub2sectors'] == 'x') &           
                    (esto_energy_use_filtered['fuels'] == esto_key[1]) &
                    (esto_energy_use_filtered['subfuels'] == esto_key[2])
                ]
                
                if not esto_nonspec_data.empty:
                    nonspecified_branches_leap[leap_branch] = (
                        nonspecified_branches_leap[leap_branch][0],
                        nonspecified_branches_leap[leap_branch][1] + esto_nonspec_data[BASE_YEAR].values[0]
                    )

    for branch, (leap_total, esto_total) in nonspecified_branches_leap.items():
        if leap_total != esto_total:
            print(f"Discrepancy found for Nonspecified branch {branch}: LEAP = {leap_total}, ESTO = {esto_total}")
            # You can add additional handling logic here if needed
    print("Nonspecified branch validation complete.")

def validate_and_fix_shares_normalise_to_one(df, BASE_YEAR, LEAP_BRANCH_TO_EXPRESSION_MAPPING, EXAMPLE_SAMPLE_SIZE=5):
    """
    Validate and (if needed) normalize share measures so that sibling branches sum to 100.
    Updates df in-place for columns 'value' and/or BASE_YEAR when present.
    """
    tol = 1e-3

    def to_tuple(branch):
        if isinstance(branch, tuple):
            return branch
        if pd.isna(branch):
            return ()
        return tuple(str(branch).split('\\'))

    #make the df tall to make it easier to work with
    df_wide = df.copy()
    df = pd.melt(
        df_wide,
        id_vars=['Branch Path', 'Variable', 'Scenario', 'Region', 'Scale', 'Units', 'Per...', 'Method','Level 1', 'Level 2', 'Level 3', 'Level 4', 'Level 5', 'Level 6', '#N/A'],
        var_name='Date',
        value_name='value'
    )
    
    # Pre-compute branch hierarchies for all branches at once
    df['branch_tuple'] = df['Branch Path'].apply(to_tuple)
    
    for measure in SHARE_MEASURES:
        measure_data = df[df['Variable'] == measure].copy()
        if measure_data.empty:
            continue

        # Create parent-child mapping vectorized
        measure_data['branch_levels'] = measure_data['branch_tuple'].apply(len)
        
        issues_found = []
        
        # Group by Date first for efficiency
        for date in measure_data['Date'].unique():
            # breakpoint()
            date_data = measure_data[measure_data['Date'] == date].copy()
            
            # For each hierarchy level, calculate parent sums
            max_levels = date_data['branch_levels'].max() if not date_data.empty else 0
            
            for level in range(1, max_levels + 1):
                level_data = date_data[date_data['branch_levels'] == level].copy()
                if level_data.empty:
                    continue
                
                # Create parent paths for all branches at this level
                level_data['parent_tuple'] = level_data['branch_tuple'].apply(
                    lambda x: x[:-1] if len(x) > 0 else ()
                )
                
                # Group by parent and calculate sums
                parent_sums = level_data.groupby('parent_tuple')['value'].agg(['sum', 'count']).reset_index()
                parent_sums.columns = ['parent_tuple', 'total_sum', 'child_count']
                
                # Find parents where sum != 100.0 (within tolerance)
                problematic_parents = parent_sums[abs(parent_sums['total_sum'] - 100.0) > tol]
                
                if not problematic_parents.empty:
                    # Merge back to get children needing normalization
                    normalization_data = level_data.merge(
                        problematic_parents[['parent_tuple', 'total_sum']], 
                        on='parent_tuple', 
                        how='inner'
                    )
                    
                    # Calculate normalized values #todo need to find a way to deal with split equally better since we should ideally have real numbers otherwise we are just making things up
                    def calculate_normalized_value(row, SPLIT_EQUALLY=True):
                        if row['total_sum'] == 0.0 and row['child_count'] == 1:
                            return 100.0
                        elif row['total_sum'] == 0.0 and SPLIT_EQUALLY:
                            return 100.0 / row['child_count']
                        elif row['total_sum'] == 0.0:
                            return 0.0
                        else:
                            return (row['value'] / row['total_sum']) * 100.0
                    
                    # Add child count info
                    normalization_data = normalization_data.merge(
                        parent_sums[['parent_tuple', 'child_count']], 
                        on='parent_tuple'
                    )
                    
                    normalization_data['normalized_value'] = normalization_data.apply(
                        calculate_normalized_value, axis=1
                    )
                    
                    # Update original dataframe using vectorized operations
                    for idx, row in normalization_data.iterrows():
                        mask = (
                            (df['Variable'] == measure) & 
                            (df['branch_tuple'] == row['branch_tuple']) & 
                            (df['Date'] == date)
                        )
                        df.loc[mask, 'value'] = row['normalized_value']
                    
                    # Record issues for reporting
                    for parent_tuple in problematic_parents['parent_tuple'].unique():
                        parent_info = problematic_parents[problematic_parents['parent_tuple'] == parent_tuple].iloc[0]
                        children_info = normalization_data[
                            normalization_data['parent_tuple'] == parent_tuple
                        ][['branch_tuple', 'value', 'normalized_value']].values.tolist()
                        
                        issues_found.append({
                            'parent': parent_tuple,
                            'year': date,
                            'total_share': parent_info['total_sum'],
                            'children': [(c, o, n) for c, o, n in children_info],
                            'Variable': measure
                        })

        # Report issues (same as before)
        if issues_found:
            print(f"⚠️  {len(issues_found)} normalization issues found and corrected for variable '{measure}':")
            for issue in issues_found[:EXAMPLE_SAMPLE_SIZE]:
                parent_str = ' -> '.join(issue['parent']) if issue['parent'] else 'Root'
                total = issue.get('total_share', 0.0)
                year = issue.get('year', 'Unknown')
                print(f"   • Parent: {parent_str}, Year: {year}, Original Total: {total:.3f}")
                for child, original, normalized in issue['children'][:3]:
                    child_str = ' -> '.join(child) if isinstance(child, tuple) else str(child)
                    print(f"     - {child_str}: {original:.3f} → {normalized:.3f}")
        else:
            print(f"✅ All shares already normalized to 100.0 for variable '{measure}'")
            
    #make it wide again:
    df = df.pivot(
        index=['Branch Path', 'Variable', 'Scenario', 'Region', 'Scale', 'Units', 'Per...', 'Method','Level 1', 'Level 2', 'Level 3', 'Level 4', 'Level 5', 'Level 6', '#N/A'],
        columns='Date',
        values='value'
    ).reset_index()
    breakpoint()
    return df



######################
# ------------------------------------------------------------
#VALIDATION
# ------------------------------------------------------------
# ============================================================
# LEAP_transfers_transport_validation.py
# ============================================================
# Validates and optionally corrects stock/sales shares consistency
# across all hierarchical levels of the transport dataset.
# ============================================================

import pandas as pd

def validate_shares(df, tolerance=0.01, auto_correct=False, road_only=False):
    """
    Validate that stocks and sales shares sum to ~1.0 at each hierarchy level.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe containing at least:
        ['Scenario', 'Transport Type', 'Medium', 'Vehicle Type', 'Date',
         'Vehicle_sales_share', 'Stock Share'] (if applicable)
    tolerance : float
        Allowed deviation from 1.0 before flagging (default 0.01 = ±1%)
    auto_correct : bool
        If True, renormalize groups that deviate within 5*tolerance.

    Returns
    -------
    df : pandas.DataFrame
        Possibly corrected DataFrame.
    report : pandas.DataFrame
        Report summarizing which groups failed validation.
    """

    print("\n=== Validating Transport Shares Consistency ===")
    issues = []

    if road_only:
        df_non_road = df[df["Medium"] != "road"].copy()
        df = df[df["Medium"] == "road"].copy()
    def check_and_fix(group, column):
        """Helper to check one share column per group."""
        total = group[column].sum(skipna=True)
        deviation = abs(total - 1.0)
        status = "OK"
        if pd.isna(total) or len(group) == 0:
            status = "Empty"
        elif deviation > tolerance:
            status = "FAIL"
            if auto_correct and deviation < 5 * tolerance:
                group[column] /= total
                status = "Corrected"
        return total, deviation, status, group

    # Define combinations to check
    group_levels = [
        ["Scenario", "Transport Type", "Medium", "Vehicle Type", "Date"],
        # ["Scenario", "Transport Type", "Medium", "Date"],#i think we just want the most detailed levelsand we calcualte the upper levels from these later in calculate_measures()
    ]

    # Check both shares if available
    for col in ["Vehicle_sales_share", "Stock Share"]:
        if col not in df.columns:
            continue
        for levels in group_levels:
            grouped = df.groupby(levels, group_keys=False)
            for key, g in grouped:
                total, dev, status, new_g = check_and_fix(g, col)
                issues.append({
                    "Share Type": col,
                    "Group": key,
                    "Total": total,
                    "Deviation": dev,
                    "Status": status,
                    "Group Size": len(g)
                })
                if auto_correct and status == "Corrected":
                    df.loc[g.index, col] = new_g[col].values

    report = pd.DataFrame(issues)
    fails = report[report["Status"].isin(["FAIL", "Corrected"])]

    #exclude base year values if it is on Vehicle_sales_share
    fails = fails[~(fails["Share Type"].eq("Vehicle_sales_share") & fails["Group"].apply(lambda x: x[-1] == 2022))]

    print(f"Checked {len(report)} groups.")
    if len(fails) == 0:
        print("✅ All share groups are consistent.")
    else:
        print(f"⚠️  {len(fails)} groups deviated from 1.0 "
              f"({(len(fails)/len(report))*100:.1f}% of total).")
        print("Sample issues:")
        print(fails.head(10).to_string(index=False))

    print("=" * 60)
    if road_only:
        df = pd.concat([df, df_non_road], ignore_index=True)
    return df, report