#%%
#take in  all transport balances data.xlsx
# and filter for scenario = 'Reference', Ecoomy = '00_APEC', Sector = '15_transport_sector' , set subtotal_layout to False and then search for non-zeros in the BASE_YEAR
#filter for unique rows over the set: sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels 
#and save as TRANSPORT_all_APPLICABLE_historical_sectors_fuels_9th_outlook.xlsx

#we will do a simialr version to this as well but search for Ecoomy!='00_APEC' or any aggregates (so the number should be 21 or less) , drop zeros in BASE_YEAR and save as TRANSPORT_all_NONAPEC_historical_energy_use.xlsx
# >eventually this will need to be shifted to be compared to the data in the ESTO file but for now it seems better to stay with our more useful 9th outlook data structure, since at the least, it handles subtotals.
import pandas as pd
# Read the input Excel file
df = pd.read_csv('../../data/merged_file_energy_ALL_20250814.csv')
df_apec = pd.read_excel('../../data/all transport balances data.xlsx')
BASE_YEAR = 2021  # set to the earliest base year of all economies
# Filter for the Reference scenario, APEC economy, and transport sector
filtered_df = df_apec[(df_apec['scenarios'] == 'reference') & 
                (df_apec['economy'] == '00_APEC') & 
                (df_apec['sectors'] == '15_transport_sector') & 
                (df_apec['subtotal_layout'] == False)]

# Filter for non-zero values in BASE_YEAR column
non_zero_df = filtered_df[filtered_df[BASE_YEAR] != 0]

# Get unique rows over the specified column set
unique_rows = non_zero_df[['sectors', 'sub1sectors', 'sub2sectors', 
                                                 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']].drop_duplicates(subset=['sectors', 'sub1sectors', 'sub2sectors', 
                                                 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'])

# Save the result
unique_rows.to_excel('../../data/TRANSPORT_all_APPLICABLE_historical_sectors_fuels_9th_outlook.xlsx', index=False)

######################

ALL_ECONOMY_IDS = ["01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN"]
BASE_YEAR_TO_ECONOMY = {
    2021: ["16_RUS"],
    2022: ["01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN"]
}
# For the second part - non-APEC economies
non_apec_df = pd.DataFrame()
for BASE_YEAR, economies in BASE_YEAR_TO_ECONOMY.items():
    # Filter for the specified economies
    for economy in economies:
        non_apec_df = pd.concat([non_apec_df, df[(df['economy'] == economy) & (df['scenarios'] == 'reference') & (df['sectors'] == '15_transport_sector') & (df['subtotal_layout'] == False) & (df[str(BASE_YEAR)] != 0)]])
        
# Save the result
non_apec_df.to_excel('../../data/TRANSPORT_all_NONAPEC_historical_energy_use.xlsx', index=False)

#%%


# ============================================================
# VALIDATION: Transport Mapping Consistency (with measures)
# ============================================================
from collections import defaultdict

def validate_all_mappings_with_measures(
    SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_EXPRESSION_MAP_EXPANDED,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
):
    """Validate cross-consistency across all LEAP mapping layers, including measures."""
    print("\n=== Transport Mapping Validation (with Measures) ===")

    # ------------------------------------------------------------
    # 1. Basic completeness within SECTOR_FUEL_TO_LEAP_BRANCH_MAP
    # ------------------------------------------------------------
    reverse_map = defaultdict(list)
    nonspecified_map = defaultdict(list)
    duplicates = []
    empty = []
    all_leap_branches_mapped = set()

    for key, leap_list in SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
        if not leap_list:
            empty.append(key)
            continue
        for leap in leap_list:
            if leap == "NONSPECIFIED":
                nonspecified_map[key].append(leap)
                continue
            if leap in all_leap_branches_mapped:
                duplicates.append((key, leap))
            all_leap_branches_mapped.add(leap)
            reverse_map[leap].append(key)

    print(f"→ {len(SECTOR_FUEL_TO_LEAP_BRANCH_MAP)} sector-fuel keys checked.")
    print(f"→ {len(all_leap_branches_mapped)} unique LEAP branches mapped.")
    if empty:
        print(f"⚠️  {len(empty)} sector-fuel keys are empty.")
    if duplicates:
        print(f"⚠️  {len(duplicates)} duplicate LEAP branch references detected.")

    # ------------------------------------------------------------
    # 2. Parse measure-level keys
    # ------------------------------------------------------------
    expr_with_measures = set(LEAP_BRANCH_TO_EXPRESSION_MAP_EXPANDED.keys())
    measure_names = {k[0] for k in expr_with_measures}
    branch_only_keys = {k[1:] for k in expr_with_measures}

    print(f"\nFound {len(expr_with_measures)} expression entries across {len(measure_names)} measures.")
    print(f"Unique branch tuples represented: {len(branch_only_keys)}")

    # ------------------------------------------------------------
    # 3. Validate measure names against LEAP_MEASURE_CONFIG
    # ------------------------------------------------------------
    valid_measures = {m for group in LEAP_MEASURE_CONFIG.values() for m in group.keys()}
    invalid_measures = measure_names - valid_measures
    if invalid_measures:
        print(f"❌ Invalid measure names detected: {invalid_measures}")

    # ------------------------------------------------------------
    # 4. Cross-check branch consistency across dicts
    # ------------------------------------------------------------
    branches_source = set(LEAP_BRANCH_TO_SOURCE_MAP.keys())
    branches_shortnames = {b for lst in SHORTNAME_TO_LEAP_BRANCHES.values() for b in lst}
    all_expected_branches = branch_only_keys | branches_source | branches_shortnames

    missing_in_source = branch_only_keys - branches_source
    missing_in_shortnames = branch_only_keys - branches_shortnames
    unmapped_to_sector = branch_only_keys - all_leap_branches_mapped

    print("\n--- Cross-dictionary consistency ---")
    print(f"Total LEAP branches (union of all): {len(all_expected_branches)}")
    if missing_in_source:
        print(f"⚠️  {len(missing_in_source)} branches in EXPANDED map missing in SOURCE.")
    if missing_in_shortnames:
        print(f"⚠️  {len(missing_in_shortnames)} branches missing in SHORTNAME lists.")
    if unmapped_to_sector:
        print(f"⚠️  {len(unmapped_to_sector)} branches not referenced in any SECTOR_FUEL mapping.")

    # ------------------------------------------------------------
    # 5. Optional reporting
    # ------------------------------------------------------------
    def show_examples(label, s):
        print(f"  {label} ({len(s)}):")
        for x in list(s)[:10]:
            print(f"   {x}")

    if invalid_measures: show_examples("Invalid measures", invalid_measures)
    if missing_in_source: show_examples("Missing in SOURCE", missing_in_source)
    if missing_in_shortnames: show_examples("Missing in SHORTNAMES", missing_in_shortnames)
    if unmapped_to_sector: show_examples("Unmapped branches", unmapped_to_sector)

    # ------------------------------------------------------------
    # 6. Build outputs
    # ------------------------------------------------------------
    summary = {
        "reverse_map": dict(reverse_map),
        "nonspecified_map": dict(nonspecified_map),
        "duplicates": duplicates,
        "empty_sectorfuel": empty,
        "invalid_measures": invalid_measures,
        "missing_in_source": missing_in_source,
        "missing_in_shortnames": missing_in_shortnames,
        "unmapped_to_sector": unmapped_to_sector,
    }

    print("\n✅ Validation complete.")
    print("======================================\n")
    return summary


# Example usage
if __name__ == "__main__":
    from your_module import (
        SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAP_EXPANDED,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
    )

    results = validate_all_mappings_with_measures(
        SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAP_EXPANDED,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
    )