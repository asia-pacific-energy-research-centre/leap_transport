#%% ============================================================
# VALIDATION: Transport Mapping Consistency (with Measures)
# ============================================================
from collections import defaultdict, Counter
import pandas as pd

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
    LEAP_MEASURE_CONFIG,
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
    #only include those that are in most_detailed_leap_branches
    missing_in_source = {b for b in missing_in_source if b in most_detailed_leap_branches}
    unmapped_to_sector = {b for b in unmapped_to_sector if b in most_detailed_leap_branches}
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

    if not any([missing_in_source, missing_in_expression, missing_in_shortnames, unmapped_to_sector]):
        print("✅ All LEAP branches consistently represented across all mappings.")

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
if __name__ == "__main__":
    from LEAP_transfers_transport_MAPPINGS import (
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
    )
    from LEAP_BRANCH_TO_EXPRESSION_MAPPING import LEAP_BRANCH_TO_EXPRESSION_MAPPING

    results = validate_all_mappings_with_measures(
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAPPING,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
        EXAMPLE_SAMPLE_SIZE=1000
    )
#%%