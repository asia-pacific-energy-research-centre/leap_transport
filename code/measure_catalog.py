"""Lookup utilities for LEAP transport measure metadata.

This module centralizes access to the declarative measure configuration
so other packages can work with LEAP names, units, and weighting rules
without pulling in the heavier processing helpers.
"""

from branch_mappings import LEAP_MEASURE_CONFIG, SHORTNAME_TO_LEAP_BRANCHES
from measure_metadata import (
    DEFAULT_WEIGHT_PRIORITY,
    SHORTNAME_TO_ANALYSIS_TYPE,
    SOURCE_MEASURE_TO_UNIT,
    SOURCE_WEIGHT_PRIORITY,
)

def create_leap_branch_to_analysis_type_mapping():
    """Create a mapping of LEAP branch paths to analysis types."""
    leap_branch_to_analysis_type_map = {}
    for shortname, branches in SHORTNAME_TO_LEAP_BRANCHES.items():
        analysis_type = SHORTNAME_TO_ANALYSIS_TYPE.get(shortname, None)
        if analysis_type is None:
            raise ValueError(
                f"Shortname {shortname} not found in SHORTNAME_TO_ANALYSIS_TYPE mapping. Analysis type cannot be determined."
            )
            continue
        for branch in branches:
            leap_branch_to_analysis_type_map[branch] = analysis_type
    return leap_branch_to_analysis_type_map

LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP = create_leap_branch_to_analysis_type_mapping()

# def get_leap_branch_to_analysis_type_mapping(leap_branch):
#     """Return the analysis type associated with a LEAP branch path."""
#     for shortname, branches in SHORTNAME_TO_LEAP_BRANCHES.items():
#         if leap_branch in branches:
#             analysis_type = SHORTNAME_TO_ANALYSIS_TYPE.get(shortname, None)
#             if analysis_type is None:
#                 print(
#                     f"Shortname {shortname} not found in SHORTNAME_TO_ANALYSIS_TYPE mapping. Analysis type cannot be determined."
#                 )
#                 return None
#             return analysis_type
#     print(
#         f"LEAP branch {leap_branch} not found in SHORTNAME_TO_LEAP_BRANCHES mapping. Analysis type cannot be determined."
#     )
#     return None


def list_all_measures(shortname=None):
    """Pretty-print all measures for human inspection."""
    print("=== MEASURE CONFIGURATION ===")

    config_items = (
        LEAP_MEASURE_CONFIG.items()
        if shortname is None
        else {shortname: LEAP_MEASURE_CONFIG[shortname]}.items()
    )

    for branch_name, measures in config_items:
        print(f"\n[{branch_name}]")
        for measure_name, measure_data in measures.items():
            source = measure_data.get("source_mapping")
            source_str = "None" if source is None else str(source)
            unit = measure_data.get("unit", "")
            factor = measure_data.get("factor", 1)
            print(f"  {measure_name:25} → {source_str:30} | {unit:35} | Scale={factor}")
    print("==============================\n")


def get_leap_measure(name: str, shortname: str) -> dict:
    """Get LEAP measure metadata dict, or None if not found."""
    return LEAP_MEASURE_CONFIG[shortname].get(name)


def get_source_unit(measure: str):
    """Return (unit, scale) tuple for a source measure."""
    return SOURCE_MEASURE_TO_UNIT.get(measure, (None, 1))


def get_weight_priority(measure: str):
    """Return the list of candidate weight columns for a given measure."""
    return SOURCE_WEIGHT_PRIORITY.get(measure, DEFAULT_WEIGHT_PRIORITY)


__all__ = [
    "get_leap_branch_to_analysis_type_mapping",
    "list_all_measures",
    "get_leap_measure",
    "get_source_unit",
    "get_weight_priority",
]
