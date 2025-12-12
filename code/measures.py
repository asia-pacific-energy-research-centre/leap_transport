"""Transport measure helper facade.

The transport measure utilities have been split into focused modules:
- :mod:`measure_catalog` for static metadata lookups
- :mod:`measure_processing` for scaling/aggregation routines
- :mod:`preprocessing` for source data adjustments prior to LEAP

This module keeps backward compatibility by re-exporting the most-used
functions so existing notebooks and scripts can continue importing from
``measures`` while new code can opt into the narrower modules.
"""

from measure_catalog import (
    LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP,
    get_leap_measure,
    get_source_unit,
    get_weight_priority,
    list_all_measures,
)
from measure_processing import (
    aggregate_measures,
    aggregate_weighted,
    apply_scaling,
    calculate_measures,
    filter_source_dataframe_by_categories,
    get_source_categories,
    process_measures_for_leap,
)
from preprocessing import (
    allocate_fuel_alternatives_energy_and_activity,
    calculate_sales,
)

__all__ = [
    # Catalog helpers
    "LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP",
    "get_leap_measure",
    "get_source_unit",
    "get_weight_priority",
    "list_all_measures",
    # Processing helpers
    "aggregate_measures",
    "aggregate_weighted",
    "apply_scaling",
    "calculate_measures",
    "filter_source_dataframe_by_categories",
    "get_source_categories",
    "process_measures_for_leap",
    # Preprocessing helpers
    "allocate_fuel_alternatives_energy_and_activity",
    "calculate_sales",
]
