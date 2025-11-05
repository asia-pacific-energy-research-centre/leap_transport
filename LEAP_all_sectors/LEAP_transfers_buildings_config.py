"""Configuration describing the simplified buildings sector structure."""

from __future__ import annotations

from typing import Dict, List, Mapping, Sequence, Tuple

from LEAP_BRANCH_TO_EXPRESSION_MAPPING import ALL_YEARS


# ---------------------------------------------------------------------------
# Branch structure
# ---------------------------------------------------------------------------

# ``BUILDINGS_BRANCH_STRUCTURE`` defines the hierarchy of building branches
# underneath ``Demand\Buildings``.  Each top-level key is a building category
# (Residential, Commercial, etc.), each nested dictionary enumerates the major
# end-uses, and the innermost lists contain the fuels tracked for that
# end-use.  The helper ``_expand_branch_structure`` below turns this nested
# representation into the concrete tuples required by the generic sector
# processor configuration.
BUILDINGS_BRANCH_STRUCTURE: Mapping[str, Mapping[str, Sequence[str]]] = {
    'Residential': {
        'Space Heating': ['Electricity', 'Natural gas'],
        'Water Heating': ['Electricity', 'Natural gas'],
        'Appliances': ['Electricity'],
        'Lighting': ['Electricity'],
    },
    'Commercial': {
        'Office': ['Electricity', 'Natural gas'],
        'Retail': ['Electricity', 'Natural gas'],
        'Hospitality': ['Electricity', 'Natural gas'],
        'Public Services': ['Electricity'],
    },
    'Data Centers': {
        'IT Equipment': ['Electricity'],
        'Cooling': ['Electricity'],
        'Infrastructure': ['Electricity'],
    },
}


def _expand_branch_structure(
    structure: Mapping[str, Mapping[str, Sequence[str]]]
) -> Tuple[List[Tuple[str, str, str]], Dict[Tuple[str, str, str], Tuple[str, str, str]]]:
    """Return the full list of LEAP branch tuples and matching source tuples."""

    leap_branches: List[Tuple[str, str, str]] = []
    source_map: Dict[Tuple[str, str, str], Tuple[str, str, str]] = {}
    for building_type, major_uses in structure.items():
        for major_use, fuels in major_uses.items():
            for fuel in fuels:
                leap_tuple = (building_type, major_use, fuel)
                leap_branches.append(leap_tuple)
                source_map[leap_tuple] = leap_tuple
    return leap_branches, source_map


_ALL_BUILDING_BRANCHES, _ALL_BUILDING_SOURCES = _expand_branch_structure(
    BUILDINGS_BRANCH_STRUCTURE
)


BUILDINGS_SHORTNAME_TO_LEAP_BRANCHES = {
    'Buildings end uses': _ALL_BUILDING_BRANCHES,
}


BUILDINGS_LEAP_BRANCH_TO_SOURCE_MAP = _ALL_BUILDING_SOURCES

BUILDINGS_MEASURE_CONFIG = {
    'Buildings end uses': {
        'Activity Level': {
            'source_mapping': 'Activity',
            'LEAP_units': 'Floor area',
            'LEAP_Scale': '',
            'LEAP_Per': '',
        },
        'Final Energy Intensity': {
            'source_mapping': 'Intensity',
            'LEAP_units': 'GJ/m2',
            'LEAP_Scale': '',
            'LEAP_Per': '',
        },
        'Final Energy Consumption': {
            'source_mapping': 'Energy',
            'LEAP_units': 'PJ',
            'LEAP_Scale': '',
            'LEAP_Per': '',
        },
    },
}

# We rely on the default expression mode (Data, ALL_YEARS) supplied to the processor,
# so the explicit mapping can remain empty for now.
BUILDINGS_EXPRESSION_MAPPING = {}

DEFAULT_BUILDINGS_EXPRESSION_MODE = ('Data', ALL_YEARS)
