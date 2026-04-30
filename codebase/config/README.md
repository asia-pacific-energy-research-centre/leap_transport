# Configurations Guide

`codebase/configurations/` is the canonical import path for this mapping layer.
This legacy `codebase/config/` package remains as a compatibility shim.

This folder holds the static configuration used to translate 9th-edition transport model outputs and ESTO energy balances into the LEAP transport branch structure.

The main thing to understand is that most files here are not generic settings. They are explicit crosswalks between three different category systems:

- the 9th transport model categories, such as transport type, medium, vehicle type, drive, fuel, and measure names;
- the ESTO transport balances categories, such as sector, fuel group, and fuel code;
- the LEAP transport categories, such as `Passenger road`, `Freight road`, `Passenger non road`, `Pipeline transport`, and their technology and fuel branches.

These mappings are deliberately written out in clear dictionaries instead of being hidden behind compact string rules. That makes the reconciliation logic easier to audit, easier to validate, and easier for Codex or another coding agent to modify without guessing. A lot of the work is not one-to-one mapping: some source categories split across several LEAP branches, some LEAP branches combine multiple source rows, and some branches need proxy data because the exact category is not present in the source data.

## Why the Mappings Exist

The LEAP model uses a newer and more detailed transport structure than the 9th transport output and the ESTO balances. To build a consistent LEAP import, the code needs to answer several separate questions:

- What branches exist in the source transport data?
- What branches exist in LEAP?
- Which source transport rows should feed each LEAP branch?
- Which ESTO balance rows should be used to reconcile exact historical energy values?
- Which LEAP branches cannot be reconciled to ESTO because ESTO has no equivalent category?
- Which LEAP measures should be written at each branch, with what units, scaling, and expression settings?

Keeping those answers in config files is important because it separates modelling assumptions from workflow code. The pipeline code can then read the mappings and apply them consistently instead of embedding one-off category decisions in processing functions.

## File Overview

### `basic_mappings.py`

Defines the basic source and LEAP category trees.

Important objects:

- `EXPECTED_COLS_IN_SOURCE`: the columns expected in the 9th transport model output.
- `SOURCE_CSV_TREE`: the nested 9th transport structure, from transport type to medium, vehicle type, drive, and implied fuel.
- `LEAP_STRUCTURE`: the nested LEAP transport branch structure.
- `ESTO_TRANSPORT_SECTOR_TUPLES`: the ESTO transport sector/fuel combinations used for balance matching.
- `ALL_PATHS_SOURCE` and `ALL_PATHS_LEAP`: generated sets of all paths in the source and LEAP trees.
- `add_fuel_column()`: expands rows where a drive implies multiple fuels, for example PHEV rows with both electricity and liquid fuel.

This file is the base vocabulary. Other mappings should be consistent with the source and LEAP paths defined here.

### `branch_mappings.py`

This is the central mapping file. Most category conversion lives here.

Important objects:

- `SHORTNAME_TO_LEAP_BRANCHES`: groups LEAP branches by modelling level, such as road transport type, road vehicle type, road technology, road fuel, non-road transport type, non-road vehicle type, and non-road fuel.
- `ALL_LEAP_BRANCHES_TRANSPORT`: flattened list of the branches in `SHORTNAME_TO_LEAP_BRANCHES`.
- `LEAP_BRANCH_TO_SOURCE_MAP`: maps each LEAP branch to the 9th transport source tuple that should provide its modelled values.
- `PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY`: creates source-like rows for LEAP branches that need a proxy row because the exact source row does not exist or does not have activity. For example, a future technology may use the efficiency pattern of a related existing technology while having its own LEAP branch.
- `COMBINATION_SOURCE_ROWS`: defines rows that need to be built from multiple 9th transport source rows before they can map cleanly into LEAP.
- `NINTH_SOURCE_TO_LEAP_BRANCH_MAP`: maps 9th transport source sector/fuel tuples to LEAP branches. The same table is currently reused as the legacy bridge for ESTO-side reconciliation, but its key space is 9th-derived, not raw ESTO flow/product vocabulary.
- `UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT`: lists LEAP branches that should not be expected in ESTO because ESTO has no matching category.
- `LEAP_MEASURE_CONFIG`: defines the LEAP measures available at each branch group, including source measure names, units, scaling, and conversion factors.
- `DEFAULT_BRANCH_SHARE_SETTINGS_DICT`: default split settings used for share-type branches.
- validation helpers such as `identify_missing_esto_mappings_for_leap_branches()` and `validate_branch_combinations_across_mappings()`.

This file contains several different mapping layers because they answer different questions. `LEAP_BRANCH_TO_SOURCE_MAP` is about getting modelled transport measures from the 9th transport output. `NINTH_SOURCE_TO_LEAP_BRANCH_MAP` is the 9th-derived bridge table currently reused for reconciliation against the ESTO surface. `UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT` is about documenting the branches where ESTO cannot provide a direct historical balance, instead of letting those appear as accidental gaps.

### `branch_expression_mapping.py`

Defines how LEAP expressions should be written for measure/branch combinations.

Important objects:

- `ALL_YEARS`: the model years covered by expression settings.
- `LEAP_BRANCH_TO_EXPRESSION_MAPPING`: maps tuples like `(measure, branch...)` to expression settings, usually `("Data", ALL_YEARS)`.
- `_ensure_entries()`: helper used to add repeated measure/branch expression entries.
- measure groups such as `ROAD_FUEL_MEASURES`, `ROAD_TECH_MEASURES`, and `NONROAD_FUEL_MEASURES`.

This file exists because branch mapping and expression writing are related but not identical. A branch can exist in LEAP and map to a source row, but the import still needs to know which LEAP measure expression is expected at that branch.

### `measure_metadata.py`

Defines reusable metadata for source measures and aggregation behavior.

Important objects:

- `SOURCE_MEASURE_TO_UNIT`: source measure units and scaling factors, for example converting stocks from millions to actual stock counts.
- `SOURCE_WEIGHT_PRIORITY`: preferred weights for weighted-average aggregation.
- `AGGREGATION_RULES`: whether a measure should be summed, weighted, treated as a share, or treated as a growth rate.
- `AGGREGATION_BASE_MEASURES`: the base measure used when calculating derived shares.
- `CALCULATED_MEASURES`: source-side measures that are calculated rather than read directly.
- `SHORTNAME_TO_ANALYSIS_TYPE`: maps branch groups to LEAP analysis types, such as `Stock` or `Intensity`.
- `SHARE_MEASURES`: LEAP measures that should be treated as shares.

This file keeps measure logic separate from branch logic. That matters because the same branch mapping can be used for different measures, but those measures may aggregate differently.

### `measure_catalog.py`

Provides lightweight lookup functions around the measure and branch metadata.

Important objects:

- `LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP`: maps each LEAP branch tuple to its LEAP analysis type.
- `list_all_measures()`: prints measure configuration for inspection.
- `get_leap_measure()`: gets LEAP measure metadata for a measure and branch group.
- `get_source_unit()`: gets source units and scaling.
- `get_weight_priority()`: gets aggregation weight priority for a source measure.

This file is a convenience layer. It lets processing code ask for measure metadata without reaching directly into every config dictionary.

### `transport_economy_config.py`

Defines run-level configuration for each economy and scenario.

Important objects:

- `DEFAULT_TRANSPORT_ECONOMY` and `DEFAULT_TRANSPORT_SCENARIO`.
- `COMMON_CONFIG`: shared input paths, balance paths, lifecycle profile paths, and final year.
- `ECONOMY_METADATA`: economy-specific LEAP region names, base years, short names, and source filenames.
- `TRANSPORT_ECONOMY_CONFIGS`: generated economy/scenario configuration used by the workflow.
- `get_transport_run_config()`, `list_transport_run_configs()`, and `load_transport_run_config()`: helpers for selecting and resolving a run configuration.
- lifecycle helpers that prefer economy-specific survival and vintage profile files when they exist.

This file is separate from the mapping files because it controls which data files and LEAP region are used for a run, not how categories map.

### `unused_leap_to_ninth_mappings.py`

Historical reference material for earlier LEAP-to-9th mapping work.

This file is not imported by the active config files. It contains old or exploratory mappings from detailed 9th-edition transport categories to broader LEAP categories, including examples of how the numbered 9th category hierarchy can be interpreted as LEAP branches.

It is still useful because it shows the conceptual bridge between:

- detailed 9th-edition transport categories, such as `15_02_01_02_car` or `15_02_02_04_heavy_truck`;
- broader 9th/ESTO balance categories, such as road, rail, domestic navigation, pipeline transport, and nonspecified transport;
- the newer LEAP categories, such as `Passenger road`, `Freight road`, `LPVs`, `Trucks`, `Passenger non road`, `Freight non road`, `Pipeline transport`, and `Nonspecified transport`.

That context helps explain why the active mappings in `branch_mappings.py` are shaped the way they are. The active code often has to move from a detailed 9th transport model row to a LEAP branch, then separately reconcile LEAP fuel branches back to higher-level ESTO balance categories. `unused_leap_to_ninth_mappings.py` is not the source of truth for the workflow, but it is a useful orientation file when trying to understand how the 9th-edition transport hierarchy, 9th/ESTO balance sectors, and LEAP branch structure relate to each other.

Keep it as reference unless a future workflow intentionally revives it. If it is revived, it should be reviewed against the active mappings before being imported anywhere.

### `__init__.py`

Marks this directory as a Python package and provides the package-level docstring.

## How the Major Mapping Layers Fit Together

The normal flow is:

1. `basic_mappings.py` defines the allowed source and LEAP branch structures.
2. `branch_mappings.py` maps LEAP branches to 9th transport rows for modelled transport measures.
3. `branch_mappings.py` also maps ESTO balance rows to LEAP branches for historical energy reconciliation.
4. `measure_metadata.py` and `LEAP_MEASURE_CONFIG` define how each measure should be scaled, aggregated, and written to LEAP.
5. `branch_expression_mapping.py` defines the LEAP expression settings for each measure/branch combination.
6. `transport_economy_config.py` selects the actual economy, scenario, input files, and output files for a run.

`unused_leap_to_ninth_mappings.py` sits outside that active flow. It helps document how the category systems relate, but the workflow does not currently import it.

## Why ESTO Mapping Is Separate From Source Mapping

`LEAP_BRANCH_TO_SOURCE_MAP` and `NINTH_SOURCE_TO_LEAP_BRANCH_MAP` should not be merged.

`LEAP_BRANCH_TO_SOURCE_MAP` is for the 9th transport model output. It maps modelled activity, stocks, efficiency, mileage, sales, and related transport measures into the LEAP branch structure.

`NINTH_SOURCE_TO_LEAP_BRANCH_MAP` is the 9th transport bridge table. It maps 9th sector/fuel categories to LEAP branches so the workflow can align modeled transport output with the LEAP hierarchy. This is why a single source row often maps to many LEAP branches. Do not treat this table as the raw ESTO flow/product vocabulary.

The split keeps the code clear about whether a value came from the transport model or from official energy balances.

## Why Some Branches Are Marked Unmappable

`UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT` is not a failure list. It is an explicit list of branches that should not be expected to have a direct ESTO balance match.

Typical reasons include:

- ESTO has no hydrogen transport fuel category.
- ESTO has no ammonia transport fuel category.
- ESTO does not separately identify some future or low-carbon fuels, such as efuels.
- ESTO may not split fuel use finely enough for categories such as LNG trucks or electric aircraft.
- Some entries are intermediate LEAP branches with no fuel at the final level, so they are structural parents rather than direct ESTO energy categories.

Listing these branches explicitly lets validation distinguish between a real missing mapping and a known modelling limitation.

## Design Notes For Future Edits

When adding or changing a LEAP branch, check all relevant layers:

- Add the branch to `LEAP_STRUCTURE` if it is part of the LEAP tree.
- Add it to the right group in `SHORTNAME_TO_LEAP_BRANCHES`.
- Add or update its `LEAP_BRANCH_TO_SOURCE_MAP` entry if it should receive 9th transport model values.
- Add proxy or combination logic if the source data needs more than a direct one-to-one mapping.
- Add it to `NINTH_SOURCE_TO_LEAP_BRANCH_MAP` if it belongs in the 9th-derived bridge table.
- Add it to `UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT` if ESTO does not have a matching category and that absence is intentional.
- Add expression settings in `branch_expression_mapping.py` if a LEAP measure needs to be written at that branch.
- Confirm measure units, scaling, and aggregation behavior in `LEAP_MEASURE_CONFIG` and `measure_metadata.py`.

The validation helpers in `branch_mappings.py` are there to catch gaps across these layers. Use them after mapping changes so missing branches are found as config problems rather than later as incorrect LEAP imports.
