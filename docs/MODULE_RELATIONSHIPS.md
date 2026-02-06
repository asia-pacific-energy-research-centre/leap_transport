# Python Module Relationships

This is a full scan of every `code/*.py` module, with its purpose and how it connects to the rest of the repo.

## Repository picture

The codebase is organized as a pipeline with 6 layers:

1. `Configuration and path resolution`
2. `Static mapping and metadata`
3. `Data preparation and measure processing`
4. `Pipeline orchestration`
5. `Validation and reconciliation`
6. `Standalone utilities`

High-level flow:

`transport_economy_config` -> `MAIN_leap_import` -> (`preprocessing`, `measure_processing`, `sales_curve_estimate`) -> export workbooks -> (`mappings_validation`, `energy_use_reconciliation_road`) -> optional COM write to LEAP.

## Module-by-module map

### `code/MAIN_leap_import.py`

Role:

- Main orchestrator and runtime entrypoint.
- Runs input creation, optional sales workflows, export creation, optional COM write, and optional reconciliation.

Consumes:

- Core configs/mappings: `transport_economy_config`, `branch_mappings`, `branch_expression_mapping`, `basic_mappings`.
- Processing helpers: `preprocessing`, `measure_processing`, `sales_curve_estimate`, `esto_data`.
- QA/reconciliation: `mappings_validation`, `energy_use_reconciliation_road`.
- Path utility: `path_utils`.
- External helper package: `leap_utils.*`.

Used by:

- Direct script execution only (`python code/MAIN_leap_import.py`).

### `code/basic_mappings.py`

Role:

- Defines source schema (`EXPECTED_COLS_IN_SOURCE`) and source hierarchy (`SOURCE_CSV_TREE`).
- Defines LEAP hierarchy (`LEAP_STRUCTURE`) and tuple path sets.
- Adds source `Fuel` rows (`add_fuel_column`).

Consumes:

- No local modules.

Used by:

- `branch_mappings.py` (path sets), `measure_processing.py` (category tree), `MAIN_leap_import.py` (schema check/fuel expansion).

### `code/branch_expression_mapping.py`

Role:

- Large expression map `LEAP_BRANCH_TO_EXPRESSION_MAPPING`.
- Defines `ALL_YEARS` and helper expansion logic.

Consumes:

- No local modules.

Used by:

- `MAIN_leap_import.py` (expression building), `branch_mappings.py` (share settings consistency).

### `code/branch_mappings.py`

Role:

- Canonical transport mapping source.
- Defines LEAP branches, source-branch links, proxy/combo row rules, ESTO mapping, unmappable branches, and `LEAP_MEASURE_CONFIG`.

Consumes:

- `basic_mappings`, `branch_expression_mapping`.

Used by:

- `MAIN_leap_import.py`, `measure_catalog.py`, `esto_data.py`, `mappings_validation.py`, `energy_use_reconciliation_road.py`.

### `code/energy_use_reconciliation_road.py`

Role:

- Transport-specific reconciliation math for road/non-road branches.
- Implements adjustment and energy calculation callbacks for the shared reconciliation engine.

Consumes:

- `branch_mappings`, `esto_data` and external `leap_utils.energy_use_reconciliation`.

Used by:

- `MAIN_leap_import.py` during reconciliation stage.

### `code/esto_data.py`

Role:

- ESTO extraction helpers.
- Converts ESTO sector/fuel rows into LEAP-aligned rows, especially for "Other" branches.

Consumes:

- `branch_mappings`, `measure_metadata`, `path_utils`.

Used by:

- `MAIN_leap_import.py`, `mappings_validation.py`, `energy_use_reconciliation_road.py`.

### `code/historical_exports.py`

Role:

- Standalone script for historical transport export files (APEC and non-APEC).

Consumes:

- `path_utils`.

Used by:

- Standalone execution; not part of the main import pipeline.

### `code/lifecycle_profile_editor.py`

Role:

- Lifecycle profile editing/conversion toolkit.
- Builds vintage profiles from survival profiles; plotting and profile smoothing helpers.

Consumes:

- `path_utils`.

Used by:

- `sales_curve_estimate.py`.

### `code/mappings_validation.py`

Role:

- Validation suite for mapping consistency, share totals, and ESTO-vs-export energy checks.

Consumes:

- `measure_metadata`, `measure_catalog`, `branch_mappings`, `esto_data`, `path_utils`.

Used by:

- `MAIN_leap_import.py` before final export and as base-year QA gate.

### `code/measure_catalog.py`

Role:

- Lightweight access layer over measure metadata/config.
- Builds `LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP` and provides lookup utilities.

Consumes:

- `branch_mappings`, `measure_metadata`.

Used by:

- `MAIN_leap_import.py`, `measure_processing.py`, `mappings_validation.py`, `measures.py`.

### `code/measure_metadata.py`

Role:

- Pure metadata module.
- Units/scales, weighting priority, aggregation rules, calculated-measure lists, share measure list, analysis types.

Consumes:

- No local modules.

Used by:

- `measure_catalog.py`, `measure_processing.py`, `esto_data.py`, `mappings_validation.py`.

### `code/measure_processing.py`

Role:

- Core measure preparation engine.
- Filters source rows, computes calculated measures, applies aggregation and scaling, and returns LEAP-ready measure tables.

Consumes:

- `basic_mappings`, `measure_catalog`, `measure_metadata`.

Used by:

- `MAIN_leap_import.py`, `measures.py`.

### `code/measures.py`

Role:

- Backward-compatibility facade that re-exports APIs from `measure_catalog`, `measure_processing`, and `preprocessing`.

Consumes:

- `measure_catalog`, `measure_processing`, `preprocessing`.

Used by:

- Legacy imports/scripts (no internal imports currently).

### `code/path_utils.py`

Role:

- Central path resolver anchored on repo root.
- Prevents path bugs by resolving relative paths consistently.

Consumes:

- No local modules.

Used by:

- `MAIN_leap_import.py`, `transport_economy_config.py`, `esto_data.py`, `historical_exports.py`, `lifecycle_profile_editor.py`, `sales_curve_estimate.py`, `mappings_validation.py`.

### `code/preprocessing.py`

Role:

- Source dataframe preprocessing: sales derivation, low-carbon fuel allocation, and share normalization.

Consumes:

- No local modules.

Used by:

- `MAIN_leap_import.py`, `measures.py`.

### `code/sales_curve_estimate.py`

Role:

- Passenger/freight sales curve estimation from lifecycle profiles, stock trajectories, and ESTO context.

Consumes:

- `path_utils`, `lifecycle_profile_editor`.

Used by:

- `MAIN_leap_import.py`.

### `code/transport_economy_config.py`

Role:

- Economy/scenario config registry with path defaults.
- Returns config objects with resolved absolute paths.

Consumes:

- `path_utils` (inside loader function).

Used by:

- `MAIN_leap_import.py`.

## File type grouping

### 1) Configuration and path plumbing

- `code/transport_economy_config.py`
- `code/path_utils.py`

### 2) Static mapping and metadata definitions

- `code/basic_mappings.py`
- `code/branch_mappings.py`
- `code/branch_expression_mapping.py`
- `code/measure_metadata.py`

### 3) Transformation/compute logic

- `code/preprocessing.py`
- `code/measure_processing.py`
- `code/sales_curve_estimate.py`
- `code/esto_data.py`

### 4) Validation and reconciliation

- `code/mappings_validation.py`
- `code/energy_use_reconciliation_road.py`

### 5) Orchestration and runtime entrypoint

- `code/MAIN_leap_import.py`

### 6) Compatibility and standalone tools

- `code/measures.py` (compatibility facade)
- `code/lifecycle_profile_editor.py` (profile utility toolkit)
- `code/historical_exports.py` (historical export utility)

## Practical maintainer guidance

If you change a module, check these linked modules next:

- `branch_mappings.py` -> always re-check `measure_catalog.py`, `mappings_validation.py`, and `MAIN_leap_import.py` behavior.
- `measure_metadata.py` -> re-check `measure_processing.py` aggregations and unit scaling.
- `preprocessing.py` -> re-check final QA in `mappings_validation.py`.
- `sales_curve_estimate.py` or `lifecycle_profile_editor.py` -> re-check passenger/freight outputs and file paths from `transport_economy_config.py`.

