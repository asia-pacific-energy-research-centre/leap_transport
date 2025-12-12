# LEAP transport toolkit

Transport-only pipeline for building LEAP import/export files, applying transport measures, and reconciling ESTO balances. Generic helpers now live in the separate `leap_utilities` repo; this project holds the transport mappings, measures, and workflows.

## Setup
- Windows with LEAP desktop (COM needs LEAP open).
- Clone this repo and have `../leap_utilities` available; install the helpers into the environment with `pip install -e ../leap_utilities` if not already on `PYTHONPATH`.
- Create the environment from the repo root: `conda env create --prefix ./env_leap --file ./config/env_leap.yml`; activate via `conda activate ./env_leap`.

## Run the transport import
- Edit the config block at the bottom of `code/MAIN_leap_import.py` to point to your transport model, ESTO balances, fuel outputs, lifecycle profiles, and desired export/import filenames.
- Toggle runtime flags in the same block:
  - `RUN_INPUT_CREATION` builds LEAP export/import files and can write values to LEAP via COM.
  - `RUN_PASSENGER_SALES` / `RUN_FREIGHT_SALES` generate sales curves from survival/vintage profiles.
  - `RUN_RECONCILIATION` runs ESTO vs LEAP checks/adjustments after export creation.
- Run from the repo root so relative paths resolve: `python code/MAIN_leap_import.py`.
- Outputs land in `results/` (export workbook, passenger/freight sales CSVs) with checkpoints in `intermediate_data/`; import-structure files sit in `data/import_files/`.

## Key files
- `code/MAIN_leap_import.py`: orchestrates preprocessing, mapping, export creation, optional COM writes, and reconciliation.
- `code/branch_mappings.py`, `branch_expression_mapping.py`, `basic_mappings.py`: mapping tables between the transport model, ESTO sectors/fuels, and LEAP branches/expressions.
- `code/measure_*` and `code/measure_processing.py`: transport measure catalog and processing logic.
- `code/energy_use_reconciliation_road.py`: transport-specific reconciliation strategies and adjustments.
- `code/sales_curve_estimate.py`: builds passenger/freight sales curves from lifecycle profiles.
- `config/env_leap.yml`: conda env; `config/TypeLib_LEAP_API_full.txt`: LEAP COM type library.

## Notes
- Keep LEAP open when using COM; set `CHECK_BRANCHES_IN_LEAP_USING_COM` and `SET_VARS_IN_LEAP_USING_COM` in `code/MAIN_leap_import.py` to control COM usage.
- Large data files stay under `data/` and are ignored by Git; exports and checkpoints live in `results/` and `intermediate_data/`.
- Shared COM/reconciliation utilities are maintained in `../leap_utilities` (installed as the `leap_utils` package).
