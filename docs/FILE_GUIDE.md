# File Guide

This guide explains the important files without requiring you to read the entire codebase first.

For a full scan of every Python module and dependency links, see `docs/MODULE_RELATIONSHIPS.md`.
For architecture and retest planning, see `docs/SYSTEM_ARCHITECTURE.md` and `docs/CHANGE_IMPACT_MATRIX.md`.

## Core entrypoint

- `code/MAIN_leap_import.py`

What it does:

- Loads transport data.
- Runs preprocessing and measure logic.
- Builds LEAP export tables and expressions.
- Saves output workbooks.
- Optionally writes to LEAP through COM.
- Optionally runs reconciliation.

## Config and path handling

- `code/transport_economy_config.py`
- `code/path_utils.py`

What they do:

- Define economy/scenario-specific file paths and output names.
- Resolve relative paths against repository root.

## Mapping layer

- `code/basic_mappings.py`
- `code/branch_mappings.py`
- `code/branch_expression_mapping.py`

What they do:

- Define expected source schema and basic fuel mapping rules.
- Define LEAP branch tuple structure and source-to-LEAP mappings.
- Define expression generation templates used for final LEAP variables.

## Measure layer

- `code/measure_catalog.py`
- `code/measure_metadata.py`
- `code/measure_processing.py`
- `code/measures.py`

What they do:

- Centralize measure definitions and metadata.
- Process source values into per-measure dataframes by branch.

## Data prep and validation

- `code/preprocessing.py`
- `code/esto_data.py`
- `code/mappings_validation.py`

What they do:

- Expand and normalize source values.
- Merge required ESTO-related rows.
- Validate mapping consistency and key share/energy checks.

## Sales estimation

- `code/sales_curve_estimate.py`
- `code/lifecycle_profile_editor.py`

What they do:

- Estimate passenger and freight sales curves using survival and vintage profiles.

## Reconciliation

- `code/energy_use_reconciliation_road.py`
- `code/historical_exports.py`

What they do:

- Apply transport-specific reconciliation behavior.
- Produce adjustment summaries and optional historical context.

## Runtime directories

- `data/`: source and template inputs.
- `intermediate_data/`: checkpoint pickles for reruns.
- `results/`: exports, archives, and reconciliation reports.
- `data/errors/`: debug files generated on failed validation steps.

## Suggested reading order for new maintainers

1. `README.md`
2. `docs/START_HERE.md`
3. `code/transport_economy_config.py`
4. `code/MAIN_leap_import.py`
5. `docs/RUNBOOK.md`
6. Any mapping module relevant to your change.
