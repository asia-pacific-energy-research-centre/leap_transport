# Change Impact Matrix

This matrix shows what to retest when a module changes.

## How to use

1. Find the changed file(s).
2. Run the listed checks in the "Must Verify" column.
3. Review downstream modules listed in "Likely Impacted".

## Module impact matrix

| Module | Core responsibility | Likely impacted modules | Must verify after change | Risk level |
|---|---|---|---|---|
| `code/MAIN_leap_import.py` | End-to-end orchestration and run flags | Almost all runtime modules | Full dry run (no COM), output workbook creation, checkpoint behavior, optional reconciliation path | High |
| `code/transport_economy_config.py` | Economy/scenario path registry | `MAIN_leap_import.py` | Config load for each edited economy, path resolution, output file naming | Medium |
| `code/path_utils.py` | Root-based path normalization | All modules doing file I/O | Run from repo root and non-root shell; verify all referenced paths resolve | High |
| `code/basic_mappings.py` | Source schema + source/LEAP hierarchy primitives | `branch_mappings.py`, `measure_processing.py`, `MAIN_leap_import.py` | Source schema validation, fuel expansion behavior, no broken path tuples | High |
| `code/branch_mappings.py` | Canonical branch/source/ESTO/measure mappings | `measure_catalog.py`, `esto_data.py`, `mappings_validation.py`, `energy_use_reconciliation_road.py`, `MAIN_leap_import.py` | Mapping validation pass, branch iteration runs, no missing keys, reconciliation branch rule build | Very High |
| `code/branch_expression_mapping.py` | Variable-to-expression mapping by branch/year | `MAIN_leap_import.py`, `branch_mappings.py` | Expression generation success for edited branches/measures, no missing expression errors | High |
| `code/measure_metadata.py` | Units/scaling/aggregation metadata | `measure_catalog.py`, `measure_processing.py`, `esto_data.py`, `mappings_validation.py` | Unit scaling sanity, weighted aggregation results, share behavior, no divide/normalization regressions | High |
| `code/measure_catalog.py` | Metadata access facade + branch analysis type map | `measure_processing.py`, `mappings_validation.py`, `MAIN_leap_import.py` | `LEAP_BRANCH_TO_ANALYSIS_TYPE_MAP` completeness and measure lookup correctness | Medium |
| `code/measure_processing.py` | Core measure filtering/aggregation/scaling engine | `MAIN_leap_import.py`, `measures.py` | Branch-level measure outputs, share totals, weighted average columns, export row counts | Very High |
| `code/preprocessing.py` | Source preprocessing (sales, fuel split, share normalization) | `MAIN_leap_import.py`, `measures.py` | Duplicates check, sales outputs, fuel-split logic, share normalization warnings | Very High |
| `code/esto_data.py` | ESTO extraction and "Other" branch synthesis | `MAIN_leap_import.py`, `mappings_validation.py`, `energy_use_reconciliation_road.py` | ESTO filtering for economy/scenario/year, other-row generation, unit conversion correctness | High |
| `code/mappings_validation.py` | Mapping/share/energy QA gates | `MAIN_leap_import.py` | Validation pass/fail behavior, base-year ESTO totals match checks, share normalization corrections | High |
| `code/energy_use_reconciliation_road.py` | Transport-specific reconciliation math and adjustments | `MAIN_leap_import.py` | Reconciliation convergence (`Scale Factor` -> 1), change-table outputs, no invalid adjustments | Very High |
| `code/sales_curve_estimate.py` | Passenger/freight sales curve estimation | `MAIN_leap_import.py` | Passenger and freight CSV outputs, profile loading, no NaN/infinite sales | Medium |
| `code/lifecycle_profile_editor.py` | Survival/vintage profile tooling | `sales_curve_estimate.py` | Profile load/build functions, resulting vintage series shape, optional plotting path | Medium |
| `code/measures.py` | Backward-compatibility exports | Legacy scripts/notebooks | Import compatibility for old `from measures import ...` usage | Low |
| `code/historical_exports.py` | Standalone historical export utility | Standalone operation only | Script run and output files for APEC/non-APEC datasets | Low |

## Scenario-based retest matrix

| Change scenario | Minimum retest set |
|---|---|
| Edited only docs/config text | Run one dry run to ensure no accidental flag/path changes |
| Edited any mapping dictionary (`branch_mappings`, `basic_mappings`, `branch_expression_mapping`) | `validate_all_mappings_with_measures` path + full dry run + base-year validation |
| Edited measure logic (`measure_processing`, `measure_metadata`, `preprocessing`) | Dry run + inspect export sheet samples for affected branches + share/energy validation |
| Edited reconciliation logic (`energy_use_reconciliation_road`, `esto_data`) | Reconciliation-enabled run + confirm convergence + inspect change reports |
| Edited path/config runtime (`path_utils`, `transport_economy_config`) | Run from clean shell, verify all input/output paths resolve and write successfully |
| Edited COM-related behavior (`MAIN_leap_import` + COM flags) | One file-only run first, then controlled COM run with LEAP open |

## Quick verification checklist by severity

### Very High-risk file changed

- Full dry run with COM off.
- Reconciliation run if reconciliation path touched.
- Check outputs in `results/`, `results/reconciliation/`, and error files in `data/errors/`.

### High-risk file changed

- Dry run with relevant stage enabled.
- Validate mapping/share/energy consistency in generated workbook.

### Medium/Low-risk file changed

- Targeted run for affected feature only.
- Smoke test imports and output file creation.
