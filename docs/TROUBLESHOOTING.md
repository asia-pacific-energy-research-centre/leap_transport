# Troubleshooting

## `ModuleNotFoundError: leap_utils`, `ModuleNotFoundError: code.*`, or `ModuleNotFoundError: codebase.*`

Cause:

- Helper package is not installed or not on Python path.

Fix:

```bash
pip install -e ../leap_utilities
```

Then rerun.

## Run fails with missing source columns

Cause:

- Transport input file does not match expected schema.

Fix:

- Compare input columns with `EXPECTED_COLS_IN_SOURCE` in `code/basic_mappings.py`.
- Add missing columns or fix naming before rerun.

## Duplicate rows error after preprocessing

Cause:

- Proxy/combo row creation generated duplicates.

Fix:

- Inspect `data/errors/duplicate_source_rows.csv`.
- Resolve source duplication or mapping collisions.

## LEAP branch/variable not found (COM stage)

Cause:

- Branch structure in LEAP model does not match mappings.

Fix:

- Try a run with `CHECK_BRANCHES_IN_LEAP_USING_COM=True`.
- Use `AUTO_SET_MISSING_BRANCHES=True` cautiously.
- Manually create unsupported road stock branches in LEAP UI where API cannot auto-create.

## Reconciliation fails (scale factors not all 1.0 after rerun)

Cause:

- Mismatched mapping, branch rule, or unexpected export structure.

Fix:

- Confirm mapping sets in `code/branch_mappings.py`.
- Recheck ESTO input path and scenario filter.
- Run with reconciliation reports on and inspect generated change tables.

## File paths resolve incorrectly

Cause:

- Script run from wrong working directory.

Fix:

- Always execute from repository root:

```bash
python code/MAIN_leap_import.py
```

Relative paths are resolved against repo root via `code/path_utils.py`.

## Unsure if run touched LEAP model

Quick check:

- If `SET_VARS_IN_LEAP_USING_COM=False`, LEAP should not be modified.
- If `SET_VARS_IN_LEAP_USING_COM=True`, confirm changes in selected branches and scenario.

## Useful cleanup when debugging

- Delete specific checkpoint files under `intermediate_data/` if you need a fully fresh pipeline stage.
- Keep archived outputs for comparison instead of deleting `results/archive/`.
