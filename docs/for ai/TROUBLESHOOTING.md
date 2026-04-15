# Troubleshooting

## `ModuleNotFoundError` for `leap_utils`, `codebase.*`, or helper modules

Cause:

- Helper package/repo is not importable.

Fix:

```bash
pip install -e ../leap_utilities
```

Then rerun from repo root.

## `LEAP API usage is disabled because the LEAP API is currently buggy...`

Cause:

- COM flags were enabled, but current code intentionally blocks COM access.

Fix:

- Set COM flags to `False` in `codebase/transport_workflow.py`:
  - `CHECK_BRANCHES_IN_LEAP_USING_COM`
  - `SET_VARS_IN_LEAP_USING_COM`
  - `INTERNATIONAL_CHECK_BRANCHES_IN_LEAP_USING_COM`
  - `INTERNATIONAL_SET_VARS_IN_LEAP_USING_COM`

## Run fails with missing source columns

Cause:

- Input file schema does not match expected transport source columns.

Fix:

- Compare against `EXPECTED_COLS_IN_SOURCE` in `codebase/config/basic_mappings.py`.
- Fix upstream column naming or add missing fields.

## Duplicate rows error after preprocessing

Cause:

- Proxy/combination row creation produced duplicate source keys.

Fix:

- Inspect `data/errors/duplicate_source_rows.csv`.
- Resolve mapping collisions or duplicate upstream rows.

## `reconcile_only` run fails with missing checkpoint input

Typical error context:

- Reconciliation expects `intermediate_data/export_df_for_viewing_checkpoint2_<economy>_<scenario>.pkl`.

Fix:

1. Run `RUN_PROFILE = "input_only"` (or `"full"`) first for that economy/scenario.
2. Confirm checkpoint file exists in `intermediate_data/`.
3. Rerun with `RUN_PROFILE = "reconcile_only"`.

## Template alignment failure (`Strict template alignment failed`)

Cause:

- Export keys (`Branch Path`, `Variable`, `Scenario`, `Region`) do not exactly match template keys.

Fix:

- Check branch/variable naming and scenario/region values.
- Confirm template file path in config:
  - `data/import_files/DEFAULT_transport_leap_import_TGT_REF_CA.xlsx`
- Review alignment reports in `results/` when generated:
  - `template_alignment_dropped_*_leap_sheet.csv`
  - `template_alignment_dropped_*_for_viewing_sheet.csv`

## Dashboard step fails with `No comparison input files found`

Cause:

- `results/checkpoint_audit/transport_pre_recon_vs_raw_disaggregated_*.csv` files are missing.

Fix:

- Generate comparison CSVs first using `codebase/results_analysis/transport_pre_recon_vs_raw_disaggregated.py`.
- Or set `RUN_RESULTS_DASHBOARD = False` until those inputs exist.

## File paths resolve incorrectly

Cause:

- Script executed outside repository root.

Fix:

Run from repo root:

```bash
python3 codebase/transport_workflow.py
```

## Unsure if run touched LEAP model

Current behavior:

- With current code, COM write path is disabled and COM-enabled runs fail fast.
- In practice, successful runs are file-based outputs only.

## Useful cleanup for debugging

- Remove specific stale checkpoints under `intermediate_data/` for the exact economy/scenario you are rerunning.
- Keep archives in `results/archive/` and `results/reconciliation/archive/` for comparison.
