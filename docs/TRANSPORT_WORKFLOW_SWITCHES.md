# Transport Workflow Switches

This is the switch reference for `codebase/transport_workflow.py`.

## 1) Scope and selection

- `TRANSPORT_ECONOMY_SELECTION`
  - Controls domestic economy scope.
  - Typical: `"20_USA"` or `"all"`.
  - If not `"all"`, `ALL_RUN_MODE` is ignored.

- `TRANSPORT_SCENARIO_SELECTION`
  - Scenario or scenarios to run.
  - Accepts single string or list (for example `"Reference"` or `["Reference", "Target"]`).
  - `Current Accounts` is auto-generated and should not be provided manually.

- `ALL_RUN_MODE`
  - Applies when `TRANSPORT_ECONOMY_SELECTION == "all"`.
  - Values:
    - `"separate"`: run each economy separately.
    - `"apec"`: run only synthetic `00_APEC`.
    - `"both"`: run separate economies, then `00_APEC`.

- `PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC`
  - In `all+apec` mode, optionally pre-runs separate input prep before synthetic aggregation.

- `APEC_REGION`
  - Synthetic APEC token for aggregation logic.

- `APEC_LEAP_REGION_OVERRIDE`
  - Optional LEAP-region override for synthetic exports.

- `APEC_BASE_YEAR`, `APEC_FINAL_YEAR`
  - Year bounds used for synthetic run configuration.

## 2) Stage and execution behavior

- `RUN_PROFILE`
  - Values:
    - `"input_only"`
    - `"reconcile_only"`
    - `"full"`
  - Alias parsing in pipeline also accepts `input`, `reconcile`, `all`.

- `RUN_RESULTS_DASHBOARD`
  - Runs post-processing dashboard workflow.
  - Requires comparison CSV inputs in `results/checkpoint_audit/`.

- `RUN_OUTPUT_MODE`
  - Console verbosity:
    - `"full"`
    - `"stage_economy"`

## 3) Input and checkpoint controls

- `INPUT_DATA_SOURCE`
  - `"raw"`: rebuild preprocessing.
  - `"checkpoint"`: load preprocessed input checkpoint.
  - Aliases accepted: `ckpt`, `pkl`.

- `CHECKPOINT_LOAD_STAGE`
  - Resume export pipeline stage:
    - `"none"`, `"halfway"`, `"three_quarter"`, `"export"`
  - Aliases accepted: `half`, `threequarter`, `three_quart`, `threequart`.

- `MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE`
  - Enables template alignment gate against LEAP import template structure.

## 4) Sales and turnover policy controls

- `SALES_MODE`
  - `"none"`, `"passenger"`, `"freight"`, `"both"`.
  - Alias `all` maps to `both`.

- `PASSENGER_PLOT`
  - Enables passenger diagnostic plotting.

- `SCENARIO_SALES_POLICY_SETTINGS`
  - Scenario-keyed policy payload.
  - Mode blocks: `passenger`, `freight`.
  - Supported keys include:
    - `turnover_policies`
    - `drive_turnover_policy`
    - `drive_policy_dataframe`
    - `drive_policy_checkpoint_path`
    - `drive_policy_stocks_col`
    - `drive_policy_vehicle_type_map`
    - `analysis_initial_fleet_age_shift_years`

## 5) Reconciliation controls

- `APPLY_ADJUSTMENTS_TO_FUTURE_YEARS`
  - Propagates base-year adjustment factors forward.

- `REPORT_ADJUSTMENT_CHANGES`
  - Emits detailed reconciliation delta reports.

- `ESTO_ZERO_ENERGY_FALLBACK_RULES`
  - Targeted fallback rules when LEAP is near-zero but ESTO target is non-zero.
  - Supported rule types:
    - `mode_stock_seed`
    - `scalar_min`

## 6) Domestic LEAP COM flags

- `CHECK_BRANCHES_IN_LEAP_USING_COM`
- `SET_VARS_IN_LEAP_USING_COM`
- `AUTO_SET_MISSING_BRANCHES`
- `ENSURE_FUELS_IN_LEAP`

Current behavior: domestic pipeline currently raises an error if COM access flags are enabled (LEAP API path is intentionally disabled in code).

## 7) International workflow flags

- `RUN_INTERNATIONAL_WORKFLOW`
- `INTERNATIONAL_INPUT_PATH`
- `INTERNATIONAL_OUTPUT_DIR`
- `INTERNATIONAL_EMIT_QUALITY_REPORT`
- `INTERNATIONAL_EMIT_MEDIUM_SUMMARY`
- `INTERNATIONAL_CHECK_BRANCHES_IN_LEAP_USING_COM`
- `INTERNATIONAL_SET_VARS_IN_LEAP_USING_COM`
- `INTERNATIONAL_AUTO_SET_MISSING_BRANCHES`
- `INTERNATIONAL_ENSURE_FUELS_IN_LEAP`

Current behavior: international workflow also blocks COM-enabled execution for now.

## 8) Failure handling

- `CRITICAL_FAILURE_PATTERNS`
  - Error signatures treated as hard failures to avoid partial combined outputs.

## 9) Practical presets

- Safe first run:
  - `RUN_PROFILE = "input_only"`
  - `RUN_INTERNATIONAL_WORKFLOW = False` (optional)
  - `INPUT_DATA_SOURCE = "raw"`
  - `CHECKPOINT_LOAD_STAGE = "none"`
  - all COM flags `False`

- Reconciliation-only rerun:
  - `RUN_PROFILE = "reconcile_only"`
  - Ensure checkpoint exists: `intermediate_data/export_df_for_viewing_checkpoint2_<economy>_<scenario>.pkl`

- Full file-generation flow (no COM writes):
  - `RUN_PROFILE = "full"`
  - keep all COM flags `False`
  - keep reporting flags `True`
