# Runbook

This is the day-to-day operating guide for maintainers.

## 1) Pre-run checklist

- You are in repo root: `leap_transport/`.
- Conda env is active: `conda activate ./env_leap`.
- Input files referenced by the selected economy config exist.
- LEAP is open if you will use COM checks/writes.
- You have write access to `results/`, `intermediate_data/`, and `data/errors/`.

## 2) Choose economy config

Economy configs live in `code/transport_economy_config.py`.

Each economy+scenario defines:

- Source transport model file.
- ESTO balances path.
- Fuel output path.
- Lifecycle profiles.
- Output workbook paths.

At bottom of `code/MAIN_leap_import.py`, choose config with:

```python
transport_economy, transport_scenario, transport_cfg = load_transport_run_config("20_USA", 'Reference')
```

## 3) Main run flags

Edit flags near the bottom of `code/MAIN_leap_import.py`.

Top-level stage flags:

- `RUN_INPUT_CREATION`
- `RUN_PASSENGER_SALES`
- `RUN_FREIGHT_SALES`
- `RUN_RECONCILIATION`

Reconciliation behavior:

- `APPLY_ADJUSTMENTS_TO_FUTURE_YEARS`
- `REPORT_ADJUSTMENT_CHANGES`

COM behavior (inside `load_transport_into_leap(...)` and reconciliation call):

- `CHECK_BRANCHES_IN_LEAP_USING_COM`
- `SET_VARS_IN_LEAP_USING_COM`
- `AUTO_SET_MISSING_BRANCHES`

## 4) Checkpoints and reruns

`load_transport_into_leap(...)` includes checkpoint toggles:

- `LOAD_INPUT_CHECKPOINT`
- `LOAD_HALFWAY_CHECKPOINT`
- `LOAD_THREEQUART_WAY_CHECKPOINT`
- `LOAD_EXPORT_DF_CHECKPOINT`

Checkpoint files are written to `intermediate_data/`.

Use these only for debugging or long reruns. For clean production runs, prefer all `False` unless intentional.

## 5) Execute

Run from repo root:

```bash
python code/MAIN_leap_import.py
```

## 6) Validate output

Minimum checks after run:

- Export workbook exists at `transport_cfg.transport_export_path`.
- Passenger/freight sales CSVs were created if enabled.
- No unresolved errors in terminal output.
- If reconciliation enabled, files in `results/reconciliation/` were updated.

If COM writes were enabled:

- Spot-check a few LEAP branches for expected expressions and units.

## 7) Typical run profiles

### Profile A: First-time onboarding

- Input creation only.
- No reconciliation.
- No COM access.

### Profile B: File-only production

- Input creation + reconciliation.
- COM disabled.
- Handover via generated workbook and reconciliation reports.

### Profile C: Full LEAP sync

- Input creation + reconciliation.
- COM checks and writes enabled.
- LEAP model manually reviewed after run.

## 8) Adding a new economy

1. Add economy/scenario entry in `code/transport_economy_config.py`.
2. Ensure all referenced files exist.
3. Set `load_transport_run_config("<new_code>", "<scenario>")` in `code/MAIN_leap_import.py`.
4. Run safe dry run first.
5. Run full profile once dry run passes.

## 9) Important behaviors to know

- Source file must include all fields in `EXPECTED_COLS_IN_SOURCE` (`code/basic_mappings.py`).
- Duplicates after proxy/combo row creation fail the run and write details to `data/errors/duplicate_source_rows.csv`.
- Final export is expression-based; method metadata is preserved for reference.
- Reconciliation archives overwritten prior outputs in `results/archive/` and `results/reconciliation/archive/`.
