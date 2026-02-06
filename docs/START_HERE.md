# Start Here

This guide is for someone taking over the toolkit who needs a low-risk first run.

## 1) Understand the workflow in one minute

The pipeline has two major stages:

1. `Input creation`: transform transport model data into LEAP export/import files.
2. `Reconciliation`: align LEAP base-year energy with ESTO totals.

Both are controlled by flags at the bottom of `code/MAIN_leap_import.py`.

## 2) Prepare your environment

From repo root:

```bash
conda env create --prefix ./env_leap --file ./config/env_leap.yml
conda activate ./env_leap
```

Install helper utilities if needed:

```bash
pip install -e ../leap_utilities
```

## 3) Pick economy and scenario

Open `code/MAIN_leap_import.py` and locate:

```python
transport_economy, transport_scenario, transport_cfg = load_transport_run_config("20_USA", 'Reference')
```

Change this to your target economy/scenario that exists in `code/transport_economy_config.py`.

## 4) Do a safe dry run first

Set these flags in `code/MAIN_leap_import.py`:

- `RUN_INPUT_CREATION = True`
- `RUN_RECONCILIATION = False`
- In the `load_transport_into_leap(...)` call, set `CHECK_BRANCHES_IN_LEAP_USING_COM=False`.
- In the `load_transport_into_leap(...)` call, set `SET_VARS_IN_LEAP_USING_COM=False`.

Then run:

```bash
python code/MAIN_leap_import.py
```

Check that a workbook appears in `results/`.

## 5) Move to full run

After dry run succeeds:

- Enable `RUN_RECONCILIATION = True`.
- Enable COM checks/writes only when LEAP is open and model path is confirmed.

Use `docs/RUNBOOK.md` for full operating steps.

## 6) If you are stuck

Go to `docs/TROUBLESHOOTING.md` first. Most issues are path, mapping, or branch existence problems.
