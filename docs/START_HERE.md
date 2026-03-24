# Start Here

This guide is for someone taking over the toolkit who needs a low-risk first run.

## 1) Understand the workflow in one minute

The pipeline has two major stages:

1. `Input creation`: transform transport model data into LEAP export/import files.
2. `Reconciliation`: align LEAP base-year energy with ESTO totals.

Both are controlled by flags in `code/transport_workflow.py`.

## 2) Prepare your environment

From repo root:

```bash
conda env create --prefix ./env_leap --file ./config/env_leap.yml
conda activate ./env_leap
```

Install helper utilities if needed (works for both the legacy `leap_utils` folder and renamed `codebase` folder in that repo):

```bash
pip install -e ../leap_utilities
```

## 3) Pick economy and scenario

Open `code/transport_workflow.py` and set:

```python
TRANSPORT_ECONOMY_SELECTION = "20_USA"
TRANSPORT_SCENARIO_SELECTION = "Reference"
```

Ensure the selected economy/scenario exists in `code/transport_economy_config.py`.

## 4) Do a safe dry run first

Set these flags in `code/transport_workflow.py`:

- `RUN_PROFILE = "input_only"`
- `CHECK_BRANCHES_IN_LEAP_USING_COM = False`
- `SET_VARS_IN_LEAP_USING_COM = False`

Then run:

```bash
python code/transport_workflow.py
```

Check that a workbook appears in `results/`.

## 5) Move to full run

After dry run succeeds:

- Switch to `RUN_PROFILE = "full"`.
- Enable COM checks/writes only when LEAP is open and model path is confirmed.

Use `docs/RUNBOOK.md` for full operating steps.

## 6) If you are stuck

Go to `docs/TROUBLESHOOTING.md` first. Most issues are path, mapping, or branch existence problems.
