# LEAP Transport Toolkit

This repository converts transport model outputs into LEAP-ready import/export files for the 10th edition workflow.

If you are new to this project, start here:

1. Read `docs/START_HERE.md`.
2. Run the lifecycle stage first if turnover assumptions changed: `docs/LIFECYCLE_WORKFLOW.md`.
3. Run one dry run with COM writes turned off.
4. Use `docs/RUNBOOK.md` when you are ready for a full run.

## Why this exists

The transport pipeline still depends on legacy 9th-edition shaped data. This code handles the translation into LEAP branch structures, variables, and expressions, and then reconciles base-year energy against ESTO balances.

## What this repo does

- Reads transport model outputs for one economy/scenario.
- Expands and maps source rows to LEAP branch tuples.
- Applies measure logic and builds LEAP expressions.
- Writes LEAP export/import workbooks.
- Generates lifecycle inputs (`vehicle_survival_modified.xlsx`, `vintage_modelled_from_survival.xlsx`) used by stock/sales calculations.
- Optionally writes values directly into an open LEAP model using COM.
- Runs reconciliation adjustments against ESTO and archives change reports.

## Who this is for

- Maintainers running or updating transport imports.
- Analysts adding new economy configs.
- Handover recipients who need a practical operating guide.

## Repository map

- `codebase/lifecycle_profile_workflow.py`: top-level lifecycle generation workflow (survival -> vintage).
- `codebase/transport_workflow.py`: main transport entry point (run settings + orchestration call).
- `codebase/functions/transport_workflow_pipeline.py`: transport workflow implementation (processing, export, reconciliation).
- `codebase/config/transport_economy_config.py`: economy/scenario-specific file paths and defaults.
- `codebase/config/branch_mappings.py`: branch tuple definitions and measure config.
- `codebase/functions/measure_processing.py`: measure preparation logic per branch.
- `codebase/functions/energy_use_reconciliation_road.py`: transport reconciliation functions.
- `codebase/sales_workflow.py`: policy-aware lifecycle/sales workflow wrapper.
- `codebase/functions/lifecycle_profile_editor.py`: lifecycle profile editor and vintage-from-survival generator.
- `config/env_leap.yml`: Python environment dependencies.
- `data/`: source inputs and templates.
- `results/`: output files and archived versions.
- `intermediate_data/`: checkpoints to speed reruns.

## Setup

### 1) Prerequisites

- Windows machine with LEAP desktop installed.
- LEAP model available locally.
- Conda available.
- Access to shared helper package from the separate `leap_utilities` repo (`leap_utils` legacy path or `codebase` renamed path).

### 2) Environment

From repo root:

```bash
conda env create --prefix ./env_leap --file ./config/env_leap.yml
conda activate ./env_leap
```

If helper modules are not already importable:

```bash
pip install -e ../leap_utilities
```

## Running the pipeline

### 1) Lifecycle stage (upstream)

Run first when survival/vintage assumptions need to be refreshed:

```bash
python codebase/lifecycle_profile_workflow.py
```

### 2) Transport workflow

The script is configured in `codebase/transport_workflow.py`.

### Common run modes

- `RUN_INPUT_CREATION = True`: build export/import files.
- `RUN_RECONCILIATION = True`: apply ESTO reconciliation and save change reports.
- `SET_VARS_IN_LEAP_USING_COM = True`: write expressions to LEAP through COM.

Run from repo root:

```bash
python codebase/transport_workflow.py
```

## Recommended first run (safe)

Use this combination first:

- `RUN_INPUT_CREATION = True`
- `RUN_RECONCILIATION = False`
- `SET_VARS_IN_LEAP_USING_COM = False`
- `CHECK_BRANCHES_IN_LEAP_USING_COM = False`

This generates files without touching LEAP.

## Outputs

- Main workbook: configured `transport_export_path` (usually under `results/`).
- Sales files: `results/passenger_sales_*.csv`, `results/freight_sales_*.csv`.
- Lifecycle files: `data/lifecycle_profiles/vehicle_survival_modified.xlsx`, `data/lifecycle_profiles/vintage_modelled_from_survival.xlsx`.
- Reconciliation reports: `results/reconciliation/*.csv`.
- Checkpoints: `intermediate_data/*.pkl`.
- Error exports: `data/errors/*.csv`.

## Known constraints

- Keep LEAP open when using COM writes/checks.
- LEAP API cannot fully auto-create all stock-based road nodes; some branches may need manual creation in LEAP.
- Some share/percentage variables may need manual scale verification in LEAP UI.

## Handover docs

- `docs/START_HERE.md`: minimum onboarding path.
- `docs/LIFECYCLE_WORKFLOW.md`: dedicated lifecycle generation workflow.
- `docs/TRANSPORT_WORKFLOW_SWITCHES.md`: full switch reference for `codebase/transport_workflow.py`.
- `docs/RUNBOOK.md`: detailed operating guide.
- `docs/TROUBLESHOOTING.md`: common failures and fixes.
- `docs/FILE_GUIDE.md`: what each core module is responsible for.
- `docs/MODULE_RELATIONSHIPS.md`: scanned map of every `codebase/*.py` file and how modules depend on each other.
- `docs/SYSTEM_ARCHITECTURE.md`: in-depth architecture (layering, data flow, control flags, and boundaries).
- `docs/PROCESS_FLOW.md`: plain-English beginning-to-end process flow (single, all, and 00_APEC modes).
- `docs/CHANGE_IMPACT_MATRIX.md`: change-impact and retest matrix.
- `docs/leap-system.drawio`: editable Draw.io system architecture diagram.
