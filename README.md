# LEAP Transport Toolkit

`leap_transport` turns APERC transport model outputs into LEAP-ready import
workbooks. It reads 9th-edition-shaped transport projections, maps them into the
10th-edition LEAP transport branch structure, and can reconcile base-year
transport energy against ESTO balances.

## Start Here

For most work, you only need to touch three things:

1. `config/env_leap.yml`: defines the Python environment.
2. `codebase/transport_workflow.py`: the main file where you choose the economy,
   scenario, and run mode.
3. `results/`: the folder where generated LEAP workbooks and reports appear.

The usual workflow is:

1. Open this folder in Visual Studio Code.
2. Create and select the `env_leap` Python environment.
3. Edit the run settings near the top of `codebase/transport_workflow.py`.
4. Run `python codebase/transport_workflow.py`.
5. Import the generated workbook into LEAP.

Keep the LEAP COM/API settings turned off unless the code has been deliberately
updated and tested for API use. Workbook import is the recommended route.

## Project Layout

The active code lives under `codebase/`. Older examples or notes may mention a
`code/` folder, but this repository uses `codebase/`.

- `codebase/transport_workflow.py`: main file to run. Edit settings here.
- `codebase/lifecycle_profile_workflow.py`: run this when survival or vintage
  lifecycle profiles need to be regenerated.
- `codebase/sales_workflow.py`: passenger/freight sales and turnover helpers.
- `codebase/functions/`: implementation code used by the workflow files.
- `codebase/config/`: economy settings, mappings, measure metadata, and LEAP
  expression rules.
- `config/env_leap.yml`: Conda environment definition.
- `data/`: input files, templates, lifecycle files, and error outputs.
- `intermediate_data/`: checkpoint files used for reruns and reconciliation.
- `results/`: generated workbooks, CSVs, diagnostics, and reconciliation files.
- `docs/`: detailed workflow, switch, mapping, and architecture notes.

As a new user, avoid editing files in `codebase/functions/` until you are trying
to change how the workflow works internally.

## Setup

### 1. Install Prerequisites

You need:

- Visual Studio Code.
- The VS Code `Python` extension.
- The VS Code `Jupyter` extension if you want interactive notebooks.
- Conda, usually through Miniconda or Anaconda.
- LEAP desktop installed on Windows for the final model import work.

### 2. Open A Terminal At The Repo Root

You can use either the VS Code terminal or a normal Windows terminal.

Option A, from VS Code:

1. Open this repository folder.
2. Open the built-in terminal with `Terminal > New Terminal`.
3. Check that you are in the folder containing `README.md`.

Option B, from Windows:

1. Open Command Prompt, PowerShell, or Anaconda Prompt.
2. Move into this repository folder with `cd`.

For example, in Command Prompt or Anaconda Prompt:

```cmd
cd C:\Users\Work\github\leap_transport
```

In PowerShell, the same command also works:

```powershell
cd C:\Users\Work\github\leap_transport
```

Check that the terminal is in the repo root. In Command Prompt, use:

```cmd
cd
```

In PowerShell, use:

```powershell
Get-Location
```

In a bash-style terminal, use:

```bash
pwd
```

### 3. Create The Python Environment

Run this from the repo root:

```bash
conda env create --prefix ./env_leap --file ./config/env_leap.yml
```

This creates a local environment folder called `env_leap`. It may take several
minutes.

If `env_leap` already exists, activate it instead. To update an existing
environment after `config/env_leap.yml` changes, run:

```bash
conda env update --prefix ./env_leap --file ./config/env_leap.yml --prune
```

Activate it:

```bash
conda activate ./env_leap
```

In PowerShell, use:

```powershell
conda activate .\env_leap
```

When activation works, your terminal prompt usually includes `env_leap`.

You can do a quick environment check with:

```bash
python -c "import pandas, openpyxl, matplotlib; print('environment ok')"
```

Shared helpers that used to come from the sibling `../leap_utilities` repo are
vendored into `codebase/functions/leap_utilities_functions.py`. That snapshot
was taken on 16/04/2026 and should be reviewed against upstream by 16/04/2027.

### 4. Select The Environment In VS Code

1. Press `Ctrl+Shift+P`.
2. Search for `Python: Select Interpreter`.
3. Choose the interpreter inside this repository's `env_leap` folder.

Typical paths:

- Windows: `.\env_leap\Scripts\python.exe`
- WSL/Linux-style terminal: `./env_leap/bin/python`

This makes VS Code run scripts with the same packages used by the terminal.

### 5. Use Interactive `#%%` Cells In VS Code

The recommended interactive workflow is to use `#%%` cells inside normal
`.py` files. This keeps the code as plain Python while still letting you run one
section at a time, like a notebook.

Create or open a Python file and split it into cells like this:

```python
#%%
import pandas as pd

#%%
df = pd.read_csv("some_input.csv")
df.head()
```

In VS Code:

1. Make sure the `Python` and `Jupyter` extensions are installed.
2. Select the `env_leap` interpreter using `Python: Select Interpreter`.
3. Open a `.py` file containing `#%%` cells.
4. Click `Run Cell`, `Run Below`, or `Run Above` above a cell.
5. If VS Code asks for a kernel, choose the `env_leap` interpreter.

You can still use `.ipynb` notebooks if you prefer them, but `#%%` cells are
usually easier to review, edit, and compare in git.

If VS Code does not show the environment as a kernel, run this once from the
activated environment:

```bash
python -m ipykernel install --user --name leap_transport_env --display-name "Python (leap_transport env_leap)"
```

Reload VS Code, then choose `Python (leap_transport env_leap)` as the kernel.

## First Safe Run

Start with a run that only creates files. This checks the pipeline without
writing anything into LEAP.

Open `codebase/transport_workflow.py` and set:

```python
RUN_PROFILE = "input_only"
INPUT_DATA_SOURCE = "raw"
CHECKPOINT_LOAD_STAGE = "none"
RUN_RESULTS_DASHBOARD = False
RUN_INTERNATIONAL_WORKFLOW = False

CHECK_BRANCHES_IN_LEAP_USING_COM = False
SET_VARS_IN_LEAP_USING_COM = False
AUTO_SET_MISSING_BRANCHES = False
ENSURE_FUELS_IN_LEAP = False

INTERNATIONAL_CHECK_BRANCHES_IN_LEAP_USING_COM = False
INTERNATIONAL_SET_VARS_IN_LEAP_USING_COM = False
INTERNATIONAL_AUTO_SET_MISSING_BRANCHES = False
INTERNATIONAL_ENSURE_FUELS_IN_LEAP = False
```

Then choose one economy and one scenario near the top of the same file:

```python
TRANSPORT_ECONOMY_SELECTION = "20_USA"
TRANSPORT_SCENARIO_SELECTION = "Reference"
```

Run from the repo root:

```bash
python codebase/transport_workflow.py
```

If the command finishes successfully, check `results/` for the generated
workbook and CSV files.

## Normal Transport Run

After the safe run works, change:

```python
RUN_PROFILE = "full"
```

Run again:

```bash
python codebase/transport_workflow.py
```

Keep the COM/API flags off. The output workbook should be imported into LEAP
manually.

## Reconciliation-Only Run

Use this when the export file already exists and you only want to rerun the
ESTO reconciliation step:

```python
RUN_PROFILE = "reconcile_only"
```

The reconciliation-only run expects this checkpoint to exist:

```text
intermediate_data/export_df_for_viewing_checkpoint2_<economy>_<scenario>.pkl
```

Review the files written to `results/reconciliation/` before using the final
LEAP inputs.

## Lifecycle Profiles

Lifecycle profiles describe fleet turnover and age structure:

- Survival profile: the probability a vehicle is still on the road at each age.
- Vintage profile: the age distribution of vehicles in the base-year stock.

Run the lifecycle workflow when survival curves, vintage curves, or turnover
assumptions have changed:

```bash
python codebase/lifecycle_profile_workflow.py
```

Main outputs:

- `data/lifecycle_profiles/vehicle_survival_modified.xlsx`
- `data/lifecycle_profiles/vintage_modelled_from_survival.xlsx`

These files are used by the sales and transport workflows. Existing lifecycle
outputs are archived before replacement.

Important LEAP note: lifecycle profile settings are made in Current Accounts.
The workbook import does not automatically create or assign every lifecycle
profile, so check those profiles manually in LEAP before final import work.

## Key Settings In `transport_workflow.py`

These are the settings most users need.

| Setting | What it does |
| --- | --- |
| `TRANSPORT_ECONOMY_SELECTION` | One economy such as `"20_USA"`, or `"all"`. |
| `TRANSPORT_SCENARIO_SELECTION` | One scenario such as `"Reference"`, or a list such as `["Reference", "Target"]`. |
| `ALL_RUN_MODE` | Used only when economy is `"all"`: `"separate"`, `"apec"`, or `"both"`. |
| `RUN_PROFILE` | `"input_only"`, `"reconcile_only"`, or `"full"`. |
| `INPUT_DATA_SOURCE` | `"raw"` to rebuild from source inputs, or `"checkpoint"` to resume from saved preprocessed data. |
| `CHECKPOINT_LOAD_STAGE` | `"none"` for normal runs, or a saved stage such as `"halfway"`, `"three_quarter"`, or `"export"`. |
| `SALES_MODE` | `"none"`, `"passenger"`, `"freight"`, or `"both"`. |
| `RUN_RESULTS_DASHBOARD` | Builds diagnostic dashboard files after the run. |
| `MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE` | Checks that generated rows match the LEAP import template structure. |

Use `docs/TRANSPORT_WORKFLOW_SWITCHES.md` for the full switch reference.

## Moving Outputs Into LEAP

Use workbook import.

Recommended process:

1. Keep all COM/API flags off.
2. Generate the export workbook from this repo.
3. Import the generated workbook into LEAP.
4. Check key branches, units, lifecycle profiles, sales shares, device shares,
   mileage, and fuel economy in LEAP.
5. Run reconciliation if needed.
6. Review reconciliation reports before finalising the scenario.

Direct COM/API writes are not recommended right now. The LEAP API path has been
unstable, and the current code intentionally blocks COM-enabled domestic and
international execution. If you see a LEAP API disabled error, turn the COM/API
flags off and rerun.

## Outputs To Check

Common outputs:

- `results/*transport_leap_export*.xlsx`: LEAP import/export workbook.
- `results/passenger_sales_*.csv`: passenger sales outputs.
- `results/freight_sales_*.csv`: freight sales outputs.
- `results/reconciliation/*.csv`: reconciliation reports.
- `results/transport_all_run_summary_*.csv`: all-run summaries.
- `results/runtime_stage_timings_*.csv`: runtime timing summaries.
- `intermediate_data/*.pkl`: checkpoints.
- `data/errors/*.csv`: validation/debug files written when something fails.

If dashboards are enabled, the dashboard files are written under:

```text
results/diagnostics/transport_results_series_comparison/
```

## Common Input Dependencies

Domestic workflow:

- `data/import_files/DEFAULT_transport_leap_import_TGT_REF_CA.xlsx`
- `data/transport_data_9th/model_output_detailed_2/`
- `data/transport_data_9th/model_output_with_fuels/`
- merged ESTO energy input configured in
  `codebase/config/transport_economy_config.py`
- `data/lifecycle_profiles/vehicle_survival_modified.xlsx`
- `data/lifecycle_profiles/vintage_modelled_from_survival.xlsx`

Lifecycle workflow:

- `data/lifecycle_profiles/vehicle_survival_original.xlsx`
- `data/lifecycle_profiles/vehicle_survival_modified.xlsx`
- `data/lifecycle_profiles/vintage_modelled_from_survival.xlsx`

International workflow:

- `data/international_bunker_outputs_20250421.csv`

## Model Logic In Brief

The road model is sales and stock based. The core accounting is:

```text
Energy = Stock x Mileage x Efficiency
```

Stocks evolve from new sales and retirements. Retirements are controlled by
survival profiles, while the base-year fleet age structure is represented by
vintage profiles. The model therefore needs sales shares, lifecycle profiles,
mileage, fuel economy, and stock or ownership assumptions to remain internally
consistent.

For passenger road, the intended modelling logic is:

1. Estimate a long-run vehicle ownership saturation level for each economy.
2. Use an S-curve where ownership is still approaching saturation.
3. Split the ownership envelope into vehicle types such as LPVs, motorcycles,
   and buses.
4. Convert annual stock targets into sales after accounting for surviving stock.

Motorcycles and buses can be converted into car-equivalent ownership when
setting the total ownership envelope, so a shift toward buses can reduce the
number of cars needed while still meeting transport demand.

Freight road follows the same stock-turnover logic but does not use the same
vehicle ownership saturation framing. Its sales trend is currently tied to
freight and industrial activity assumptions. Non-road transport is simpler:
activity is split across child branches and final energy intensity converts
activity into energy.

## LEAP Branch Structure

Road transport generally follows:

```text
Transport type > Vehicle type > Engine type > Fuel type
```

Common LEAP variables by level:

- Transport type: top-level stock, sales, retirements, and total final energy.
- Vehicle type: stock or stock share in Current Accounts, sales share in
  projection scenarios, and calculated sales/retirements.
- Engine type: first sales year, stock share, sales share, scrappage settings,
  retirements, and calculated stock.
- Fuel type: device share, fuel economy, correction factors, mileage, and final
  on-road mileage/fuel economy.

Non-road transport normally uses activity and final energy intensity instead of
the detailed road stock-flow structure.

## Glossary

- Current Accounts: LEAP's base-year calibration scenario.
- Projection scenario: a future scenario such as Reference or Target.
- Stock: vehicles currently in operation.
- Sales: new vehicles added in a year.
- Retirements: vehicles leaving the stock in a year.
- Sales Share: the share of parent-branch sales allocated to a child branch.
- Stock Share: the share of parent-branch stock allocated to a child branch,
  mostly used in Current Accounts.
- Device Share: the split of an engine type across fuels. These should sum to
  100 percent under the engine type.
- Scrappage: additional early retirement beyond the normal survival profile.
- Fuel Economy: energy consumed per unit of distance travelled.
- Mileage: annual distance travelled per vehicle.
- Final Energy Intensity: energy use per unit of activity for non-road branches.
- Reconciliation: adjustment of LEAP-side base-year transport energy so it
  matches ESTO totals.
- Checkpoint: a saved intermediate file that lets the workflow resume without
  rerunning every earlier step.

## Adding Or Updating An Economy

1. Add or update metadata and scenario paths in
   `codebase/config/transport_economy_config.py`.
2. Confirm detailed model outputs, fuel-detail files, merged-energy data, and
   lifecycle profile inputs exist.
3. Run `input_only` first.
4. Check generated workbook rows against the LEAP template.
5. Run `full` or `reconcile_only` once the dry run is clean.

## Common Problems

- `conda` is not recognized: open an Anaconda/Miniconda terminal, or make sure
  Conda is installed and initialized for your shell.
- `ModuleNotFoundError` for `functions.leap_utilities_functions`: confirm you
  are running from the repo root and that `codebase/functions/leap_utilities_functions.py`
  exists.
- Missing input file under `data/`: check the selected economy and scenario in
  `codebase/transport_workflow.py`, then check the paths in
  `codebase/config/transport_economy_config.py`.
- Missing checkpoint: use `INPUT_DATA_SOURCE = "raw"` and
  `CHECKPOINT_LOAD_STAGE = "none"`, or run `input_only` before
  `reconcile_only`.
- LEAP API or COM disabled error: keep all COM/API flags set to `False` and use
  workbook import.
- No output appears in `results/`: confirm the terminal is at the repo root and
  that the active terminal environment is `env_leap`.

## Where To Read More

- `docs/PROCESS_FLOW.md`: main operating runbook and end-to-end process flow.
- `docs/TRANSPORT_WORKFLOW_SWITCHES.md`: full switch reference.
- `docs/LIFECYCLE_WORKFLOW.md`: survival and vintage profile workflow.
- `docs/TRANSPORT_MAPPING_STYLE_COLUMN_GUIDE.md`: mapping spreadsheet guidance.
- `docs/FILE_GUIDE.md`: file-by-file guide to the active codebase.
- `docs/for ai/SYSTEM_ARCHITECTURE.md`: architecture and data-flow detail.
- `docs/for ai/MODULE_RELATIONSHIPS.md`: module dependency map.
- `docs/for ai/CHANGE_IMPACT_MATRIX.md`: what to retest after each type of
  change.
- `docs/for ai/TROUBLESHOOTING.md`: common failures and fixes.

The external Word documents in the APERC guides folder explain the model logic
and manual LEAP workflow in more narrative detail:

- `transport model documentation.docx`
- `transport model process.docx`
