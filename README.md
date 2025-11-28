# LEAP utilities toolkit

Lightweight helpers to prepare data for LEAP, reconcile energy use with ESTO balances, and keep results tidy. Designed for busy researchers—minimal setup, clear entry points, and short runs.

## Quick start
1. Windows + Anaconda/Miniconda.
2. In an Anaconda Prompt, `cd` to this folder.
3. Create the environment: `conda env create --prefix ./env_leap --file ./config/env_leap.yml`.
4. Activate when working: `conda activate ./env_leap`.

## Where to begin
- **Non-transport tasks:** `code/MAIN_other_sectors.py` copies 9th edition balances into LEAP key assumptions and is a template for other sectors.
- **Energy reconciliation:** `energy_use_reconciliation.py` aligns sector totals with ESTO; plug in your own branch-specific calculators.

## Folder map
- `code/` Python scripts (start with the files above).
- `config/` Conda environment and LEAP type library.
- `data/` Input sources (ESTO balances, model files).
- `intermediate_data/` Checkpoints created during runs.
- `results/` Finished LEAP import/export files and reconciliation reports.
- `plotting_output/` Charts produced by optional plotting steps.

## Coding style at a glance
- Scripts are split into `#%%` cells for quick interactive runs (VS Code/Jupyter).
- Functions first, orchestration later: utilities and data loaders sit near the top; `MAIN_*` blocks call them in order.
- Prefer small, single-purpose functions over classes; keep inputs/outputs explicit.
- Breakpoints are sprinkled in so you can step through cells when debugging.

## Tips
- Open LEAP before pushing values via COM.
- Run commands from this folder so `./config/env_leap.yml` is found.
- Large data files are ignored by Git; keep source data in `data/`.
