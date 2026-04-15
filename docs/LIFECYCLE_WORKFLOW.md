# Lifecycle Workflow

This is the dedicated upstream workflow for lifecycle profile generation.

Use this before transport input creation when you need to change survival/vintage assumptions that drive stock turnover and sales patterns.

## 1) Entrypoint

Run:

```bash
python codebase/lifecycle_profile_workflow.py
```

This script is a top-level workflow wrapper around `codebase/functions/lifecycle_profile_editor.py`.

## 2) What it produces

By default, it runs two stages:

1. Build modified survival profile:
   - Input: `data/lifecycle_profiles/vehicle_survival_original.xlsx`
   - Output: `data/lifecycle_profiles/vehicle_survival_modified.xlsx`
2. Derive steady-state vintage from that survival profile:
   - Input: `data/lifecycle_profiles/vehicle_survival_modified.xlsx`
   - Output: `data/lifecycle_profiles/vintage_modelled_from_survival.xlsx`

These files are then consumed by the main transport workflow (`codebase/transport_workflow.py`).

## 3) Archiving behavior

Writes are handled by `save_lifecycle_profile_excel(...)` in
`codebase/functions/lifecycle_profile_editor.py`.

If an output file already exists, it is moved to `data/lifecycle_profiles/archive/`
before the replacement file is written.

## 4) Configuring the lifecycle workflow

Open `codebase/lifecycle_profile_workflow.py` and adjust:

- Stage toggles:
  - `RUN_SURVIVAL_PROFILE_EDIT`
  - `RUN_VINTAGE_FROM_SURVIVAL`
- Survival-stage parameters:
  - input/output paths
  - age-band scaling and smoothing
  - plotting and verbosity flags
- Vintage-stage parameters:
  - input/output paths
  - turnover bounds
  - simulation flags

## 5) Validation

Quick syntax check:

```bash
python3 -m py_compile codebase/functions/lifecycle_profile_editor.py codebase/lifecycle_profile_workflow.py codebase/transport_workflow.py
```

## 6) Where it sits in the full pipeline

Recommended run sequence:

1. `python codebase/lifecycle_profile_workflow.py`
2. `python codebase/transport_workflow.py` with `RUN_PROFILE = "input_only"` (safe first run)
3. `python codebase/transport_workflow.py` with reconciliation/full profile as needed
