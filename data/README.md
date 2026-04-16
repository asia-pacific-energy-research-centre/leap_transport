# Data Folder Guide

This folder holds the inputs, generated workbooks, scratch files, and archives used by the transport pipeline.

## What stays here

### Active inputs the code reads directly

- `import_files/DEFAULT_transport_leap_import_TGT_REF_CA.xlsx`
  - LEAP import template used by the main transport workflow.
- `transport_data_9th/model_output_detailed_2/`
  - Raw 9th-edition transport model outputs by economy.
- `transport_data_9th/model_output_with_fuels/`
  - Fuel-split companion files for the same economies.
- `merged_file_energy_ALL_20250814_pretrump.csv`
  - Default merged ESTO energy input used by the domestic workflow and several helper functions.
- `merged_file_energy_00_APEC_20251106.csv`
  - APEC merged-energy input used by historical export helpers.
- `merged_file_energy_00_APEC_20250814_pretrump.csv`
  - Legacy APEC fallback that is still referenced by `codebase/functions/merged_energy_io.py`.
- `international_bunker_outputs_20250421.csv`
  - Input for the international transport workflow.
- `lifecycle_profiles/vehicle_survival_modified.xlsx`
  - Lifecycle survival profile consumed by sales and transport workflows.
- `lifecycle_profiles/vintage_modelled_from_survival.xlsx`
  - Lifecycle vintage profile consumed by sales and transport workflows.

### Generated outputs that are still part of the workflow

- `TRANSPORT_all_APPLICABLE_historical_sectors_fuels_9th_outlook.xlsx`
- `TRANSPORT_all_NONAPEC_historical_energy_use.xlsx`
  - Historical export workbooks created from the merged-energy inputs.
- `errors/duplicate_source_rows.csv`
  - Debug export written when source-key validation fails.

### Runtime scratch and intermediate files

- `temp/`
  - Temporary pipeline artifacts, checkpoints, and provenance files.
  - Safe to clean if you are not in the middle of a run.
- `temp/*.pkl`, `temp/*.csv`
  - Intermediate working files produced during preprocessing or validation.

### Archives

- `archive/`
  - Superseded source files, old exports, ad hoc analysis workbooks, and one-off experiments.
  - Includes legacy USA working copies such as `USA fuels model output.csv` and `USA transport file.xlsx`.
- `import_files/archive/`
  - Older import templates kept out of the active path.
- `lifecycle_profiles/archive/`
  - Previous lifecycle profile versions automatically retained when new ones are written.

## Files that look like archive candidates

These files are visible at the top level but are not referenced by the current code paths I checked:

- `12_NZ_NON_ROAD_DETAILED_model_output20250226 fuels.csv`
- `12_NZ_NON_ROAD_DETAILED_model_output20250226 regular.csv`
- `all transport balances data.xlsx`
- `merged_file_energy_ALL_20251106.csv`

Why they may still matter:

- The two `12_NZ...` files look like legacy working copies or alternate snapshots that can be useful for manual analysis, comparison, or regeneration.
- `all transport balances data.xlsx` is a legacy workbook name that appears only in commented-out validation code.
- `merged_file_energy_ALL_20251106.csv` is an alternate merged-energy snapshot that is not part of the current defaults.

If nobody is using them for ad hoc work, they are reasonable candidates to move into `archive/`.

## Cleanup rule of thumb

- Keep files here if code imports them or workflows regenerate them automatically.
- Move files to `archive/` if they are superseded, manually curated, or only needed for historical reference.
- Keep `temp/` disposable.

## Common workflow dependencies

- Domestic transport workflow:
  - `import_files/DEFAULT_transport_leap_import_TGT_REF_CA.xlsx`
  - `merged_file_energy_ALL_20250814_pretrump.csv`
  - `lifecycle_profiles/vehicle_survival_modified.xlsx`
  - `lifecycle_profiles/vintage_modelled_from_survival.xlsx`
  - `transport_data_9th/model_output_detailed_2/`
  - `transport_data_9th/model_output_with_fuels/`
- Lifecycle workflow:
  - `lifecycle_profiles/vehicle_survival_original.xlsx`
  - `lifecycle_profiles/vehicle_survival_modified.xlsx`
  - `lifecycle_profiles/vintage_modelled_from_survival.xlsx`
- International workflow:
  - `international_bunker_outputs_20250421.csv`

## Notes

- `.gitkeep` files exist so empty directories remain in version control.
- Some filenames in `archive/` are old snapshots of the same data and may be useful only for traceability.
- `merged_file_energy_00_APEC_20250814_pretrump.csv` is still used as a fallback path in `codebase/functions/merged_energy_io.py`, so keep it until you intentionally switch the default APEC input.
- If you remove or rename any active input, update the matching path in `codebase/config/transport_economy_config.py` or the relevant workflow module.
