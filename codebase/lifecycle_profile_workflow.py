"""Standalone lifecycle profile workflow entrypoint.

This script makes lifecycle profile generation an explicit upstream run stage:
1) create/update `vehicle_survival_modified.xlsx`
2) derive `vintage_modelled_from_survival.xlsx` from that survival curve
"""
#%%
from __future__ import annotations

from functions.lifecycle_profile_editor import (
    build_vintage_from_survival_excel,
    main as run_lifecycle_profile_editor,
)
from functions.path_utils import resolve_str


# Stage toggles
RUN_SURVIVAL_PROFILE_EDIT = True
RUN_VINTAGE_FROM_SURVIVAL = True

# Stage 1: survival profile editing
SURVIVAL_LIFECYCLE_TYPE = "vehicle_survival"
SURVIVAL_BASE_YEAR = None
SURVIVAL_ORIGINAL_PATH = "data/lifecycle_profiles/vehicle_survival_original.xlsx"
SURVIVAL_MODIFIED_PATH = "data/lifecycle_profiles/vehicle_survival_modified.xlsx"
SURVIVAL_SCALE_AGE_BAND_AGE_MIN = 4
SURVIVAL_SCALE_AGE_BAND_AGE_MAX = 15
SURVIVAL_SCALE_AGE_BAND_FACTOR = 1.0
SURVIVAL_SMOOTHING_DICT = {1: 2}
SURVIVAL_AUTO_OPEN = False
SURVIVAL_VERBOSE_EXPLANATIONS = True
SURVIVAL_PLOT_PROFILES = False

# Stage 2: vintage generation from survival
VINTAGE_INPUT_SURVIVAL_PATH = SURVIVAL_MODIFIED_PATH
VINTAGE_OUTPUT_PATH = "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx"
VINTAGE_SHEET_NAME = "Lifecycle Profiles"
VINTAGE_TOTAL_STOCK = 1000.0
VINTAGE_PROFILE_NAME_SUFFIX = " (steady-state vintage from survival)"
VINTAGE_AUTO_OPEN = False
VINTAGE_ANNUAL_SURVIVAL_OUTPUT_PATH = None
VINTAGE_RUN_SIMULATION = False
VINTAGE_SIMULATION_YEARS = 60
VINTAGE_TURNOVER_RATE_BOUNDS = (0.03, 0.07)
VINTAGE_VERBOSE_EXPLANATIONS = True


def run_with_config() -> dict[str, str | float]:
    """Run selected lifecycle workflow stages and return key outputs."""
    if not RUN_SURVIVAL_PROFILE_EDIT and not RUN_VINTAGE_FROM_SURVIVAL:
        print("[INFO] No lifecycle stages selected. Set at least one RUN_* flag to True.")
        return {}

    outputs: dict[str, str | float] = {}

    if RUN_SURVIVAL_PROFILE_EDIT:
        print("=== Lifecycle stage 1/2: build modified survival profile")
        run_lifecycle_profile_editor(
            lifecycle_type=SURVIVAL_LIFECYCLE_TYPE,
            base_year=SURVIVAL_BASE_YEAR,
            original_path=SURVIVAL_ORIGINAL_PATH,
            new_path=SURVIVAL_MODIFIED_PATH,
            scale_age_band_age_min=SURVIVAL_SCALE_AGE_BAND_AGE_MIN,
            scale_age_band_age_max=SURVIVAL_SCALE_AGE_BAND_AGE_MAX,
            scale_age_band_factor=SURVIVAL_SCALE_AGE_BAND_FACTOR,
            smoothing_dict=SURVIVAL_SMOOTHING_DICT,
            auto_open=SURVIVAL_AUTO_OPEN,
            verbose_explanations=SURVIVAL_VERBOSE_EXPLANATIONS,
            plot_profiles=SURVIVAL_PLOT_PROFILES,
        )
        outputs["vehicle_survival_modified_path"] = resolve_str(SURVIVAL_MODIFIED_PATH)
    else:
        print("=== Lifecycle stage 1/2 skipped: RUN_SURVIVAL_PROFILE_EDIT=False")

    if RUN_VINTAGE_FROM_SURVIVAL:
        print("=== Lifecycle stage 2/2: derive steady-state vintage from survival")
        saved_path, constant_sales = build_vintage_from_survival_excel(
            survival_excel_path=resolve_str(VINTAGE_INPUT_SURVIVAL_PATH),
            vintage_excel_path=resolve_str(VINTAGE_OUTPUT_PATH),
            sheet_name=VINTAGE_SHEET_NAME,
            total_stock=VINTAGE_TOTAL_STOCK,
            profile_name_suffix=VINTAGE_PROFILE_NAME_SUFFIX,
            auto_open=VINTAGE_AUTO_OPEN,
            annual_survival_output_path=(
                resolve_str(VINTAGE_ANNUAL_SURVIVAL_OUTPUT_PATH)
                if VINTAGE_ANNUAL_SURVIVAL_OUTPUT_PATH
                else None
            ),
            run_simulation=VINTAGE_RUN_SIMULATION,
            simulation_years=VINTAGE_SIMULATION_YEARS,
            turnover_rate_bounds=VINTAGE_TURNOVER_RATE_BOUNDS,
            verbose_explanations=VINTAGE_VERBOSE_EXPLANATIONS,
        )
        outputs["vintage_modelled_from_survival_path"] = str(saved_path)
        outputs["implied_constant_annual_sales"] = float(constant_sales)
    else:
        print("=== Lifecycle stage 2/2 skipped: RUN_VINTAGE_FROM_SURVIVAL=False")

    print("=== Lifecycle workflow complete")
    for key, value in outputs.items():
        print(f" - {key}: {value}")

    return outputs


if __name__ == "__main__":
    run_with_config()
#%%
