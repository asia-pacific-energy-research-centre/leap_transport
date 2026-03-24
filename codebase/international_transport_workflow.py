"""Thin standalone entrypoint for international transport export + LEAP sync."""
#%%
from __future__ import annotations

from functions.international_transport_pipeline import (
    InternationalExportConfig,
    run_international_export_workflow,
)


INTERNATIONAL_INPUT_PATH = "data/international_bunker_outputs_20250421.csv"
INTERNATIONAL_OUTPUT_DIR = "results/international"
INTERNATIONAL_SCENARIO = ["Reference", "Target"]
INTERNATIONAL_SCOPE = "20_USA"
BASE_YEAR = 2022
FINAL_YEAR = 2060
EMIT_QUALITY_REPORT = True
EMIT_MEDIUM_SUMMARY = True

# COM / validation flags
CHECK_BRANCHES_IN_LEAP_USING_COM = True
SET_VARS_IN_LEAP_USING_COM = True
AUTO_SET_MISSING_BRANCHES = True
ENSURE_FUELS_IN_LEAP = True


def build_config() -> InternationalExportConfig:
    return InternationalExportConfig(
        input_path=INTERNATIONAL_INPUT_PATH,
        output_dir=INTERNATIONAL_OUTPUT_DIR,
        scenario=INTERNATIONAL_SCENARIO,
        scope=INTERNATIONAL_SCOPE,
        base_year=BASE_YEAR,
        final_year=FINAL_YEAR,
        emit_quality_report=EMIT_QUALITY_REPORT,
        emit_medium_summary=EMIT_MEDIUM_SUMMARY,
        check_branches_in_leap_using_com=CHECK_BRANCHES_IN_LEAP_USING_COM,
        set_vars_in_leap_using_com=SET_VARS_IN_LEAP_USING_COM,
        auto_set_missing_branches=AUTO_SET_MISSING_BRANCHES,
        ensure_fuels_in_leap=ENSURE_FUELS_IN_LEAP,
    )


def run_with_config() -> dict[str, str]:
    return run_international_export_workflow(build_config())


if __name__ == "__main__":
    run_with_config()
#%%
