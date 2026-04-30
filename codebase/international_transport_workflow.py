"""Thin standalone entrypoint for international transport export + LEAP sync."""
#%%
from __future__ import annotations

from functions.international_transport_pipeline import (
    InternationalExportConfig,
    run_international_export_workflow,
)
from functions.transport_workflow_pipeline import list_transport_run_configs
from functions.workflow_utilities import resolve_scenario_selection


INTERNATIONAL_INPUT_PATH = "data/international_bunker_outputs_20250421.csv"
INTERNATIONAL_OUTPUT_DIR = "results/international"
INTERNATIONAL_SCENARIO = ["Reference", "Target"]
# Set to an economy code like "20_USA", synthetic "00_APEC", or "all".
INTERNATIONAL_SCOPE = "all"
BASE_YEAR = 2022
FINAL_YEAR = 2060
EMIT_QUALITY_REPORT = True
EMIT_MEDIUM_SUMMARY = True
RECONCILE_TO_ESTO = True
MAPPING_WORKBOOK_PATH = "config/leap_mappings 25042026.xlsx"
MAPPING_ESTO_PATH = "data/00APEC_2024_low_with_subtotals.csv"
EMIT_RECONCILIATION_REPORT = True

# COM / validation flags
CHECK_BRANCHES_IN_LEAP_USING_COM = False
SET_VARS_IN_LEAP_USING_COM = False
AUTO_SET_MISSING_BRANCHES = False
ENSURE_FUELS_IN_LEAP = False


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
        reconcile_to_esto=RECONCILE_TO_ESTO,
        mapping_workbook_path=MAPPING_WORKBOOK_PATH,
        mapping_esto_path=MAPPING_ESTO_PATH,
        emit_reconciliation_report=EMIT_RECONCILIATION_REPORT,
    )


def _resolve_scopes_for_run() -> list[str]:
    scope_text = str(INTERNATIONAL_SCOPE).strip()
    if not scope_text:
        raise ValueError("INTERNATIONAL_SCOPE must be a non-empty string.")
    if scope_text.lower() != "all":
        return [scope_text]

    scenario_labels = resolve_scenario_selection(INTERNATIONAL_SCENARIO)
    scopes: list[str] = []
    seen: set[str] = set()
    for scenario in scenario_labels:
        for economy, _ in list_transport_run_configs(scenario):
            economy_token = str(economy).strip()
            if not economy_token or economy_token in seen:
                continue
            scopes.append(economy_token)
            seen.add(economy_token)

    if not scopes:
        raise RuntimeError(
            "INTERNATIONAL_SCOPE='all' resolved no configured economies from "
            "functions.transport_workflow_pipeline.list_transport_run_configs()."
        )
    return scopes


def run_with_config() -> list[dict[str, str]]:
    base_config = build_config()
    results: list[dict[str, str]] = []
    for scope in _resolve_scopes_for_run():
        config = InternationalExportConfig(
            input_path=base_config.input_path,
            output_dir=base_config.output_dir,
            scenario=base_config.scenario,
            scope=scope,
            base_year=base_config.base_year,
            final_year=base_config.final_year,
            emit_quality_report=base_config.emit_quality_report,
            emit_medium_summary=base_config.emit_medium_summary,
            check_branches_in_leap_using_com=base_config.check_branches_in_leap_using_com,
            set_vars_in_leap_using_com=base_config.set_vars_in_leap_using_com,
            auto_set_missing_branches=base_config.auto_set_missing_branches,
            ensure_fuels_in_leap=base_config.ensure_fuels_in_leap,
            reconcile_to_esto=base_config.reconcile_to_esto,
            mapping_workbook_path=base_config.mapping_workbook_path,
            mapping_esto_path=base_config.mapping_esto_path,
            emit_reconciliation_report=base_config.emit_reconciliation_report,
        )
        print(f"[INFO] Running international workflow for scope={scope}")
        output_paths = run_international_export_workflow(config)
        results.append({"scope": scope, **{k: str(v) for k, v in output_paths.items()}})
    return results


if __name__ == "__main__":
    run_with_config()
#%%
