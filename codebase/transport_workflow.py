#%%
"""Thin transport workflow entrypoint.

Keep run-time settings here and delegate heavy logic to
`functions/transport_workflow_pipeline.py`.
"""
from __future__ import annotations

from datetime import datetime

from functions import transport_workflow_pipeline as pipeline
from functions.international_transport_pipeline import (
    InternationalExportConfig,
    run_international_export_workflow,
)
from functions.workflow_utilities import (
    annotate_domestic_records,
    archive_config_folder_if_size_changed,
    output_filter_context,
    raise_for_critical_failures,
    resolve_sales_policy_settings_for_scenario,
    resolve_scenario_selection,
    save_combined_scenario_workbook,
)

#%%
# Select economy config by code (e.g. "12_NZ", "20_USA") or "all".
TRANSPORT_ECONOMY_SELECTION = "05_PRC"
# Select one scenario (e.g. "Reference") or many (e.g. ["Reference", "Target"]).
TRANSPORT_SCENARIO_SELECTION: str | list[str] = ["Reference", "Target"]#"Reference", 
# Applies only when TRANSPORT_ECONOMY_SELECTION == "all" (ignored otherwise):
# "separate" -> run each configured economy independently (01_AUS ... 21_VN).
# "apec"     -> run one synthetic 00_APEC case (aggregated from all configured economies).
# "both"     -> run all separate economies first, then run synthetic 00_APEC.
# Note: combined passenger/freight CSVs are built from the "separate" runs only.
# Reconciliation scope in all-mode:
# - "separate" or "both": reconciliation can run for all configured economies.
# - "apec": reconciliation runs for 00_APEC only (separate economies are input-prep only, if enabled).
ALL_RUN_MODE = "separate"
# If True, all+apec runs will also perform per-economy input setup (input-only
# behavior) before the synthetic 00_APEC run.
PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC = True
APEC_REGION = "APEC"
# Region name written into the synthetic 00_APEC LEAP export file. This could
# also be used as the LEAP API target region, but API export is inactive for now.
APEC_LEAP_REGION_OVERRIDE = "China"
APEC_BASE_YEAR = 2022
APEC_FINAL_YEAR = 2060

# INPUT CREATION VARS
RUN_PROFILE = "full"  # "input_only", "reconcile_only", "full"
RUN_RESULTS_DASHBOARD = True
# Controls console output volume:
# - "full": keep all print output (default legacy behavior)
# - "stage_economy": keep high-level stage/economy progress + errors only
RUN_OUTPUT_MODE = "stage_economy"

# Archive codebase/config into codebase/config/archive when tracked config file
# sizes differ from the last recorded snapshot.
ARCHIVE_CONFIG_ON_SIZE_CHANGE = True

# Checkpoint/loading options
# Where to load the preprocessed input dataframe from: "raw" or "checkpoint"
INPUT_DATA_SOURCE = "raw"
# Resume export pipeline from a single stage: "none", "halfway", "three_quarter", "export"
CHECKPOINT_LOAD_STAGE = "none"
MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE = True

# Controls which sales streams are generated: "none" skips sales outputs,
# "passenger"/"freight" run one stream, and "both" runs both.
SALES_MODE = "both"
PASSENGER_PLOT = False
# Optional scenario-specific policy settings passed to
# sales_workflow wrappers through functions.transport_workflow_pipeline.
# Keys should match the scenario names in TRANSPORT_SCENARIO_SELECTION
# (case-insensitive).
#
# Allowed keys under each mode ("passenger"/"freight"):
# - turnover_policies
# - drive_turnover_policy
# - drive_policy_dataframe
# - drive_policy_checkpoint_path
# - drive_policy_stocks_col
# - drive_policy_vehicle_type_map
# - analysis_initial_fleet_age_shift_years

# SCENARIO_SALES_POLICY_SETTINGS: dict[str, dict[str, dict]] | None = None
# Example:
SCENARIO_SALES_POLICY_SETTINGS = {
    "Reference": {
        "passenger": {
            "turnover_policies": {
                "LPV": {"survival_multiplier": 1.4},
                "MC": {"survival_multiplier": 1.4},
                "Bus": {"survival_multiplier": 1.4},
            },
        },
        "freight": {
            "turnover_policies": {
                "Trucks": {"survival_multiplier": 1.4},
                "LCVs": {"survival_multiplier": 1.4},
            },
        },
    },
}#{
#     "Reference": {
#         "passenger": {
#             # optional; can be empty or omitted
#         },
#     },
#     "Target": {
#         "passenger": {
#             "drive_turnover_policy": {
#                 "ICE": {
#                     "drives": ["ice_d", "ice_g"],
#                     "additional_retirement_rate": {
#                         year: min(0.25, 0.01 * (year - 2034))
#                         for year in range(2035, 2061)
#                     },
#                 }
#             },
#             "analysis_initial_fleet_age_shift_years": {"LPV": 2.0},
#         },
#         "freight": {
#             "analysis_initial_fleet_age_shift_years": {"Trucks": 1.5, "LCVs": 1.0},
#         },
#     },
# }


# RECONCILIATION VARS
APPLY_ADJUSTMENTS_TO_FUTURE_YEARS = True
REPORT_ADJUSTMENT_CHANGES = True
# Optional convergence-time fallback injections keyed by ESTO energy key.
# This only applies when reconciliation detects LEAP~0 and ESTO>0 for a key.
# Think of this as:
#   "ESTO key" -> "How to unstick the mapped LEAP branch(es) for that key"
# Rule `type` options:
# - "mode_stock_seed": raise mode stock via parent stock + mode stock share.
#   If `min_mode_stock` is omitted, required mode stock is inferred from ESTO
#   target energy using existing Device Share, Mileage, and Fuel Economy.
#   Optional: set `device_path` to choose which device branch is used for
#   that inference (default: mode_path + "\\Electricity").
# - "scalar_min": set a single branch variable to at least min_value
# Economy scoping:
# - set `"economy": "all"` for a default rule across all economies
# - add economy-specific rules (e.g. `"economy": "20_USA"`) for the same ESTO key
#   and those will override the `"all"` defaults for that economy.
ESTO_ZERO_ENERGY_FALLBACK_RULES: dict[str, list[dict[str, float | str]]] = {
    # For ESTO key 15_02_road | 17_electricity | x, use LPV BEV-medium
    # stock seeding to create non-zero road electricity energy.
    "15_02_road | 17_electricity | x": [
        {
            "type": "mode_stock_seed",
            "economy": "all",
            "parent_path": r"Demand\Passenger road\LPVs",
            "mode_path": r"Demand\Passenger road\LPVs\BEV medium",
        },
    ],
    # "15_02_road | 16_others | 16_06_biodiesel": [
    #     {
    #         "type": "mode_stock_seed",
    #         "economy": "all",
    #         "parent_path": r"Demand\Freight road\Trucks",
    #         "mode_path": r"Demand\Freight road\Trucks\ICE heavy",
    #         "device_path": r"Demand\Freight road\Trucks\ICE heavy\Biodiesel",
    #     }
    # ],
    # "15_03_rail | 16_others | 16_06_biodiesel": [
    #     {
    #         "type": "scalar_min",
    #         "economy": "all",
    #         "branch_path": r"Demand\Freight non road\Rail\Biodiesel",
    #         "variable": "Activity Level",
    #         "min_value": 1.0,
    #     }
    # ],
    # Example (different ESTO key): rail electricity mismatch fallback using
    # scalar floor on Activity Level.
    # "15_03_rail | 17_electricity | x": [
    #     {
    #         "type": "scalar_min",
    #         "economy": "all",
    #         "branch_path": r"Demand\Freight non road\Rail\Electricity",
    #         "variable": "Activity Level",
    #         "min_value": 1.0,
    #     },
    # ],
    # Example (same key as above) with economy-specific override that replaces
    # the "all" rule set for that key/economy.
    # "15_02_road | 17_electricity | x": [
    #     {
    #         "type": "mode_stock_seed",
    #         "economy": "09_ROK",
    #         "parent_path": r"Demand\Passenger road\LPVs",
    #         "mode_path": r"Demand\Passenger road\LPVs\BEV medium",
    #         "device_path": r"Demand\Passenger road\LPVs\BEV medium\Electricity",
    #     },
    # ],
}

# LEAP API / COM / validation flags
CHECK_BRANCHES_IN_LEAP_USING_COM = False
SET_VARS_IN_LEAP_USING_COM = False
AUTO_SET_MISSING_BRANCHES = False
ENSURE_FUELS_IN_LEAP = False

# International transport integration flags
RUN_INTERNATIONAL_WORKFLOW = True
INTERNATIONAL_INPUT_PATH = "data/international_bunker_outputs_20250421.csv"
INTERNATIONAL_OUTPUT_DIR = "results/international"
INTERNATIONAL_EMIT_QUALITY_REPORT = True
INTERNATIONAL_EMIT_MEDIUM_SUMMARY = True

# International LEAP API / COM / validation flags
INTERNATIONAL_CHECK_BRANCHES_IN_LEAP_USING_COM = False
INTERNATIONAL_SET_VARS_IN_LEAP_USING_COM = False
INTERNATIONAL_AUTO_SET_MISSING_BRANCHES = False
INTERNATIONAL_ENSURE_FUELS_IN_LEAP = False

# Fail fast when a run hits severe reconciliation failures so we do not
# silently write partial combined outputs.
CRITICAL_FAILURE_PATTERNS: tuple[str, ...] = (
    "failed to converge",
    "non-finite scale factors",
    "mismatches remain above tolerance",
    "adjustment multipliers remain outside tolerance",
)


def _apply_runtime_settings(*, scenario: str, date_id: str) -> None:
    """Push local settings into the pipeline module before execution."""
    pipeline.TRANSPORT_ECONOMY_SELECTION = TRANSPORT_ECONOMY_SELECTION
    pipeline.TRANSPORT_SCENARIO_SELECTION = scenario
    pipeline.ALL_RUN_MODE = ALL_RUN_MODE
    pipeline.APEC_REGION = APEC_REGION
    pipeline.APEC_LEAP_REGION_OVERRIDE = APEC_LEAP_REGION_OVERRIDE
    pipeline.APEC_BASE_YEAR = APEC_BASE_YEAR
    pipeline.APEC_FINAL_YEAR = APEC_FINAL_YEAR

    pipeline.RUN_PROFILE = RUN_PROFILE
    pipeline.RUN_INPUT_CREATION, pipeline.RUN_RECONCILIATION = pipeline.resolve_run_profile(RUN_PROFILE)
    pipeline.PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC = PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC
    pipeline.SALES_MODE = SALES_MODE
    pipeline.RUN_PASSENGER_SALES, pipeline.RUN_FREIGHT_SALES = pipeline.resolve_sales_mode(SALES_MODE)
    pipeline.PASSENGER_PLOT = PASSENGER_PLOT
    (
        pipeline.PASSENGER_SALES_POLICY_SETTINGS,
        pipeline.FREIGHT_SALES_POLICY_SETTINGS,
    ) = resolve_sales_policy_settings_for_scenario(
        SCENARIO_SALES_POLICY_SETTINGS,
        scenario,
    )

    pipeline.APPLY_ADJUSTMENTS_TO_FUTURE_YEARS = APPLY_ADJUSTMENTS_TO_FUTURE_YEARS
    pipeline.REPORT_ADJUSTMENT_CHANGES = REPORT_ADJUSTMENT_CHANGES
    pipeline.ESTO_ZERO_ENERGY_FALLBACK_RULES = ESTO_ZERO_ENERGY_FALLBACK_RULES

    pipeline.CHECK_BRANCHES_IN_LEAP_USING_COM = CHECK_BRANCHES_IN_LEAP_USING_COM
    pipeline.SET_VARS_IN_LEAP_USING_COM = SET_VARS_IN_LEAP_USING_COM
    pipeline.AUTO_SET_MISSING_BRANCHES = AUTO_SET_MISSING_BRANCHES
    pipeline.ENSURE_FUELS_IN_LEAP = ENSURE_FUELS_IN_LEAP

    pipeline.INPUT_DATA_SOURCE = INPUT_DATA_SOURCE
    pipeline.LOAD_INPUT_CHECKPOINT = pipeline.resolve_input_checkpoint(INPUT_DATA_SOURCE)

    pipeline.CHECKPOINT_LOAD_STAGE = CHECKPOINT_LOAD_STAGE
    (
        pipeline.LOAD_HALFWAY_CHECKPOINT,
        pipeline.LOAD_THREEQUART_WAY_CHECKPOINT,
        pipeline.LOAD_EXPORT_DF_CHECKPOINT,
    ) = pipeline.resolve_export_checkpoint_flags(CHECKPOINT_LOAD_STAGE)

    pipeline.MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE = MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE
    pipeline.DATE_ID = date_id


def run_with_config() -> list[dict]:
    """Run transport workflow using constants defined in this file."""
    scenario_list = resolve_scenario_selection(TRANSPORT_SCENARIO_SELECTION)
    date_id = datetime.now().strftime("%Y%m%d")
    if ARCHIVE_CONFIG_ON_SIZE_CHANGE:
        archived_config_dir = archive_config_folder_if_size_changed(
            stamp=datetime.now().strftime("%Y%m%d_%H%M%S"),
        )
        if archived_config_dir:
            print(f"[INFO] Archived config snapshot to {archived_config_dir}")

    records: list[dict] = []
    with output_filter_context(RUN_OUTPUT_MODE):
        for scenario in scenario_list:
            print(f"\n=== Starting workflow for scenario '{scenario}' ===")
            _apply_runtime_settings(scenario=scenario, date_id=date_id)
            domestic_records = annotate_domestic_records(pipeline.run_transport_workflow())
            records.extend(domestic_records)
            raise_for_critical_failures(
                records=domestic_records,
                scenario=scenario,
                critical_failure_patterns=CRITICAL_FAILURE_PATTERNS,
            )

            if RUN_INTERNATIONAL_WORKFLOW:
                economy_selection = str(TRANSPORT_ECONOMY_SELECTION).strip()
                is_all_mode, _, run_separate, run_apec = pipeline.resolve_transport_run_mode(
                    economy_selection,
                    ALL_RUN_MODE,
                )
                if is_all_mode:
                    scopes: list[str] = []
                    seen: set[str] = set()
                    if run_separate:
                        for economy, _ in pipeline.list_transport_run_configs(scenario):
                            economy_token = str(economy).strip()
                            if not economy_token or economy_token in seen:
                                continue
                            scopes.append(economy_token)
                            seen.add(economy_token)
                    if run_apec and "00_APEC" not in seen:
                        scopes.append("00_APEC")
                else:
                    scopes = [economy_selection]

                if not scopes:
                    raise RuntimeError(
                        "International workflow requested but no scopes were resolved "
                        f"for scenario '{scenario}'."
                    )
                for scope in scopes:
                    scope_key = str(scope).strip()
                    run_type = "international_apec" if scope_key.upper() == "00_APEC" else "international_separate"
                    international_record: dict[str, str] = {
                        "domain": "international",
                        "economy": scope_key,
                        "scope": scope_key,
                        "scenario": str(scenario).strip(),
                        "run_type": run_type,
                        "status": "success",
                        "error": "",
                        "international_workbook": "",
                        "international_medium_summary": "",
                        "international_quality": "",
                    }

                    config = InternationalExportConfig(
                        input_path=INTERNATIONAL_INPUT_PATH,
                        output_dir=INTERNATIONAL_OUTPUT_DIR,
                        scenario=[str(scenario).strip()],
                        scope=scope_key,
                        base_year=APEC_BASE_YEAR,
                        final_year=APEC_FINAL_YEAR,
                        emit_quality_report=INTERNATIONAL_EMIT_QUALITY_REPORT,
                        emit_medium_summary=INTERNATIONAL_EMIT_MEDIUM_SUMMARY,
                        check_branches_in_leap_using_com=INTERNATIONAL_CHECK_BRANCHES_IN_LEAP_USING_COM,
                        set_vars_in_leap_using_com=INTERNATIONAL_SET_VARS_IN_LEAP_USING_COM,
                        auto_set_missing_branches=INTERNATIONAL_AUTO_SET_MISSING_BRANCHES,
                        ensure_fuels_in_leap=INTERNATIONAL_ENSURE_FUELS_IN_LEAP,
                    )
                    try:
                        output_paths = run_international_export_workflow(config)
                        international_record["international_workbook"] = str(
                            output_paths.get("workbook", "")
                        ).strip()
                        international_record["international_medium_summary"] = str(
                            output_paths.get("medium_summary", "")
                        ).strip()
                        international_record["international_quality"] = str(
                            output_paths.get("quality", "")
                        ).strip()
                    except Exception as exc:
                        international_record["status"] = "failed"
                        international_record["error"] = str(exc)
                        print(
                            "[ERROR] International workflow failed for "
                            f"scope={international_record['scope']} scenario={international_record['scenario']}: {exc}"
                        )
                    records.append(international_record)
                    raise_for_critical_failures(
                        records=[international_record],
                        scenario=scenario,
                        critical_failure_patterns=CRITICAL_FAILURE_PATTERNS,
                    )

        combined_output = save_combined_scenario_workbook(
            records=records,
            scenario_list=scenario_list,
            date_id=date_id,
            include_international=RUN_INTERNATIONAL_WORKFLOW,
            fallback_base_year=APEC_BASE_YEAR,
            fallback_final_year=APEC_FINAL_YEAR,
        )
        if not combined_output:
            raise RuntimeError(
                "Combined workbook generation did not produce an output path."
            )
        if RUN_RESULTS_DASHBOARD:
            from results_analysis.results_dashboard_workflow import run_dashboard_workflow

            include_economies: tuple[str, ...] | None
            if str(TRANSPORT_ECONOMY_SELECTION).strip().lower() == "all":
                include_economies = None
            else:
                include_economies = (str(TRANSPORT_ECONOMY_SELECTION).strip(),)

            print(
                "[INFO] Running results dashboard workflow "
                f"(scenarios={tuple(scenario_list)}, include_economies={include_economies})"
            )
            run_dashboard_workflow(
                scenarios=tuple(str(s).strip() for s in scenario_list),
                include_economies=include_economies,
            )

    return records

#%%
if __name__ == "__main__":
    run_with_config()
#%%
