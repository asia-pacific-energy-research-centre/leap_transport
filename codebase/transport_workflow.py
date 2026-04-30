#%%
"""Thin transport workflow entrypoint.

Keep run-time settings here and delegate heavy logic to
`functions/transport_workflow_pipeline.py`.
"""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import pandas as pd

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
# #### Scope selection ####
# Select economy config by code (e.g. "12_NZ", "20_USA") or "all".
TRANSPORT_ECONOMY_SELECTION = "all"
# Select one scenario (e.g. "Reference") or many (e.g. ["Reference", "Target"]).
TRANSPORT_SCENARIO_SELECTION: str | list[str] = ["Reference", "Target"]#"Reference", 

# #### All-economy run mode ####
# Applies only when TRANSPORT_ECONOMY_SELECTION == "all" (ignored otherwise):
# "separate" -> run each configured economy independently (01_AUS ... 21_VN).
# "apec"     -> run one synthetic 00_APEC case (aggregated from all configured economies).
# "both"     -> run all separate economies first, then run synthetic 00_APEC.
# Note: combined passenger/freight CSVs are built from the "separate" runs only.
# Reconciliation scope in all-mode:
# - "separate" or "both": reconciliation can run for all configured economies.
# - "apec": reconciliation runs for 00_APEC only (separate economies are input-prep only, if enabled).
ALL_RUN_MODE = "both"  # "separate", "apec", "both"

# #### Synthetic 00_APEC run settings ####
# These settings are only used when TRANSPORT_ECONOMY_SELECTION == "all" and
# ALL_RUN_MODE includes the synthetic aggregated APEC run ("apec" or "both").
# If True, the workflow also performs per-economy input setup before building
# the synthetic 00_APEC run.
PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC = True
# Synthetic region label for the aggregated 00_APEC run configuration.
APEC_REGION = "APEC"
# Optional region name written into the synthetic 00_APEC LEAP export/template
# outputs. This does not change which economy is being aggregated; it only
# changes the LEAP-facing region label used for that synthetic run.
APEC_LEAP_REGION_OVERRIDE = "China"
# Mapping workbook used for the workbook-backed ESTO/LEAP/Ninth audit surface.
APEC_MAPPING_WORKBOOK_PATH = "config/leap_mappings 25042026.xlsx"
# Shared ESTO-style balance file used for workbook-backed mapping audits.
APEC_ESTO_BALANCES_PATH = "data/00APEC_2024_low_with_subtotals.csv"
# Year bounds for the synthetic 00_APEC run.
APEC_BASE_YEAR = 2022
APEC_FINAL_YEAR = 2060

# #### Run stages and outputs ####
# Top-level execution mode for the domestic transport workflow.
RUN_PROFILE = "reconcile_only"  # "input_only", "reconcile_only", "full"
# Enable/disable the downstream results dashboard workflow.
RUN_RESULTS_DASHBOARD = True
# Controls console output volume:
# - "full": keep all print output (default legacy behavior)
# - "stage_economy": keep high-level stage/economy progress + errors only
RUN_OUTPUT_MODE = "stage_economy"

# #### Config archiving ####
# Archive codebase/configurations into codebase/configurations/archive when tracked config file
# sizes differ from the last recorded snapshot.
ARCHIVE_CONFIG_ON_SIZE_CHANGE = True

# #### Input and checkpoint controls ####
# Where to load the preprocessed input dataframe from: "raw" or "checkpoint"
INPUT_DATA_SOURCE = "raw"
# Resume export pipeline from a single stage: "none", "halfway", "three_quarter", "export"
CHECKPOINT_LOAD_STAGE = "none"
# If True, merge against the LEAP import template and enforce structure checks.
MERGE_IMPORT_EXPORT_AND_CHECK_STRUCTURE = True

# #### Sales outputs and policy tuning ####
# Controls which sales streams are generated: "none" skips sales outputs,
# "passenger"/"freight" run one stream, and "both" runs both.
SALES_MODE = "both"
# Enables passenger sales diagnostic plotting.
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
INTERNATIONAL_RECONCILE_TO_ESTO = True
INTERNATIONAL_MAPPING_WORKBOOK_PATH = APEC_MAPPING_WORKBOOK_PATH
INTERNATIONAL_MAPPING_ESTO_PATH = APEC_ESTO_BALANCES_PATH
INTERNATIONAL_EMIT_RECONCILIATION_REPORT = True

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


_OUTPUTS_DIR = Path(__file__).parent / "outputs"
_TIMING_CSV = _OUTPUTS_DIR / "run_timing_log.csv"
_TIMING_COLS = [
    "timestamp", "scenario", "economy", "run_type",
    "run_profile", "sales_mode", "all_run_mode", "input_data_source", "checkpoint_load_stage",
    "status", "instance_number", "duration_hours", "duration_minutes", "duration_seconds",
    "duration_formatted",
]


def _fmt_duration(seconds: float) -> tuple[int, int, float, str]:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return h, m, s, f"{h}h {m}m {s:.1f}s"


def _record_run_timings(records: list[dict], settings_meta: dict) -> str:
    """Append timed records to run_timing_log.csv, incrementing instance_number per (economy, scenario, run_type)."""
    _OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    existing = pd.read_csv(_TIMING_CSV) if _TIMING_CSV.exists() else pd.DataFrame()

    if not existing.empty and {"economy", "scenario", "run_type", "instance_number"}.issubset(existing.columns):
        max_instances: dict[tuple, int] = (
            existing.groupby(["economy", "scenario", "run_type"])["instance_number"]
            .max()
            .to_dict()
        )
    else:
        max_instances = {}

    batch_counts: dict[tuple, int] = {}
    new_rows = []
    for rec in records:
        economy = str(rec.get("economy", ""))
        scenario = str(rec.get("scenario", ""))
        run_type = str(rec.get("run_type", ""))
        _dur_raw = rec.get("duration_seconds")
        try:
            dur: float | None = float(_dur_raw) if _dur_raw is not None else None
        except (TypeError, ValueError):
            dur = None

        key = (economy, scenario, run_type)
        prior_max = max_instances.get(key, 0)
        instance_number = prior_max + batch_counts.get(key, 0) + 1
        batch_counts[key] = batch_counts.get(key, 0) + 1

        if dur is not None:
            h, m, s, fmt = _fmt_duration(dur)
        else:
            h = m = s = None
            fmt = rec.get("duration_formatted", "")

        new_rows.append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scenario": scenario,
            "economy": economy,
            "run_type": run_type,
            "run_profile": settings_meta.get("run_profile", ""),
            "sales_mode": settings_meta.get("sales_mode", ""),
            "all_run_mode": settings_meta.get("all_run_mode", ""),
            "input_data_source": settings_meta.get("input_data_source", ""),
            "checkpoint_load_stage": settings_meta.get("checkpoint_load_stage", ""),
            "status": rec.get("status", ""),
            "instance_number": instance_number,
            "duration_hours": h,
            "duration_minutes": m,
            "duration_seconds": round(s, 1) if s is not None else None,
            "duration_formatted": fmt,
        })

    new_df = pd.DataFrame(new_rows, columns=_TIMING_COLS)
    combined = pd.concat([existing.reindex(columns=_TIMING_COLS), new_df], ignore_index=True) if not existing.empty else new_df
    combined.to_csv(_TIMING_CSV, index=False)
    print(f"[TIMING] Run log updated: {_TIMING_CSV}")
    return str(_TIMING_CSV)


def _plot_run_times() -> None:
    """Bar chart of average run duration per run_type from the full timing log."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[TIMING] matplotlib not available — skipping run time chart.")
        return

    if not _TIMING_CSV.exists():
        return

    df = pd.read_csv(_TIMING_CSV)
    if df.empty:
        return

    df["total_seconds"] = (
        pd.to_numeric(df["duration_hours"], errors="coerce").fillna(0) * 3600
        + pd.to_numeric(df["duration_minutes"], errors="coerce").fillna(0) * 60
        + pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0)
    )
    df = df[df["total_seconds"] > 0]
    if df.empty:
        return

    grouped = (
        df.groupby("run_type")["total_seconds"]
        .agg(mean="mean", count="count")
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(max(6, len(grouped) * 2), 5))
    bars = ax.bar(grouped["run_type"], grouped["mean"] / 60, color="steelblue", alpha=0.8)
    for bar, (_, row) in zip(bars, grouped.iterrows()):
        _, _m, _s, label = _fmt_duration(row["mean"])
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            f"avg: {label}\n(n={int(row['count'])})",
            ha="center", va="bottom", fontsize=8,
        )
    ax.set_xlabel("Run Type")
    ax.set_ylabel("Average Duration (minutes)")
    ax.set_title("Average Run Duration by Type (all logged runs)")
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()

    chart_path = _OUTPUTS_DIR / "run_timing_chart.png"
    plt.savefig(chart_path, dpi=120)
    plt.close(fig)
    print(f"[TIMING] Run time chart saved: {chart_path}")


def _apply_runtime_settings(*, scenario: str, date_id: str) -> None:
    """Push local settings into the pipeline module before execution."""
    pipeline.TRANSPORT_ECONOMY_SELECTION = TRANSPORT_ECONOMY_SELECTION
    pipeline.TRANSPORT_SCENARIO_SELECTION = scenario
    pipeline.ALL_RUN_MODE = ALL_RUN_MODE
    pipeline.APEC_REGION = APEC_REGION
    pipeline.APEC_LEAP_REGION_OVERRIDE = APEC_LEAP_REGION_OVERRIDE
    pipeline.APEC_MAPPING_WORKBOOK_PATH = APEC_MAPPING_WORKBOOK_PATH
    pipeline.APEC_ESTO_BALANCES_PATH = APEC_ESTO_BALANCES_PATH
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
                        "international_esto_reconciliation": "",
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
                        reconcile_to_esto=INTERNATIONAL_RECONCILE_TO_ESTO,
                        mapping_workbook_path=INTERNATIONAL_MAPPING_WORKBOOK_PATH,
                        mapping_esto_path=INTERNATIONAL_MAPPING_ESTO_PATH,
                        emit_reconciliation_report=INTERNATIONAL_EMIT_RECONCILIATION_REPORT,
                    )
                    _intl_t0 = time.perf_counter()
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
                        international_record["international_esto_reconciliation"] = str(
                            output_paths.get("esto_reconciliation", "")
                        ).strip()
                    except Exception as exc:
                        international_record["status"] = "failed"
                        international_record["error"] = str(exc)
                        print(
                            "[ERROR] International workflow failed for "
                            f"scope={international_record['scope']} scenario={international_record['scenario']}: {exc}"
                        )
                    _intl_dur = time.perf_counter() - _intl_t0
                    _, _, _, _ifmt = _fmt_duration(_intl_dur)
                    international_record["duration_seconds"] = str(round(_intl_dur, 3))
                    international_record["duration_formatted"] = _ifmt
                    print(f"[TIMING] International {scope_key} | {scenario} — {_ifmt}")
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

        settings_meta = {
            "run_profile": RUN_PROFILE,
            "sales_mode": SALES_MODE,
            "all_run_mode": ALL_RUN_MODE,
            "input_data_source": INPUT_DATA_SOURCE,
            "checkpoint_load_stage": CHECKPOINT_LOAD_STAGE,
        }
        try:
            _record_run_timings(records, settings_meta)
            _plot_run_times()
        except Exception as _timing_exc:
            print(f"[WARN] Failed to save run timing log: {_timing_exc}")

        timed = [r for r in records if r.get("duration_seconds") is not None]
        if timed:
            print("\n=== Run timing summary ===")
            for r in timed:
                print(f"  {r.get('economy','?'):12s} | {r.get('scenario','?'):12s} | {r.get('run_type','?'):25s} | {r.get('duration_formatted','?')}")
            print("==========================\n")

    return records

#%%
if __name__ == "__main__":
    run_with_config()
#%%
