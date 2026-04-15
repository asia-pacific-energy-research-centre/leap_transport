#%%
"""Thin transport workflow entrypoint.

Keep run-time settings here and delegate heavy logic to
`functions/transport_workflow_pipeline.py`.
"""
from __future__ import annotations

import io
import sys
import warnings
from contextlib import contextmanager
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

import pandas as pd

from functions import transport_workflow_pipeline as pipeline
from functions.international_transport_pipeline import (
    InternationalExportConfig,
    run_international_export_workflow,
)

#%%
# Select economy config by code (e.g. "12_NZ", "20_USA") or "all".
TRANSPORT_ECONOMY_SELECTION = "20_USA"
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
ALL_RUN_MODE = "both"
# If True, all+apec runs will also perform per-economy input setup (input-only
# behavior) before the synthetic 00_APEC run.
PREPARE_SEPARATE_INPUTS_WHEN_RUNNING_APEC = True
APEC_REGION = "APEC"# For testing synthetic 00_APEC runs in a single-region LEAP area, map APEC
# exports/IDs to an existing LEAP region name.
APEC_LEAP_REGION_OVERRIDE = "United States of America"
APEC_BASE_YEAR = 2022
APEC_FINAL_YEAR = 2060

# INPUT CREATION VARS
RUN_PROFILE = "reconcile_only"  # "input_only", "reconcile_only", "full"
RUN_RESULTS_DASHBOARD = True
# Controls console output volume:
# - "full": keep all print output (default legacy behavior)
# - "stage_economy": keep high-level stage/economy progress + errors only
RUN_OUTPUT_MODE = "stage_economy"

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
CHECK_BRANCHES_IN_LEAP_USING_COM = True
SET_VARS_IN_LEAP_USING_COM = True
AUTO_SET_MISSING_BRANCHES = True
ENSURE_FUELS_IN_LEAP = True

# International transport integration flags
RUN_INTERNATIONAL_WORKFLOW = True
INTERNATIONAL_INPUT_PATH = "data/international_bunker_outputs_20250421.csv"
INTERNATIONAL_OUTPUT_DIR = "results/international"
INTERNATIONAL_EMIT_QUALITY_REPORT = True
INTERNATIONAL_EMIT_MEDIUM_SUMMARY = True

# International LEAP API / COM / validation flags
INTERNATIONAL_CHECK_BRANCHES_IN_LEAP_USING_COM = True
INTERNATIONAL_SET_VARS_IN_LEAP_USING_COM = True
INTERNATIONAL_AUTO_SET_MISSING_BRANCHES = True
INTERNATIONAL_ENSURE_FUELS_IN_LEAP = True

# Fail fast when a run hits severe reconciliation failures so we do not
# silently write partial combined outputs.
CRITICAL_FAILURE_PATTERNS: tuple[str, ...] = (
    "failed to converge",
    "non-finite scale factors",
    "mismatches remain above tolerance",
    "adjustment multipliers remain outside tolerance",
)


class _FilteredLineStream(io.TextIOBase):
    """Line-buffered stream wrapper that forwards only allowed log lines."""

    def __init__(self, target_stream: io.TextIOBase, *, allow_line) -> None:
        self._target_stream = target_stream
        self._allow_line = allow_line
        self._pending = ""

    def write(self, text: str) -> int:
        self._pending += text
        while True:
            newline_index = self._pending.find("\n")
            if newline_index < 0:
                break
            line = self._pending[: newline_index + 1]
            self._pending = self._pending[newline_index + 1 :]
            self._emit(line)
        return len(text)

    def flush(self) -> None:
        if self._pending:
            self._emit(self._pending)
            self._pending = ""
        self._target_stream.flush()

    def _emit(self, line: str) -> None:
        stripped = line.strip()
        if self._allow_line(stripped):
            self._target_stream.write(line)


def _allow_stage_economy_log_line(line: str) -> bool:
    if not line:
        return False

    error_tokens = (
        "[ERROR]",
        "Traceback (most recent call last):",
        "RuntimeError:",
        "Critical transport workflow failure detected",
    )
    if any(token in line for token in error_tokens):
        return True

    stage_prefixes = (
        "=== Starting workflow for scenario",
        "=== Running ",
        "=== Loading Transport Data for ",
        "=== Transport data successfully filled into LEAP.",
        "=== Transport data reconciliation completed.",
        "=== Transport data loading process completed.",
        "[INFO] International transport export complete",
    )
    if any(line.startswith(prefix) for prefix in stage_prefixes):
        return True

    return False


@contextmanager
def _output_filter_context(mode: str):
    normalized_mode = str(mode).strip().lower()
    if normalized_mode == "full":
        yield
        return
    if normalized_mode != "stage_economy":
        raise ValueError("RUN_OUTPUT_MODE must be either 'full' or 'stage_economy'.")

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = _FilteredLineStream(original_stdout, allow_line=_allow_stage_economy_log_line)
    sys.stderr = _FilteredLineStream(original_stderr, allow_line=_allow_stage_economy_log_line)
    try:
        with warnings.catch_warnings():
            # Reduce noisy pandas runtime warnings in concise mode.
            warnings.filterwarnings("ignore", category=FutureWarning)
            warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
            yield
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def _sanitize_filename_token(value: str) -> str:
    token = "".join(ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in str(value))
    token = "_".join(part for part in token.split("_") if part)
    return token or "scenarios"


def _drop_empty_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    unnamed_cols = [
        col
        for col in df.columns
        if str(col).startswith("Unnamed") and df[col].isna().all()
    ]
    return df.drop(columns=unnamed_cols, errors="ignore")


def _deduplicate_current_accounts_rows(
    df: pd.DataFrame,
    *,
    current_accounts_label: str = "Current Accounts",
) -> tuple[pd.DataFrame, int]:
    if "Scenario" not in df.columns:
        return df, 0

    current_accounts_mask = (
        df["Scenario"].astype(str).str.strip().str.lower()
        == current_accounts_label.lower()
    )
    if not current_accounts_mask.any():
        return df, 0

    key_cols = [
        col
        for col in ("Branch Path", "Variable", "Scenario", "Region")
        if col in df.columns
    ]
    current_accounts_df = df.loc[current_accounts_mask]
    if key_cols:
        keep_rows = ~current_accounts_df.duplicated(subset=key_cols, keep="first")
    else:
        keep_rows = ~current_accounts_df.duplicated(keep="first")

    deduped_current_accounts = current_accounts_df.loc[keep_rows]
    removed = int(len(current_accounts_df) - len(deduped_current_accounts))

    combined = pd.concat(
        [df.loc[~current_accounts_mask], deduped_current_accounts],
        ignore_index=True,
    )
    return combined, removed


def _infer_year_bounds(df: pd.DataFrame) -> tuple[int, int]:
    year_cols = [
        int(col)
        for col in df.columns
        if str(col).isdigit() and len(str(col)) == 4
    ]
    if not year_cols:
        return APEC_BASE_YEAR, APEC_FINAL_YEAR
    return min(year_cols), max(year_cols)


def _save_combined_scenario_workbook(
    *,
    records: list[dict],
    scenario_list: Sequence[str],
    date_id: str,
    include_international: bool,
) -> str | None:
    selected_scenarios: dict[str, str] = {}
    for scenario in scenario_list:
        scenario_label = str(scenario).strip()
        if not scenario_label:
            continue
        selected_scenarios.setdefault(scenario_label.lower(), scenario_label)

    successful_paths: list[Path] = []
    seen_paths: set[str] = set()
    scenario_records: dict[str, list[dict]] = {key: [] for key in selected_scenarios}
    scenario_domains_with_paths: dict[str, set[str]] = {
        key: set() for key in selected_scenarios
    }

    def _append_path_if_valid(raw_path: str, *, record_domain: str, scenario_key: str) -> None:
        resolved_path = Path(str(raw_path))
        path_key = str(resolved_path.resolve())
        if path_key in seen_paths:
            scenario_domains_with_paths.setdefault(scenario_key, set()).add(record_domain)
            return
        if not resolved_path.exists():
            print(f"[WARN] Skipping missing export file: {resolved_path}")
            return
        successful_paths.append(resolved_path)
        seen_paths.add(path_key)
        scenario_domains_with_paths.setdefault(scenario_key, set()).add(record_domain)

    for record in records:
        scenario_name = str(record.get("scenario", "")).strip().lower()
        if scenario_name not in selected_scenarios:
            continue
        scenario_records.setdefault(scenario_name, []).append(record)
        if str(record.get("status", "")).strip().lower() != "success":
            continue

        domain = str(record.get("domain", "domestic")).strip().lower() or "domestic"
        domestic_export_path = record.get("transport_export_path")
        if domestic_export_path:
            _append_path_if_valid(
                str(domestic_export_path),
                record_domain=domain,
                scenario_key=scenario_name,
            )
        international_export_path = record.get("international_workbook")
        if international_export_path:
            _append_path_if_valid(
                str(international_export_path),
                record_domain=domain,
                scenario_key=scenario_name,
            )

    if not successful_paths:
        print("[WARN] No successful scenario export files were found to combine.")
        return None

    required_domains = {"domestic"}
    if include_international:
        required_domains.add("international")

    missing_scenario_requirements: list[str] = []
    for scenario_key, scenario_label in selected_scenarios.items():
        available_domains = scenario_domains_with_paths.get(scenario_key, set())
        missing_domains = required_domains - available_domains
        if missing_domains:
            missing_scenario_requirements.append(
                f"{scenario_label} (missing {', '.join(sorted(missing_domains))})"
            )
    if missing_scenario_requirements:
        detail_lines: list[str] = []
        for scenario_key in selected_scenarios:
            failed_errors = {
                str(record.get("error", "")).strip()
                for record in scenario_records.get(scenario_key, [])
                if str(record.get("status", "")).strip().lower() != "success"
                and str(record.get("error", "")).strip()
            }
            if failed_errors:
                detail_lines.append(
                    f"- {selected_scenarios[scenario_key]}: {sorted(failed_errors)[0]}"
                )
        details = "\n".join(detail_lines)
        raise RuntimeError(
            "Combined workbook could not be assembled with required sectors.\n"
            f"Missing scenario/domain combinations: {', '.join(missing_scenario_requirements)}"
            + (f"\nFirst recorded failures:\n{details}" if details else "")
        )

    leap_frames: list[pd.DataFrame] = []
    viewing_frames: list[pd.DataFrame] = []
    for workbook_path in successful_paths:
        leap_df = pd.read_excel(workbook_path, sheet_name="LEAP", header=2)
        viewing_df = pd.read_excel(workbook_path, sheet_name="FOR_VIEWING", header=2)
        leap_frames.append(_drop_empty_unnamed_columns(leap_df))
        viewing_frames.append(_drop_empty_unnamed_columns(viewing_df))

    combined_leap_df = pd.concat(leap_frames, ignore_index=True)
    combined_viewing_df = pd.concat(viewing_frames, ignore_index=True)

    combined_leap_df, removed_leap = _deduplicate_current_accounts_rows(combined_leap_df)
    combined_viewing_df, removed_viewing = _deduplicate_current_accounts_rows(combined_viewing_df)
    if removed_leap or removed_viewing:
        print(
            "[INFO] Deduplicated Current Accounts rows in combined workbook: "
            f"LEAP={removed_leap}, FOR_VIEWING={removed_viewing}"
        )

    included_scenarios = [selected_scenarios[key] for key in selected_scenarios]
    scenario_token = _sanitize_filename_token("_".join(included_scenarios))
    if include_international:
        combined_filename = (
            f"results/transport_leap_export_combined_domestic_international_"
            f"{scenario_token}_{date_id}.xlsx"
        )
        model_name = f"Transport Combined Domestic+International ({', '.join(included_scenarios)})"
    else:
        combined_filename = f"results/transport_leap_export_combined_{scenario_token}_{date_id}.xlsx"
        model_name = f"Transport Combined ({', '.join(included_scenarios)})"
    combined_output_path = pipeline.resolve_str(combined_filename)
    base_year, final_year = _infer_year_bounds(combined_viewing_df)

    archived_output = pipeline._archive_existing_output_file(combined_output_path, date_id=date_id)
    if archived_output:
        print(f"[INFO] Archived previous combined scenario export to {archived_output}")

    pipeline.save_export_files(
        combined_leap_df,
        combined_viewing_df,
        combined_output_path,
        base_year,
        final_year,
        model_name=model_name,
    )
    print(f"[INFO] Wrote combined scenario export: {combined_output_path}")
    return combined_output_path


def _resolve_scenario_selection(selection: str | Sequence[str]) -> list[str]:
    """Normalize scenario selection into ordered unique labels."""
    if isinstance(selection, str):
        raw_values = [selection]
    else:
        raw_values = list(selection)

    scenarios: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        token = str(value).strip()
        if not token:
            continue
        if token.lower() == "current accounts":
            print(
                "[INFO] Skipping explicit 'Current Accounts' in TRANSPORT_SCENARIO_SELECTION; "
                "it is generated automatically from each scenario run."
            )
            continue
        key = token.lower()
        if key in seen:
            continue
        scenarios.append(token)
        seen.add(key)

    if not scenarios:
        raise ValueError(
            "TRANSPORT_SCENARIO_SELECTION must include at least one non-empty "
            "scenario name other than 'Current Accounts'."
        )
    return scenarios


def _resolve_sales_policy_settings_for_scenario(
    scenario: str,
) -> tuple[dict | None, dict | None]:
    if not SCENARIO_SALES_POLICY_SETTINGS:
        return None, None

    scenario_key = str(scenario).strip().lower()
    matched_settings = None
    for raw_key, settings in SCENARIO_SALES_POLICY_SETTINGS.items():
        if str(raw_key).strip().lower() == scenario_key:
            matched_settings = settings
            break

    if matched_settings is None:
        return None, None
    if not isinstance(matched_settings, Mapping):
        raise TypeError(
            "SCENARIO_SALES_POLICY_SETTINGS entries must be mappings with "
            f"'passenger' and/or 'freight' keys. Got {type(matched_settings).__name__} "
            f"for scenario '{scenario}'."
        )

    passenger_settings = matched_settings.get("passenger")
    freight_settings = matched_settings.get("freight")
    if passenger_settings is not None and not isinstance(passenger_settings, Mapping):
        raise TypeError(
            "Passenger policy settings must be a mapping when provided "
            f"(scenario '{scenario}')."
        )
    if freight_settings is not None and not isinstance(freight_settings, Mapping):
        raise TypeError(
            "Freight policy settings must be a mapping when provided "
            f"(scenario '{scenario}')."
        )

    return (
        dict(passenger_settings) if passenger_settings is not None else None,
        dict(freight_settings) if freight_settings is not None else None,
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
    ) = _resolve_sales_policy_settings_for_scenario(scenario)

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


def _annotate_domestic_records(records: Sequence[dict]) -> list[dict]:
    annotated: list[dict] = []
    for record in records:
        updated = dict(record)
        updated.setdefault("domain", "domestic")
        annotated.append(updated)
    return annotated


def _resolve_international_scopes_for_scenario(scenario: str) -> list[str]:
    economy_selection = str(TRANSPORT_ECONOMY_SELECTION).strip()
    is_all_mode, _, run_separate, run_apec = pipeline.resolve_transport_run_mode(
        economy_selection,
        ALL_RUN_MODE,
    )

    if not is_all_mode:
        return [economy_selection]

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
    return scopes


def _run_international_for_scope(*, scenario: str, scope: str) -> dict:
    run_type = "international_apec" if str(scope).strip().upper() == "00_APEC" else "international_separate"
    record: dict[str, str] = {
        "domain": "international",
        "economy": str(scope).strip(),
        "scope": str(scope).strip(),
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
        scope=str(scope).strip(),
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
        record["international_workbook"] = str(output_paths.get("workbook", "")).strip()
        record["international_medium_summary"] = str(output_paths.get("medium_summary", "")).strip()
        record["international_quality"] = str(output_paths.get("quality", "")).strip()
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = str(exc)
        print(
            "[ERROR] International workflow failed for "
            f"scope={record['scope']} scenario={record['scenario']}: {exc}"
        )
    return record


def _is_critical_failure_record(record: dict) -> bool:
    if str(record.get("status", "")).strip().lower() == "success":
        return False
    if str(record.get("domain", "")).strip().lower() == "international":
        return True
    error_text = str(record.get("error", "")).strip().lower()
    if not error_text:
        return False
    return any(pattern in error_text for pattern in CRITICAL_FAILURE_PATTERNS)


def _raise_for_critical_failures(*, records: Sequence[dict], scenario: str) -> None:
    critical_records = [record for record in records if _is_critical_failure_record(record)]
    if not critical_records:
        return

    details: list[str] = []
    for record in critical_records[:5]:
        economy = str(record.get("economy", "<unknown>")).strip() or "<unknown>"
        run_type = str(record.get("run_type", "")).strip() or "run"
        error = str(record.get("error", "")).strip() or "No error details were captured."
        details.append(f"- {economy} ({run_type}): {error}")

    details_block = "\n".join(details)
    raise RuntimeError(
        "Critical transport workflow failure detected; aborting run so partial combined exports are not produced.\n"
        f"Scenario: {scenario}\n"
        f"Failures:\n{details_block}"
    )


def _run_results_dashboard(*, scenario_list: Sequence[str]) -> None:
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


def run_with_config() -> list[dict]:
    """Run transport workflow using constants defined in this file."""
    scenario_list = _resolve_scenario_selection(TRANSPORT_SCENARIO_SELECTION)
    date_id = datetime.now().strftime("%Y%m%d")

    records: list[dict] = []
    with _output_filter_context(RUN_OUTPUT_MODE):
        for scenario in scenario_list:
            print(f"\n=== Starting workflow for scenario '{scenario}' ===")
            _apply_runtime_settings(scenario=scenario, date_id=date_id)
            domestic_records = _annotate_domestic_records(pipeline.run_transport_workflow())
            records.extend(domestic_records)
            _raise_for_critical_failures(records=domestic_records, scenario=scenario)

            if RUN_INTERNATIONAL_WORKFLOW:
                scopes = _resolve_international_scopes_for_scenario(scenario)
                if not scopes:
                    raise RuntimeError(
                        "International workflow requested but no scopes were resolved "
                        f"for scenario '{scenario}'."
                    )
                for scope in scopes:
                    international_record = _run_international_for_scope(
                        scenario=scenario,
                        scope=scope,
                    )
                    records.append(international_record)
                    _raise_for_critical_failures(records=[international_record], scenario=scenario)

        combined_output = _save_combined_scenario_workbook(
            records=records,
            scenario_list=scenario_list,
            date_id=date_id,
            include_international=RUN_INTERNATIONAL_WORKFLOW,
        )
        if not combined_output:
            raise RuntimeError(
                "Combined workbook generation did not produce an output path."
            )
        if RUN_RESULTS_DASHBOARD:
            _run_results_dashboard(scenario_list=scenario_list)

    return records

#%%
if __name__ == "__main__":
    run_with_config()
#%%
