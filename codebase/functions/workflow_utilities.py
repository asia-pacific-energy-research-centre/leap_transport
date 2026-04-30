"""Technical helpers shared by workflow entrypoints."""

from __future__ import annotations

import io
import json
import shutil
import sys
import warnings
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd

from configurations.transport_economy_config import COMBINED_EXPORT_DIR, ECONOMY_METADATA
from functions import transport_workflow_pipeline as pipeline

CONFIG_ARCHIVE_MANIFEST_FILENAME = "_config_file_size_manifest.json"


class FilteredLineStream(io.TextIOBase):
    """Line-buffered stream wrapper that forwards only allowed log lines."""

    def __init__(
        self,
        target_stream: io.TextIOBase,
        *,
        allow_line: Callable[[str], bool],
    ) -> None:
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


def allow_stage_economy_log_line(line: str) -> bool:
    """Return whether a log line should be visible in stage/economy output mode."""
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
    return any(line.startswith(prefix) for prefix in stage_prefixes)


@contextmanager
def output_filter_context(mode: str) -> Iterator[None]:
    """Apply workflow console filtering for the selected output mode."""
    normalized_mode = str(mode).strip().lower()
    if normalized_mode == "full":
        yield
        return
    if normalized_mode != "stage_economy":
        raise ValueError("RUN_OUTPUT_MODE must be either 'full' or 'stage_economy'.")

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = FilteredLineStream(
        original_stdout,
        allow_line=allow_stage_economy_log_line,
    )
    sys.stderr = FilteredLineStream(
        original_stderr,
        allow_line=allow_stage_economy_log_line,
    )
    try:
        with warnings.catch_warnings():
            # Reduce noisy pandas runtime warnings in concise mode.
            warnings.filterwarnings("ignore", category=FutureWarning)
            setting_with_copy_warning = getattr(
                pd.errors,
                "SettingWithCopyWarning",
                None,
            )
            if setting_with_copy_warning is not None:
                warnings.filterwarnings("ignore", category=setting_with_copy_warning)
            yield
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def sanitize_filename_token(value: str) -> str:
    token = "".join(ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in str(value))
    token = "_".join(part for part in token.split("_") if part)
    return token or "scenarios"


def drop_empty_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    unnamed_cols = [
        col
        for col in df.columns
        if str(col).startswith("Unnamed") and df[col].isna().all()
    ]
    return df.drop(columns=unnamed_cols, errors="ignore")


def deduplicate_current_accounts_rows(
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


def infer_year_bounds(
    df: pd.DataFrame,
    *,
    fallback_base_year: int,
    fallback_final_year: int,
) -> tuple[int, int]:
    year_cols = [
        int(col)
        for col in df.columns
        if str(col).isdigit() and len(str(col)) == 4
    ]
    if not year_cols:
        return fallback_base_year, fallback_final_year
    return min(year_cols), max(year_cols)


def resolve_scenario_selection(selection: str | Sequence[str]) -> list[str]:
    """Normalize scenario selection into ordered unique labels."""
    raw_values = [selection] if isinstance(selection, str) else list(selection)

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


def resolve_sales_policy_settings_for_scenario(
    scenario_sales_policy_settings: Mapping[str, Mapping] | None,
    scenario: str,
) -> tuple[dict | None, dict | None]:
    if not scenario_sales_policy_settings:
        return None, None

    scenario_key = str(scenario).strip().lower()
    matched_settings = None
    for raw_key, settings in scenario_sales_policy_settings.items():
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


def _build_unique_directory_path(path: Path) -> Path:
    """Return a non-existing directory path by suffixing an increment if needed."""
    candidate = path
    counter = 1
    while candidate.exists():
        candidate = path.with_name(f"{path.name}_{counter:02d}")
        counter += 1
    return candidate


def _iter_archivable_config_files(config_dir: Path, archive_dir: Path) -> Iterator[Path]:
    archive_dir = archive_dir.resolve()
    for path in sorted(config_dir.rglob("*")):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        try:
            resolved_path = path.resolve()
        except OSError:
            continue
        if resolved_path == archive_dir or archive_dir in resolved_path.parents:
            continue
        yield path


def collect_config_file_size_snapshot(
    config_dir: str | Path = "codebase/config",
    archive_dir: str | Path | None = None,
) -> dict[str, int]:
    """Return repo-relative config file sizes, excluding generated archive/cache files."""
    resolved_config_dir = Path(pipeline.resolve_str(config_dir))
    resolved_archive_dir = (
        Path(pipeline.resolve_str(archive_dir))
        if archive_dir is not None
        else resolved_config_dir / "archive"
    )
    if not resolved_config_dir.exists():
        raise FileNotFoundError(f"Config directory not found: {resolved_config_dir}")
    if not resolved_config_dir.is_dir():
        raise NotADirectoryError(f"Config path is not a directory: {resolved_config_dir}")

    snapshot: dict[str, int] = {}
    for path in _iter_archivable_config_files(resolved_config_dir, resolved_archive_dir):
        relative_path = path.relative_to(resolved_config_dir).as_posix()
        snapshot[relative_path] = path.stat().st_size
    return snapshot


def _load_config_archive_manifest(manifest_path: Path) -> dict[str, int] | None:
    if not manifest_path.exists():
        return None
    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None

    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, dict):
        return None
    normalized: dict[str, int] = {}
    for key, value in files.items():
        try:
            normalized[str(key)] = int(value)
        except (TypeError, ValueError):
            return None
    return normalized


def _config_size_changes(
    previous: dict[str, int] | None,
    current: dict[str, int],
) -> list[dict[str, int | str | None]]:
    if previous is None:
        return [
            {
                "path": path,
                "previous_size": None,
                "current_size": size,
            }
            for path, size in sorted(current.items())
        ]

    changes: list[dict[str, int | str | None]] = []
    for path in sorted(set(previous) | set(current)):
        previous_size = previous.get(path)
        current_size = current.get(path)
        if previous_size != current_size:
            changes.append(
                {
                    "path": path,
                    "previous_size": previous_size,
                    "current_size": current_size,
                }
            )
    return changes


def archive_config_folder_if_size_changed(
    *,
    config_dir: str | Path = "codebase/config",
    archive_dir: str | Path | None = None,
    stamp: str | None = None,
) -> Path | None:
    """
    Snapshot `codebase/config` when any tracked config file size changes.

    The latest size snapshot is stored in the archive directory. The first run
    creates a baseline archive so there is a recoverable copy for later diffs.
    """
    resolved_config_dir = Path(pipeline.resolve_str(config_dir))
    resolved_archive_dir = (
        Path(pipeline.resolve_str(archive_dir))
        if archive_dir is not None
        else resolved_config_dir / "archive"
    )
    resolved_archive_dir.mkdir(parents=True, exist_ok=True)

    current_snapshot = collect_config_file_size_snapshot(
        resolved_config_dir,
        archive_dir=resolved_archive_dir,
    )
    manifest_path = resolved_archive_dir / CONFIG_ARCHIVE_MANIFEST_FILENAME
    previous_snapshot = _load_config_archive_manifest(manifest_path)
    changes = _config_size_changes(previous_snapshot, current_snapshot)
    if not changes:
        return None

    safe_stamp = (
        str(stamp).strip()
        if stamp is not None and str(stamp).strip()
        else datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    snapshot_dir = _build_unique_directory_path(resolved_archive_dir / f"config_{safe_stamp}")
    snapshot_dir.mkdir(parents=True)

    for path in _iter_archivable_config_files(resolved_config_dir, resolved_archive_dir):
        relative_path = path.relative_to(resolved_config_dir)
        target = snapshot_dir / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)

    snapshot_payload = {
        "archived_at": safe_stamp,
        "config_dir": str(resolved_config_dir),
        "trigger": "file_size_changed" if previous_snapshot is not None else "baseline",
        "changes": changes,
        "files": current_snapshot,
    }
    with (snapshot_dir / CONFIG_ARCHIVE_MANIFEST_FILENAME).open("w", encoding="utf-8") as handle:
        json.dump(snapshot_payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(snapshot_payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return snapshot_dir


def annotate_domestic_records(records: Sequence[dict]) -> list[dict]:
    annotated: list[dict] = []
    for record in records:
        updated = dict(record)
        updated.setdefault("domain", "domestic")
        annotated.append(updated)
    return annotated


def is_critical_failure_record(record: dict, critical_failure_patterns: Sequence[str]) -> bool:
    if str(record.get("status", "")).strip().lower() == "success":
        return False
    if str(record.get("domain", "")).strip().lower() == "international":
        return True
    error_text = str(record.get("error", "")).strip().lower()
    if not error_text:
        return False
    return any(pattern in error_text for pattern in critical_failure_patterns)


def raise_for_critical_failures(
    *,
    records: Sequence[dict],
    scenario: str,
    critical_failure_patterns: Sequence[str],
) -> None:
    critical_records = [
        record
        for record in records
        if is_critical_failure_record(record, critical_failure_patterns)
    ]
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


def _selected_scenarios_by_key(scenario_list: Sequence[str]) -> dict[str, str]:
    selected_scenarios: dict[str, str] = {}
    for scenario in scenario_list:
        scenario_label = str(scenario).strip()
        if scenario_label:
            selected_scenarios.setdefault(scenario_label.lower(), scenario_label)
    return selected_scenarios


def _collect_successful_export_paths(
    *,
    records: Sequence[dict],
    selected_scenarios: Mapping[str, str],
) -> tuple[list[Path], dict[str, list[dict]], dict[str, set[str]]]:
    successful_paths: list[Path] = []
    seen_paths: set[str] = set()
    scenario_records: dict[str, list[dict]] = {key: [] for key in selected_scenarios}
    scenario_domains_with_paths: dict[str, set[str]] = {
        key: set() for key in selected_scenarios
    }

    for record in records:
        scenario_key = str(record.get("scenario", "")).strip().lower()
        if scenario_key not in selected_scenarios:
            continue
        scenario_records.setdefault(scenario_key, []).append(record)
        if str(record.get("status", "")).strip().lower() != "success":
            continue

        record_domain = str(record.get("domain", "domestic")).strip().lower() or "domestic"
        for path_key in ("transport_export_path", "international_workbook"):
            raw_path = record.get(path_key)
            if not raw_path:
                continue

            resolved_path = Path(str(raw_path))
            resolved_key = str(resolved_path.resolve())
            if resolved_key in seen_paths:
                scenario_domains_with_paths.setdefault(scenario_key, set()).add(record_domain)
                continue
            if not resolved_path.exists():
                print(f"[WARN] Skipping missing export file: {resolved_path}")
                continue
            successful_paths.append(resolved_path)
            seen_paths.add(resolved_key)
            scenario_domains_with_paths.setdefault(scenario_key, set()).add(record_domain)

    return successful_paths, scenario_records, scenario_domains_with_paths


def _raise_for_missing_scenario_domains(
    *,
    selected_scenarios: Mapping[str, str],
    scenario_records: Mapping[str, Sequence[dict]],
    scenario_domains_with_paths: Mapping[str, set[str]],
    include_international: bool,
) -> None:
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
    if not missing_scenario_requirements:
        return

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


def _resolve_combined_economy_token(
    *,
    records: Sequence[dict],
    selected_scenarios: Mapping[str, str],
) -> str:
    economies: list[str] = []
    seen: set[str] = set()
    for record in records:
        scenario_key = str(record.get("scenario", "")).strip().lower()
        if scenario_key not in selected_scenarios:
            continue
        if str(record.get("status", "")).strip().lower() != "success":
            continue
        economy = str(record.get("economy", "")).strip()
        if not economy or economy in seen:
            continue
        economies.append(economy)
        seen.add(economy)

    if not economies:
        return "all"
    if len(economies) == 1:
        return sanitize_filename_token(economies[0])
    total_economies = len(ECONOMY_METADATA)
    if len(economies) >= total_economies:
        return "All_econs"
    return f"{len(economies)}_econs"


def save_combined_scenario_workbook(
    *,
    records: Sequence[dict],
    scenario_list: Sequence[str],
    date_id: str,
    include_international: bool,
    fallback_base_year: int,
    fallback_final_year: int,
) -> str | None:
    selected_scenarios = _selected_scenarios_by_key(scenario_list)
    successful_paths, scenario_records, scenario_domains_with_paths = (
        _collect_successful_export_paths(
            records=records,
            selected_scenarios=selected_scenarios,
        )
    )

    if not successful_paths:
        print("[WARN] No successful scenario export files were found to combine.")
        return None

    _raise_for_missing_scenario_domains(
        selected_scenarios=selected_scenarios,
        scenario_records=scenario_records,
        scenario_domains_with_paths=scenario_domains_with_paths,
        include_international=include_international,
    )

    leap_frames: list[pd.DataFrame] = []
    viewing_frames: list[pd.DataFrame] = []
    for workbook_path in successful_paths:
        leap_df = pd.read_excel(workbook_path, sheet_name="LEAP", header=2)
        viewing_df = pd.read_excel(workbook_path, sheet_name="FOR_VIEWING", header=2)
        leap_frames.append(drop_empty_unnamed_columns(leap_df))
        viewing_frames.append(drop_empty_unnamed_columns(viewing_df))

    combined_leap_df = pd.concat(leap_frames, ignore_index=True)
    combined_viewing_df = pd.concat(viewing_frames, ignore_index=True)

    combined_leap_df, removed_leap = deduplicate_current_accounts_rows(combined_leap_df)
    combined_viewing_df, removed_viewing = deduplicate_current_accounts_rows(
        combined_viewing_df
    )
    if removed_leap or removed_viewing:
        print(
            "[INFO] Deduplicated Current Accounts rows in combined workbook: "
            f"LEAP={removed_leap}, FOR_VIEWING={removed_viewing}"
        )

    included_scenarios = [selected_scenarios[key] for key in selected_scenarios]
    economy_token = _resolve_combined_economy_token(
        records=records,
        selected_scenarios=selected_scenarios,
    )
    scenario_token = sanitize_filename_token("_".join(included_scenarios))
    if include_international:
        combined_filename = (
            f"{COMBINED_EXPORT_DIR}/transport_leap_export_combined_{economy_token}_domestic_international_"
            f"{scenario_token}_{date_id}.xlsx"
        )
        model_name = f"Transport Combined Domestic+International ({', '.join(included_scenarios)})"
    else:
        combined_filename = (
            f"{COMBINED_EXPORT_DIR}/transport_leap_export_combined_{economy_token}_"
            f"{scenario_token}_{date_id}.xlsx"
        )
        model_name = f"Transport Combined ({', '.join(included_scenarios)})"

    combined_output_path = pipeline.resolve_str(combined_filename)
    Path(combined_output_path).parent.mkdir(parents=True, exist_ok=True)
    base_year, final_year = infer_year_bounds(
        combined_viewing_df,
        fallback_base_year=fallback_base_year,
        fallback_final_year=fallback_final_year,
    )

    archived_output = pipeline._archive_existing_output_file(
        combined_output_path,
        date_id=date_id,
    )
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
