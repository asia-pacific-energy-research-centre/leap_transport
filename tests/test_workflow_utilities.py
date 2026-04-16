import io
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
CODE_DIR = REPO_ROOT / "codebase"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from functions.workflow_utilities import (
    FilteredLineStream,
    allow_stage_economy_log_line,
    annotate_domestic_records,
    archive_config_folder_if_size_changed,
    collect_config_file_size_snapshot,
    deduplicate_current_accounts_rows,
    drop_empty_unnamed_columns,
    infer_year_bounds,
    output_filter_context,
    resolve_sales_policy_settings_for_scenario,
    resolve_scenario_selection,
    sanitize_filename_token,
)


class WorkflowUtilitiesTests(unittest.TestCase):
    def test_filtered_line_stream_buffers_partial_lines(self):
        target = io.StringIO()
        stream = FilteredLineStream(
            target,
            allow_line=lambda line: line.startswith("[keep]"),
        )

        self.assertEqual(stream.write("[keep] partial"), 14)
        self.assertEqual(target.getvalue(), "")

        stream.write(" line\n[drop] line\n[keep] tail")
        self.assertEqual(target.getvalue(), "[keep] partial line\n")

        stream.flush()
        self.assertEqual(target.getvalue(), "[keep] partial line\n[keep] tail")

    def test_stage_economy_filter_allows_stage_and_error_lines(self):
        self.assertTrue(
            allow_stage_economy_log_line("=== Starting workflow for scenario 'Reference' ===")
        )
        self.assertTrue(allow_stage_economy_log_line("[ERROR] Something failed"))
        self.assertFalse(allow_stage_economy_log_line("ordinary verbose progress"))
        self.assertFalse(allow_stage_economy_log_line(""))

    def test_output_filter_context_filters_stdout(self):
        target = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = target
        try:
            with output_filter_context("stage_economy"):
                print("ordinary verbose progress")
                print("=== Running economy 20_USA ===")
        finally:
            sys.stdout = original_stdout

        self.assertEqual(target.getvalue(), "=== Running economy 20_USA ===\n")

    def test_sanitize_filename_token_removes_empty_parts(self):
        self.assertEqual(sanitize_filename_token("Reference / Target"), "Reference_Target")
        self.assertEqual(sanitize_filename_token("***"), "scenarios")

    def test_drop_empty_unnamed_columns_only_drops_blank_unnamed_columns(self):
        df = pd.DataFrame(
            {
                "A": [1, 2],
                "Unnamed: 1": [None, None],
                "Unnamed: 2": [None, "kept"],
            }
        )

        result = drop_empty_unnamed_columns(df)

        self.assertEqual(list(result.columns), ["A", "Unnamed: 2"])

    def test_deduplicate_current_accounts_rows_uses_key_columns(self):
        df = pd.DataFrame(
            {
                "Branch Path": ["a", "a", "a"],
                "Variable": ["Activity", "Activity", "Activity"],
                "Scenario": ["Current Accounts", "Current Accounts", "Target"],
                "Region": ["20_USA", "20_USA", "20_USA"],
                2022: [1, 2, 3],
            }
        )

        result, removed = deduplicate_current_accounts_rows(df)

        self.assertEqual(removed, 1)
        self.assertEqual(len(result), 2)

    def test_infer_year_bounds_uses_fallback_when_no_year_columns(self):
        df = pd.DataFrame({"Scenario": ["Reference"]})

        self.assertEqual(
            infer_year_bounds(df, fallback_base_year=2022, fallback_final_year=2060),
            (2022, 2060),
        )

    def test_resolve_scenario_selection_deduplicates_and_skips_current_accounts(self):
        result = resolve_scenario_selection(
            ["Reference", "reference", "Current Accounts", "Target"]
        )

        self.assertEqual(result, ["Reference", "Target"])

    def test_resolve_sales_policy_settings_for_scenario(self):
        settings = {
            "Reference": {
                "passenger": {"turnover_policies": {"LPV": {"survival_multiplier": 1.1}}},
                "freight": {"turnover_policies": {"Trucks": {"survival_multiplier": 1.2}}},
            }
        }

        passenger, freight = resolve_sales_policy_settings_for_scenario(
            settings,
            "reference",
        )

        self.assertEqual(passenger["turnover_policies"]["LPV"]["survival_multiplier"], 1.1)
        self.assertEqual(freight["turnover_policies"]["Trucks"]["survival_multiplier"], 1.2)

    def test_annotate_domestic_records_does_not_mutate_input(self):
        records = [{"economy": "20_USA"}]

        result = annotate_domestic_records(records)

        self.assertEqual(result, [{"economy": "20_USA", "domain": "domestic"}])
        self.assertEqual(records, [{"economy": "20_USA"}])

    def test_collect_config_file_size_snapshot_excludes_archive_and_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "config"
            archive_dir = config_dir / "archive"
            cache_dir = config_dir / "__pycache__"
            config_dir.mkdir()
            archive_dir.mkdir()
            cache_dir.mkdir()
            (config_dir / "a.py").write_text("abc", encoding="utf-8")
            (archive_dir / "old.py").write_text("ignored", encoding="utf-8")
            (cache_dir / "a.pyc").write_bytes(b"ignored")

            result = collect_config_file_size_snapshot(config_dir)

            self.assertEqual(result, {"a.py": 3})

    def test_archive_config_folder_creates_baseline_then_noops_when_sizes_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "config"
            config_dir.mkdir()
            (config_dir / "a.py").write_text("abc", encoding="utf-8")

            first_archive = archive_config_folder_if_size_changed(
                config_dir=config_dir,
                stamp="20260101",
            )
            second_archive = archive_config_folder_if_size_changed(
                config_dir=config_dir,
                stamp="20260101",
            )

            self.assertIsNotNone(first_archive)
            self.assertTrue((first_archive / "a.py").exists())
            self.assertTrue((config_dir / "archive" / "_config_file_size_manifest.json").exists())
            self.assertIsNone(second_archive)

    def test_archive_config_folder_archives_again_when_file_size_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp) / "config"
            config_dir.mkdir()
            target = config_dir / "a.py"
            target.write_text("abc", encoding="utf-8")

            archive_config_folder_if_size_changed(
                config_dir=config_dir,
                stamp="20260101",
            )
            target.write_text("abcdef", encoding="utf-8")

            changed_archive = archive_config_folder_if_size_changed(
                config_dir=config_dir,
                stamp="20260101",
            )

            self.assertIsNotNone(changed_archive)
            self.assertEqual(changed_archive.name, "config_20260101_01")
            self.assertEqual((changed_archive / "a.py").read_text(encoding="utf-8"), "abcdef")


if __name__ == "__main__":
    unittest.main()
