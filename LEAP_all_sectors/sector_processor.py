"""Generic orchestration logic for loading sector data into LEAP."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .sector_config import SectorConfig
from .sector_core import (
    BranchTuple,
    build_expression_from_mapping,
    connect_to_leap,
    diagnose_leap_branch,
    safe_branch_call,
    safe_set_variable,
)


@dataclass
class SectorProcessingResult:
    """Simple container summarising a LEAP loading run."""

    total_written: int
    total_skipped: int
    missing_branches: int
    missing_variables: int
    validation_reports: List[pd.DataFrame]
    leap_data_log: Optional[pd.DataFrame]
    import_dataframe: Optional[pd.DataFrame]


class SectorProcessor:
    """Drive the data→LEAP workflow for an arbitrary sector."""

    def __init__(self, config: SectorConfig, *, set_vars_in_leap_using_com: bool = True):
        self.config = config
        self.set_vars_in_leap_using_com = set_vars_in_leap_using_com

    # ------------------------------------------------------------------
    # Data preparation utilities
    # ------------------------------------------------------------------
    def prepare_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
        """Run configured preprocessors, share normalisers and validators."""

        for preprocessor in self.config.preprocessors:
            df = preprocessor(df)

        for normaliser in self.config.share_normalisers:
            df = normaliser(df)

        reports: List[pd.DataFrame] = []
        for validator in self.config.validators:
            df, report = validator(df)
            if report is not None and not report.empty:
                reports.append(report)
        return df, reports

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    def create_log_frame(self) -> pd.DataFrame:
        """Initialise an empty logging frame respecting the config."""

        columns: List[str] = []
        if self.config.scenario_column:
            columns.append(self.config.scenario_column)
        columns.append(self.config.time_column)
        columns.extend(self.config.source_dimension_order)
        columns.extend(self.config.log_extra_columns)
        columns.extend([
            "Measure",
            "Value",
            "Branch_Path",
            "LEAP_Tuple",
            "Source_Tuple",
            "Units",
            "Scale",
            "Per...",
        ])
        return pd.DataFrame(columns=columns)

    def log_leap_data(
        self,
        log_df: Optional[pd.DataFrame],
        df_m: pd.DataFrame,
        leap_tuple: BranchTuple,
        src_tuple: Tuple[str, ...],
        branch_path: str,
        measure: str,
    ) -> Optional[pd.DataFrame]:
        """Append the processed measure data to ``log_df``."""

        if log_df is None:
            return None

        new_rows: List[Dict[str, object]] = []
        for _, row in df_m.iterrows():
            value = row.get(measure)
            if pd.isna(value):
                continue
            log_row: Dict[str, object] = {}
            if self.config.scenario_column and self.config.scenario_column in df_m.columns:
                log_row[self.config.scenario_column] = row.get(self.config.scenario_column)
            time_value = row.get(self.config.time_column)
            if pd.isna(time_value):
                continue
            log_row[self.config.time_column] = int(time_value)
            for idx, column in enumerate(self.config.source_dimension_order):
                log_row[column] = row.get(column)
                if column not in df_m.columns and idx < len(leap_tuple):
                    log_row[column] = leap_tuple[idx]
            for column in self.config.log_extra_columns:
                log_row[column] = row.get(column)
            log_row.update(
                {
                    "Measure": measure,
                    "Value": float(value),
                    "Branch_Path": branch_path,
                    "LEAP_Tuple": str(leap_tuple),
                    "Source_Tuple": str(src_tuple),
                }
            )
            new_rows.append(log_row)

        if not new_rows:
            return log_df

        new_df = pd.DataFrame(new_rows)
        if log_df.empty:
            return new_df
        return pd.concat([log_df, new_df], ignore_index=True)

    # ------------------------------------------------------------------
    # Branch mapping helpers
    # ------------------------------------------------------------------
    def build_branch_path(self, leap_tuple: BranchTuple) -> str:
        """Return the LEAP branch path for ``leap_tuple``."""

        suffix = "\\".join([part for part in leap_tuple if part])
        return f"{self.config.leap_root}\\{suffix}" if suffix else self.config.leap_root

    def compute_group_columns(self, src_tuple: Tuple[str, ...]) -> List[str]:
        """Determine grouping columns for aggregations."""

        group_cols: List[str] = []
        if self.config.time_column:
            group_cols.append(self.config.time_column)
        for base_col in self.config.grouping_base_columns:
            if base_col and base_col not in group_cols:
                group_cols.append(base_col)
        for idx in range(min(len(src_tuple), len(self.config.source_dimension_order))):
            col = self.config.source_dimension_order[idx]
            if col not in group_cols:
                group_cols.append(col)
        return group_cols

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------
    def write_measures_to_leap(
        self,
        leap_app,
        branch_obj,
        df_copy: pd.DataFrame,
        leap_tuple: BranchTuple,
        src_tuple: Tuple[str, ...],
        branch_path: str,
        filtered_measure_config: Mapping[str, Dict[str, object]],
        shortname: str,
        group_columns: Sequence[str],
        save_log: bool,
        log_df: Optional[pd.DataFrame],
    ) -> Tuple[int, int, Optional[pd.DataFrame]]:
        """Process measures for a branch and optionally write them into LEAP."""

        processed = self.config.measures_processor(
            df_copy,
            filtered_measure_config,
            shortname,
            group_columns,
            leap_tuple,
            src_tuple,
        )

        written = 0
        missing_variables = 0
        for measure, df_m in processed.items():
            if save_log:
                before_len = len(log_df) if log_df is not None else 0
                log_df = self.log_leap_data(log_df, df_m, leap_tuple, src_tuple, branch_path, measure)
                if log_df is not None:
                    after_len = len(log_df)
                    if after_len > before_len:
                        meta_cfg = filtered_measure_config.get(measure, {}) if filtered_measure_config else {}
                        meta_values = {
                            "LEAP_units": meta_cfg.get("LEAP_units"),
                            "LEAP_Scale": meta_cfg.get("LEAP_Scale"),
                            "LEAP_Per": meta_cfg.get("LEAP_Per"),
                        }
                        meta_values = self.config.metadata_resolver(meta_values, leap_tuple, src_tuple)
                        log_df.loc[log_df.index[before_len:after_len], "Units"] = meta_values.get("LEAP_units")
                        log_df.loc[log_df.index[before_len:after_len], "Scale"] = meta_values.get("LEAP_Scale")
                        log_df.loc[log_df.index[before_len:after_len], "Per..."] = meta_values.get("LEAP_Per")

            expr = build_expression_from_mapping(
                leap_tuple,
                df_m,
                measure,
                self.config.expression_mapping,
                default_mode=self.config.default_expression_mode,
            )

            if expr and self.set_vars_in_leap_using_com and branch_obj is not None:
                success = safe_set_variable(branch_obj, measure, expr, branch_path)
                if success:
                    written += 1
                else:
                    missing_variables += 1
            elif expr:
                print(f"[INFO] Prepared but not applied: {measure} on {branch_path}")
                written += 1
        return written, missing_variables, log_df

    def summarize_and_export(
        self,
        total_written: int,
        total_skipped: int,
        missing_branches: int,
        missing_variables: int,
        leap_data_log: Optional[pd.DataFrame],
        save_log: bool,
        log_filename: str,
        create_import_files: bool,
        import_filename: str,
        import_builder: Optional[Callable[[pd.DataFrame, str], Optional[pd.DataFrame]]],
    ) -> Optional[pd.DataFrame]:
        """Print summary information and optionally persist outputs."""

        print("\n=== Summary ===")
        print(f"✅ Variables written: {total_written}")
        print(f"⚠️  Skipped (no data or invalid tuples): {total_skipped}")
        print(f"❌ Missing LEAP branches: {missing_branches}")
        print(f"❌ Missing variables: {missing_variables}")
        print("================\n")

        if save_log and leap_data_log is not None and not leap_data_log.empty:
            print(f"Saving LEAP data log to {log_filename}...")
            with pd.ExcelWriter(log_filename, engine="openpyxl") as writer:
                leap_data_log.to_excel(writer, sheet_name="All_Data", index=False)

            if create_import_files and import_builder is not None:
                print("\n=== Exporting LEAP import-compatible files ===")
                export_df = import_builder(leap_data_log, import_filename)
                if export_df is not None:
                    print(f"LEAP import file created at: {import_filename}")
                    return export_df
        else:
            print("No LEAP import file created.")
        return None

    def load_into_leap(
        self,
        data_loader: Callable[..., pd.DataFrame],
        *,
        loader_kwargs: Optional[Dict[str, object]] = None,
        diagnose_method: str = "first_of_each_length",
        save_log: bool = True,
        log_filename: str = "leap_data_log.xlsx",
        create_import_files: bool = False,
        import_filename: str = "leap_import.xlsx",
        import_builder: Optional[Callable[[pd.DataFrame, str], Optional[pd.DataFrame]]] = None,
    ) -> SectorProcessingResult:
        """Run the full pipeline for the configured sector."""

        loader_kwargs = loader_kwargs or {}
        df_raw = data_loader(**loader_kwargs)
        df, validation_reports = self.prepare_dataframe(df_raw)

        leap_app = connect_to_leap() if self.set_vars_in_leap_using_com else None
        log_df = self.create_log_frame() if save_log else None

        total_written = 0
        total_skipped = 0
        missing_branches = 0
        missing_variables = 0
        first_branch_diagnosed = False
        first_of_each_length_diagnosed = set()

        for leap_tuple, src_tuple in self.config.leap_branch_to_source_map.items():
            df_copy = df.copy()
            if df_copy.empty:
                total_skipped += 1
                continue

            if src_tuple is None:
                print(
                    f"[INFO] Skipping LEAP branch {leap_tuple} as it has no source mapping."
                )
                total_skipped += 1
                continue

            branch_path = self.build_branch_path(leap_tuple)
            group_columns = self.compute_group_columns(src_tuple)

            branch_obj = None
            if self.set_vars_in_leap_using_com:
                branch_obj = safe_branch_call(leap_app, branch_path, timeout_msg=False)
                if branch_obj is None:
                    print(f"[WARN] Missing LEAP branch: {branch_path}")
                    missing_branches += 1
                    continue

            shortname = self.config.get_shortname_for_branch(leap_tuple)
            if not shortname:
                print(f"[ERROR] Could not identify unique measure config for {branch_path}")
                total_skipped += 1
                continue

            filtered_measure_config = self.config.measure_config.get(shortname, {})
            expected_measures = set(filtered_measure_config.keys())

            if self.set_vars_in_leap_using_com:
                if diagnose_method == "first_branch" and not first_branch_diagnosed:
                    diagnose_leap_branch(leap_app, branch_path, leap_tuple, expected_measures)
                    first_branch_diagnosed = True
                elif diagnose_method == "first_of_each_length" and len(leap_tuple) not in first_of_each_length_diagnosed:
                    diagnose_leap_branch(leap_app, branch_path, leap_tuple, expected_measures)
                    first_of_each_length_diagnosed.add(len(leap_tuple))
                elif diagnose_method == "all":
                    diagnose_leap_branch(leap_app, branch_path, leap_tuple, expected_measures)

            written, missing_vars, log_df = self.write_measures_to_leap(
                leap_app,
                branch_obj,
                df_copy,
                leap_tuple,
                tuple(src_tuple),
                branch_path,
                filtered_measure_config,
                shortname,
                group_columns,
                save_log,
                log_df,
            )
            total_written += written
            missing_variables += missing_vars
            if written == 0:
                total_skipped += 1

        import_df = self.summarize_and_export(
            total_written,
            total_skipped,
            missing_branches,
            missing_variables,
            log_df,
            save_log,
            log_filename,
            create_import_files,
            import_filename,
            import_builder,
        )

        return SectorProcessingResult(
            total_written=total_written,
            total_skipped=total_skipped,
            missing_branches=missing_branches,
            missing_variables=missing_variables,
            validation_reports=validation_reports,
            leap_data_log=log_df,
            import_dataframe=import_df,
        )


__all__ = ["SectorProcessor", "SectorProcessingResult"]
