"""Local snapshot of LEAP utility helpers used by this repository.

These functions were imported from the sibling ``leap_utilities`` repository so
``leap_transport`` can run its normal workbook export and reconciliation paths
without requiring that repo at runtime.

Snapshot date: 16/04/2026.
Review date: 16/04/2027.

If the upstream ``leap_utilities`` helpers change materially before that review
date, compare this file against the upstream repo and port the relevant changes.
The direct LEAP COM/API workflow is still intentionally disabled by the calling
workflow; the COM helper names are kept here only so existing imports resolve.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd


# Branch type constants copied from leap_utilities/codebase/configuration/config.py.
BRANCH_DEMAND_CATEGORY = 1
BRANCH_DEMAND_TECHNOLOGY = 4
BRANCH_DEMAND_FUEL = 36


# These globals are patched by transport_workflow_pipeline when structure checks
# need all transport economy regions.
scenario_dict = {
    "Current Accounts": {
        "scenario_name": "Current Accounts",
        "scenario_code": "CA",
        "scenario_id": 1,
    },
    "Target": {
        "scenario_name": "Target",
        "scenario_code": "TGT",
        "scenario_id": 3,
    },
    "Reference": {
        "scenario_name": "Reference",
        "scenario_code": "REF",
        "scenario_id": 4,
    },
}

region_id_name_dict = {
    "12_NZ": {
        "region_id": 2,
        "region_name": "New Zealand",
        "region_code": "12_NZ",
    },
    "20_USA": {
        "region_id": 1,
        "region_name": "United States of America",
        "region_code": "20_USA",
    },
}


LEAP_API_DISABLED_ERROR = (
    "LEAP COM/API helper '{operation}' is unavailable in the local vendored "
    "helper module. The transport workflow currently disables direct LEAP API "
    "use; keep the COM/API flags off and use workbook import."
)


def _raise_local_leap_api_disabled(operation: str) -> None:
    raise RuntimeError(LEAP_API_DISABLED_ERROR.format(operation=operation))


def connect_to_leap(force_rebuild: bool = True):
    """Placeholder for the disabled LEAP COM connection helper."""

    _raise_local_leap_api_disabled("connect_to_leap")


def ensure_branch_exists(*args, **kwargs):
    """Placeholder for the disabled LEAP COM branch creation/check helper."""

    _raise_local_leap_api_disabled("ensure_branch_exists")


def diagnose_measures_in_leap_branch(*args, **kwargs):
    """Placeholder for the disabled LEAP COM branch diagnostic helper."""

    _raise_local_leap_api_disabled("diagnose_measures_in_leap_branch")


def ensure_fuel_exists(*args, **kwargs):
    """Placeholder for the disabled LEAP COM fuel creation/check helper."""

    _raise_local_leap_api_disabled("ensure_fuel_exists")


def safe_set_variable(*args, **kwargs):
    """Placeholder for the disabled LEAP COM variable write helper."""

    _raise_local_leap_api_disabled("safe_set_variable")


def build_expr(points, expression_type="Interp"):
    """Build a LEAP-compatible expression from year/value points."""

    if not points:
        return None
    df = pd.DataFrame(points, columns=["year", "value"]).dropna(
        subset=["year", "value"]
    )
    if df["year"].duplicated().any():
        duplicated = df.loc[df["year"].duplicated(), "year"].tolist()
        raise ValueError(f"Duplicate years found while building expression: {duplicated}")
    df = df.sort_values("year")
    pts = list(zip(df["year"].astype(int), df["value"].astype(float)))
    if len(pts) == 1:
        return str(pts[0][1])
    if expression_type == "":
        raise ValueError(
            "expression_type cannot be empty when more than one point is present."
        )
    return f"{expression_type}(" + ", ".join(f"{y}, {v:.6g}" for y, v in pts) + ")"


def define_value_based_on_src_tuple(meta_values, src_tuple):
    """Resolve source-tuple placeholders in LEAP metadata values."""

    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * 5)[:5]
    for col in ["LEAP_units", "LEAP_Scale", "LEAP_Per"]:
        val = meta_values.get(col)
        if val is not None and isinstance(val, str) and "$" in val:
            parts = val.split("$")
            if len(parts) != 2:
                raise ValueError(f"Unexpected format for metadata value: {val}")
            if val == "Passenger-km$Tonne-km":
                if "passenger" in ttype:
                    resolved_value = "Passenger-km"
                elif "freight" in ttype:
                    resolved_value = "Tonne-km"
                else:
                    raise ValueError(
                        f"Unexpected ttype for resolving Passenger-km$Tonne-km: {ttype}"
                    )
                meta_values[col] = resolved_value
            elif val == "of Tonne-km$of Passenger-km":
                if "passenger" in ttype:
                    resolved_value = "of Passenger-km"
                elif "freight" in ttype:
                    resolved_value = "of Tonne-km"
                else:
                    raise ValueError(
                        "Unexpected ttype for resolving "
                        f"of Tonne-km$of Passenger-km: {ttype}"
                    )
                meta_values[col] = resolved_value
            else:
                raise ValueError(f"Unknown placeholder in metadata value: {val}")
    return meta_values


def create_transport_export_df():
    """Initialize the long-form dataframe used before LEAP workbook export."""

    return pd.DataFrame(
        columns=[
            "Date",
            "Transport_Type",
            "Medium",
            "Vehicle_Type",
            "Technology",
            "Fuel",
            "Measure",
            "Value",
            "Branch_Path",
            "LEAP_Tuple",
            "Source_Tuple",
        ]
    )


def write_row_to_leap_export_df(
    export_df, leap_tuple, src_tuple, branch_path, measure, df_m
):
    """Append one processed measure dataframe to the long-form export dataframe."""

    new_rows = []
    for _, row in df_m.iterrows():
        if pd.notna(row[measure]):
            new_rows.append(
                {
                    "Date": int(row["Date"]),
                    "Transport_Type": leap_tuple[0] if len(leap_tuple) > 0 else pd.NA,
                    "Medium": leap_tuple[1] if len(leap_tuple) > 1 else pd.NA,
                    "Vehicle_Type": leap_tuple[2] if len(leap_tuple) > 2 else pd.NA,
                    "Technology": leap_tuple[3] if len(leap_tuple) > 3 else pd.NA,
                    "Fuel": leap_tuple[4] if len(leap_tuple) > 4 else pd.NA,
                    "Measure": measure,
                    "Value": float(row[measure]),
                    "Branch_Path": branch_path,
                    "LEAP_Tuple": str(leap_tuple),
                    "Source_Tuple": str(src_tuple),
                }
            )
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        export_df = (
            pd.concat([export_df, new_df], ignore_index=True)
            if not export_df.empty
            else new_df.copy()
        )
    return export_df


def build_expression_from_mapping(
    branch_tuple,
    df_m,
    measure,
    mapping=None,
    all_years=None,
):
    """Build a LEAP expression for a branch/measure using expression mapping rules."""

    if mapping is None:
        raise ValueError("A LEAP branch expression mapping must be provided.")
    if all_years is None:
        raise ValueError("all_years must be provided when building expressions.")

    entry = (measure,) + branch_tuple
    mapping_entry = mapping.get(entry, ("Data", all_years))
    mode, arg = mapping_entry

    if mode != "SingleValue":
        valid = df_m[pd.notna(df_m["Value"])]
        if len(valid) == 1:
            mode = "SingleValue"

    if mode == "Data":
        pts = [
            (int(r["Date"]), float(r["Value"]))
            for _, r in df_m.iterrows()
            if pd.notna(r["Value"])
        ]
        return build_expr(pts, "Data") if pts else None, "Data"

    if mode == "Interp":
        start, end = arg[0], arg[-1]
        df_filtered = df_m[(df_m["Date"] >= start) & (df_m["Date"] <= end)]
        pts = [
            (int(r["Date"]), float(r["Value"]))
            for _, r in df_filtered.iterrows()
            if pd.notna(r["Value"])
        ]
        return build_expr(pts, "Interp") if pts else None, "Interp"

    if mode == "Flat":
        year = arg[0]
        value_col = "Value" if "Value" in df_m.columns else measure
        val = df_m.loc[df_m["Date"] == year, value_col].mean()
        return str(float(val)) if pd.notna(val) else None, "Flat"

    if mode == "SingleValue":
        valid = df_m[pd.notna(df_m["Value"])]
        if len(valid) == 1:
            return str(float(valid["Value"].iloc[0])), "SingleValue"
        print(
            f"[WARN] Expected single value for {branch_tuple} but found "
            f"{len(valid)} rows. Falling back to Data."
        )
        pts = [(int(r["Date"]), float(r["Value"])) for _, r in valid.iterrows()]
        return build_expr(pts, "Data") if pts else None, "Data"

    if mode == "Custom":
        func = globals().get(arg)
        if callable(func):
            return func(branch_tuple, df_m, measure), "Custom"
        print(f"[WARN] Custom function '{arg}' not found for {branch_tuple}")
        return None, None

    print(f"[WARN] Unknown mode '{mode}' for {branch_tuple}. Using raw data.")
    pts = [
        (int(r["Date"]), float(r["Value"]))
        for _, r in df_m.iterrows()
        if pd.notna(r["Value"])
    ]
    return build_expr(pts, "Data") if pts else None, "Data"


def finalise_export_df(log_df, scenario, region, base_year, final_year):
    """Create a LEAP-compatible wide import dataframe from long export rows."""

    print("\n=== Creating LEAP Import File (structured) ===")

    if log_df is None or log_df.empty:
        print("[ERROR] No data available for export.")
        return None

    log_df = log_df[(log_df["Date"] >= base_year) & (log_df["Date"] <= final_year)]
    numeric_values = pd.to_numeric(log_df["Value"], errors="coerce")
    negative_mask = numeric_values < 0
    if negative_mask.any():
        negative_count = int(negative_mask.sum())
        sample_paths = (
            log_df.loc[negative_mask, "Branch_Path"]
            .dropna()
            .astype(str)
            .head(5)
            .tolist()
        )
        print(
            "[WARN] Found "
            f"{negative_count} negative export values; clipping them to 0.0. "
            f"Sample branches: {sample_paths}"
        )
        log_df = log_df.copy()
        log_df.loc[negative_mask, "Value"] = 0.0

    for col in ["Units", "Scale", "Per..."]:
        if log_df[col].isna().all():
            log_df[col] = "N/A"
        elif log_df[col].isnull().all():
            log_df[col] = "null"
        elif (log_df[col] == "").all():
            log_df[col] = "empty"
        elif (log_df[col] == None).all():
            log_df[col] = "None"

    pivot_df = (
        log_df.pivot(
            index=["Branch_Path", "Scenario", "Measure", "Units", "Scale", "Per..."],
            columns="Date",
            values="Value",
        )
        .reset_index()
    )

    for col in ["Units", "Scale", "Per..."]:
        pivot_df[col] = pivot_df[col].replace(
            {"N/A": pd.NA, "null": pd.NA, "empty": "", "None": None}
        )
        log_df[col] = log_df[col].replace(
            {"N/A": pd.NA, "null": pd.NA, "empty": "", "None": None}
        )

    year_cols = sorted(
        [int(c) for c in pivot_df.columns if isinstance(c, (int, float))]
    )
    pivot_df["Branch Path"] = pivot_df["Branch_Path"]
    pivot_df["Variable"] = pivot_df["Measure"]
    pivot_df["Region"] = region

    max_levels = pivot_df["Branch_Path"].apply(lambda x: len(str(x).split("\\"))).max()
    for i in range(1, max_levels + 1):
        pivot_df[f"Level {i}"] = pivot_df["Branch_Path"].apply(
            lambda x: str(x).split("\\")[i - 1]
            if len(str(x).split("\\")) >= i
            else ""
        )

    var_order = [
        "Total Activity",
        "Activity Level",
        "Final Energy Intensity",
        "Total Final Energy Consumption",
        "Stock",
        "Sales Share",
        "Efficiency",
        "Turnover Rate",
        "Occupancy or Load",
    ]
    pivot_df["Variable_sort_order"] = pivot_df["Variable"].apply(
        lambda v: var_order.index(v) if v in var_order else len(var_order)
    )
    pivot_df = pivot_df.sort_values(by=["Branch_Path", "Variable_sort_order"]).drop(
        columns="Variable_sort_order"
    )

    base_cols = [
        "Branch Path",
        "Variable",
        "Scenario",
        "Region",
        "Scale",
        "Units",
        "Per...",
    ]
    level_cols = [f"Level {i}" for i in range(1, max_levels + 1)]
    export_df = pivot_df[base_cols + year_cols + level_cols].copy()

    if "Scenario" in export_df.columns and year_cols:
        current_accounts_labels = {"current accounts", "current account"}
        scenario_tokens = (
            export_df["Scenario"].fillna("").astype(str).str.strip().str.lower()
        )
        current_accounts_mask = scenario_tokens.isin(current_accounts_labels)
        non_current_mask = ~current_accounts_mask
        base_year_int = int(base_year)
        for year in year_cols:
            year_int = int(year)
            if year_int > base_year_int:
                export_df.loc[current_accounts_mask, year_int] = pd.NA
            elif year_int == base_year_int:
                export_df.loc[non_current_mask, year_int] = pd.NA

    return export_df


def save_export_files(
    leap_export_df,
    export_df_for_viewing,
    leap_export_filename,
    base_year,
    final_year,
    model_name,
):
    """Save LEAP and FOR_VIEWING sheets to the workbook import file."""

    out_path = Path(leap_export_filename)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    leap_export_df2 = leap_export_df.copy()
    export_df_for_viewing2 = export_df_for_viewing.copy()

    id_cols = ["BranchID", "VariableID", "ScenarioID", "RegionID"]

    def _ensure_id_cols_first(df: pd.DataFrame) -> pd.DataFrame:
        for col in id_cols:
            if col not in df.columns:
                df = df.assign(**{col: pd.NA})
        rest = [c for c in df.columns if c not in id_cols]
        return df[id_cols + rest]

    leap_export_df2 = _ensure_id_cols_first(leap_export_df2)
    export_df_for_viewing2 = _ensure_id_cols_first(export_df_for_viewing2)

    def _normalize_sheet_columns(df: pd.DataFrame, *, sheet_kind: str) -> pd.DataFrame:
        out = df.copy()

        def _level_sort_key(column: object) -> tuple[int, str]:
            text = str(column)
            if text.startswith("Level "):
                token = text.replace("Level ", "", 1).replace("...", "").strip()
                if token.isdigit():
                    return int(token), ""
            return 9999, text

        id_cols = ["BranchID", "VariableID", "ScenarioID", "RegionID"]
        key_cols = ["Branch Path", "Variable", "Scenario", "Region"]
        meta_cols = ["Scale", "Units", "Per..."]
        level_cols = sorted(
            [col for col in out.columns if str(col).startswith("Level ")],
            key=_level_sort_key,
        )
        year_cols = [
            year for year in range(int(base_year), int(final_year) + 1)
        ]

        if sheet_kind == "LEAP":
            if "Method" in out.columns:
                out = out.drop(columns=["Method"])
            for col in year_cols:
                if col in out.columns:
                    out = out.drop(columns=[col])
            desired = id_cols + key_cols + meta_cols + ["Expression", "Unnamed: 12"] + level_cols
        else:
            if "Expression" in out.columns:
                out = out.drop(columns=["Expression"])
            desired = id_cols + key_cols + meta_cols + ["Method"] + year_cols + level_cols
            for col in year_cols:
                if col not in out.columns:
                    out[col] = pd.NA
            if "Method" not in out.columns:
                out["Method"] = pd.NA

        for col in desired:
            if col not in out.columns:
                out[col] = pd.NA

        ordered = desired + [col for col in out.columns if col not in desired]
        return out.loc[:, ordered].copy()

    leap_export_df2 = _normalize_sheet_columns(leap_export_df2, sheet_kind="LEAP")
    export_df_for_viewing2 = _normalize_sheet_columns(
        export_df_for_viewing2, sheet_kind="FOR_VIEWING"
    )

    def _warn_missing_ids(df: pd.DataFrame, *, label: str) -> None:
        missing_mask = df[id_cols].isna().any(axis=1)
        if not missing_mask.any():
            return
        missing_counts = {col: int(df[col].isna().sum()) for col in id_cols}
        print(
            f"[WARN] {label}: {int(missing_mask.sum())} row(s) still have missing ID values "
            "at save time."
        )
        print(f"[WARN] {label}: missing counts by column -> {missing_counts}")
        preview_cols = [col for col in id_cols + ["Branch Path", "Variable", "Scenario", "Region"] if col in df.columns]
        print(df.loc[missing_mask, preview_cols].head(20).to_string(index=False))

    _warn_missing_ids(leap_export_df2, label="LEAP sheet")
    _warn_missing_ids(export_df_for_viewing2, label="FOR_VIEWING sheet")

    header_data_0 = {col: "" for col in leap_export_df2.columns}
    header_data_0["Branch Path"] = "Area:"
    header_data_0["Variable"] = model_name
    header_data_0["Scenario"] = "Ver:"
    header_data_0["Region"] = "2"
    header_row_0 = pd.DataFrame([header_data_0])
    nas = pd.DataFrame([{col: pd.NA for col in leap_export_df2.columns}])
    header_row_2 = pd.DataFrame(
        [leap_export_df2.columns], columns=leap_export_df2.columns
    )
    leap_export_df2 = pd.concat(
        [header_row_0, nas, header_row_2, leap_export_df2], ignore_index=True
    )

    header_data_0_view = {col: "" for col in export_df_for_viewing2.columns}
    header_data_0_view["Branch Path"] = "Area:"
    header_data_0_view["Variable"] = model_name
    header_data_0_view["Scenario"] = "Ver:"
    header_data_0_view["Region"] = "2"
    header_row_0_view = pd.DataFrame([header_data_0_view])
    nas = pd.DataFrame([{col: pd.NA for col in export_df_for_viewing2.columns}])
    header_row_2_view = pd.DataFrame(
        [export_df_for_viewing2.columns], columns=export_df_for_viewing2.columns
    )
    export_df_for_viewing2 = pd.concat(
        [header_row_0_view, nas, header_row_2_view, export_df_for_viewing2],
        ignore_index=True,
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        export_df_for_viewing2.to_excel(
            writer, sheet_name="FOR_VIEWING", index=False, header=False, startrow=0
        )
        leap_export_df2.to_excel(
            writer, sheet_name="LEAP", index=False, header=False, startrow=0
        )
    print(
        "[OK] Created file for importing into leap, and viewing at "
        f"{leap_export_filename}, with {len(export_df_for_viewing)} entries."
    )
    print(f" - Years covered: {base_year}-{final_year}")
    print(f" - Variables: {leap_export_df['Variable'].nunique()}")
    print(f" - Branches: {export_df_for_viewing['Branch Path'].nunique()}")
    print("=" * 60)


def merge_template_ids_into_export_df(
    export_df: pd.DataFrame,
    import_filename,
    *,
    label: str = "export dataframe",
) -> pd.DataFrame:
    """Fill LEAP ID columns by matching an export dataframe against a template."""

    key_cols = ["Branch Path", "Variable", "Scenario", "Region"]
    id_cols = ["BranchID", "VariableID", "ScenarioID", "RegionID"]

    template_df = pd.read_excel(import_filename, sheet_name="Export", header=2)
    missing_template_cols = [col for col in key_cols + id_cols if col not in template_df.columns]
    if missing_template_cols:
        raise ValueError(
            "Import template is missing required columns for ID merge: "
            + ", ".join(missing_template_cols)
        )

    template_df = template_df[key_cols + id_cols].copy()
    duplicate_keys = template_df.duplicated(subset=key_cols, keep=False)
    if duplicate_keys.any():
        print(
            f"[WARN] Template used for {label} contains {int(duplicate_keys.sum())} duplicate key row(s) "
            f"across {key_cols}; using the first match per key."
        )
        template_df = template_df.drop_duplicates(subset=key_cols, keep="first").copy()
    template_df = template_df.rename(columns={col: f"{col}_template" for col in id_cols})

    merged = export_df.copy().merge(template_df, how="left", on=key_cols)
    for id_col in id_cols:
        template_col = f"{id_col}_template"
        if id_col in merged.columns:
            merged[id_col] = merged[template_col].combine_first(merged[id_col])
        else:
            merged[id_col] = merged[template_col]
        merged = merged.drop(columns=[template_col])

    missing_mask = merged[id_cols].isna().any(axis=1)
    if missing_mask.any():
        missing_counts = {col: int(merged[col].isna().sum()) for col in id_cols}
        print(
            f"[WARN] {label}: {int(missing_mask.sum())} row(s) still have missing ID values after template merge."
        )
        print(f"[WARN] {label}: missing counts by column -> {missing_counts}")
        preview_cols = [col for col in key_cols + id_cols if col in merged.columns]
        print(merged.loc[missing_mask, preview_cols].head(20).to_string(index=False))

    for id_col in id_cols:
        if id_col in merged.columns:
            merged[id_col] = merged[id_col].astype("Int64")

    return merged


def check_scenario_and_region_ids(import_df, scenario, region):
    """Filter/retarget a LEAP template dataframe to the requested scenario/region."""

    dict_regions = [
        payload["region_name"]
        for payload in region_id_name_dict.values()
        if isinstance(payload, dict)
    ]
    if region not in dict_regions:
        raise ValueError(
            f"[ERROR] Region {region} is not in region_id_name_dict: {dict_regions}."
        )

    import_df = import_df.copy()
    template_regions = [
        str(value).strip()
        for value in import_df.get("Region", pd.Series(dtype=object)).dropna().unique()
        if str(value).strip()
    ]
    if region in template_regions:
        import_df = import_df[import_df["Region"] == region].copy()
    elif template_regions:
        source_region = template_regions[0]
        import_df = import_df[
            import_df["Region"].astype(str).str.strip() == source_region
        ].copy()
        print(
            "[INFO] Structure-check template region fallback: "
            f"using template region='{source_region}' rows for requested "
            f"region='{region}'."
        )
    else:
        raise ValueError("[ERROR] No regions found in import_df during structure checks.")

    region_ids = [
        payload.get("region_id")
        for payload in region_id_name_dict.values()
        if isinstance(payload, dict)
        and str(payload.get("region_name", "")).strip() == region
    ]
    if len(region_ids) != 1:
        raise ValueError(
            f"[ERROR] Multiple or no region ids found for region {region}."
        )
    import_df["Region"] = region
    import_df["RegionID"] = region_ids[0]

    dict_scenarios = [
        scenario_dict[key]["scenario_name"]
        for key in scenario_dict
        if isinstance(scenario_dict[key], dict)
    ]
    if scenario not in dict_scenarios:
        raise ValueError(
            f"[ERROR] Scenario {scenario} is not in scenario_dict: {dict_scenarios}."
        )

    import_df = import_df[
        (import_df["Scenario"] == scenario)
        | (import_df["Scenario"] == "Current Accounts")
    ].copy()

    import_scenarios = [
        value
        for value in import_df["Scenario"].dropna().unique()
        if value != "Current Accounts"
    ]
    if len(import_scenarios) != 1:
        raise ValueError(
            "[ERROR] More or less than one scenario found in import_df during "
            f"structure checks: {import_scenarios}."
        )

    scenario_ids = [
        scenario_dict[key]["scenario_id"]
        for key in scenario_dict
        if scenario_dict[key]["scenario_name"] == scenario
    ]
    if len(scenario_ids) != 1:
        raise ValueError(f"[ERROR] Multiple scenario ids found for scenario {scenario}.")
    import_df.loc[import_df["Scenario"] != "Current Accounts", "Scenario"] = scenario
    import_df.loc[
        import_df["Scenario"] != "Current Accounts", "ScenarioID"
    ] = scenario_ids[0]

    return import_df


def _choose_export_column(comparison_df: pd.DataFrame, column: str) -> pd.Series:
    export_col = f"{column}_export"
    import_col = f"{column}_import"
    if export_col in comparison_df.columns:
        return comparison_df[export_col]
    if column in comparison_df.columns:
        return comparison_df[column]
    if import_col in comparison_df.columns:
        return comparison_df[import_col]
    return pd.Series([pd.NA] * len(comparison_df), index=comparison_df.index)


def _logical_level_column_name(column: object) -> str | None:
    """Return the unsuffixed ``Level N`` name for merged import/export columns."""
    name = str(column)
    if not name.startswith("Level "):
        return None
    for suffix in ("_export", "_import"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    level_text = name.replace("Level ", "", 1).replace("...", "").strip()
    if not level_text.isdigit():
        return None
    return f"Level {int(level_text)}"


def join_and_check_import_structure_matches_export_structure(
    import_filename,
    export_df,
    export_df_for_viewing,
    scenario,
    region,
    STRICT_CHECKS=True,
    current_accounts_label="Current Accounts",
):
    """Attach LEAP template IDs and check generated rows against the template."""

    import_df = pd.read_excel(import_filename, sheet_name="Export", header=2)
    non_current_scenarios = export_df.loc[
        export_df["Scenario"] != current_accounts_label, "Scenario"
    ].unique()
    if len(non_current_scenarios) != 1:
        raise ValueError(
            "[ERROR] More or less than one non-Current Accounts scenario found "
            "in export_df during structure checks."
        )

    import_df = check_scenario_and_region_ids(import_df, scenario, region)
    key_cols = ["Branch Path", "Variable", "Scenario", "Region"]
    id_cols = ["BranchID", "VariableID", "ScenarioID", "RegionID"]
    unneeded_vars = {
        "Fraction of Scrapped Replaced",
        "Max Scrappage Fraction",
        "Scrappage",
        "Fuel Economy Correction Factor",
        "Mileage Correction Factor",
        "First Sales Year",
    }

    parts: list[pd.DataFrame] = []
    for scenario_ in [current_accounts_label, scenario]:
        import_slice = import_df[
            (import_df["Scenario"] == scenario_) & (import_df["Region"] == region)
        ].copy()
        export_slice = export_df[
            (export_df["Scenario"] == scenario_) & (export_df["Region"] == region)
        ].copy()

        comparison_df = import_slice.merge(
            export_slice,
            how="outer",
            on=key_cols,
            suffixes=("_import", "_export"),
            indicator=True,
        )
        comparison_df = comparison_df[
            ~comparison_df["Variable"].isin(unneeded_vars)
        ].copy()
        comparison_df = comparison_df[
            ~(
                (comparison_df["Variable"] == "Stock")
                & (comparison_df["Scenario"] != current_accounts_label)
                & (comparison_df["_merge"] == "left_only")
            )
        ].copy()

        missing = comparison_df[comparison_df["_merge"] != "both"]
        if not missing.empty:
            msg = (
                f"[WARN] {len(missing)} row(s) differ between import template and "
                f"export dataframe for scenario '{scenario_}'."
            )
            if STRICT_CHECKS:
                sample = missing[key_cols + ["_merge"]].head(20)
                raise ValueError(msg + "\n" + sample.to_string(index=False))
            print(msg)
            print(missing[key_cols + ["_merge"]].head(20))
        comparison_df = comparison_df[comparison_df["_merge"] == "both"].copy()

        for col in ["Scale", "Units", "Per..."]:
            import_col = f"{col}_import"
            export_col = f"{col}_export"
            if import_col in comparison_df.columns and export_col in comparison_df.columns:
                diff_mask = (
                    comparison_df[import_col].fillna("NA")
                    != comparison_df[export_col].fillna("NA")
                )
                if diff_mask.any():
                    msg = (
                        f"[WARN] Differences found between import and export "
                        f"dataframes in column {col} for scenario '{scenario_}'."
                    )
                    if STRICT_CHECKS:
                        sample = comparison_df.loc[diff_mask, key_cols].head(20)
                        raise ValueError(msg + "\n" + sample.to_string(index=False))
                    print(msg)

        out = pd.DataFrame(index=comparison_df.index)
        for col in id_cols + key_cols:
            out[col] = comparison_df[col]
        for col in ["Scale", "Units", "Per...", "Expression"]:
            out[col] = _choose_export_column(comparison_df, col)

        level_cols = sorted(
            {
                logical_col
                for col in comparison_df.columns
                if (logical_col := _logical_level_column_name(col)) is not None
            },
            key=lambda c: int(c.replace("Level ", "")),
        )
        year_cols = [
            col
            for col in comparison_df.columns
            if len(str(col)) == 4 and str(col).isdigit()
        ]
        for col in year_cols + level_cols:
            out[col] = _choose_export_column(comparison_df, col)

        base_cols = [
            "BranchID",
            "VariableID",
            "ScenarioID",
            "RegionID",
            "Branch Path",
            "Variable",
            "Scenario",
            "Region",
            "Scale",
            "Units",
            "Per...",
            "Expression",
        ]
        remaining_cols = [col for col in out.columns if col not in base_cols]
        parts.append(out[base_cols + remaining_cols].copy())

    new_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    id_lookup = new_df[key_cols + id_cols].drop_duplicates()
    export_df_for_viewing = export_df_for_viewing.merge(
        id_lookup,
        how="left",
        on=key_cols,
    )

    for col in id_cols:
        if col in new_df.columns:
            new_df[col] = new_df[col].astype("Int64")
        if col in export_df_for_viewing.columns:
            export_df_for_viewing[col] = export_df_for_viewing[col].astype("Int64")
        if STRICT_CHECKS and col in export_df_for_viewing.columns:
            missing_ids = export_df_for_viewing[col].isna()
            if missing_ids.any():
                sample = export_df_for_viewing.loc[missing_ids, key_cols].head(20)
                raise ValueError(
                    f"Some rows in export_df_for_viewing have NA values in {col}.\n"
                    + sample.to_string(index=False)
                )

    front = ["RegionID", "BranchID", "VariableID", "ScenarioID"]
    export_df_for_viewing = export_df_for_viewing[
        front + [col for col in export_df_for_viewing.columns if col not in front]
    ]

    return new_df, export_df_for_viewing


def separate_current_accounts_from_scenario(
    export_df, base_year, scenario, current_accounts_label="Current Accounts"
):
    """Clone generated export data to populate Current Accounts base-year rows."""

    export_df["Scenario"] = scenario
    ca_export_df = export_df.copy()
    ca_export_df["Scenario"] = current_accounts_label
    ca_export_df = ca_export_df[ca_export_df.Date == base_year]

    vars_to_only_keep_in_ca = ["Stock Share", "Stock"]
    export_df = export_df[~export_df["Measure"].isin(vars_to_only_keep_in_ca)]

    return pd.concat([export_df, ca_export_df], ignore_index=True)


def build_branch_path(branch_tuple: Tuple[str, ...], root: str = "Demand") -> str:
    """Convert a tuple of branch labels into a LEAP-style branch path string."""

    segments = [root, *branch_tuple]
    cleaned_segments = [segment for segment in segments if segment]
    return "\\".join(cleaned_segments)


DEFAULT_STRATEGIES: Dict[str, Sequence[str]] = {
    "Intensity": ["Activity Level", "Final Energy Intensity"],
    "Stock": ["Stock", "Mileage", "Fuel Economy"],
}


def build_branch_rules_from_mapping(
    esto_to_leap_mapping: Mapping[Tuple[str, ...], Sequence[Tuple[str, ...]]],
    unmappable_branches: Sequence[Tuple[str, ...]],
    all_leap_branches: Sequence[Tuple[str, ...]],
    analysis_type_lookup: Callable[[Tuple[str, ...]], str],
    root: str = "Demand",
) -> Dict[Tuple[str, ...], List[Dict[str, object]]]:
    """Construct branch rules from an ESTO-to-LEAP mapping."""

    rules: Dict[Tuple[str, ...], List[Dict[str, object]]] = {}
    for esto_key, leap_branches in esto_to_leap_mapping.items():
        rules[esto_key] = [
            {
                "branch_tuple": branch,
                "calculation_strategy": analysis_type_lookup(branch),
                "root": root,
                "input_variables_override": None,
            }
            for branch in leap_branches
        ]
    return rules


def _default_input_series_provider(
    export_df: pd.DataFrame,
    base_year: int | str,
    branch_path: str,
    input_variables: Sequence[str],
) -> List[pd.Series]:
    """Return input variable series for a Branch Path/Variable mask."""

    if base_year not in export_df.columns:
        raise KeyError(f"Base year column '{base_year}' not found in export data.")

    series_list: List[pd.Series] = []
    for variable in input_variables:
        mask = (export_df["Branch Path"] == branch_path) & (
            export_df["Variable"] == variable
        )
        series = export_df.loc[mask, base_year]
        if series.empty:
            raise ValueError(
                f"No values found for variable '{variable}' on branch "
                f"'{branch_path}' in {base_year}."
            )
        series_list.append(series)
    return series_list


def calculate_branch_energy(
    export_df: pd.DataFrame,
    base_year: int | str,
    rule: Mapping[str, object],
    strategies: Mapping[str, Sequence[str]],
    combination_fn: Optional[Callable[[List[pd.Series]], pd.Series]] = None,
    energy_fn: Optional[
        Callable[
            [
                pd.DataFrame,
                int | str,
                Mapping[str, object],
                Mapping[str, Sequence[str]],
                Optional[Callable],
            ],
            float,
        ]
    ] = None,
    input_series_provider: Optional[
        Callable[[pd.DataFrame, int | str, str, Sequence[str]], List[pd.Series]]
    ] = None,
) -> float:
    """Calculate energy for a single branch rule."""

    if energy_fn:
        return float(energy_fn(export_df, base_year, rule, strategies, combination_fn))

    input_vars = rule.get("input_variables_override") or strategies[
        rule["calculation_strategy"]
    ]
    branch_path = build_branch_path(rule["branch_tuple"], root=rule.get("root", "Demand"))
    provider = input_series_provider or _default_input_series_provider
    series_list = provider(export_df, base_year, branch_path, input_vars)

    if not series_list:
        return 0.0

    if combination_fn:
        combined = combination_fn(series_list)
    else:
        combined = series_list[0]
        for additional in series_list[1:]:
            combined = combined * additional

    return float(combined.sum())


def _compute_scale_factor(leap_total: float, esto_total: float) -> float:
    if abs(leap_total) <= 1e-12:
        if abs(esto_total) <= 1e-12:
            return 1.0
        return float("inf")
    return esto_total / leap_total


def get_adjustment_year_columns(
    export_df: pd.DataFrame,
    base_year: int | str,
    include_future_years: bool = False,
    apply_adjustments_to_past_years: bool = False,
) -> List[int | str]:
    """Return the year columns that should be adjusted."""

    if not include_future_years:
        return [base_year]

    years: List[int | str] = []
    year_columns = [
        col for col in export_df.columns if len(str(col)) == 4 and str(col).isdigit()
    ]
    for col in sorted(year_columns, key=lambda c: int(str(c))):
        if not apply_adjustments_to_past_years and int(col) < int(base_year):
            continue
        years.append(col)

    return years


def _apply_proportional_adjustment(
    export_df: pd.DataFrame,
    base_year: int | str,
    rule: Mapping[str, object],
    scale_factor: float,
    strategies: Mapping[str, Sequence[str]],
    year_columns: Optional[Sequence[int | str]] = None,
) -> None:
    """Generic fallback adjustment: multiply each input variable by scale_factor."""

    years_to_adjust = list(year_columns or [base_year])
    input_vars = rule.get("input_variables_override") or strategies[
        rule["calculation_strategy"]
    ]
    branch_path = build_branch_path(rule["branch_tuple"], root=rule.get("root", "Demand"))
    for variable in input_vars:
        mask = (export_df["Branch Path"] == branch_path) & (
            export_df["Variable"] == variable
        )
        if not mask.any():
            continue
        for year_col in years_to_adjust:
            if year_col not in export_df.columns:
                continue
            export_df.loc[mask, year_col] = export_df.loc[mask, year_col] * scale_factor


def reconcile_energy_use(
    export_df: pd.DataFrame,
    base_year: int | str,
    branch_mapping_rules: Mapping[Tuple[str, ...], Sequence[Mapping[str, object]]],
    esto_energy_totals: Mapping[Tuple[str, ...], float],
    strategies: Optional[Mapping[str, Sequence[str]]] = None,
    tolerance: float = 1e-6,
    adjustment_fn: Optional[
        Callable[
            [
                pd.DataFrame,
                int | str,
                Mapping[str, object],
                float,
                Mapping[str, Sequence[str]],
                Optional[Sequence[int | str]],
            ],
            None,
        ]
    ] = None,
    combination_fn: Optional[Callable[[List[pd.Series]], pd.Series]] = None,
    energy_fn: Optional[
        Callable[
            [
                pd.DataFrame,
                int | str,
                Mapping[str, object],
                Mapping[str, Sequence[str]],
                Optional[Callable],
            ],
            float,
        ]
    ] = None,
    input_series_provider: Optional[
        Callable[[pd.DataFrame, int | str, str, Sequence[str]], List[pd.Series]]
    ] = None,
    apply_adjustments_to_future_years: bool = False,
    apply_adjustments_to_past_years: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compare modelled totals with ESTO totals and scale inputs when needed."""

    working_df = export_df.copy()
    strategy_lookup = {**DEFAULT_STRATEGIES, **(strategies or {})}
    if energy_fn and not adjustment_fn:
        raise ValueError(
            "Provide a custom adjustment_fn when using a custom energy_fn so "
            "scale factors are applied correctly."
        )
    adjust = adjustment_fn or _apply_proportional_adjustment
    adjustment_year_columns = get_adjustment_year_columns(
        working_df,
        base_year,
        include_future_years=apply_adjustments_to_future_years,
        apply_adjustments_to_past_years=apply_adjustments_to_past_years,
    )

    results = []
    for esto_key, rules in branch_mapping_rules.items():
        leap_total = 0.0
        adjusted_paths: List[str] = []

        for rule in rules:
            try:
                energy = calculate_branch_energy(
                    working_df,
                    base_year,
                    rule,
                    strategy_lookup,
                    combination_fn,
                    energy_fn=energy_fn,
                    input_series_provider=input_series_provider,
                )
            except Exception as exc:
                branch_path = build_branch_path(
                    rule["branch_tuple"], root=rule.get("root", "Demand")
                )
                raise RuntimeError(
                    f"Failed to calculate LEAP energy for ESTO key {esto_key} "
                    f"at branch {branch_path}."
                ) from exc
            leap_total += energy

        esto_total = float(esto_energy_totals.get(esto_key, 0.0))
        scale_factor = _compute_scale_factor(leap_total, esto_total)
        finite_scale_factor = math.isfinite(scale_factor)

        if (
            abs(leap_total - esto_total) > tolerance
            and scale_factor != 1.0
            and finite_scale_factor
        ):
            for rule in rules:
                try:
                    adjust(
                        working_df,
                        base_year,
                        rule,
                        scale_factor,
                        strategy_lookup,
                        adjustment_year_columns,
                    )
                    adjusted_paths.append(
                        build_branch_path(
                            rule["branch_tuple"], root=rule.get("root", "Demand")
                        )
                    )
                except Exception as exc:
                    branch_path = build_branch_path(
                        rule["branch_tuple"], root=rule.get("root", "Demand")
                    )
                    raise RuntimeError(
                        "Failed to apply reconciliation adjustment for ESTO key "
                        f"{esto_key} at branch {branch_path} using scale factor "
                        f"{scale_factor}."
                    ) from exc

        results.append(
            {
                "ESTO Key": " | ".join(esto_key),
                "LEAP Energy Use": leap_total,
                "ESTO Energy Use": esto_total,
                "Scale Factor": scale_factor,
                "Adjusted Branches": ", ".join(adjusted_paths),
            }
        )

    summary_df = pd.DataFrame(results)
    return working_df, summary_df


def _build_change_table_for_years(
    original_df: pd.DataFrame,
    adjusted_df: pd.DataFrame,
    years: Sequence[int | str],
    tol: float = 1e-9,
) -> pd.DataFrame:
    """Build a long-form table of value changes for the provided years."""

    if not years:
        return pd.DataFrame(
            columns=[
                "Branch Path",
                "Variable",
                "Year",
                "Original",
                "Adjusted",
                "Abs Change",
                "Pct Change",
            ]
        )

    meta_cols = [col for col in ("Scenario", "Economy") if col in original_df.columns]
    frames: List[pd.DataFrame] = []

    for year_col in years:
        if year_col not in original_df.columns or year_col not in adjusted_df.columns:
            continue

        orig_vals = pd.to_numeric(original_df[year_col], errors="coerce")
        adj_vals = pd.to_numeric(adjusted_df[year_col], errors="coerce")
        diff = adj_vals - orig_vals

        mask = ~((orig_vals.isna()) & (adj_vals.isna())) & diff.abs().gt(tol)
        if not mask.any():
            continue

        pct_change = diff / orig_vals.replace(0, pd.NA)

        frame = pd.DataFrame(
            {
                "Branch Path": original_df.loc[mask, "Branch Path"],
                "Variable": original_df.loc[mask, "Variable"],
                "Year": year_col,
                "Original": orig_vals.loc[mask],
                "Adjusted": adj_vals.loc[mask],
                "Abs Change": diff.loc[mask],
                "Pct Change": pct_change.loc[mask],
            }
        )

        for meta_col in meta_cols:
            frame[meta_col] = original_df.loc[mask, meta_col]

        frames.append(frame)

    if not frames:
        return pd.DataFrame(
            columns=[
                "Branch Path",
                "Variable",
                "Year",
                "Original",
                "Adjusted",
                "Abs Change",
                "Pct Change",
                *meta_cols,
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(
        by="Abs Change", key=lambda s: s.abs(), ascending=False
    )
    return combined.reset_index(drop=True)


def build_adjustment_change_tables(
    original_df: pd.DataFrame,
    adjusted_df: pd.DataFrame,
    base_year: int | str,
    include_future_years: bool = False,
    tol: float = 1e-9,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Create tables describing LEAP input changes during reconciliation."""

    base_changes = _build_change_table_for_years(
        original_df, adjusted_df, [base_year], tol=tol
    )

    future_changes = None
    if include_future_years:
        future_years = [
            year
            for year in get_adjustment_year_columns(
                adjusted_df, base_year, include_future_years=True
            )
            if year != base_year
        ]
        future_changes = _build_change_table_for_years(
            original_df, adjusted_df, future_years, tol=tol
        )

    return base_changes, future_changes


def build_esto_totals_from_dataframe(
    esto_df: pd.DataFrame,
    key_columns: Sequence[str],
    value_column: str,
) -> Dict[Tuple[str, ...], float]:
    """Convert an ESTO dataframe into a tuple-keyed total mapping."""

    return {
        tuple(row[col] for col in key_columns): float(row[value_column])
        for _, row in esto_df.iterrows()
    }
