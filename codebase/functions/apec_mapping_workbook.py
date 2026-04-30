"""Workbook-backed helpers for transport ESTO and LEAP mapping audits."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from configurations.transport_economy_config import APEC_ESTO_BALANCES_PATH
from functions.path_utils import resolve_str

MAPPING_WORKBOOK_SHEETS: tuple[str, ...] = (
    "leap_combined_esto",
    "leap_combined_ninth",
    "sector_flow_final_proposed",
    "fuel_product_final_proposed",
    "sector_ninth_final_proposed",
    "fuel_ninth_final_proposed",
)


def _normalize_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[str, int] = {}
    for col in df.columns:
        if isinstance(col, str) and col.isdigit():
            rename_map[col] = int(col)
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "1", "y", "yes", "t", "on"}


def _normalize_code(value: object) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _normalize_label(value: object) -> str:
    return str(value or "").strip().lower()


def _active_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Drop rows flagged for removal from a mapping worksheet."""
    active = pd.Series(True, index=frame.index)
    if "remove_row" in frame.columns:
        active &= ~frame["remove_row"].map(_to_bool)
    if "duplicate_to_remove" in frame.columns:
        active &= ~frame["duplicate_to_remove"].map(_to_bool)
    return frame.loc[active].copy()


def _dedupe_preserve_order(values: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[tuple[str, ...]] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _workbook_path_to_branch_prefix(path_value: object) -> tuple[str, ...]:
    parts = [
        str(part).strip()
        for part in str(path_value or "").replace("\\", "/").split("/")
        if str(part).strip()
    ]
    if parts and parts[0] == "Transport non road":
        parts = parts[1:]
    return tuple(parts)


def _is_transport_esto_row(row: pd.Series) -> bool:
    flow = str(row.get("esto_flow", "")).strip()
    if flow.startswith("15"):
        return True
    branch_prefix = _workbook_path_to_branch_prefix(row.get("leap_sector_name_full_path", ""))
    return bool(branch_prefix) and branch_prefix[0] in {
        "Passenger road",
        "Freight road",
        "Passenger non road",
        "Freight non road",
        "International transport",
        "Pipeline transport",
        "Nonspecified transport",
    }


def _branches_for_workbook_row(
    row: pd.Series,
    all_leap_branches: list[tuple[str, ...]] | tuple[tuple[str, ...], ...],
) -> list[tuple[str, ...]]:
    prefix = _workbook_path_to_branch_prefix(row.get("leap_sector_name_full_path", ""))
    fuel = str(row.get("raw_leap_fuel_name", "")).strip()
    if not fuel:
        return []

    fuel_norm = _normalize_label(fuel)
    matches = [
        tuple(branch)
        for branch in all_leap_branches
        if len(branch) >= len(prefix) + 1
        and tuple(branch[: len(prefix)]) == prefix
        and _normalize_label(branch[-1]) == fuel_norm
    ]
    if matches:
        return _dedupe_preserve_order(matches)

    if prefix and _normalize_label(prefix[-1]) == fuel_norm and tuple(prefix) in all_leap_branches:
        return [tuple(prefix)]
    return []


def load_mapping_workbook(mapping_workbook_path: str | Path) -> dict[str, pd.DataFrame]:
    """Load the transport mapping workbook into a sheet-name keyed dict."""
    resolved = resolve_str(mapping_workbook_path)
    if resolved is None:
        raise ValueError("Mapping workbook path cannot be None.")

    workbook: dict[str, pd.DataFrame] = {}
    for sheet_name in MAPPING_WORKBOOK_SHEETS:
        frame = pd.read_excel(resolved, sheet_name=sheet_name)
        frame = frame.fillna("")
        if sheet_name in {"leap_combined_esto", "leap_combined_ninth"}:
            frame = _active_rows(frame)
        workbook[sheet_name] = frame
    return workbook


def load_apec_esto_balances(
    apec_esto_path: str | Path = APEC_ESTO_BALANCES_PATH,
    *,
    economy: str | None = None,
) -> pd.DataFrame:
    """Load the ESTO balance CSV and normalize columns used by the audit."""
    resolved = resolve_str(apec_esto_path)
    if resolved is None:
        raise ValueError("ESTO path cannot be None.")

    df = pd.read_csv(resolved)
    if "flows" not in df.columns or "products" not in df.columns:
        raise ValueError(
            "ESTO file must contain 'flows' and 'products' columns."
        )

    df = _normalize_year_columns(df)
    if "is_subtotal" not in df.columns:
        df["is_subtotal"] = False

    if "economy" in df.columns:
        df["economy"] = df["economy"].astype(str).str.strip()
        if economy is not None:
            economy_norm = _normalize_code(economy)
            if economy_norm and economy_norm != "00apec":
                df = df[df["economy"].map(_normalize_code) == economy_norm].copy()
                if df.empty:
                    raise ValueError(
                        f"No ESTO rows found for economy '{economy}' in '{resolved}'."
                    )
    df["esto_flow"] = df["flows"].astype(str).str.strip()
    df["esto_product"] = df["products"].astype(str).str.strip()
    df["is_subtotal"] = df["is_subtotal"].map(_to_bool)

    year_cols = [col for col in df.columns if isinstance(col, int)]
    for year_col in year_cols:
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

    return df


def _agg_balance_surface(esto_df: pd.DataFrame) -> pd.DataFrame:
    year_cols = [col for col in esto_df.columns if isinstance(col, int)]
    agg_map = {year_col: "sum" for year_col in year_cols}
    agg_map["is_subtotal"] = "max"

    grouped = (
        esto_df.groupby(["esto_flow", "esto_product"], as_index=False)
        .agg(agg_map)
        .reset_index(drop=True)
    )
    return grouped


def build_transport_mapping_audit(
    mapping_workbook_path: str | Path,
    esto_path: str | Path = APEC_ESTO_BALANCES_PATH,
    *,
    economy: str | None = None,
    include_ninth: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Return audit-ready mapping tables.

    The `leap_combined_esto` sheet is merged against the aggregated ESTO
    balance surface for the requested economy when provided.
    """
    workbook = load_mapping_workbook(mapping_workbook_path)
    esto_df = load_apec_esto_balances(esto_path, economy=economy)
    esto_agg = _agg_balance_surface(esto_df)

    esto_map = workbook["leap_combined_esto"].copy()

    esto_audit = esto_map.merge(
        esto_agg,
        on=["esto_flow", "esto_product"],
        how="left",
        suffixes=("", "_esto"),
    )

    tables = {
        "leap_combined_esto": esto_audit,
        "apec_esto_surface": esto_agg,
    }
    if include_ninth:
        tables["leap_combined_ninth"] = workbook["leap_combined_ninth"].copy()
    return tables


def build_workbook_esto_to_leap_mapping(
    mapping_workbook_path: str | Path,
    all_leap_branches: list[tuple[str, ...]] | tuple[tuple[str, ...], ...],
) -> dict[tuple[str, str], list[tuple[str, ...]]]:
    """Build raw ESTO (flow, product) to detailed LEAP branch mapping from workbook rows."""
    workbook = load_mapping_workbook(mapping_workbook_path)
    esto_map = workbook["leap_combined_esto"].copy()
    transport_rows = esto_map[esto_map.apply(_is_transport_esto_row, axis=1)].copy()
    if "leap_is_subtotal" in transport_rows.columns:
        transport_rows = transport_rows[~transport_rows["leap_is_subtotal"].map(_to_bool)].copy()
    if "esto_pair_is_subtotal" in transport_rows.columns:
        transport_rows = transport_rows[~transport_rows["esto_pair_is_subtotal"].map(_to_bool)].copy()

    mapping: dict[tuple[str, str], list[tuple[str, ...]]] = {}
    unmatched_rows = []
    for _, row in transport_rows.iterrows():
        flow = str(row.get("esto_flow", "")).strip()
        product = str(row.get("esto_product", "")).strip()
        if not flow or not product:
            continue

        branches = _branches_for_workbook_row(row, all_leap_branches)
        if not branches:
            unmatched_rows.append(
                {
                    "leap_sector_name_full_path": row.get("leap_sector_name_full_path", ""),
                    "raw_leap_fuel_name": row.get("raw_leap_fuel_name", ""),
                    "esto_flow": flow,
                    "esto_product": product,
                }
            )
            continue

        key = (flow, product)
        mapping.setdefault(key, []).extend(branches)

    mapping = {
        key: _dedupe_preserve_order(branches)
        for key, branches in mapping.items()
        if branches
    }
    if not mapping:
        raise ValueError(
            "No workbook ESTO-to-LEAP transport mapping rows matched LEAP branches."
        )
    if unmatched_rows:
        preview = pd.DataFrame(unmatched_rows).head(10).to_string(index=False)
        print(
            "[WARN] Some workbook transport mapping rows did not match LEAP branches; "
            f"matched_keys={len(mapping)}, unmatched_rows={len(unmatched_rows)}.\n{preview}"
        )
    return mapping


def build_workbook_international_esto_to_leaf_mapping(
    mapping_workbook_path: str | Path,
) -> dict[tuple[str, str], list[tuple[str, str]]]:
    """Build raw ESTO bunker (flow, product) to international (medium, fuel) mapping."""
    workbook = load_mapping_workbook(mapping_workbook_path)
    esto_map = workbook["leap_combined_esto"].copy()
    transport_rows = esto_map[esto_map.apply(_is_transport_esto_row, axis=1)].copy()
    if "leap_is_subtotal" in transport_rows.columns:
        transport_rows = transport_rows[~transport_rows["leap_is_subtotal"].map(_to_bool)].copy()
    if "esto_pair_is_subtotal" in transport_rows.columns:
        transport_rows = transport_rows[~transport_rows["esto_pair_is_subtotal"].map(_to_bool)].copy()

    mapping: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for _, row in transport_rows.iterrows():
        prefix = _workbook_path_to_branch_prefix(row.get("leap_sector_name_full_path", ""))
        if len(prefix) < 2 or prefix[0] != "International transport":
            continue
        medium = str(prefix[1]).strip()
        fuel = str(row.get("raw_leap_fuel_name", "")).strip()
        flow = str(row.get("esto_flow", "")).strip()
        product = str(row.get("esto_product", "")).strip()
        if not medium or not fuel or not flow or not product:
            continue
        key = (flow, product)
        mapping.setdefault(key, []).append((medium, fuel))

    mapping = {
        key: _dedupe_preserve_order(leaves)
        for key, leaves in mapping.items()
        if leaves
    }
    if not mapping:
        raise ValueError(
            "No workbook ESTO-to-international transport rows were found."
        )
    return mapping


def build_workbook_international_esto_energy_totals(
    *,
    esto_path: str | Path,
    economy: str,
    base_year: int | str,
    mapping_keys: list[tuple[str, str]] | tuple[tuple[str, str], ...] | set[tuple[str, str]],
    absolute_values: bool = True,
) -> dict[tuple[str, str], float]:
    """Build raw ESTO bunker totals, defaulting to absolute demand-side values."""
    totals = build_workbook_esto_energy_totals(
        esto_path=esto_path,
        economy=economy,
        base_year=base_year,
        mapping_keys=mapping_keys,
    )
    if absolute_values:
        totals = {
            key: abs(value) if not pd.isna(value) else value
            for key, value in totals.items()
        }
    return totals


def build_workbook_esto_energy_totals(
    *,
    esto_path: str | Path,
    economy: str,
    base_year: int | str,
    mapping_keys: list[tuple[str, str]] | tuple[tuple[str, str], ...] | set[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    """Build raw ESTO (flow, product) totals from the flow/product ESTO CSV."""
    esto_df = load_apec_esto_balances(esto_path, economy=economy)
    year_col = int(base_year) if str(base_year).isdigit() else base_year
    if year_col not in esto_df.columns:
        raise ValueError(f"Base year '{base_year}' was not found in ESTO file '{esto_path}'.")

    agg = _agg_balance_surface(esto_df)
    totals_lookup = agg.set_index(["esto_flow", "esto_product"])[year_col]

    totals: dict[tuple[str, str], float] = {}
    missing_keys: list[tuple[str, str]] = []
    for key in sorted(set(mapping_keys)):
        if key not in totals_lookup.index:
            missing_keys.append(key)
            totals[key] = 0.0
            continue
        value = pd.to_numeric(pd.Series([totals_lookup.loc[key]]), errors="coerce").iloc[0]
        totals[key] = float(value) if not pd.isna(value) else float("nan")

    if missing_keys:
        preview = "\n".join(f"  - {flow} | {product}" for flow, product in missing_keys[:10])
        print(
            "[WARN] Workbook ESTO mapping references flow/product pairs missing from "
            "the ESTO file; treating them as zero totals.\n"
            f"Missing keys: {len(missing_keys)}\n{preview}"
        )
    return totals


def write_transport_mapping_audit(
    *,
    mapping_workbook_path: str | Path,
    esto_path: str | Path = APEC_ESTO_BALANCES_PATH,
    output_dir: str | Path,
    scenario: str,
    economy: str | None = None,
    include_ninth: bool = False,
) -> dict[str, Path]:
    """Write workbook-backed transport audit tables to CSV files."""
    audit_tables = build_transport_mapping_audit(
        mapping_workbook_path=mapping_workbook_path,
        esto_path=esto_path,
        economy=economy,
        include_ninth=include_ninth,
    )

    resolved_output_dir = Path(resolve_str(output_dir) or output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    output_paths: dict[str, Path] = {}
    for sheet_name, frame in audit_tables.items():
        economy_tag = str(economy).strip() if economy else "all"
        output_path = resolved_output_dir / f"transport_{sheet_name}_{economy_tag}_{scenario}.csv"
        frame.to_csv(output_path, index=False)
        output_paths[sheet_name] = output_path

    return output_paths


def build_apec_mapping_audit(
    mapping_workbook_path: str | Path,
    apec_esto_path: str | Path = APEC_ESTO_BALANCES_PATH,
    *,
    economy: str | None = None,
    include_ninth: bool = False,
) -> dict[str, pd.DataFrame]:
    """Backward-compatible wrapper for the transport mapping audit helper."""
    return build_transport_mapping_audit(
        mapping_workbook_path=mapping_workbook_path,
        esto_path=apec_esto_path,
        economy=economy,
        include_ninth=include_ninth,
    )


def write_apec_mapping_audit(
    *,
    mapping_workbook_path: str | Path,
    apec_esto_path: str | Path = APEC_ESTO_BALANCES_PATH,
    output_dir: str | Path,
    scenario: str,
    economy: str | None = None,
    include_ninth: bool = False,
) -> dict[str, Path]:
    """Backward-compatible wrapper for the transport mapping audit writer."""
    return write_transport_mapping_audit(
        mapping_workbook_path=mapping_workbook_path,
        esto_path=apec_esto_path,
        output_dir=output_dir,
        scenario=scenario,
        economy=economy,
        include_ninth=include_ninth,
    )
