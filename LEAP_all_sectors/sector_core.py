"""Common LEAP integration helpers shared across sectors.

This module centralises the COM connection helpers and generic
expression builders that were previously embedded inside the buildings
loader. By keeping the functions here free of buildings specific
imports they can be reused by any sector implementation (buildings,
industry, etc.).
"""

from __future__ import annotations

from typing import Iterable, Mapping, Optional, Sequence, Tuple

import pandas as pd
from win32com.client import Dispatch, GetActiveObject, gencache


BranchTuple = Tuple[str, ...]
ExpressionMapping = Mapping[BranchTuple, Tuple[str, Sequence[int]]]


# ---------------------------------------------------------------------------
# Connection & COM helpers
# ---------------------------------------------------------------------------

def connect_to_leap():
    """Connect to an existing LEAP session or start a new one.

    The helper performs a couple of sanity checks so that downstream
    scripts can safely assume that ``Branch`` calls will not hang.  The
    return value is ``None`` when a COM connection cannot be
    established.
    """

    print("[INFO] Connecting to LEAP...")
    try:
        gencache.EnsureDispatch("LEAP.LEAPApplication")
        try:
            leap_app = GetActiveObject("LEAP.LEAPApplication")
            print("[SUCCESS] Connected to existing LEAP instance")
        except Exception:
            leap_app = Dispatch("LEAP.LEAPApplication")
            print("[SUCCESS] Created new LEAP instance")

        try:
            areas = leap_app.Areas
            if areas.Count == 0:
                print("[WARN] LEAP has no project loaded - Branch() calls will fail")
                print("[WARN] Please load a project in LEAP first")
            else:
                active_area = leap_app.ActiveArea
                print(
                    f"[INFO] LEAP ready - Active area: '{active_area}' with {areas.Count} area(s)"
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"[WARN] Cannot check LEAP project state: {exc}")
        return leap_app
    except Exception as exc:
        print(f"[ERROR] LEAP connection failed: {exc}")
        return None


def safe_branch_call(leap_obj, branch_path: str, timeout_msg: bool = True):
    """Safely look up a LEAP branch.

    This function is a defensive wrapper around ``LEAPApplication.Branch``
    which prevents the calling script from hanging when the branch does
    not exist.
    """

    if leap_obj is None:
        if timeout_msg:
            print(f"[ERROR] No LEAP connection for branch '{branch_path}'")
        return None

    try:
        branch = leap_obj.Branch(branch_path)
        if timeout_msg:
            print(f"[SUCCESS] Found branch: {branch_path}")
        return branch
    except Exception as exc:
        if timeout_msg:
            error_str = str(exc)
            if len(error_str) > 60:
                error_str = error_str[:60] + "..."
            print(f"[INFO] Branch '{branch_path}' not accessible: {error_str}")
        return None


# ---------------------------------------------------------------------------
# Expression helpers
# ---------------------------------------------------------------------------

def build_expr(points: Iterable[Tuple[int, float]], expression_type: str = "Interp") -> Optional[str]:
    """Build a LEAP compatible expression from a set of ``(year, value)`` pairs."""

    points = list(points)
    if not points:
        return None

    df = pd.DataFrame(points, columns=["year", "value"]).dropna(subset=["year", "value"])
    if df["year"].duplicated().any():
        raise ValueError("Year values must be unique for a LEAP expression")

    df = df.sort_values("year")
    pts = list(zip(df["year"].astype(int), df["value"].astype(float)))
    if len(pts) == 1:
        return str(pts[0][1])

    joined_points = ", ".join(f"{year}, {value:.6g}" for year, value in pts)
    return f"{expression_type}({joined_points})"


def build_expression_from_mapping(
    branch_tuple: BranchTuple,
    df_m: pd.DataFrame,
    measure: str,
    expression_mapping: ExpressionMapping,
    default_mode: Tuple[str, Sequence[int]] = ("Data", ()),
) -> Optional[str]:
    """Create the LEAP expression to assign for ``measure`` on ``branch_tuple``.

    ``expression_mapping`` mimics ``LEAP_BRANCH_TO_EXPRESSION_MAPPING`` but
    is supplied at runtime so that non-buildings sectors can plug in
    their own configuration.
    """

    mode, arg = expression_mapping.get(branch_tuple, default_mode)

    if mode == "Data":
        points = [
            (int(row["Date"]), float(row[measure]))
            for _, row in df_m.iterrows()
            if pd.notna(row.get(measure))
        ]
        return build_expr(points, "Data") if points else None

    if mode == "Interp":
        if not arg:
            raise ValueError(
                f"Interpolation requested for {branch_tuple} but no year window provided."
            )
        start, end = arg[0], arg[-1]
        df_filtered = df_m[(df_m["Date"] >= start) & (df_m["Date"] <= end)]
        points = [
            (int(row["Date"]), float(row[measure]))
            for _, row in df_filtered.iterrows()
            if pd.notna(row.get(measure))
        ]
        return build_expr(points, "Interp") if points else None

    if mode == "Flat":
        if not arg:
            raise ValueError(f"Flat expression requested for {branch_tuple} without target year")
        year = arg[0]
        value = df_m.loc[df_m["Date"] == year, measure].mean()
        return str(float(value)) if pd.notna(value) else None

    if mode == "Custom":
        func_name = arg[0] if arg else None
        if not func_name:
            print(f"[WARN] Custom mode for {branch_tuple} but no function name supplied")
            return None
        custom_func = globals().get(func_name)
        if custom_func is None:
            print(f"[WARN] Custom function '{func_name}' not found for {branch_tuple}")
            return None
        try:
            return custom_func(branch_tuple, df_m, measure)
        except Exception as exc:  # pragma: no cover - defensive path
            print(f"[ERROR] Custom expression failed for {branch_tuple}: {exc}")
            return None

    print(f"[WARN] Unknown expression mode '{mode}' for {branch_tuple}. Using raw data.")
    points = [
        (int(row["Date"]), float(row[measure]))
        for _, row in df_m.iterrows()
        if pd.notna(row.get(measure))
    ]
    return build_expr(points, "Data") if points else None


# ---------------------------------------------------------------------------
# Diagnostics & logging helpers
# ---------------------------------------------------------------------------

def safe_set_variable(obj, varname: str, expr: str, context: str = "") -> bool:
    """Assign ``expr`` to the LEAP variable ``varname`` if it exists."""

    try:
        variable = obj.Variable(varname)
        if variable is None:
            print(f"[WARN] Missing variable '{varname}' on {context} within LEAP.")
            return False

        previous = variable.Expression
        if previous and previous.strip():
            print(f"[INFO] Clearing previous expression for '{varname}' on {context}")
            variable.Expression = ""
            try:
                obj.Application.RefreshBranches()
            except Exception:  # pragma: no cover - best effort refresh
                pass

        variable.Expression = expr
        short_expr = expr[:80] + ("..." if len(expr) > 80 else "")
        print(f"[SET] {context} → {varname} = {short_expr}")
        return True
    except Exception as exc:
        print(f"[ERROR] Failed setting {varname} on {context}: {exc}")
        return False


def diagnose_leap_branch(
    leap_app,
    branch_path: str,
    leap_tuple: BranchTuple,
    expected_vars: Optional[Iterable[str]] = None,
    verbose: bool = False,
):
    """Print diagnostic information about a LEAP branch."""

    branch = safe_branch_call(leap_app, branch_path, timeout_msg=False)
    if branch is None:
        print(f"[ERROR] Could not access branch {branch_path}")
        print("=" * 50)
        return

    try:
        if verbose:
            print(f"\n=== Diagnosing Branch: {leap_tuple} ===")
        count = branch.Variables.Count
        available = [branch.Variables.Item(i + 1).Name for i in range(count)]
        if expected_vars:
            missing = set(expected_vars) - set(available)
            if missing:
                print(f"Missing expected variables from LEAP: {sorted(missing)}")
        if verbose:
            print(f"Available variables: {sorted(available)}")
    except Exception as exc:
        print(f"[ERROR] Could not enumerate variables in branch {branch_path}: {exc}")
    print("=" * 50)


__all__ = [
    "BranchTuple",
    "ExpressionMapping",
    "build_expr",
    "build_expression_from_mapping",
    "connect_to_leap",
    "diagnose_leap_branch",
    "safe_branch_call",
    "safe_set_variable",
]
