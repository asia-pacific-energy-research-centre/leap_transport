# ============================================================
# LEAP_transfers_transport_core.py
# ============================================================
# Core helper functions for LEAP transport data integration.
# Provides connection, diagnostics, normalization, logging,
# and activity level utilities shared by loader scripts.
# ============================================================

import pandas as pd
from win32com.client import Dispatch, GetActiveObject, gencache

# ------------------------------------------------------------
# Connection & Core Helpers
# ------------------------------------------------------------
def connect_to_leap():
    """Safely connect to an open LEAP instance."""
    gencache.EnsureDispatch("LEAP.LEAPApplication")
    try:
        return GetActiveObject("LEAP.LEAPApplication")
    except Exception:
        return Dispatch("LEAP.LEAPApplication")


def build_expr(points, expression_type="Interp"):
    """Build a LEAP-compatible Interp() expression."""
    if not points:
        return None
    df = pd.DataFrame(points, columns=["year", "value"]).dropna(subset=["year", "value"])
    if df["year"].duplicated().any():
        breakpoint()
    df = df.sort_values("year")
    pts = list(zip(df["year"].astype(int), df["value"].astype(float)))
    if len(pts) == 1:
        return str(pts[0][1])
    return f"{expression_type}(" + ", ".join(f"{y}, {v:.6g}" for y, v in pts) + ")"


def safe_set_variable(obj, varname, expr, context=""):
    """Safely assign expressions to LEAP variables with logging."""
    try:
        var = obj.Variable(varname)
        if var is None:
            print(f"[WARN] Missing variable '{varname}' on {context} within LEAP.")
            return False
        prev_expr = var.Expression
        if prev_expr and prev_expr.strip():
            print(f"[INFO] Clearing previous expression for '{varname}' on {context}")
            var.Expression = ""
            try:
                obj.Application.RefreshBranches()
            except Exception:
                pass
        var.Expression = expr
        short_expr = expr[:80] + ("..." if len(expr) > 80 else "")
        print(f"[SET] {context} → {varname} = {short_expr}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed setting {varname} on {context}: {e}")
        return False


# ------------------------------------------------------------
# Diagnostics and Data Analysis
# ------------------------------------------------------------
def diagnose_leap_branch(L, branch_path, leap_tuple, expected_vars=None, verbose=False):
    """Diagnose what variables are available in a LEAP branch."""
    try:
        branch = L.Branch(branch_path)
        if verbose:
            print(f"\n=== Diagnosing Branch: {leap_tuple} ===")
        var_count = branch.Variables.Count
        available_vars = [branch.Variables.Item(i + 1).Name for i in range(var_count)]
        if expected_vars:
            missing = set(expected_vars) - set(available_vars)
            if missing:
                print(f"Missing expected variables from LEAP: {sorted(missing)}")
        if verbose:
            print(f"Available variables: {sorted(available_vars)}")
    except Exception as e:
        print(f"[ERROR] Could not access or enumerate branch {branch_path}: {e}")
    print("=" * 50)


def normalize_sales_shares(df):
    """Normalize vehicle sales shares within each group."""
    def scale_group(g):
        s = g["Vehicle_sales_share"].sum(skipna=True)
        if pd.isna(s) or s == 0:
            return g
        g["Vehicle_sales_share"] /= s
        return g
    return df.groupby(["Scenario", "Medium", "Vehicle Type", "Date"], group_keys=False).apply(scale_group)


def analyze_data_quality(df):
    """Analyze data quality issues in the transport dataset."""
    print("\n=== Data Quality Analysis ===")
    missing_data = df.isnull().sum()
    if missing_data.any():
        print("Missing data by column:")
        for col, count in missing_data[missing_data > 0].items():
            pct = (count / len(df)) * 100
            print(f"  {col}: {count} ({pct:.1f}%)")

    if 'Vehicle_sales_share' in df.columns:
        zero_shares = (df['Vehicle_sales_share'] == 0).sum()
        print(f"\nZero vehicle sales shares: {zero_shares} ({(zero_shares/len(df)*100):.1f}%)")
    print("=" * 40)


# ------------------------------------------------------------
# Activity Levels
# ------------------------------------------------------------
def ensure_activity_levels(L):
    """Ensure 'Activity Level' variables exist in all transport branches."""
    print("\n=== Checking and fixing Activity Levels ===")
    try:
        transport_branch = L.Branch("Demand")
        if not transport_branch.Variable("Activity Level").Expression:
            transport_branch.Variable("Activity Level").Expression = "100"
        for sub in ["Passenger", "Freight"]:
            try:
                b = L.Branch(f"Demand\\{sub}")
                if not b.Variable("Activity Level").Expression:
                    b.Variable("Activity Level").Expression = "50"
            except Exception:
                print(f"[WARN] Could not access Demand\\{sub}")
    except Exception as e:
        print(f"[ERROR] Activity Level setup failed: {e}")
    print("==============================================\n")


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
def create_leap_data_log():
    """Initialize DataFrame to log all data written to LEAP."""
    return pd.DataFrame(columns=[
        'Date', 'Transport_Type', 'Medium', 'Vehicle_Type', 'Technology', 'Fuel',
        'Measure', 'Value', 'Branch_Path', 'LEAP_Tuple', 'Source_Tuple'
    ])


def log_leap_data(log_df, leap_tuple, src_tuple, branch_path, measure, df_m):
    """Add processed measure data to the log DataFrame."""
    new_rows = []
    for _, row in df_m.iterrows():
        if pd.notna(row[measure]):
            new_rows.append({
                'Date': int(row["Date"]),
                'Transport_Type': leap_tuple[0] if len(leap_tuple) > 0 else pd.NA,
                'Medium': leap_tuple[1] if len(leap_tuple) > 1 else pd.NA,
                'Vehicle_Type': leap_tuple[2] if len(leap_tuple) > 2 else pd.NA,
                'Technology': leap_tuple[3] if len(leap_tuple) > 3 else pd.NA,
                'Fuel': leap_tuple[4] if len(leap_tuple) > 4 else pd.NA,
                'Measure': measure,
                'Value': float(row[measure]),
                'Branch_Path': branch_path,
                'LEAP_Tuple': str(leap_tuple),
                'Source_Tuple': str(src_tuple)
            })
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        log_df = pd.concat([log_df, new_df], ignore_index=True) if not log_df.empty else new_df.copy()
    return log_df


def save_leap_data_log(log_df, filename="leap_data_log.xlsx"):
    """Save the complete LEAP data log to Excel with summaries."""
    print(f"\n=== Saving LEAP Data Log to {filename} ===")
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        log_df.to_excel(writer, sheet_name='All_Data', index=False)
    print(f"✅ Saved {len(log_df)} data points to {filename}")
    print("=" * 50)


######################
# ------------------------------------------------------------
#VALIDATION
# ------------------------------------------------------------
# ============================================================
# LEAP_transfers_transport_validation.py
# ============================================================
# Validates and optionally corrects stock/sales shares consistency
# across all hierarchical levels of the transport dataset.
# ============================================================

import pandas as pd

def validate_shares(df, tolerance=0.01, auto_correct=False):
    """
    Validate that stocks and sales shares sum to ~1.0 at each hierarchy level.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe containing at least:
        ['Scenario', 'Transport Type', 'Medium', 'Vehicle Type', 'Date',
         'Vehicle_sales_share', 'Stock Share'] (if applicable)
    tolerance : float
        Allowed deviation from 1.0 before flagging (default 0.01 = ±1%)
    auto_correct : bool
        If True, renormalize groups that deviate within 5*tolerance.

    Returns
    -------
    df : pandas.DataFrame
        Possibly corrected DataFrame.
    report : pandas.DataFrame
        Report summarizing which groups failed validation.
    """

    print("\n=== Validating Transport Shares Consistency ===")
    issues = []

    def check_and_fix(group, column):
        """Helper to check one share column per group."""
        total = group[column].sum(skipna=True)
        deviation = abs(total - 1.0)
        status = "OK"
        if pd.isna(total) or len(group) == 0:
            status = "Empty"
        elif deviation > tolerance:
            status = "FAIL"
            if auto_correct and deviation < 5 * tolerance:
                group[column] /= total
                status = "Corrected"
        return total, deviation, status, group

    # Define combinations to check
    group_levels = [
        ["Scenario", "Transport Type", "Medium", "Vehicle Type", "Date"],
        ["Scenario", "Transport Type", "Medium", "Date"],
    ]

    # Check both shares if available
    for col in ["Vehicle_sales_share", "Stock Share"]:
        if col not in df.columns:
            continue
        for levels in group_levels:
            grouped = df.groupby(levels, group_keys=False)
            for key, g in grouped:
                total, dev, status, new_g = check_and_fix(g, col)
                issues.append({
                    "Share Type": col,
                    "Group": key,
                    "Total": total,
                    "Deviation": dev,
                    "Status": status,
                    "Group Size": len(g)
                })
                if auto_correct and status == "Corrected":
                    df.loc[g.index, col] = new_g[col].values

    report = pd.DataFrame(issues)
    fails = report[report["Status"].isin(["FAIL", "Corrected"])]

    print(f"Checked {len(report)} groups.")
    if len(fails) == 0:
        print("✅ All share groups are consistent.")
    else:
        print(f"⚠️  {len(fails)} groups deviated from 1.0 "
              f"({(len(fails)/len(report))*100:.1f}% of total).")
        print("Sample issues:")
        print(fails.head(10).to_string(index=False))

    print("=" * 60)
    return df, report

def build_expression_from_mapping(branch_tuple, df_m, measure):
    """
    Builds the correct LEAP expression for a branch based on LEAP_BRANCH_TO_EXPRESSION_MAP.
    
    Parameters:
    - branch_tuple: tuple key from LEAP_BRANCH_TO_EXPRESSION_MAP
    - df_m: DataFrame containing 'Date' and the measure column
    - measure: measure name string (e.g., 'Stock Share', 'Activity Level')

    Returns:
    - expr: string suitable for LEAP variable.Expression
    """
    from LEAP_transfers_transport_MAPPINGS import LEAP_BRANCH_TO_EXPRESSION_MAP, ALL_YEARS

    mapping = LEAP_BRANCH_TO_EXPRESSION_MAP.get(branch_tuple, ('Data', ALL_YEARS))
    mode, arg = mapping

    # Default: Data from all available years
    if mode == 'Data':
        pts = [
            (int(r["Date"]), float(r[measure]))
            for _, r in df_m.iterrows()
            if pd.notna(r[measure])
        ]
        return build_expr(pts, "Data") if pts else None

    # Interp between given years
    elif mode == 'Interp':
        start, end = arg[0], arg[-1]
        df_filtered = df_m[(df_m["Date"] >= start) & (df_m["Date"] <= end)]
        pts = [
            (int(r["Date"]), float(r[measure]))
            for _, r in df_filtered.iterrows()
            if pd.notna(r[measure])
        ]
        return build_expr(pts, "Interp") if pts else None

    # Flat value (constant for a single year)
    elif mode == 'Flat':
        year = arg[0]
        val = df_m.loc[df_m["Date"] == year, measure].mean()
        return str(float(val)) if pd.notna(val) else None

    # Custom function for special logic
    elif mode == 'Custom':
            #e.g. example from chatgpt:
            # def calc_phev_split(branch_tuple, df_m, measure):
            # """
            # Example custom handler: calculate PHEV electricity/gasoline split.
            # """
            # # Example: assume 60% electricity by 2060 increasing linearly from 20% in 2022
            # start_val, end_val = 0.2, 0.6
            # years = sorted(df_m["Date"].unique())
            # vals = [start_val + (end_val - start_val) * (y - years[0]) / (years[-1] - years[0]) for y in years]
            # pts = list(zip(years, vals))
            # return build_interp_expr(pts)
        func_name = arg
        try:
            func = globals().get(func_name)
            if callable(func):
                return func(branch_tuple, df_m, measure)
            else:
                print(f"[WARN] Custom function '{func_name}' not found for {branch_tuple}")
                return None
        except Exception as e:
            print(f"[ERROR] Custom expression failed for {branch_tuple}: {e}")
            return None

    # Default fallback
    else:
        print(f"[WARN] Unknown mode '{mode}' for {branch_tuple}. Using raw data.")
        pts = [
            (int(r["Date"]), float(r[measure]))
            for _, r in df_m.iterrows()
            if pd.notna(r[measure])
        ]
        return build_expr(pts, "Data") if pts else None
