#%%
# ============================================================
# LEAP_transfers_transport.py
# ============================================================
# Transport loader for LEAP using dynamic mappings & measure config
# ------------------------------------------------------------
#   - Automatically scales source measures (from 9th edition dataset)
#   - Filters source data via CSV_TREE structure
#   - Handles Activity Level setup and missing branches
#   - Writes all measures to LEAP with progress logging
# ============================================================

import pandas as pd
from win32com.client import Dispatch, GetActiveObject, gencache
from LEAP_transfers_transport_MAPPINGS import (LEAP_BRANCH_TO_SOURCE_MAP, SHORTNAME_TO_LEAP_BRANCHES)
from LEAP_tranposrt_measures_config import (
    filter_source_dataframe,
    process_measures_for_leap,
    LEAP_MEASURE_CONFIG,
    list_all_measures,
    # LEAP_MEASURES_DETAILS, --- IGNORE ---
)

# ------------------------------------------------------------
# Basic helpers
# ------------------------------------------------------------
def connect_to_leap():
    """Safely connect to an open LEAP instance."""
    gencache.EnsureDispatch("LEAP.LEAPApplication")
    try:
        return GetActiveObject("LEAP.LEAPApplication")
    except Exception:
        return Dispatch("LEAP.LEAPApplication")

def build_interp_expr(points):
    """
    Builds a clean LEAP Interp() expression from (year, value) points.
    Handles duplicate years by taking the last non-null value.
    """
    if not points:
        return None

    # Convert to DataFrame to easily handle duplicates
    df = pd.DataFrame(points, columns=["year", "value"])
    df = df.dropna(subset=["year", "value"])
    df = df.groupby("year", as_index=False)["value"].last()  # or use .mean() if averaging makes more sense
    df = df.sort_values("year")

    pts = list(zip(df["year"].astype(int), df["value"].astype(float)))

    if len(pts) == 1:
        return str(pts[0][1])

    expr = "Interp(" + ", ".join(f"{y}, {v:.6g}" for y, v in pts) + ")"
    return expr
# def build_interp_expr(points):
#     """Build LEAP-compatible Interp() expressions from (year, value) pairs."""
#     pts = [(int(y), float(v)) for (y, v) in points if pd.notna(y) and pd.notna(v)]
#     if not pts:
#         return None
#     pts.sort(key=lambda x: x[0])
#     if len(pts) == 1:
#         return str(pts[0][1])
#     return "Interp(" + ", ".join(f"{y}, {v}" for y, v in pts) + ")"

def safe_set_variable(obj, varname, expr, context=""):
    """
    Safely assign expressions to LEAP variables.
    - Clears any pre-existing expressions to avoid appending duplicate Interp() content.
    - Returns True if set successfully, False otherwise.
    - Logs clear warnings/errors for missing or failed assignments.
    """
    try:
        var = obj.Variable(varname)
        if var is None:
            print(f"[WARN] Missing variable '{varname}' on {context} within LEAP.")
            return False

        # --- Clear any previous content to avoid concatenation issues ---
        prev_expr = var.Expression
        if prev_expr and prev_expr.strip():
            print(f"[INFO] Clearing previous expression for '{varname}' on {context}")
            var.Expression = ""
            try:
                obj.Application.RefreshBranches()
            except Exception:
                # Not all LEAP versions expose Application under obj
                pass

        # --- Now safely assign new expression ---
        var.Expression = expr

        # Optional logging (only first 80 chars for readability)
        short_expr = expr[:80] + ("..." if len(expr) > 80 else "")
        print(f"[SET] {context} → {varname} = {short_expr}")

        return True

    except Exception as e:
        print(f"[ERROR] Failed setting {varname} on {context}: {e}")
        return False

def diagnose_leap_branch(L, branch_path,leap_tuple,expected_vars=None, verbose=False):
    """Diagnose what variables are available in a LEAP branch."""
    try:
        branch = L.Branch(branch_path)
        if verbose:
            print(f"\n=== Diagnosing Branch: {leap_tuple} ===")
        
        # Try to get all variables (this might vary by LEAP version)
        try:
            var_count = branch.Variables.Count
            if verbose:
                print(f"Total variables in branch: {var_count}")
            
            available_vars = []
            for i in range(var_count):
                var = branch.Variables.Item(i+1)
                available_vars.append(var.Name)
            if verbose:
                print(f"Available variables: {sorted(available_vars)}")
            if expected_vars:
                missing = set(expected_vars) - set(available_vars)
                if missing:
                    if not verbose:
                        print(f"\n=== Diagnosing Branch: {leap_tuple} ===")
                        print(f"Total variables in branch: {var_count}")
                        print(f"Available variables: {sorted(available_vars)}")
                    print(f"Missing expected variables from LEAP: {sorted(missing)}")
                    breakpoint()
            # breakpoint()
        except Exception as e:
            print(f"Could not enumerate variables: {e}")
            
    except Exception as e:
        print(f"Could not access branch {branch_path}: {e}")
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
    
    # Check for missing data
    missing_data = df.isnull().sum()
    if missing_data.any():
        print("Missing data by column:")
        for col, count in missing_data[missing_data > 0].items():
            pct = (count / len(df)) * 100
            print(f"  {col}: {count} ({pct:.1f}%)")
    
    # Check vehicle sales share issues
    if 'Vehicle_sales_share' in df.columns:
        zero_shares = (df['Vehicle_sales_share'] == 0).sum()
        print(f"\nZero vehicle sales shares: {zero_shares} ({(zero_shares/len(df)*100):.1f}%)")
        
        # Group by scenario/transport/medium/vehicle/date and check sums
        grouped = df.groupby(['Scenario', 'Transport Type', 'Medium', 'Vehicle Type', 'Date'])['Vehicle_sales_share'].sum()
        problematic = grouped[(grouped < 0.95) | (grouped > 1.05)]
        print(f"Groups with sales shares not summing to ~1.0: {len(problematic)}")
        
        if len(problematic) > 0:
            print("Sample problematic groups:")
            print(problematic.head(10))
    
    # Check data coverage by transport type/medium
    if all(col in df.columns for col in ['Transport Type', 'Medium', 'Vehicle Type']):
        coverage = df.groupby(['Transport Type', 'Medium', 'Vehicle Type']).size()
        print(f"\nData coverage (records per transport/medium/vehicle combination):")
        print(coverage.describe())
    
    print("=" * 40)


# ------------------------------------------------------------
# Activity Level Auto-Fix
# ------------------------------------------------------------
def ensure_activity_levels(L):
    """
    Ensures top-level and mid-level 'Activity Level' variables exist and sum properly.
    Adds defaults for Transport, Passenger, Freight, and their mode branches.
    """
    print("\n=== Checking and fixing Activity Levels ===")
    try:
        # Top-level Transport
        transport_branch = L.Branch("Demand\\Transport")
        var = transport_branch.Variable("Activity Level")
        if not var.Expression or var.Expression.strip() in ["", "0"]:
            var.Expression = "100"
            print("[INFO] Set 'Demand\\Transport' Activity Level = 100%")

        # Passenger & Freight
        for sub in ["Passenger", "Freight"]:
            try:
                b = L.Branch(f"Demand\\Transport\\{sub}")
                v = b.Variable("Activity Level")
                if not v.Expression or v.Expression.strip() in ["", "0"]:
                    v.Expression = "50"  # default equal split
                    print(f"[INFO] Defaulted {sub} Activity Level = 50%")
            except Exception:
                print(f"[WARN] Could not access Demand\\Transport\\{sub}")

        # Modes under Passenger/Freight (Air, Road, Rail, Shipping)
        for sub in ["Passenger", "Freight"]:
            for mode in ["Air", "Road", "Rail", "Shipping"]:
                try:
                    b = L.Branch(f"Demand\\Transport\\{sub}\\{mode}")
                    v = b.Variable("Activity Level")
                    if not v.Expression or v.Expression.strip() in ["", "0"]:
                        v.Expression = "25"  # balanced default
                        print(f"[INFO] Defaulted {sub}\\{mode} Activity Level = 25%")
                except Exception:
                    continue
    except Exception as e:
        print(f"[ERROR] Activity Level setup failed: {e}")
    print("==============================================\n")
    
# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap_v3(excel_path, economy, validate=True, diagnose_method='first_of_each_length', base_year=2022, final_year=2060):
    """Main data import function.
    diagnose_method can be:
        'first_branch_diagnosed' - diagnose only the first branch processed
        'first_of_each_length' - diagnose the first branch of each tuple length (2,3,4,5)
        'all' - diagnose every branch processed (very verbose)"""
    print(f"\n=== Loading Transport Data for {economy} ===")
    df = pd.read_excel(excel_path)
    df = df[df["Economy"] == economy]
    #filter for years
    df = df[(df["Date"] >= base_year) & (df["Date"] <= final_year)]
    
    # Analyze data quality before processing
    analyze_data_quality(df)
    
    df = normalize_sales_shares(df)
    L = connect_to_leap()
    ensure_activity_levels(L)

    total_written = 0
    total_skipped = 0
    missing_branches = 0
    missing_variables = 0
    first_branch_diagnosed = False
    first_of_each_length_diagnosed = set()
    for leap_tuple, src_tuple in LEAP_BRANCH_TO_SOURCE_MAP.items():
        # unpack source and leap mappings
        if len(src_tuple) == 4:
            ttype, medium, vtype, drive = src_tuple
        elif len(src_tuple) == 3:
            ttype, medium, vtype = src_tuple
            drive = None
        elif len(src_tuple) == 2:
            ttype, medium = src_tuple
            vtype, drive = None,None
        else:
            continue
        if len(leap_tuple) == 5:
            leap_ttype, leap_medium, leap_vtype, leap_drive, leap_fuel = leap_tuple
        elif len(leap_tuple) == 4:
            leap_ttype, leap_medium, leap_vtype, leap_drive = leap_tuple
            leap_fuel = None
        elif len(leap_tuple) == 3:
            leap_ttype, leap_medium, leap_vtype = leap_tuple
            leap_drive, leap_fuel = None, None
        elif len(leap_tuple) == 2:
            leap_ttype, leap_medium = leap_tuple
            leap_vtype, leap_drive, leap_fuel = None, None, None
        else:
            continue
        
        #check if any are lists
        if isinstance(ttype, list):
            raise ValueError(f"Transport Type cannot be a list in mapping: {src_tuple}")#we would need to change get_source_categories to handle lists in this case
        if isinstance(medium, list):
            raise ValueError(f"Medium cannot be a list in mapping: {src_tuple}")#we would need to change get_source_categories to handle lists in this case
        if isinstance(vtype, list):
            #this is ok.
            pass
        if isinstance(drive, list):
            raise ValueError(f"Drive cannot be a list in mapping: {src_tuple}")#we would need to change get_source_categories to handle lists in this case

        # subset source data
        df_sub = filter_source_dataframe(df, ttype, medium, vtype, drive)
        if df_sub.empty:
            print(f"[WARN] No data for mapping {leap_tuple} ← {src_tuple}")
            total_skipped += 1
            continue

        # determine LEAP branch structure
        try:
            if medium in ["air", "rail", "ship"]:
                analysis_type = "Energy Intensity"
            else:
                analysis_type = "Stock"
            branch_path = f"Demand\\Transport\\{leap_ttype}" + (f"\\{leap_medium}" if leap_medium else "") + (f"\\{leap_vtype}" if leap_vtype else "") + (f"\\{leap_drive}" if leap_drive else "") + (f"\\{leap_fuel}" if leap_fuel else "")
        except Exception as e:
            raise ValueError(f"Error constructing branch path for {leap_tuple}: {e}")
            

        # connect to LEAP branch
        try:
            branch = L.Branch(branch_path)
        except Exception:
            print(f"[WARN] Missing LEAP branch: {branch_path}")
            missing_branches += 1
            continue

        print(f"[INFO] Writing {analysis_type} type measures to: {branch_path}")

        #identify the shortname for this tuple in SHORTNAME_TO_LEAP_BRANCHES:
        expected_shortname = set()
        for shortname, branches in SHORTNAME_TO_LEAP_BRANCHES.items():
            if leap_tuple in branches:
                expected_shortname.add(shortname)
        #if len expected vars>1 its a bit weird
        if len(expected_shortname) != 1:
            breakpoint()
            raise ValueError(f"Multiple or 0 expected measures found for branch {branch_path}: {expected_measures}")
        shortname = expected_shortname.pop()

        # Filter LEAP_MEASURE_CONFIG to only those expected for this branch
        filtered_measure_config = LEAP_MEASURE_CONFIG[shortname]
        expected_measures = set(filtered_measure_config.keys())
        
        # Diagnose the first branch, or each branch of each length or every branch for debugging purposes
        if diagnose_method == 'first_branch_diagnosed' and not first_branch_diagnosed:
            # breakpoint()
            diagnose_leap_branch(L, branch_path,leap_tuple, expected_measures)
            first_branch_diagnosed = True
        elif diagnose_method == 'first_of_each_length' and len(leap_tuple) not in first_of_each_length_diagnosed:
            diagnose_leap_branch(L, branch_path,leap_tuple, expected_measures)
            first_of_each_length_diagnosed.add(len(leap_tuple))
        elif diagnose_method == 'all':
            diagnose_leap_branch(L, branch_path,leap_tuple, expected_measures)
        elif diagnose_method not in ['first_branch_diagnosed', 'first_of_each_length', 'all']:
            print(f"[WARN] Unknown diagnose_method '{diagnose_method}'. Skipping diagnosis.")

        # process measures
        processed_measures = process_measures_for_leap(df_sub, filtered_measure_config, shortname)
        written_this_branch = 0

        for measure, df_m in processed_measures.items():
            # if measure not in LEAP_MEASURE_CONFIG:
            #     continue
            pts = [
                (int(r["Date"]), float(r[measure]))
                for _, r in df_m.iterrows()
                if pd.notna(r[measure])
            ]
            expr = build_interp_expr(pts)
            if expr:
                success = safe_set_variable(branch, measure, expr, branch_path)
                if success:
                    written_this_branch += 1
                else:
                    missing_variables += 1

        if written_this_branch == 0:
            print(f"[INFO] No valid measures for {branch_path}")
            total_skipped += 1
        else:
            total_written += written_this_branch

    # Summary log
    print("\n=== Summary ===")
    print(f"✅ Variables written: {total_written}")
    print(f"⚠️  Skipped (no data or invalid tuples): {total_skipped}")
    print(f"❌ Missing LEAP branches: {missing_branches}")
    print(f"❌ Missing variables: {missing_variables}")
    print("================\n")

    # Validate shares
    if validate:
        print("\n=== Validating Vehicle Sales Shares ===")
        #set nas to 0
        df["Vehicle_sales_share"] = df["Vehicle_sales_share"].fillna(0)
        grouped = (
            df.groupby(["Scenario", "Transport Type", "Medium", "Vehicle Type", "Date"])["Vehicle_sales_share"].sum()
        )
        bad = grouped[(grouped < 0.99) | (grouped > 1.01)]
        if bad.empty:
            print("All groups sum to ~1.0 ✅")
        else:
            breakpoint()#whats the issue here? i think we migth get one because it now adds to 100?
            print("Groups deviating from 1.0:")
            print(bad)
    # breakpoint()#Error: Activity shares under branch "Transport" sum to 200.0%.
    # When using shares, the sum of all immediately neighboring branches must be 100%.
    # If neighboring branches need not sum to 100%, consider using percent saturations.
    L.RefreshBranches()
    L.ActiveView = "Results"
    print("\n=== Transport data successfully filled into LEAP. ===\n")

#%%
# ------------------------------------------------------------
# Optional: review measures & run loader
# ------------------------------------------------------------
if __name__ == "__main__":
    pd.options.display.float_format = "{:,.3f}".format
    list_all_measures()
    load_transport_into_leap_v3(
        excel_path=r"../../data/bd dummy transport file - 2100.xlsx",
        economy="02_BD",
        validate=True,
        diagnose_method='all',  # Options: 'first_branch_diagnosed', 'first_of_each_length', 'all'
        base_year=2022,
        final_year=2024
    )
#%%
