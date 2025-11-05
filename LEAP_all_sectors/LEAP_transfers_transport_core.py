# ============================================================
# LEAP_transfers_transport_core.py
# ============================================================
# Core helper functions for LEAP transport data integration.
# Provides connection, diagnostics, normalization, logging,
# and activity level utilities shared by loader scripts.
# ============================================================

import pandas as pd
from win32com.client import Dispatch, GetActiveObject, gencache
from LEAP_transfers_transport_MAPPINGS import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG
)
from LEAP_tranposrt_measures_config import SHORTNAME_TO_ANALYSIS_TYPE, get_leap_branch_to_analysis_type_mapping

from LEAP_BRANCH_TO_EXPRESSION_MAPPING import LEAP_BRANCH_TO_EXPRESSION_MAPPING, ALL_YEARS

# ------------------------------------------------------------
# Connection & Core Helpers
# ------------------------------------------------------------
def connect_to_leap():
    """Enhanced LEAP connection with project readiness checks."""
    print("[INFO] Connecting to LEAP...")
    
    try:
        gencache.EnsureDispatch("LEAP.LEAPApplication")
        try:
            leap_app = GetActiveObject("LEAP.LEAPApplication")
            print("[SUCCESS] Connected to existing LEAP instance")
        except:
            leap_app = Dispatch("LEAP.LEAPApplication")
            print("[SUCCESS] Created new LEAP instance")
        
        # Check if LEAP is ready for Branch() calls
        try:
            areas = leap_app.Areas
            if areas.Count == 0:
                print("[WARN] LEAP has no project loaded - Branch() calls will fail")
                print("[WARN] Please load a project in LEAP first")
            else:
                active_area = leap_app.ActiveArea
                print(f"[INFO] LEAP ready - Active area: '{active_area}' with {areas.Count} area(s)")
        except Exception as e:
            print(f"[WARN] Cannot check LEAP project state: {e}")
        
        return leap_app
        
    except Exception as e:
        print(f"[ERROR] LEAP connection failed: {e}")
        return None

def safe_branch_call(leap_obj, branch_path, timeout_msg=True):
    """
    Safe Branch() call that won't hang - use this instead of L.Branch() directly.
    
    Args:
        leap_obj: LEAP application object
        branch_path: string path to branch (e.g., "Demand", "Key\\Population")
        timeout_msg: whether to print timeout messages
        
    Returns:
        branch object if successful, None if failed
        
    Usage:
        L = connect_to_leap()
        branch = safe_branch_call(L, "Demand")
        if branch:
            variables = branch.Variables
        else:
            print("Branch not found")
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
    except Exception as e:
        if timeout_msg:
            error_str = str(e)
            if len(error_str) > 60:
                error_str = error_str[:60] + "..."
            print(f"[INFO] Branch '{branch_path}' not accessible: {error_str}")
        return None


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
    # Use safe_branch_call instead of direct L.Branch()
    branch = safe_branch_call(L, branch_path, timeout_msg=False)
    if branch is None:
        print(f"[ERROR] Could not access branch {branch_path}")
        print("=" * 50)
        return
        
    try:
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
        print(f"[ERROR] Could not enumerate variables in branch {branch_path}: {e}")
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
    return df


# ------------------------------------------------------------
# Activity Levels
# ------------------------------------------------------------
def ensure_activity_levels(L, TRANSPORT_ROOT=r"Demand\Transport"):
    """Ensure 'Activity Level' variables exist in all transport branches."""
    print("\n=== Checking and fixing Activity Levels ===")
    try:
        breakpoint()
        transport_branch = safe_branch_call(L, TRANSPORT_ROOT, timeout_msg=False)
        if transport_branch:
            if not transport_branch.Variable("Activity Level").Expression:
                transport_branch.Variable("Activity Level").Expression = "100"
            for sub in ["Passenger", "Freight"]:
                try:
                    b = L.Branch(f"{TRANSPORT_ROOT}\\{sub}")
                    if not b.Variable("Activity Level").Expression:
                        b.Variable("Activity Level").Expression = "50"
                except Exception:
                    print(f"[WARN] Could not access {TRANSPORT_ROOT}\\{sub}")
        else:
            print("[WARN] Could not access Demand branch - skipping Activity Level setup")
    except Exception as e:
        print(f"[ERROR] Activity Level setup failed: {e}")
    print("==============================================\n")



# def ensure_activity_levels(L):
#     """Ensure 'Activity Level' variables exist in all transport branches."""
#     print("\n=== Checking and fixing Activity Levels ===")
#     if L is None or 'None' in str(type(L)):
#         print("[ERROR] No LEAP connection provided")
#         return
    
#     try:
#         # Use safe_branch_call instead of direct L.Branch()
#         transport_branch = safe_branch_call(L, "Demand", timeout_msg=False)
#         if transport_branch:
#             if not transport_branch.Variable("Activity Level").Expression:
#                 transport_branch.Variable("Activity Level").Expression = "100"
#                 print("[INFO] Set Activity Level = '100' for Demand branch")
#         else:
#             print("[WARN] Could not access Demand branch - skipping Activity Level setup")
            
#         for sub in ["Passenger", "Freight"]:
#             sub_branch = safe_branch_call(L, f"Demand\\{sub}", timeout_msg=False)
#             if sub_branch:
#                 if not sub_branch.Variable("Activity Level").Expression:
#                     sub_branch.Variable("Activity Level").Expression = "50"
#                     print(f"[INFO] Set Activity Level = '50' for Demand\\{sub}")
#             else:
#                 print(f"[WARN] Could not access Demand\\{sub}")
                
#     except Exception as e:
#         breakpoint()
#         print(f"[ERROR] Activity Level setup failed: {e}")
#     print("==============================================\n")

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

def validate_shares(df, tolerance=0.01, auto_correct=False, road_only=False):
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

    if road_only:
        df_non_road = df[df["Medium"] != "road"].copy()
        df = df[df["Medium"] == "road"].copy()
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
        # ["Scenario", "Transport Type", "Medium", "Date"],#i think we just want the most detailed levelsand we calcualte the upper levels from these later in calculate_measures()
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

    #exclude base year values if it is on Vehicle_sales_share
    fails = fails[~(fails["Share Type"].eq("Vehicle_sales_share") & fails["Group"].apply(lambda x: x[-1] == 2022))]

    print(f"Checked {len(report)} groups.")
    if len(fails) == 0:
        print("✅ All share groups are consistent.")
    else:
        print(f"⚠️  {len(fails)} groups deviated from 1.0 "
              f"({(len(fails)/len(report))*100:.1f}% of total).")
        print("Sample issues:")
        print(fails.head(10).to_string(index=False))

    print("=" * 60)
    if road_only:
        df = pd.concat([df, df_non_road], ignore_index=True)
    return df, report

def build_expression_from_mapping(branch_tuple, df_m, measure):
    """
    Builds the correct LEAP expression for a branch based on LEAP_BRANCH_TO_EXPRESSION_MAPPING.
    
    Parameters:
    - branch_tuple: tuple key from LEAP_BRANCH_TO_EXPRESSION_MAPPING
    - df_m: DataFrame containing 'Date' and the measure column
    - measure: measure name string (e.g., 'Stock Share', 'Activity Level')

    Returns:
    - expr: string suitable for LEAP variable.Expression
    """

    mapping = LEAP_BRANCH_TO_EXPRESSION_MAPPING.get(branch_tuple, ('Data', ALL_YEARS))
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

#%%
#########################################################
# ============================================================
#OTHER
#
# ============================================================

def extract_esto_sector_fuels_for_leap_branches(leap_branch_list):
    """
    Does a backwards search on the ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP to find what keys have the leap branches in their values. this is mostly useful for the Others Levels 1 and 2 mappings where we have a many-to-one mapping.
    """
    leap_branch_to_esto_sector_fuel = {}
    for leap_branch in leap_branch_list:
        leap_branch_to_esto_sector_fuel[leap_branch] = []
        for esto_sector_fuel, leap_branches2 in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
            if leap_branch in leap_branches2:
                leap_branch_to_esto_sector_fuel[leap_branch].append(esto_sector_fuel)
    return leap_branch_to_esto_sector_fuel

def extract_other_type_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx'):
    """Extract 'Other' shortname rows from ESTO and insert them into the transport dataframe."""
    
    #and insert the 'Other' shortname rows. These are those under the Other level 1 and level 2 in SHORTNAME_TO_LEAP_BRANCHES  and are basically rows that arent in this transport dataset because they were modelled separately. However to make it easy to use the same code to load them into LEAP we create rows for them here with activity levels equal to their enertgy use in the ESTO dataset and intensity=1. They will then have energy use = activity level * intensity = activity level = esto energy use. We can access their ESTO energy use from the ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP using extract_esto_sector_fuels_for_leap_branches(leap_branch_list) where leap_branch_list is the list of leap branches for the 'Other' shortnames
    
    #load esto dataset 
    esto_energy_use = pd.read_excel(TRANSPORT_ESTO_BALANCES_PATH)     
    other_shortnames = [sn for sn in SHORTNAME_TO_LEAP_BRANCHES.keys() if sn.startswith('Other')]
    other_leap_branches = []
    #extract the leap branches for these 'other' shortnames
    for sn in other_shortnames:
        other_leap_branches.extend(SHORTNAME_TO_LEAP_BRANCHES[sn])
    if len(other_leap_branches) > 0:
        other_rows = extract_esto_energy_use_for_leap_branches(other_leap_branches, esto_energy_use, economy, base_year, final_year)
        other_rows_df = pd.concat(other_rows, ignore_index=True)
        df = pd.concat([df, other_rows_df], ignore_index=True)
    return df

def extract_esto_energy_use_for_leap_branches(leap_branches, esto_energy_use,economy, base_year=2022, final_year=2060):
    #todo make sure this works with the validation def and this. 
    esto_sector_fuels_for_other = extract_esto_sector_fuels_for_leap_branches(leap_branches)
    other_rows = []
    for leap_branch, esto_rows in esto_sector_fuels_for_other.items():
        esto_rows_df_base_year = pd.DataFrame()#we will sum up all rows for this leap branch and insert their base year energy use into the transport df as activity level and energy use, with intensity =1
        esto_rows_df = pd.DataFrame()#we will sum up all rows for this leap branch and insert their values for all projected_years energy use into the transport df as activity level and energy use, with intensity =1
        if esto_rows == []:
            #this occurs if the leap branch is not mapped to any esto sector fuel. We should check if its in our list of leap branches we can skip, otehrwise warn
            LEAP_BRANCHES_TO_SKIP_IF_NO_ESTO_MAPPING = [
                ('Nonspecified transport',),
                ('Pipeline transport',) 
            ]
            if leap_branch not in LEAP_BRANCHES_TO_SKIP_IF_NO_ESTO_MAPPING:
                raise ValueError(f"Leap branch {leap_branch} has no ESTO mapping but is not in the skip list. If it is not feasible to create an esto mapping for this branch, please add it to the skip list, otherwise make sure it is mapped in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.")
            continue
        for (subsector, fuel, subfuel) in esto_rows:
            #create new rows for df using the ESTO data, filtered for the (subsector, fuel, subfuel) values, eg. ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline").
            # breakpoint()#check this works ok. worried that having only one col will cause issues
            esto_row_base_year = esto_energy_use[
                (esto_energy_use['sub1sectors'] == subsector) &
                (esto_energy_use['sub2sectors'] == 'x') &
                (esto_energy_use['fuels'] == fuel) &
                (esto_energy_use['subfuels'] == subfuel)
            ][base_year]
            esto_rows_df_base_year = pd.concat([esto_rows_df_base_year, esto_row_base_year], ignore_index=True)
            
            projected_years = [year for year in esto_energy_use.columns if isinstance(year, int) and year > base_year and year <= final_year]
            esto_row_projected_years = esto_energy_use[
                (esto_energy_use['sub1sectors'] == subsector) &
                (esto_energy_use['sub2sectors'] == 'x') &
                (esto_energy_use['fuels'] == fuel) &
                (esto_energy_use['subfuels'] == subfuel)
            ][projected_years]
            esto_rows_df = pd.concat([esto_rows_df, esto_row_projected_years], ignore_index=True)
        total_activity_level_base_year = esto_rows_df_base_year.sum().values[0]
        total_activity_levels_projected_years = esto_rows_df.sum().values
    
        #create new row in df with this activity level and intensity =1
        df_new_rows = {
            'Economy': economy,
            'Scenario': 'Reference',
            'Date': [base_year] + projected_years,
            'Transport Type': leap_branch[0],
            'Medium': leap_branch[1] if len(leap_branch) > 1 else None,#todo is it ok if we make these None? even if it doesnt amtch the way it is in ther rest of the df?
            'Vehicle Type': leap_branch[2] if len(leap_branch) > 2 else None,
            'Drive': leap_branch[3] if len(leap_branch) > 3 else None,
            'Fuel': leap_branch[4] if len(leap_branch) > 4 else None,
            'Activity': [total_activity_level_base_year] + list(total_activity_levels_projected_years),
            'Intensity': 1,
            'Energy' : [total_activity_level_base_year] + list(total_activity_levels_projected_years)
        }
        other_rows.append(pd.DataFrame(df_new_rows))
    return other_rows
#%%