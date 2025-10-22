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
from LEAP_transfers_transport_MAPPINGS import (LEAP_BRANCH_TO_SOURCE_MAP, SHORTNAME_TO_LEAP_BRANCHES, add_fuel_column)
from LEAP_tranposrt_measures_config import (
    calculate_sales,
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
    #if there are more than one value per year throw an error
    if df["year"].duplicated().any():
        breakpoint()
        # raise ValueError(f"[ERROR] Duplicate years found in points: {points}.")
    # df = df.groupby("year", as_index=False)["value"].last()  # or use .mean() if averaging makes more sense
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
                    # breakpoint()
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
        transport_branch = L.Branch("Demand")
        var = transport_branch.Variable("Activity Level")
        if not var.Expression or var.Expression.strip() in ["", "0"]:
            var.Expression = "100"
            print("[INFO] Set 'Demand' Activity Level = 100%")

        # Passenger & Freight
        for sub in ["Passenger", "Freight"]:
            try:
                b = L.Branch(f"Demand\\{sub}")
                v = b.Variable("Activity Level")
                if not v.Expression or v.Expression.strip() in ["", "0"]:
                    v.Expression = "50"  # default equal split
                    print(f"[INFO] Defaulted {sub} Activity Level = 50%")
            except Exception:
                print(f"[WARN] Could not access Demand\\{sub}")

        # Modes under Passenger/Freight (Air, Road, Rail, Shipping)
        for sub in ["Passenger", "Freight"]:
            for mode in ["Air", "Road", "Rail", "Shipping"]:
                try:
                    b = L.Branch(f"Demand\\{sub}\\{mode}")
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
# Logging Setup
# ------------------------------------------------------------

def create_leap_data_log():
    """Initialize DataFrame to log all data written to LEAP."""
    return pd.DataFrame(columns=[
        'Date', 'Transport_Type', 'Medium', 'Vehicle_Type', 'Technology', 'Fuel', 
        'Measure', 'Value', 'Branch_Path', 'LEAP_Tuple', 'Source_Tuple'
    ])

def log_leap_data(log_df, leap_tuple, src_tuple, branch_path, measure, df_m):
    """
    Add processed measure data to the log DataFrame.
    
    Parameters:
    - log_df: The cumulative log DataFrame
    - leap_tuple: LEAP branch tuple (e.g., ("Passenger", "Road", "LPVs"))  
    - src_tuple: Source data tuple (e.g., ("passenger", "road", ["LPVs"], "BEV"))
    - branch_path: Full LEAP branch path
    - measure: The measure name (e.g., "Stock", "Sales Share")
    - df_m: DataFrame with Date and measure columns
    
    Returns:
    - Updated log_df with new rows added
    """
    
    # Parse LEAP tuple components
    transport_type = leap_tuple[0] if len(leap_tuple) > 0 else pd.NA
    medium = leap_tuple[1] if len(leap_tuple) > 1 else pd.NA
    vehicle_type = leap_tuple[2] if len(leap_tuple) > 2 else pd.NA
    technology = leap_tuple[3] if len(leap_tuple) > 3 else pd.NA
    fuel = leap_tuple[4] if len(leap_tuple) > 4 else pd.NA
    
    # Create new rows for each date/value pair
    new_rows = []
    for _, row in df_m.iterrows():
        if pd.notna(row[measure]):
            try:
                new_rows.append({
                    'Date': int(row["Date"]),
                    'Transport_Type': transport_type,
                    'Medium': medium, 
                    'Vehicle_Type': vehicle_type,
                    'Technology': technology,
                    'Fuel': fuel,
                    'Measure': measure,
                    'Value': float(row[measure]),
                    'Branch_Path': branch_path,
                    'LEAP_Tuple': str(leap_tuple),
                    'Source_Tuple': str(src_tuple)
                })
            except Exception as e:
                breakpoint()
                new_rows.append({
                    'Date': int(row["Date"].values[0]),
                    'Transport_Type': transport_type,
                    'Medium': medium, 
                    'Vehicle_Type': vehicle_type,
                    'Technology': technology,
                    'Fuel': fuel,
                    'Measure': measure,
                    'Value': float(row[measure]),
                    'Branch_Path': branch_path,
                    'LEAP_Tuple': str(leap_tuple),
                    'Source_Tuple': str(src_tuple)
                })
    
    # Add new rows to log
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        # Ensure both DataFrames have the same columns before concatenation
        if log_df.empty:
            log_df = new_df.copy()
        else:
            log_df = pd.concat([log_df, new_df], ignore_index=True)
    
    return log_df

def save_leap_data_log(log_df, filename="leap_data_log.xlsx"):
    """
    Save the complete LEAP data log to Excel with multiple sheets.
    
    Parameters:
    - log_df: The complete log DataFrame
    - filename: Output filename
    """
    
    print(f"\n=== Saving LEAP Data Log to {filename} ===")
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Main data sheet
        log_df.to_excel(writer, sheet_name='All_Data', index=False)
        
        # Summary by measure
        summary_by_measure = log_df.groupby('Measure').agg({
            'Value': ['count', 'min', 'max', 'mean'],
            'Date': ['min', 'max']
        }).round(3)
        summary_by_measure.columns = ['Count', 'Min_Value', 'Max_Value', 'Mean_Value', 'First_Year', 'Last_Year']
        summary_by_measure.to_excel(writer, sheet_name='Summary_by_Measure')
        
        # Summary by transport hierarchy
        hierarchy_summary = log_df.groupby(['Transport_Type', 'Medium', 'Vehicle_Type']).agg({
            'Measure': 'nunique',
            'Date': 'nunique', 
            'Value': 'count'
        })
        hierarchy_summary.columns = ['Unique_Measures', 'Years_Covered', 'Total_Data_Points']
        hierarchy_summary.to_excel(writer, sheet_name='Summary_by_Hierarchy')
        
        # Data coverage matrix
        coverage = log_df.pivot_table(
            index=['Transport_Type', 'Medium', 'Vehicle_Type'], 
            columns='Measure',
            values='Value',
            aggfunc='count',
            fill_value=0
        )
        coverage.to_excel(writer, sheet_name='Data_Coverage_Matrix')
    
    print(f"✅ Saved {len(log_df)} data points to {filename}")
    print(f"   - {log_df['Measure'].nunique()} unique measures")
    print(f"   - {log_df['Date'].nunique()} years covered") 
    print(f"   - {log_df.groupby(['Transport_Type', 'Medium', 'Vehicle_Type']).ngroups} unique transport categories")
    print("=" * 50)
  
# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap_v3(excel_path, economy, validate=True, diagnose_method='first_of_each_length', base_year=2022, final_year=2060, save_log=True, log_filename="leap_data_log.xlsx",SET_VARS_IN_LEAP=True):
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
    df = add_fuel_column(df)
    #set all stocks and sales shares for non road mediums to 0. this just solves some complciatons..
    df.loc[df["Medium"] != "road", ["Stocks", 'Vehicle_sales_share']] = 0
    df = calculate_sales(df)
    #calcaulte sales since its so simple and useful:
    
    # Analyze data quality before processing
    analyze_data_quality(df)
    
    df = normalize_sales_shares(df)
    L = connect_to_leap()
    ensure_activity_levels(L)

    # Initialize data log
    leap_data_log = create_leap_data_log() if save_log else None
    
    total_written = 0
    total_skipped = 0
    missing_branches = 0
    missing_variables = 0
    first_branch_diagnosed = False
    first_of_each_length_diagnosed = set()
    for leap_tuple, src_tuple in LEAP_BRANCH_TO_SOURCE_MAP.items():
        # unpack source and leap mappings
        if len(src_tuple) == 5:
            ttype, medium, vtype, drive, fuel = src_tuple
            source_cols_for_grouping = ['Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel']
        elif len(src_tuple) == 4:
            ttype, medium, vtype, drive = src_tuple
            fuel = None
            source_cols_for_grouping = ['Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive']
        elif len(src_tuple) == 3:
            ttype, medium, vtype = src_tuple
            drive = None
            fuel = None
            source_cols_for_grouping = ['Date', 'Transport Type', 'Medium', 'Vehicle Type']
        elif len(src_tuple) == 2:
            ttype, medium = src_tuple
            vtype, drive, fuel = None, None, None
            source_cols_for_grouping = ['Date', 'Transport Type', 'Medium']
        else:
            continue
        # if len(leap_tuple) == 5:
        #     leap_ttype, leap_medium, leap_vtype, leap_drive, leap_fuel = leap_tuple
        if len(leap_tuple) == 4:
            leap_ttype, leap_medium, leap_vtype, leap_drive = leap_tuple
            leap_fuel = None
        elif len(leap_tuple) == 3:
            # if medium !='road'
            leap_ttype, leap_medium, leap_vtype = leap_tuple
            leap_drive, leap_fuel = None, None
        elif len(leap_tuple) == 2:
            leap_ttype, leap_medium = leap_tuple
            leap_vtype, leap_drive, leap_fuel = None, None, None
        else:
            continue
        #########TEMP
        # if medium == 'road':
        #     continue
        # elif medium == 'air' and drive == 'air_jet_fuel':
        #     breakpoint()  # how to handle energy intensity for air
        # elif leap_tuple == ("Freight", "Air", "Jet fuel"):
        #     breakpoint()  # how to handle energy intensity for air
        #########TEMP
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
        df_copy = df.copy()
        
        if df_copy.empty:
            print(f"[WARN] No data for mapping {leap_tuple} ← {src_tuple}")
            total_skipped += 1
            continue

        # determine LEAP branch structure
        try:
            if medium in ["air", "rail", "ship"]:
                analysis_type = "Energy Intensity"
            else:
                analysis_type = "Stock"
            branch_path = f"Demand\\{leap_ttype}" + (f"\\{leap_medium}" if leap_medium else "") + (f"\\{leap_vtype}" if leap_vtype else "") + (f"\\{leap_drive}" if leap_drive else "") + (f"\\{leap_fuel}" if leap_fuel else "")
        except Exception as e:
            raise ValueError(f"Error constructing branch path for {leap_tuple}: {e}")
            
        # connect to LEAP branch
        try:
            branch = L.Branch(branch_path)
        except Exception:
            print(f"[WARN] Missing LEAP branch: {branch_path}")
            missing_branches += 1
            continue

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
        
        processed_measures = process_measures_for_leap(df_copy, filtered_measure_config, shortname, source_cols_for_grouping, ttype, medium, vtype, drive, fuel)
        written_this_branch = 0
        # if medium != 'road':
        #     breakpoint()
        # if leap_tuple == ("Passenger", "Road"):
        #     breakpoint()#how do we handle stock/sales shars
        for measure, df_m in processed_measures.items():
            
            # Log data before writing to LEAP
            if save_log:
                leap_data_log = log_leap_data(leap_data_log, leap_tuple, src_tuple, branch_path, measure, df_m)
            # if measure == "Energy Intensity":
            #     breakpoint()  # we handle this later
            # if measure not in LEAP_MEASURE_CONFIG:
            #     continue
            #########TEMP
            # if medium == 'road':
            #     SET_VARS_IN_LEAP = False
            # else:
            #     SET_VARS_IN_LEAP = True
            #########TEMP
            pts = [
                (int(r["Date"]), float(r[measure]))
                for _, r in df_m.iterrows()
                if pd.notna(r[measure])
            ]
            expr = build_interp_expr(pts)
            if expr and SET_VARS_IN_LEAP:
                success = safe_set_variable(branch, measure, expr, branch_path)
                if success:
                    written_this_branch += 1
                else:
                    missing_variables += 1
            elif expr and not SET_VARS_IN_LEAP:
                print(f"[INFO] Prepared to set {measure} on {branch_path} but SET_VARS_IN_LEAP is False. Skipping actual set.")
                written_this_branch += 1  # Count as written for logging purposes
            else:
                print(f"[INFO] No valid data points for {measure} on {branch_path}. Skipping.")
                breakpoint()#dont know what this one means
        
        # if leap_tuple == ("Passenger", "Road") or leap_tuple == ("Freight", "Road", "Trucks", "ICE heavy", "Diesel") or leap_tuple == ("Passenger", "Road", "LPVs") or leap_tuple == ("Passenger", "Road", "LPVs", "BEV small"):
        #     breakpoint()#how do we handle stock/sales shares

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
    breakpoint()
    # Save comprehensive data log
    if save_log and leap_data_log is not None and not leap_data_log.empty:
        print(f"Saving LEAP data log to {log_filename}...")
        save_leap_data_log(leap_data_log, log_filename)

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
        final_year=2060,
        save_log=True,
        log_filename="../../results/BD_transport_leap_data_log.xlsx",
        SET_VARS_IN_LEAP=True
    )
    
#%%
