# ============================================================
# transport_leap_core.py
# ============================================================
# Core helper functions for LEAP transport data integration.
# Provides connection, diagnostics, normalization, logging,
# and activity level utilities shared by loader scripts.
# ============================================================

import pandas as pd
from win32com.client import Dispatch, GetActiveObject, gencache

from transport_branch_mappings import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    SHORTNAME_TO_LEAP_BRANCHES,
    LEAP_MEASURE_CONFIG,
)
from transport_measure_metadata import SHORTNAME_TO_ANALYSIS_TYPE
from transport_measure_catalog import get_leap_branch_to_analysis_type_mapping

from branch_expression_mapping import (
    LEAP_BRANCH_TO_EXPRESSION_MAPPING,
    ALL_YEARS,
)

# ------------------------------------------------------------
# Connection & Core Helpers
# ------------------------------------------------------------
def connect_to_leap():
    """Enhanced LEAP connection with project readiness checks."""
    print("[INFO] Connecting to LEAP...")
    
    try:
        # Clear win32com cache to fix corrupted type library
        import shutil
        import tempfile
        gen_py_path = gencache.GetGeneratePath()
        if gen_py_path:
            try:
                shutil.rmtree(gen_py_path)
                print("[INFO] Cleared win32com cache")
            except Exception as e:
                print(f"[WARN] Could not clear cache: {e}")
        
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

def safe_branch_call(leap_obj, branch_path, AUTO_SET_MISSING_BRANCHES=False):
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
        return None
    
    branches = leap_obj.Branches
    try:
        exists = branches.Exists(branch_path)
    except Exception as e:
        breakpoint()
        raise Exception(f"Branches.Exists failed for '{branch_path}': {e}")

    if not exists:
        if AUTO_SET_MISSING_BRANCHES:
            print(f"[INFO] AUTO_SET_MISSING_BRANCHES is set to true. The branch will be auto-created: {branch_path}")
            #set it 
        else:
            breakpoint()
            raise Exception(f"Branches.Exists returned false for '{branch_path}'. AUTO_SET_MISSING_BRANCHES is False so throwing an error.")
        return None

    branch = leap_obj.Branch(branch_path)
    return branch
    # except Exception as e:
    #     if timeout_msg:
    #         error_str = str(e)
    #         if len(error_str) > 60:
    #             error_str = error_str[:60] + "..."
    #         print(f"[INFO] Branch '{branch_path}' not accessible: {error_str}")
    #     return None


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


def define_value_based_on_src_tuple(meta_values, src_tuple):
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    for col in ['LEAP_units', 'LEAP_Scale', 'LEAP_Per']:
        val = meta_values.get(col)
        if val is not None and isinstance(val, str) and '$' in val:
            # extract the options. if there are multiple $'s throw an error, code is not designed for that
            parts = val.split('$')
            if len(parts) != 2:
                raise ValueError(f"Unexpected format for metadata value: {val}")
            #now we have special code based on what the pklaceholder is
            if val == 'Passenger-km$Tonne-km':
                if 'passenger' in ttype:
                    resolved_value = 'Passenger-km'
                elif 'freight' in ttype:
                    resolved_value = 'Tonne-km'
                else:
                    raise ValueError(f"Unexpected ttype for resolving Passenger-km$Tonne-km: {ttype}")
                meta_values[col] = resolved_value
            elif val == 'of Tonne-km$of Passenger-km':
                if 'passenger' in ttype:
                    resolved_value = 'of Passenger-km'
                elif 'freight' in ttype:
                    resolved_value = 'of Tonne-km'
                else:
                    raise ValueError(f"Unexpected ttype for resolving of Tonne-km$of Passenger-km: {ttype}")
                meta_values[col] = resolved_value
            else:
                raise ValueError(f"Unknown placeholder in metadata value: {val}")
    return meta_values
# ------------------------------------------------------------
# Activity Levels
# ------------------------------------------------------------
# def ensure_activity_levels(L, TRANSPORT_ROOT=r"Demand"):
#     """Ensure 'Activity Level' variables exist in all transport branches."""
#     print("\n=== Checking and fixing Activity Levels ===")
#     try:
#         transport_branch = safe_branch_call(L, TRANSPORT_ROOT, , AUTO_SET_MISSING_BRANCHES=AUTO_SET_MISSING_BRANCHES)
#         if transport_branch:
#             if not transport_branch.Variable("Activity Level").Expression:
#                 transport_branch.Variable("Activity Level").Expression = "100"
#             for sub in ["Passenger", "Freight"]:
#                 try:
#                     b = L.Branch(f"{TRANSPORT_ROOT}\\{sub}")
#                     if not b.Variable("Activity Level").Expression:
#                         b.Variable("Activity Level").Expression = "50"
#                 except Exception:
#                     print(f"[WARN] Could not access {TRANSPORT_ROOT}\\{sub}")
#         else:
#             print("[WARN] Could not access Demand branch - skipping Activity Level setup")
#     except Exception as e:
#         print(f"[ERROR] Activity Level setup failed: {e}")
#     print("==============================================\n")



# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
def create_leap_export_df():
    """Initialize DataFrame to log all data written to LEAP."""
    return pd.DataFrame(columns=[
        'Date', 'Transport_Type', 'Medium', 'Vehicle_Type', 'Technology', 'Fuel',
        'Measure', 'Value', 'Branch_Path', 'LEAP_Tuple', 'Source_Tuple'
    ])

def write_row_to_leap_export_df(export_df, leap_tuple, src_tuple, branch_path, measure, df_m):
    """Add processed measure data to the export DataFrame."""
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
        export_df = pd.concat([export_df, new_df], ignore_index=True) if not export_df.empty else new_df.copy()
    return export_df


def save_leap_export_df(export_df, filename="leap_export.xlsx"):#, log_tuple=None):
    """Save the complete LEAP data log to Excel with summaries."""
    print(f"\n=== Saving LEAP Data for exporting to LEAP to {filename} ===")
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        export_df.to_excel(writer, sheet_name='All_Data', index=False)
    print(f"✅ Saved {len(export_df)} data points to {filename}")
    print("=" * 50)


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
    entry = (measure,) + branch_tuple
    mapping = LEAP_BRANCH_TO_EXPRESSION_MAPPING.get(entry, ('Data', ALL_YEARS))
    mode, arg = mapping

    # Default: Data from all available years
    if mode == 'Data':
        pts = [
            (int(r["Date"]), float(r['Value']))
            for _, r in df_m.iterrows()
            if pd.notna(r['Value'])
        ]
        return build_expr(pts, "Data") if pts else None, 'Data'

    # Interp between given years
    elif mode == 'Interp':
        start, end = arg[0], arg[-1]
        df_filtered = df_m[(df_m["Date"] >= start) & (df_m["Date"] <= end)]
        pts = [
            (int(r["Date"]), float(r['Value']))
            for _, r in df_filtered.iterrows()
            if pd.notna(r['Value'])
        ]
        return build_expr(pts, "Interp") if pts else None, 'Interp'

    # Flat value (constant for a single year)
    elif mode == 'Flat':
        year = arg[0]
        val = df_m.loc[df_m["Date"] == year, measure].mean()
        return str(float(val)) if pd.notna(val) else None, 'Flat'

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
                return func(branch_tuple, df_m, measure), 'Custom'
            else:
                print(f"[WARN] Custom function '{func_name}' not found for {branch_tuple}")
                return None, None
        except Exception as e:
            print(f"[ERROR] Custom expression failed for {branch_tuple}: {e}")
            return None, None

    # Default fallback
    else:
        print(f"[WARN] Unknown mode '{mode}' for {branch_tuple}. Using raw data.")
        pts = [
            (int(r["Date"]), float(r['Value']))
            for _, r in df_m.iterrows()
            if pd.notna(r['Value'])
        ]
        return build_expr(pts, "Data") if pts else None, 'Data'

#%%

#################################################
# Auto-Creation of LEAP Branches
#################################################
# ------------------------------------------------------------
# Constants mapped to LEAP BranchType enumeration values
# According to LEAP TypeLib: 1 = DemandCategoryBranchType,
# 4 = DemandTechnologyBranchType, 36 = DemandFuelBranchType
BRANCH_DEMAND_CATEGORY = 1
BRANCH_DEMAND_TECHNOLOGY = 4
BRANCH_DEMAND_FUEL = 36


def _choose_branch_type_for_segment(current_path, segment_name, branch_tuple):
    """
    Decide what LEAP branch type to use when auto-creating a missing segment.

    Parameters
    ----------
    current_path : str
        Full path up to (but not including) this segment.
    segment_name : str
        The missing branch name we are about to create.
    branch_tuple : any
        One of the tuples stored in SHORTNAME_TO_LEAP_BRANCHES[key].
        We infer 'shortname' and branch type rules from this.
    """

    # First identify what type of branch_tuple we have by going through
    # all the keys in SHORTNAME_TO_LEAP_BRANCHES and seeing if the
    # branch_tuple matches any of the values.
    shortname = None
    for key, values in SHORTNAME_TO_LEAP_BRANCHES.items():
        if branch_tuple in values:
            shortname = key
            break
    
    if shortname is None:
        raise ValueError(f"Branch tuple {branch_tuple} not found in SHORTNAME_TO_LEAP_BRANCHES.")

    short_lower = shortname.lower()

    # ------------------------------------------------------------------
    # STOCK-BASED BRANCHES (contain '(road)' in the shortname)
    # ------------------------------------------------------------------
    # If shortname has (road) in it, it is a stock-based branch and we
    # cannot set its technology-based branches (DemandTechnologyBranchType=4)
    # within the LEAP API. However, we can set its fuel-based branches.
    #
    # So:
    #   - If shortname == 'Fuel (road)': set as DemandFuelBranchType (36)
    #   - Otherwise: raise, user must manually create that branch in LEAP
    # ------------------------------------------------------------------
    if "(road)" in short_lower:
        if shortname == "Fuel (road)":
            return BRANCH_DEMAND_FUEL
        else:
            raise RuntimeError(
                "Attempted to auto-create a stock-based ('(road)') branch that is "
                "not 'Fuel (road)'. LEAP requires these technology/category "
                "branches to be created manually in the UI.\n"
                f"  shortname: {shortname}\n"
                f"  path: {current_path}\\{segment_name}"
            )

    # ------------------------------------------------------------------
    # INTENSITY-BASED BRANCHES (no '(road)' in the shortname)
    # ------------------------------------------------------------------
    # If the shortname is not stock based, then it is intensity based and
    # we have to identify whether it is a technology branch.
    #
    # This is done by checking if the shortname is in:
    #   ['Others (level 2)', 'Fuel (non-road)']
    #
    # Since intensity-based branches don't have fuel branches at the end,
    # only technology branches, 'Fuel (non-road)' is treated as a *technology*.
    #
    # If so, we can set it as a DemandTechnologyBranchType (4).
    # Otherwise, we can set it as a DemandCategoryBranchType (1).
    # ------------------------------------------------------------------
    if shortname in ["Others (level 2)", "Fuel (non-road)"]:
        # Intensity-based technology branch
        return BRANCH_DEMAND_TECHNOLOGY

    # Fallback: generic intensity-based category
    return BRANCH_DEMAND_CATEGORY

def ensure_branch_exists(L, full_path, branch_tuple,AUTO_SET_MISSING_BRANCHES=True):
    """
    Ensures a LEAP branch exists at full_path, creating any missing segments
    using _choose_branch_type_for_segment() and LEAPApplication Add* methods.

    Parameters
    ----------
    L : LEAPApplication COM object
    full_path : str
        Example: "Demand\\Freight non road\\Air\\Aviation gasoline"
    branch_tuple : tuple
        One of the tuples stored in SHORTNAME_TO_LEAP_BRANCHES for this
        logical branch type. Used to infer whether this path is stock-based
        vs intensity-based, and whether a missing segment is a category
        vs technology.
    """
    parts = [p for p in full_path.split("\\") if p]
    parent_branch = None

    for i, part in enumerate(parts):
        current_path = "\\".join(parts[:i+1])
        # Try to get the branch via your safe helper
        br = safe_branch_call(L, current_path, AUTO_SET_MISSING_BRANCHES=AUTO_SET_MISSING_BRANCHES)
        if br is not None:
            parent_branch = br
            continue

        # Branch is missing: decide what type it should be
        parent_path = "\\".join(parts[:i]) if i > 0 else ""
        branch_type = _choose_branch_type_for_segment(
            current_path=parent_path,
            segment_name=part,
            branch_tuple=branch_tuple,
        )
        if AUTO_SET_MISSING_BRANCHES:
            # Create the new branch with LEAPApplication methods
            new_branch = _create_child_branch(L, parent_branch, part, branch_type)
        else:
            breakpoint()#not sure how this will behave
            new_branch = None
        parent_branch = new_branch

    return parent_branch

def _create_child_branch(L, parent_branch, name, branch_type):
    """
    Create a new LEAP branch under parent_branch, using LEAPApplication
    methods (AddCategory, AddTechnology, etc.).

    NOTE:
    - LEAP has no AddDemandFuel API. Demand fuel branches (type 36) are
      created implicitly when you create technologies with a fuel.
    """
    
    if parent_branch is None:
        breakpoint()
        raise RuntimeError(
            f"Cannot create top-level branch '{name}' without an existing parent. "
            "In practice, roots like 'Demand' must already exist."
        )

    # Get the parent ID from the branch
    parent_id = parent_branch.ID  # COM property: Branch.ID

    # Category: use AddCategory(parent_id, name, Scale, AcUnit)
    if branch_type == BRANCH_DEMAND_CATEGORY:
        # Use blank defaults for scale and activity unit; user can edit later.
        # AddCategory(ParentID, BName, Scale, AcUnit) :contentReference[oaicite:2]{index=2}
        return L.AddCategory(parent_id, name, "", "")

    # Technology (Activity method): use AddTechnology(...)
    if branch_type == BRANCH_DEMAND_TECHNOLOGY:
        # AddTechnology(ParentID, BName, Scale, AcUnit, Fuel, EnergyUnit) :contentReference[oaicite:3]{index=3}
        # We don't know the actual defaults from here, so use empty strings. The user will need to set them manually... they may also get set by the imported data.
        
        # and let the user fill in fuel & units in LEAP later.
        #AddTechnology(ParentID, BName, Scale, AcUnit, Fuel, EnergyUnit)
        print(f"Creating technology branch '{name}' under parent ID {parent_id}. Remember to set units manually in LEAP.")
        return L.AddTechnology(parent_id, name, "", "", name, "")

    # Demand fuel branches: LEAP exposes BranchType=36 but no AddDemandFuel.
    # These are normally created when you define a technology with an
    # associated fuel, not directly via API.
    if branch_type == BRANCH_DEMAND_FUEL:
        breakpoint()
        raise RuntimeError(
            f"Cannot auto-create demand fuel branch '{name}': LEAP API "
            "does not expose an AddDemandFuel method. Create the associated "
            "technology (with its fuel) in LEAP, or handle this branch manually."
        )

    raise RuntimeError(f"Unsupported branch_type={branch_type} for '{name}'.")


# ------------------------------------------------------------
def diagnose_measures_in_leap_branch(L, branch_path, leap_tuple, expected_vars=None, verbose=False):
    """Diagnose variables available in a LEAP branch."""
    branch = safe_branch_call(L, branch_path)
    if branch is None:
        print(f"[ERROR] Could not access branch {branch_path}")
        print("=" * 50)
        return

    try:
        var_count = branch.Variables.Count
        available_vars = [branch.Variables.Item(i + 1).Name for i in range(var_count)]

        if verbose:
            print(f"\n=== Diagnosing Branch: {leap_tuple} ===")
            print(f"Available variables: {sorted(available_vars)}")

        if expected_vars:
            missing = set(expected_vars) - set(available_vars)
            if missing:
                print(f"Missing expected variables: {sorted(missing)}")

    except Exception as e:
        print(f"[ERROR] Could not enumerate variables in '{branch_path}': {e}")

    print("=" * 50)
    return
