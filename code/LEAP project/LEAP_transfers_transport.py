#%%
# ============================================================
# leap_loader_v3.py (with Activity Level Auto-Fix)
# ============================================================

import pandas as pd
from win32com.client import Dispatch, GetActiveObject, gencache
from LEAP_tranposrt_measures_config import get_measures_for_analysis
from LEAP_transfers_transport_MAPPINGS import LEAP_TO_SOURCE_MAP


# ------------------------------------------------------------
# Basic helpers
# ------------------------------------------------------------
def connect_to_leap():
    gencache.EnsureDispatch("LEAP.LEAPApplication")
    try:
        return GetActiveObject("LEAP.LEAPApplication")
    except Exception:
        return Dispatch("LEAP.LEAPApplication")


def build_interp_expr(points):
    pts = [(int(y), float(v)) for (y, v) in points if pd.notna(y) and pd.notna(v)]
    if not pts:
        return None
    pts.sort(key=lambda x: x[0])
    if len(pts) == 1:
        return str(pts[0][1])
    return "Interp(" + ", ".join(f"{y}, {v}" for y, v in pts) + ")"


def safe_set_variable(obj, varname, expr, context=""):
    try:
        var = obj.Variable(varname)
        if var is None:
            print(f"[WARN] Missing variable '{varname}' on {context}")
            return
        var.Expression = expr
    except Exception as e:
        print(f"[ERROR] Failed setting {varname} on {context}: {e}")


def normalize_sales_shares(df):
    def scale_group(g):
        s = g["Vehicle_sales_share"].sum(skipna=True)
        if pd.isna(s) or s == 0:
            return g
        g["Vehicle_sales_share"] /= s
        return g
    return df.groupby(["Scenario", "Medium", "Vehicle Type", "Date"], group_keys=False).apply(scale_group)


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
                    # Not all sub-branches will exist (e.g., no Air for Freight in some)
                    continue
    except Exception as e:
        print(f"[ERROR] Activity Level setup failed: {e}")
    print("==============================================\n")


# ------------------------------------------------------------
# Main Loader
# ------------------------------------------------------------
def load_transport_into_leap_v3(excel_path, economy, validate=True):
    df = pd.read_excel(excel_path)
    df = df[df["Economy"] == economy]
    df = normalize_sales_shares(df)
    L = connect_to_leap()

    # Ensure valid top-level activity shares before loading data
    ensure_activity_levels(L)

    print(f"\n=== Loading Transport Data for {economy} ===")
    
    
    #sum up stocks by vtype within the road medium, since there is no stocks by drive in the source data (instead a stock share)
    df_road_stocks = df.loc[df["Medium"].str.lower() == "road"].copy()
    df_road_stocks = df_road_stocks.groupby(["Scenario", "Transport Type", "Vehicle Type", "Date"], as_index=False).agg({"Stock": "sum"})
    
    for leap_tuple, src_tuple in LEAP_TO_SOURCE_MAP.items():
        ttype, medium, vtype, drive = src_tuple

        df_sub = df[
            (df["Medium"].str.lower() == medium.lower()) &
            (df["Transport Type"].str.lower() == ttype.lower()) &
            (df["Vehicle Type"].str.lower() == vtype.lower()) &
            (df["Drive"].str.lower() == drive.lower())
        ]

        if df_sub.empty:
            raise ValueError(f"No data for mapping: {leap_tuple} <- {src_tuple}")
        try:
            # Determine LEAP branch
            if medium in ["air", "rail", "ship"]:
                leap_ttype, leap_medium, leap_fuel = leap_tuple
                analysis_type = "Energy Intensity"
                base_path = f"Demand\\Transport\\{leap_ttype}\\{leap_medium}"
                branch_path = f"{base_path}\\{leap_fuel}"
            else:
                leap_ttype, leap_medium, leap_vtype, leap_drive, leap_fuel = leap_tuple
                analysis_type = "Stock"
                base_path = f"Demand\\Transport\\{leap_ttype}\\{leap_medium}\\{leap_vtype}"
                branch_path = f"{base_path}\\{leap_drive}\\{leap_fuel}"
        except Exception as e:
            breakpoint()

        try:
            branch = L.Branch(branch_path)
        except Exception:
            print(f"[WARN] Branch missing: {branch_path}")
            continue

        print(f"[INFO] Writing {analysis_type}: {branch_path}")

        # Get measures for this analysis type
        measures = get_measures_for_analysis(analysis_type)

        # Apply measures
        for measure, meta in measures.items():
            if measure not in df_sub.columns:
                continue
            pts = [(r["Date"], r[measure] * meta["factor"]) for _, r in df_sub.iterrows() if pd.notna(r[measure])]
            expr = build_interp_expr(pts)
            if expr:
                safe_set_variable(branch, meta["leap_name"], expr, branch_path)

    # Validate shares
    if validate:
        print("\n=== Validating Vehicle Sales Shares ===")
        grouped = df.groupby(["Scenario", "Transport Type", "Medium", "Vehicle Type", "Date"])["Vehicle_sales_share"].sum()
        bad = grouped[(grouped < 0.99) | (grouped > 1.01)]
        if bad.empty:
            print("All groups sum to ~1.0")
        else:
            breakpoint()
            print("Groups deviating from 1.0:")
            print(bad)

    L.RefreshBranches()
    L.ActiveView = "Results"
    print("\n=== Transport data successfully filled into LEAP.")


#%%
# ------------------------------------------------------------
from LEAP_tranposrt_measures_config import list_all_measures

# (optional) review measures before loading
list_all_measures()

# Run loader
load_transport_into_leap_v3(
    excel_path=r"data/bd dummy transport file.xlsx",
    economy="02_BD",
    validate=True
)
#%%

