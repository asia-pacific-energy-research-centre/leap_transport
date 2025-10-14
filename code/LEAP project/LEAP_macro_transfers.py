
import pandas as pd
from win32com.client import Dispatch, GetActiveObject

GDP_CSV = r"data/APEC_GDP_data_20240902.csv"
OUTPUT_XLSX = r"data/key_assumptions_for_import.xlsx"

# Map CSV variables → LEAP Key Assumption branch names
VARIABLE_MAP = {
    "GDP_per_capita": "GDP per Capita",
    "real_GDP": "real GDP",
    "population": "Population"
}

# Scaling and units
ASSUMPTION_UNITS = {
    "population": ("Population_thousands", 1e3),
    "real_GDP": ("Real_gdp_millions", 1e6),
    "GDP_per_capita": ("Gdp_per_capita", 1)
}

def connect_to_leap():
    try:
        return GetActiveObject('LEAP.LEAPApplication')
    except Exception:
        return Dispatch('LEAP.LEAPApplication')

def build_interp_expr(series):
    pts = [(int(y), float(v)) for (y, v) in series if pd.notna(y) and pd.notna(v)]
    if not pts: return None
    pts.sort(key=lambda x: x[0])
    if len(pts) == 1:
        return str(pts[0][1])
    return "Interp(" + ", ".join(f"{y}, {v}" for y, v in pts) + ")"

def ensure_branch(L, parent_path, name, branch_type=0):
    full_path = parent_path + "\\" + name
    try:
        return L.Branch(full_path)
    except Exception:
        parent = L.Branch(parent_path)
        L.AddBranch(parent.ID, name, branch_type)
        print(f"[ADD] Created Key Assumption branch {full_path}")
        return L.Branch(full_path)

def load_key_assumptions(csv_path, mode="api"):
    """
    mode = "api" → write directly into LEAP via COM API
    mode = "excel" → export expressions to Excel for import
    """
    df = pd.read_csv(csv_path)
    records = []

    if mode == "api":
        L = connect_to_leap()

    for var, df_var in df.groupby("variable"):
        leap_name = VARIABLE_MAP.get(var)
        if not leap_name:
            print(f"[SKIP] No mapping for variable {var}")
            continue

        branch_path = f"Key Assumptions\\{leap_name}"

        # Apply scaling
        unit, factor = ASSUMPTION_UNITS.get(var, ("", 1))
        pts = [(row["year"], row["value"] * factor) for _, row in df_var.iterrows() if pd.notna(row["value"])]
        expr = build_interp_expr(pts)

        if not expr:
            continue

        if mode == "api":
            ensure_branch(L, "Key Assumptions", leap_name)
            var_obj = L.Branch(branch_path).Variable("Activity Level")
            var_obj.Expression = expr
            print(f"[SET] {branch_path} = {expr} [{unit}]")
            print(f"[CHK] stored = {var_obj.Expression}")

        elif mode == "excel":
            records.append({
                "Scenario": "Current Accounts",  # Key Assumptions usually apply here
                "Branch": branch_path,
                "Variable": "Activity Level",
                "Expression": expr,
                "Unit": unit
            })

    if mode == "api":
        L.ActiveView = "Results"
    elif mode == "excel" and records:
        out_df = pd.DataFrame(records)
        out_df.to_excel(OUTPUT_XLSX, index=False)
        print(f"✅ Key Assumptions expressions exported to {OUTPUT_XLSX}")

#%% Run
# Pick mode: "api" or "excel"
load_key_assumptions(GDP_CSV, mode="excel")
#%%
