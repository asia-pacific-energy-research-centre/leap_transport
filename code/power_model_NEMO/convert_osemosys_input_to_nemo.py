#%%
import pandas as pd
import sqlite3
from pathlib import Path
from shutil import copyfile

# -------------------------------------------------------------------
# USER CONFIGURATION
# -------------------------------------------------------------------

EXCEL_PATH = Path("POWER 20_USA_data_REF9_S3_test.xlsx")
TEMPLATE_DB = Path("nemo_template.sqlite")          # NEMO schema, made once with createnemodb() in Julia 
#> note that you may need to run the following in Julia to install NemoMod.jl:
# # using Pkg
# # Pkg.add(url="https://github.com/sei-international/NemoMod.jl")
# #then:
# # using NemoMod
# # NemoMod.createnemodb("nemo_template.sqlite")
OUTPUT_DB = Path("usa_power_nemo.sqlite")

TARGET_SCENARIO = "Reference"                       # which scenario in SCENARIO column to extract

# Turn advanced features on/off
USE_ADVANCED = {
    "ReserveMargin": True,
    "AnnualEmissionLimit": True,
}

# For each Excel sheet: define NEMO table + index columns (Excel names)
PARAM_SPECS = {
    # Demand
    "SpecifiedAnnualDemand": {
        "nemo_table": "SpecifiedAnnualDemand",
        "indices": ["REGION", "FUEL"],  # -> r, f, plus y, val
        "filter_scenario": True,
    },
    "SpecifiedDemandProfile": {
        "nemo_table": "SpecifiedDemandProfile",
        "indices": ["REGION", "FUEL", "TIMESLICE"],  # -> r, f, l, y, val
        "filter_scenario": True,
    },

    # Tech parameters
    "CapacityFactor": {
        "nemo_table": "CapacityFactor",
        "indices": ["REGION", "TECHNOLOGY", "TIMESLICE"],
        "filter_scenario": False,
    },
    "CapitalCost": {
        "nemo_table": "CapitalCost",
        "indices": ["REGION", "TECHNOLOGY"],
        "filter_scenario": False,
    },
    "FixedCost": {
        "nemo_table": "FixedCost",
        "indices": ["REGION", "TECHNOLOGY"],
        "filter_scenario": False,
    },
    "ResidualCapacity": {
        "nemo_table": "ResidualCapacity",
        "indices": ["REGION", "TECHNOLOGY"],
        "filter_scenario": False,
    },
    "EmissionActivityRatio": {
        "nemo_table": "EmissionActivityRatio",
        "indices": ["REGION", "TECHNOLOGY", "EMISSION"],
        "filter_scenario": False,
    },

    # Advanced: Reserve margin (system-level)
    "ReserveMargin": {
        "nemo_table": "ReserveMargin",
        "indices": ["REGION"],          # -> r, y, val
        "filter_scenario": False,
        "advanced_flag": "ReserveMargin",
    },

    # Advanced: Annual emission limit
    "AnnualEmissionLimit": {
        "nemo_table": "AnnualEmissionLimit",
        "indices": ["REGION", "EMISSION"],  # -> r, e, y, val
        "filter_scenario": True,
        "advanced_flag": "AnnualEmissionLimit",
    },

    # You can add more advanced ones here:
    # "EmissionsPenalty": {...},
    # "ModelPeriodEmissionLimit": {...},
}


# Map Excel index names -> NEMO index names
INDEX_NAME_MAP = {
    "REGION": "r",
    "TECHNOLOGY": "t",
    "FUEL": "f",
    "TIMESLICE": "l",
    "EMISSION": "e",
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def get_year_columns(cols):
    """Extract columns that look like years (e.g., 2017, 2018, 2030...)."""
    out = []
    for c in cols:
        try:
            y = int(str(c))
            out.append(y)
        except ValueError:
            continue
    return out


def copy_template_db(template: Path, output: Path):
    if not template.exists():
        raise FileNotFoundError(
            f"Template DB '{template}' not found. "
            "Create it once with NEMO's createnemodb() in Julia or via LEAP."
        )
    copyfile(template, output)
    print(f"Copied template DB to '{output}'.")


def load_sheet(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
    except ValueError:
        print(f"  Sheet '{sheet_name}' not found. Skipping.")
        return pd.DataFrame()

    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def filter_scenario(df: pd.DataFrame, target: str) -> pd.DataFrame:
    if "SCENARIO" not in df.columns:
        return df
    return df[df["SCENARIO"].astype(str) == target]


def build_rows_from_wide(df: pd.DataFrame, indices, year_cols):
    """
    Convert wide-year dataframe to list of dicts with
    keys: indices + ['YEAR', 'VALUE'].
    """
    rows = []
    for _, r in df.iterrows():
        base = {idx: r[idx] for idx in indices}
        for y in year_cols:
            val = r[y]
            if pd.isna(val):
                continue
            rows.append({**base, "YEAR": int(y), "VALUE": float(val)})
    return rows


def insert_rows(conn, nemo_table: str, rows: list[dict], indices: list[str]):
    if not rows:
        print(f"  No rows to insert into {nemo_table}.")
        return

    # NEMO column names
    nemo_idx_cols = [INDEX_NAME_MAP[i] for i in indices]
    cols = nemo_idx_cols + ["y", "val"]

    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {nemo_table} ({','.join(cols)}) VALUES ({placeholders})"

    data = []
    for r in rows:
        tup = [r[i] for i in indices] + [r["YEAR"], r["VALUE"]]
        data.append(tup)

    cur = conn.cursor()
    cur.executemany(sql, data)
    conn.commit()
    print(f"  Inserted {len(rows)} rows into {nemo_table}.")


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    copy_template_db(TEMPLATE_DB, OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)

    for sheet_name, spec in PARAM_SPECS.items():
        nemo_table = spec["nemo_table"]
        indices = spec["indices"]
        filter_scen = spec.get("filter_scenario", False)
        adv_flag = spec.get("advanced_flag", None)

        # If this is an advanced param and its switch is off, skip
        if adv_flag is not None and not USE_ADVANCED.get(adv_flag, False):
            print(f"\nSkipping advanced parameter '{sheet_name}' (flag {adv_flag}=False).")
            continue

        print(f"\nProcessing sheet '{sheet_name}' -> table '{nemo_table}'")

        df = load_sheet(EXCEL_PATH, sheet_name)
        if df.empty:
            print("  Sheet empty or missing. Skipping.")
            continue

        # Filter scenario if needed
        if filter_scen:
            df = filter_scenario(df, TARGET_SCENARIO)
            if df.empty:
                print(f"  No rows for scenario '{TARGET_SCENARIO}'. Skipping.")
                continue

        # Check indices exist
        missing = [i for i in indices if i not in df.columns]
        if missing:
            print(f"  Missing required columns {missing} in '{sheet_name}'. Skipping.")
            continue

        # Identify year columns
        year_cols = get_year_columns(df.columns)
        if not year_cols:
            print(f"  No year columns found in '{sheet_name}'. Skipping.")
            continue

        # Build rows & insert
        rows = build_rows_from_wide(df, indices, year_cols)
        insert_rows(conn, nemo_table, rows, indices)

    conn.close()
    print(f"\nDone. NEMO scenario DB written to '{OUTPUT_DB}'.")

#%%
if __name__ == "__main__":
    main()
#%%