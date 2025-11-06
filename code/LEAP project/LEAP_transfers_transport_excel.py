import pandas as pd
from pathlib import Path
from LEAP_transfers_transport_MAPPINGS import LEAP_MEASURE_CONFIG


# def get_leap_metadata(measure):
#     """Fetch LEAP_units, LEAP_Scale, LEAP_Per from LEAP_MEASURE_CONFIG if available."""
#     for shortname, config_group in LEAP_MEASURE_CONFIG.items():
#         if measure == shortname or measure in config_group:
#             entry = config_group if isinstance(config_group, dict) else config_group[measure]
#             units = entry.get("LEAP_units", entry.get("unit", ""))
#             scale = entry.get("LEAP_Scale", "")
#             per = entry.get("LEAP_Per", "")
#             return units, scale, per
#     return "", "", ""


def create_import_instructions_sheet(writer):
    """Create instructions sheet inside the Excel file."""
    instructions = pd.DataFrame({
        "Step": range(1, 6),
        "Action": [
            "Open LEAP → Settings → Import Data...",
            "Select this Excel file and choose the 'Data' sheet.",
            "Map Branch Path → Branch, Variable → Variable, Years → Years.",
            "Select import options (Overwrite, Add, etc.).",
            "Run import and review LEAP’s message window."
        ]
    })
    instructions.to_excel(writer, sheet_name="Instructions", index=False)


def summarize_and_create_export_df(log_df, scenario, region, method, base_year, final_year
):
    """
    Create a LEAP-compatible Excel import file using LEAP_MEASURE_CONFIG metadata.
    Matches official LEAP Excel import format.
    """
    
    print(f"\n=== Creating LEAP Import File (structured) ===")

    if log_df is None or log_df.empty:
        print("[ERROR] No data available for export.")
        return None

    # --- Filter years ---
    log_df = log_df[(log_df["Date"] >= base_year) & (log_df["Date"] <= final_year)]
    
    # --- Pivot to wide format ---
    #just so we dont get an empty pivot, if any cols are fully None or na, fille them with str version of na then repalce once pivoted
    for col in ['Units', 'Scale', 'Per...']:
        if log_df[col].isna().all():
            log_df[col] = 'N/A'
        elif log_df[col].isnull().all():
            log_df[col] = 'null'
        elif (log_df[col] == '').all():
            log_df[col] = 'empty'
        elif (log_df[col] == None).all():
            log_df[col] = 'None'
       
    pivot_df = (
        log_df.pivot(
            index=["Branch_Path", "Measure", "Units", "Scale", "Per..."],
            columns="Date",
            values="Value"
        )
        .reset_index()
    )
    
    #now replace back the na values
    for col in ['Units', 'Scale', 'Per...']:
        pivot_df[col] = pivot_df[col].replace({'N/A': pd.NA, 'null': pd.NA, 'empty': '', 'None': None})
        #and do it to log df too just in case
        log_df[col] = log_df[col].replace({'N/A': pd.NA, 'null': pd.NA, 'empty': '', 'None': None})

    # --- Identify and sort year columns ---
    year_cols = sorted([int(c) for c in pivot_df.columns if isinstance(c, (int, float))])

    # --- Fill metadata columns ---
    pivot_df["Branch Path"] = pivot_df["Branch_Path"]
    pivot_df["Variable"] = pivot_df["Measure"]
    pivot_df["Scenario"] = scenario
    pivot_df["Region"] = region
    pivot_df["Method"] = method

    # # Fetch LEAP metadata from measure config
    # breakpoint()#check if htis is working. seems to me skipping steps
    # meta = pivot_df["Variable"].apply(lambda m: pd.Series(get_leap_metadata(m)))
    # meta.columns = ["Units", "Scale", "Per..."]
    # pivot_df = pd.concat([pivot_df, meta], axis=1)

    # --- Add Level 1–N columns ---
    max_levels = pivot_df["Branch_Path"].apply(lambda x: len(str(x).split("\\"))).max()
    for i in range(1, max_levels + 1):
        pivot_df[f"Level {i}"] = pivot_df["Branch_Path"].apply(
            lambda x: str(x).split("\\")[i - 1] if len(str(x).split("\\")) >= i else ""
        )

    # --- Sort variables within each branch in LEAP-like order ---
    var_order = [
        "Total Activity",
        "Activity Level",
        "Final Energy Intensity",
        "Total Final Energy Consumption",
        "Stock",
        "Sales Share",
        "Efficiency",
        "Turnover Rate",
        "Occupancy or Load",
    ]
    pivot_df["Variable_sort_order"] = pivot_df["Variable"].apply(
        lambda v: var_order.index(v) if v in var_order else len(var_order)
    )
    pivot_df = pivot_df.sort_values(by=["Branch_Path", "Variable_sort_order"]).drop(columns="Variable_sort_order")

    # --- Column order ---
    base_cols = ["Branch Path", "Variable", "Scenario", "Region", "Scale", "Units", "Per...", "Method"]
    level_cols = [f"Level {i}" for i in range(1, max_levels + 1)]
    export_df = pivot_df[base_cols + year_cols + level_cols]

    # --- Add trailing placeholder column for #N/A ---
    export_df.loc[:, "#N/A"] = pd.NA
    
    return export_df


def save_export_file(export_df, log_df, filename, base_year, final_year):
    """Save the export DataFrame and log DataFrame to an Excel file."""
    # --- Write to Excel ---
    out_path = Path(filename)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        export_df.to_excel(writer, sheet_name="Data", index=False)
        create_import_instructions_sheet(writer)
        log_df.to_excel(writer, sheet_name="Source_Log_Data", index=False)

    print(f"✅ Created LEAP import file with {len(export_df)} entries.")
    print(f" - Years covered: {base_year}–{final_year}")
    print(f" - Variables: {export_df['Variable'].nunique()}")
    print(f" - Branches: {export_df['Branch Path'].nunique()}")
    print("=" * 60)
    
