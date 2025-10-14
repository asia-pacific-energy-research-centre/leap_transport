#%%
# -*- coding: utf-8 -*-
"""
Transport → LEAP expression exporter (Excel version for Evaluation Mode).
Takes your dataset and writes a LEAP Expressions import file.
Normalizes Vehicle Sales Shares to sum=1 per (Scenario, Medium, Vehicle Type, Year).
"""

import pandas as pd
import numpy as np
# ----------------------------
# File paths
# ----------------------------
TRANSPORT_XLSX = r"data/bd dummy transport file.xlsx"
OUTPUT_EXPRESSIONS = r"data/leap_expressions_for_import.xlsx"

# ----------------------------
# Mapping dictionaries
# ----------------------------
MEDIUM_MAP = {"road": "Road", "rail": "Rail", "air": "Air", "ship": "Shipping"}

VEHTYPE_MAP = {
    "2w": "Motorcycles",
    "car": "Cars",
    'suv': 'SUVs',
    'lt': 'Light Trucks',    
    "bus": "Buses",
    "lcv": "Light Commercial Vehicles",
    "ht": "Heavy Trucks",
    'mt': 'Medium Trucks',
    "all": "All"
}


# ---- Drive → Fuel Mapping ----
DRIVE_TO_FUEL = {
    # Air
    "air_av_gas": "Aviation Gasoline",
    "air_diesel": "Diesel",
    "air_kerosene": "Kerosene",
    "air_electric": "Electricity",
    "air_fuel_oil": "Fuel Oil",
    "air_gasoline": "Gasoline",
    "air_hydrogen": "Hydrogen",
    "air_jet_fuel": "Jet Fuel",
    "air_lpg": "LPG",

    # Rail
    "rail_fuel_oil": "Fuel Oil",
    "rail_electricity": "Electricity",
    "rail_diesel": "Diesel",
    "rail_coal": "Coal",
    "rail_gasoline": "Gasoline",
    "rail_lpg": "LPG",
    "rail_kerosene": "Kerosene",
    "rail_natural_gas": "Natural Gas",

    # Ship
    "ship_lng": "LNG",
    "ship_ammonia": "Ammonia",
    "ship_hydrogen": "Hydrogen",
    "ship_gasoline": "Gasoline",
    "ship_fuel_oil": "Fuel Oil",
    "ship_electric": "Electricity",
    "ship_diesel": "Diesel",
    "ship_kerosene": "Kerosene",
    "ship_lpg": "LPG",
    "ship_natural_gas": "Natural Gas",

    # Road
    "ice_d": "Diesel (Blend)",
    "ice_g": "Gasoline (Blend)",
    "lpg": "LPG (Blend)",
    "cng": "CNG (Blend)",
    "bev": "Electricity",
    "fcev": "Hydrogen",
    "lng": "LNG",
    "phev_d": "Diesel (Blend)",
    "phev_g": "Gasoline (Blend)",
}

MEASURE_TO_VARIABLE = {
    "Activity": "Activity Level",
    "Efficiency": "Energy Intensity",
    "Occupancy_or_load": "Occupancy",
    "Mileage": "Annual Mileage",
    "Stocks": "Stocks",
    "New_vehicle_efficiency": "New Vehicle Efficiency",
    "Average_age": "Average Age",
    "Turnover_rate": "Turnover Rate",
    "Activity_growth": "Activity Growth"
}

SCENARIO_MAP = {
    "Reference": "Baseline", 
    #"Target": "High EV"
}

# ---- Units and scaling (from your concordance) ----
MEASURE_TO_UNIT = {
    "Energy": ("PJ", 1),
    "Stocks": ("Million_stocks", 1e6),
    "New_vehicle_efficiency": ("Billion_km_per_pj", 1e-9),
    "Efficiency": ("Billion_km_per_pj", 1e-9),
    "Turnover_rate": ("%", 1),
    "Supply_side_fuel_share": ("%", 1),
    "Demand_side_fuel_share": ("%", 1),
    "Occupancy_or_load": ("Passengers_or_tonnes", 1),
    "Activity": ("Billion_passenger_km_or_freight_tonne_km", 1e9),
    "Mileage": ("Thousand_km_per_stock", 1e3),
    "Non_road_efficiency_growth": ("%", 1),
    "Vehicle_sales_share": ("%", 1),
    "New_vehicle_efficiency_growth": ("%", 1),
    "Turnover_rate_growth": ("%", 1),
    "Occupancy_or_load_growth": ("%", 1),
    "Activity_growth": ("%", 1),
    "Travel_km": ("Billion_km", 1e9),
    "Intensity": ("PJ_per_billion_passenger_or_freight_tonne_km", 1e9),
    "Gdp": ("Real_gdp_millions", 1e6),
    "Population": ("Population_thousands", 1e3),
    "Gdp_per_capita": ("Thousand_Gdp_per_capita", 1e3),
    "Stocks_per_thousand_capita": ("Stocks_per_thousand_capita", 1),
    "Average_age": ("Age", 1),
}

# ----------------------------
# Helpers
# ----------------------------
def build_interp_expr(series):
    pts = [(int(y), float(v)) for (y, v) in series if pd.notna(y) and pd.notna(v)]
    if not pts: return None
    pts.sort(key=lambda x: x[0])
    if len(pts) == 1:
        return str(pts[0][1])
    return "Interp(" + ", ".join(f"{y}, {v}" for y, v in pts) + ")"

def normalize_sales_shares(df):
    """Ensure Vehicle_sales_share sums to 1 per Scenario/Medium/VehicleType/Year."""
    def scale_group(g):
        if "Vehicle_sales_share" not in g: 
            return g
        s = g["Vehicle_sales_share"].sum(skipna=True)
        if pd.isna(s) or s == 0:
            print(f"[WARN] No sales shares for {g.iloc[0]['Scenario']}, {g.iloc[0]['Medium']}, {g.iloc[0]['Vehicle Type']}, {g.iloc[0]['Date']}")
            return g
        if abs(s - 1.0) > 1e-6:
            g["Vehicle_sales_share"] = g["Vehicle_sales_share"] / s
            print(f"[NORM] Normalized sales shares for {g.iloc[0]['Scenario']}, {g.iloc[0]['Medium']}, {g.iloc[0]['Vehicle Type']}, {g.iloc[0]['Date']} (sum before={s})")
        return g
    return df.groupby(["Scenario","Medium","Vehicle Type","Date"], group_keys=False).apply(scale_group)

# ----------------------------
# Main exporter
# ----------------------------
def export_expressions(excel_path, out_path):
    df = pd.read_excel(excel_path)
    df = normalize_sales_shares(df)

    records = []

    for (scenario, medium, vtype, drive), df_grp in df.groupby(["Scenario","Medium","Vehicle Type","Drive"]):
        scen = SCENARIO_MAP.get(str(scenario), str(scenario))
        med = MEDIUM_MAP.get(str(medium).lower(), str(medium))
        vt = VEHTYPE_MAP.get(str(vtype).lower(), str(vtype))
        tech = str(drive).upper()

        branch_path = f"Demand\\Transport\\{med}\\{vt}\\{tech}"

        for measure, leap_var in MEASURE_TO_VARIABLE.items():
            if measure not in df_grp: 
                continue
            # apply scaling
            unit, factor = MEASURE_TO_UNIT.get(measure, ("", 1))
            pts = [(row["Date"], row[measure] * factor) for _, row in df_grp.iterrows() if pd.notna(row[measure])]
            expr = build_interp_expr(pts)
            if expr:
                records.append({
                    "Scenario": scen,
                    "Branch": branch_path,
                    "Variable": leap_var,
                    "Expression": expr,
                    "Unit": unit
                })

        # PHEV main fuel fraction
        if drive in ["phev_d", "phev_g"]:
            if "Fraction_mainfuel" in df_grp:
                pts = [(row["Date"], row["Fraction_mainfuel"]) for _, row in df_grp.iterrows() if pd.notna(row["Fraction_mainfuel"])]
                expr = build_interp_expr(pts)
            else:
                expr = "0.7"  # default
            records.append({
                "Scenario": scen,
                "Branch": branch_path,
                "Variable": "Fraction Used by Main Fuel",
                "Expression": expr,
                "Unit": "Fraction"
            })

    # Save to Excel
    out_df = pd.DataFrame(records)
    out_df.to_excel(out_path, index=False)
    print(f"✅ Expressions exported to {out_path}")

# ----------------------------
# Run
# ----------------------------
#%%
export_expressions(TRANSPORT_XLSX, OUTPUT_EXPRESSIONS)

#%%




