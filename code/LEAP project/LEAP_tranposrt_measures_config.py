# ============================================================
# measures_config.py
# ============================================================
"""
Central configuration for all LEAP variables & measures.
Easily editable — just update names, add new measures, or fix scaling factors.
"""

import pandas as pd
from pandas.api.types import is_numeric_dtype
from LEAP_transfers_transport_MAPPINGS import CSV_TREE

#note that in this below: source_mapping is the name of the measure in the source dataset (e.g. 9th edition dataset) and leap_name is the name of the variable in leap that we want to map it to. factor is the scaling factor to convert from the source dataset, after the SOURCE_MEASURE_TO_UNIT scaling has been applied, to the units expected in leap.
LEAP_MEASURE_CONFIG = {
    # ==== Vehicle type (road) ====
    "Stock": {"source_mapping": "Stocks", "factor": 1, "unit": "stocks"},
    "Stock Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "Sales": {"source_mapping": "Vehicle_sales", "factor": 1, "unit": "vehicles"},
    "Sales Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "Retirements": {"source_mapping": "Turnover_rate", "factor": 1, "unit": "%"},

    # ==== Technology (road) ====
    "Device Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "First Sales Year": {"source_mapping": None, "factor": 1, "unit": "year"},
    "Fraction of Scrapped Replaced": {"source_mapping": "Fraction_scrapped_replaced", "factor": 1, "unit": "%"},
    "Fuel Economy Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
    "Fuel Economy": {"source_mapping": "Efficiency", "factor": 0.1, "unit": "MJ_per_100km"},
    "Max Scrappage Fraction": {"source_mapping": None, "factor": 1, "unit": "%"},
    "Mileage Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
    "Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
    "Sales Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "Scrappage": {"source_mapping": "Scrappage", "factor": 1, "unit": "%"},
    "Stock Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "Retirements": {"source_mapping": "Turnover_rate", "factor": 1, "unit": "%"},

    # ==== Fuel (road) ====
    "Final On-Road Fuel Economy": {"source_mapping": "Efficiency", "factor": 0.1, "unit": "MJ_per_100km"},
    "Fuel Economy": {"source_mapping": "Efficiency", "factor": 0.1, "unit": "MJ_per_100km"},
    "Fuel Economy Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
    "Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
    "Mileage Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
    "Average Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
    "Final On-Road Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
    "Device Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},

    # ==== Medium (road) ====
    "Stock": {"source_mapping": "Stocks", "factor": 1, "unit": "stocks"},
    "Stock Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "Sales": {"source_mapping": "Vehicle_sales", "factor": 1, "unit": "vehicles"},
    "Sales Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
    "Retirements": {"source_mapping": "Turnover_rate", "factor": 1, "unit": "%"},

    # ==== Medium (non-road) ====
    "Activity Level": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},
    "Final Energy Intensity": {"source_mapping": "Efficiency", "factor": 1e-9, "unit": "GJ_per_tonne_km"},
    "Total Activity": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},

    # ==== Fuel (non-road) ====
    "Activity Level": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},
    "Final Energy Intensity": {"source_mapping": "Efficiency", "factor": 1e-9, "unit": "GJ_per_tonne_km"},
    "Total Final Energy Consumption": {"source_mapping": "Energy", "factor": 1, "unit": "pj"},
    "Total Activity": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},
}

# Unit + scaling lookup used for converting raw data from the source dataset (i.e. the 9th edition dataset) to its equivalent scaled to 1. i.e. stocks are measured in millions in our source data, so we need to multiply by 1e6 to get the actual number of stocks - thereby making it useful for LEAP and calculations. Not all measures here are used in the leap measures config above, but they are included for completeness.
SOURCE_MEASURE_TO_UNIT = {
    "Energy": ("PJ", 1, 'S'),
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

DEFAULT_WEIGHT_PRIORITY = ["Activity"]
SOURCE_WEIGHT_PRIORITY = {#used for aggregating measures that need weighting when being mapped to leap measures
    "Efficiency": ["Activity",  "Stocks"],
    "Mileage": ["Stocks", "Activity"],
    "Intensity": ["Activity",  "Stocks"],
}

#this one will contain details of all the measures within the leap model so we can easily set them.
#e.g. {Measure: {Leap_name: "", unit: "", factor: float}}
LEAP_MEASURES_DETAILS = {
    'Vehicle type (road)': [
        "Stock",
        "Stock Share",
        "Sales",
        "Sales Share",
        "Retirements",
    ],
    'Technology (road)': [
        "Device Share",
        "First Sales Year",
        "Fraction of Scrapped Replaced",
        "Fuel Economy Correction Factor",
        "Fuel Economy",
        "Max Scrappage Fraction",
        "Mileage Correction Factor",
        "Mileage",
        "Sales Share",
        "Scrappage",
        "Stock Share",
        "Retirements",
    ],
    'Fuel (road)': [
        "Device Share",
        "Fuel Economy",
        "Fuel Economy Correction Factor",
        "Final On-Road Fuel Economy",
        "Mileage",
        "Mileage Correction Factor",
        "Average Mileage",
        "Final On-Road Mileage",
    ],
    'Medium (road)': [
        "Stock",
        "Stock Share",
        "Sales",
        "Sales Share",
        "Retirements",
    ],
    'Medium (non-road)': [
        "Activity Level",
        "Final Energy Intensity",
        "Total Activity",
    ],
    'Fuel (non-road)': [
        "Activity Level",
        "Final Energy Intensity",
        "Total Final Energy Consumption",
        "Total Activity",
    ],
}

AGGREGATION_RULES = {
    # Additive (sum)
    "Energy": "sum",
    "Stocks": "sum",
    "Activity": "sum",
    "Travel_km": "sum",
    "Gdp": "sum",
    "Population": "sum",

    # Weighted average (need weight)
    "Efficiency": "weighted",
    "New_vehicle_efficiency": "weighted",
    "Mileage": "weighted",
    "Intensity": "weighted",
    "Occupancy_or_load": "weighted",

    # Shares or rates (usually leave as-is or normalize later)
    "Vehicle_sales_share": "share",
    "Supply_side_fuel_share": "share",
    "Demand_side_fuel_share": "share",
    "Turnover_rate": "share",

    # Growth rates (apply later via derivative logic)
    "Activity_growth": "growth",
    "Efficiency_growth": "growth",
}

    
# ============================================================

def get_measures_for_analysis(analysis_type):
    """Return all configured measures for a given analysis type."""
    return LEAP_MEASURE_CONFIG.get(analysis_type, {})


def list_all_measures():
    """Pretty-print all measures for human inspection."""
    print("=== MEASURE CONFIGURATION ===")
    for analysis, measures in LEAP_MEASURE_CONFIG.items():
        print(f"\n[{analysis}]")
        for k, v in measures.items():
            print(f"  {k:25} → {v['leap_name']:30} | {v['unit']:35} | Scale={v['factor']}")
    print("==============================\n")


# ============================================================
# Utility functions
# ============================================================

def get_leap_measure(name: str) -> dict:
    """Get LEAP measure metadata dict, or None if not found."""
    return LEAP_MEASURE_CONFIG.get(name)


def get_source_unit(measure: str):
    """Return (unit, scale) tuple for source measure."""
    return SOURCE_MEASURE_TO_UNIT.get(measure, (None, 1))


def get_weight_priority(measure: str):
    """Return the list of candidate weight columns for a given measure."""
    return SOURCE_WEIGHT_PRIORITY.get(measure, DEFAULT_WEIGHT_PRIORITY)


def apply_scaling(series: pd.Series, leap_measure: str) -> pd.Series:
    """
    Scale a data series from source units → LEAP-ready units.
    Combines SOURCE_MEASURE_TO_UNIT scaling and LEAP_MEASURE_CONFIG factor.
    """
    meta = get_leap_measure(leap_measure)
    if meta is None:
        return series

    source_measure = meta.get("source_mapping")
    factor = meta.get("factor", 1)
    src_unit, src_scale = get_source_unit(source_measure)

    # total scaling = (source scale to base) * (LEAP-level adjustment)
    total_scale = src_scale * factor
    return series * total_scale


def apply_special_conversions(df: pd.DataFrame, measure: str):
    """
    Handles special nonlinear conversions:
      - Efficiency (Billion_km_per_pj) → Final Energy Intensity (GJ/km)
      - Fuel Economy (Billion_km_per_pj) → MJ/100km
    """
    if measure == "Final Energy Intensity" and "Efficiency" in df.columns:
        s = df["Efficiency"].astype(float)
        df[measure] = s.apply(lambda x: None if x == 0 else 1.0 / x)
    elif measure == "Fuel Economy" and "Efficiency" in df.columns:
        s = df["Efficiency"].astype(float)
        df[measure] = s.apply(lambda x: None if x == 0 else 100_000.0 / x)
    return df


def aggregate_weighted(df, measure, weight_col=None):
    """Weighted average aggregation."""
    if df.empty or measure not in df.columns:
        return None
    weight_col = weight_col or next((w for w in get_weight_priority(measure) if w in df.columns), None)
    if not weight_col:
        return None
    w = df[weight_col].fillna(0)
    m = df[measure].fillna(0)
    total_w = w.sum()
    if total_w == 0:
        return None
    return (m * w).sum() / total_w


def list_all_measures():
    """Display config table for easy inspection."""
    print("=== LEAP MEASURE CONFIG ===")
    for k, v in LEAP_MEASURE_CONFIG.items():
        src = v.get("source_mapping")
        print(f"{k:30} ← {src:25} | {v['unit']:25} | factor={v['factor']}")
    print("===========================\n")


# ============================================================
# Example: end-to-end mapping utility
# ============================================================

def convert_source_to_leap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Example pipeline:
      - For each LEAP variable in config
      - Apply scaling and nonlinear conversion if required
      - Return DataFrame with LEAP-ready values
    """
    df_out = df.copy()
    for leap_measure, meta in LEAP_MEASURE_CONFIG.items():
        src = meta["source_mapping"]
        if src not in df_out.columns:
            continue
        df_out[leap_measure] = apply_scaling(df_out[src], leap_measure)

    # apply nonlinear relationships afterwards
    df_out = apply_special_conversions(df_out, "Final Energy Intensity")
    df_out = apply_special_conversions(df_out, "Fuel Economy")
    return df_out
