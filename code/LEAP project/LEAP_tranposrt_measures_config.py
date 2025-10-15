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
    # Vehicle type (road)
    'Vehicle type (road)': {
        "Stock": {"source_mapping": "Stocks", "factor": 1, "unit": "stocks"},
        "Stock Share": {"source_mapping": "Stock_share_calc_vehicle_type", "factor": 1, "unit": "%"},
        "Sales": {"source_mapping": "Sales_calc_vehicle_type", "factor": 1, "unit": "vehicles"},
        "Sales Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
        "Retirements": {"source_mapping": None, "factor": 1, "unit": "%"},
    },
    
    # Technology (road)
    'Technology (road)': {
        "Device Share": {"source_mapping": None, "factor": 1, "unit": "%"},
        "First Sales Year": {"source_mapping": None, "factor": 1, "unit": "year"},
        "Fraction of Scrapped Replaced": {"source_mapping": None, "factor": 1, "unit": "%"},
        "Fuel Economy Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
        "Fuel Economy": {"source_mapping": "New_vehicle_efficiency", "factor": 0.1, "unit": "MJ_per_100km"},
        "Max Scrappage Fraction": {"source_mapping": None, "factor": 1, "unit": "%"},
        "Mileage Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
        "Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
        "Sales Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
        "Scrappage": {"source_mapping": "Scrappage", "factor": 1, "unit": "%"},
        "Stock Share": {"source_mapping": "Stock_share_calc_fuel", "factor": 1, "unit": "%"},
        "Retirements": {"source_mapping": None, "factor": 1, "unit": "%"},
    },
    
    # Fuel (road)
    'Fuel (road)': {
        "Device Share": {"source_mapping": "Stock_share_calc_fuel", "factor": 1, "unit": "%"},
        "Fuel Economy": {"source_mapping": "Efficiency", "factor": 0.1, "unit": "MJ_per_100km"},
        "Fuel Economy Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
        "Final On-Road Fuel Economy": {"source_mapping": "Efficiency", "factor": 0.1, "unit": "MJ_per_100km"},
        "Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
        "Mileage Correction Factor": {"source_mapping": None, "factor": 1, "unit": "factor"},
        "Average Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
        "Final On-Road Mileage": {"source_mapping": "Mileage", "factor": 1, "unit": "km_per_stock"},
    },
    
    # Medium (road)
    'Medium (road)': {
        "Stock": {"source_mapping": "Stocks", "factor": 1, "unit": "stocks"},
        "Stock Share": {"source_mapping": "Stock_share_calc_medium", "factor": 1, "unit": "%"},
        "Sales": {"source_mapping": "Sales_calc_medium", "factor": 1, "unit": "vehicles"},
        "Sales Share": {"source_mapping": "Vehicle_sales_share", "factor": 1, "unit": "%"},
        "Retirements": {"source_mapping": None, "factor": 1, "unit": "%"},
    },
    
    # Medium (non-road)
    'Medium (non-road)': {
        "Activity Level": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},#{"source_mapping": "Activity_share_calc_medium", "factor": 1, "unit": "%"},
        "Total Activity": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},
        # "Final Energy Intensity": {"source_mapping": "Intensity", "factor": 1e-9, "unit": "GJ_per_tonne_km"},
        # "Total Final Energy Consumption": {"source_mapping": "Energy", "factor": 1, "unit": "pj"},
        
    },
    
    # Fuel (non-road)
    'Fuel (non-road)': {
        "Activity Level": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},#{"source_mapping": "Activity_share_calc_fuel", "factor": 1, "unit": "%"},
        # "Final Energy Intensity": {"source_mapping": "Intensity", "factor": 1e-9, "unit": "GJ_per_tonne_km"},#Missing expected variables from LEAP
        # "Total Final Energy Consumption": {"source_mapping": "Energy", "factor": 1, "unit": "pj"},#Missing expected variables from LEAP
        "Total Activity": {"source_mapping": "Activity", "factor": 1, "unit": "Passenger_km_or_freight_tonne_km"},
    },
}

# Unit + scaling lookup used for converting raw data from the source dataset (i.e. the 9th edition dataset) to its equivalent scaled to 1. i.e. stocks are measured in millions in our source data, so we need to multiply by 1e6 to get the actual number of stocks - thereby making it useful for LEAP and calculations. Not all measures here are used in the leap measures config above, but they are included for completeness.
SOURCE_MEASURE_TO_UNIT = {
    "Energy": ("PJ", 1),
    "Stocks": ("Million_stocks", 1e6),
    "New_vehicle_efficiency": ("Billion_km_per_pj", 1e-9),
    "Efficiency": ("Billion_km_per_pj", 1e-9),
    "Turnover_rate": ("%", 1),
    "Supply_side_fuel_share": ("%", 100),
    "Demand_side_fuel_share": ("%", 100),
    "Occupancy_or_load": ("Passengers_or_tonnes", 1),
    "Activity": ("Billion_passenger_km_or_freight_tonne_km", 1e9),
    "Mileage": ("Thousand_km_per_stock", 1e3),
    "Non_road_efficiency_growth": ("%", 100),
    "Vehicle_sales_share": ("%", 100),
    "New_vehicle_efficiency_growth": ("%", 100),
    "Turnover_rate_growth": ("%", 100),
    "Occupancy_or_load_growth": ("%", 100),
    "Activity_growth": ("%", 100),
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
# LEAP_MEASURES_DETAILS = {
#     'Vehicle type (road)': [
#         "Stock",
#         "Stock Share",
#         "Sales",
#         "Sales Share",
#         "Retirements",
#     ],
#     'Technology (road)': [
#         "Device Share",
#         "First Sales Year",
#         "Fraction of Scrapped Replaced",
#         "Fuel Economy Correction Factor",
#         "Fuel Economy",
#         "Max Scrappage Fraction",
#         "Mileage Correction Factor",
#         "Mileage",
#         "Sales Share",
#         "Scrappage",
#         "Stock Share",
#         "Retirements",
#     ],
#     'Fuel (road)': [
#         "Device Share",
#         "Fuel Economy",
#         "Fuel Economy Correction Factor",
#         "Final On-Road Fuel Economy",
#         "Mileage",
#         "Mileage Correction Factor",
#         "Average Mileage",
#         "Final On-Road Mileage",
#     ],
#     'Medium (road)': [
#         "Stock",
#         "Stock Share",
#         "Sales",
#         "Sales Share",
#         "Retirements",
#     ],
#     'Medium (non-road)': [
#         "Activity Level",
#         # "Final Energy Intensity",
#         # "Total Activity",#calcaulted
#     ],
#     'Fuel (non-road)': [
#         "Activity Level",
#         "Final Energy Intensity",#not in there? maybe it takes energy use?
#         # "Total Final Energy Consumption",#calcaulted
#         # "Total Activity",#calcaulted
#     ],
# }


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
    "Activity_share_calc_medium": "share",
    "Activity_share_calc_fuel": "share",

    # Growth rates (apply later via derivative logic)
    "Activity_growth": "growth",
    "Efficiency_growth": "growth",
}

CALCULATED_MEASURES = ['Stock_share_calc_medium','Stock_share_calc_fuel','Sales_calc_vehicle_type', 'Sales_calc_medium','Stock_share_calc_vehicle_type', 'Activity_share_calc_medium', 'Activity_share_calc_fuel']

# ============================================================

# def get_measures_for_analysis(analysis_type, shortname):
#     """Return all configured measures for a given analysis type."""
#     return LEAP_MEASURE_CONFIG[shortname].get(analysis_type, {})


def list_all_measures(shortname=None):
    """Pretty-print all measures for human inspection."""
    print("=== MEASURE CONFIGURATION ===")
    
    config_items = LEAP_MEASURE_CONFIG.items() if shortname is None else {shortname: LEAP_MEASURE_CONFIG[shortname]}.items()
    
    for branch_name, measures in config_items:
        print(f"\n[{branch_name}]")
        for measure_name, measure_data in measures.items():
            source = measure_data.get("source_mapping")
            source_str = "None" if source is None else str(source)
            unit = measure_data.get("unit", "")
            factor = measure_data.get("factor", 1)
            print(f"  {measure_name:25} → {source_str:30} | {unit:35} | Scale={factor}")
    print("==============================\n")


# ============================================================
# Utility functions
# ============================================================

def get_leap_measure(name: str, shortname: str) -> dict:
    """Get LEAP measure metadata dict, or None if not found."""
    return LEAP_MEASURE_CONFIG[shortname].get(name)


def get_source_unit(measure: str):
    """Return (unit, scale) tuple for source measure."""
    return SOURCE_MEASURE_TO_UNIT.get(measure, (None, 1))


def get_weight_priority(measure: str):
    """Return the list of candidate weight columns for a given measure."""
    return SOURCE_WEIGHT_PRIORITY.get(measure, DEFAULT_WEIGHT_PRIORITY)


def apply_scaling(series: pd.Series, leap_measure: str, shortname: str) -> pd.Series:
    """
    Scale a data series from source units → LEAP-ready units.
    Combines SOURCE_MEASURE_TO_UNIT scaling and LEAP_MEASURE_CONFIG factor.
    """
    meta = get_leap_measure(leap_measure, shortname)
    if meta is None:
        return series

    source_measure = meta.get("source_mapping")
    factor = meta.get("factor", 1)
    src_unit, src_scale = get_source_unit(source_measure)

    # total scaling = (source scale to base) * (LEAP-level adjustment)
    total_scale = src_scale * factor
    return series * total_scale


def apply_special_conversions(df: pd.DataFrame, measure: str) -> pd.DataFrame:
    """
    Handles special nonlinear conversions:
      - Intensity (pj_per_km) → Final Energy Intensity (GJ/km)
      - Fuel Economy (km_per_pj) → MJ/100km
    """
    if measure == "Final Energy Intensity" and "Intensity" in df.columns:
        s = df["Intensity"].astype(float)
        df[measure] = s.apply(lambda x: None if x == 0 else 1_000.0 / x)
    elif measure == "Fuel Economy" and "Efficiency" in df.columns:
        # breakpoint()#what is in df?
        s = df["Efficiency"].astype(float)
        df[measure] = s.apply(lambda x: None if x == 0 else 100_000.0 / x)
    elif measure == "Final On-Road Fuel Economy" and "Efficiency" in df.columns:
        # breakpoint()#what is in df?
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


# def list_all_measures():
#     """Display config table for easy inspection."""
#     print("=== LEAP MEASURE CONFIG ===")
#     for k, v in LEAP_MEASURE_CONFIG.items():
#         src = v.get("source_mapping")
#         #repalce None with 'N/A' for printing
#         if src is None:
#             src = 'N/A'

#         print(f"{k:30} ← {src:25} | {v['unit']:25} | factor={v['factor']}")

# print("===========================\n")

def calculate_measures(df: pd.DataFrame, measure: str) -> pd.DataFrame:
    """
    Calculate and add specified measures to the DataFrame.
    Currently supports:
      - Stock_share_calc_medium
      - Stock_share_calc_fuel
      - Stock_share_calc_vehicle_type
      - Sales_calc_vehicle_type
      - Sales_calc_medium
      - Activity_share_calc_medium
      - Activity_share_calc_fuel
    The name at the end of the measure is the name of the column that will be grouped until, given the source columns [Transport Type, Medium, Vehicle Type, Drive, Fuel] > where Fuel is actually not a column technically could be derived from Drive.
    So for Stock_share_calc_medium, it will be grouped until medium with the cols [Transport Type], for Stock_share_calc_fuel it will be grouped until fuel with the cols [Transport Type, Medium, Vehicle Type,Drive], and for Stock_share_calc_vehicle_type it will be grouped until vehicle type with the cols [Transport Type, Medium]. etc.
    The calc methods are specific to each measure and are defined within the function.
    """
    df_out = df.copy()
    source_cols = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"]
    for col in source_cols:
        #make col have no spaces and lower case for easier matching
        col_clean = col.replace(" ", "_").lower()
        if col_clean not in measure:
            continue
        group_cols = source_cols[:source_cols.index(col)]
        break
    else:
        # This else belongs to the for loop - executes when no break occurs
        raise ValueError(f"Measure '{measure}' doesn't contain any of the expected columns {source_cols}")
    
    if 'stock_share' in measure.lower():
        # Calculate stock share
        df_out[measure] = df_out.groupby(group_cols)["Stocks"].transform(lambda x: x / x.sum() * 100 if x.sum() != 0 else 0)
    elif 'activity_share' in measure.lower():
        # Calculate activity share - similar to stock share but using Activity column. Note that since passenger and freight km are measured differently, comaprison of aactivity shares isnot valid between passenger and freight modes.
        df_out[measure] = df_out.groupby(group_cols)["Activity"].transform(lambda x: x / x.sum() * 100 if x.sum() != 0 else 0)
   
    elif 'sales_calc' in measure.lower():
        # Calculate sales as the difference in stocks year-over-year. this deliberatly ignores turnover rates.
        df_out = df_out.sort_values(by=group_cols + ["Date"])
        df_out[measure] = df_out.groupby(group_cols)["Stocks"].diff().fillna(0)
    # elif other measures:
    #     df_out[measure] = df_out.groupby(group_cols)["Other Metric"].transform(lambda x: x / x.sum() * 100 if x.sum() != 0 else 0)

    # Add other calculations for the remaining measures here
                
    return df_out[measure]
#     df_out = df.copy()
#     for leap_measure, meta in LEAP_MEASURE_CONFIG.items():
#         src = meta["source_mapping"]
#         if src not in df_out.columns:
#             continue
#         df_out[leap_measure] = apply_scaling(df_out[src], leap_measure)

#     # apply nonlinear relationships afterwards
#     df_out = apply_special_conversions(df_out, "Final Energy Intensity")
#     df_out = apply_special_conversions(df_out, "Fuel Economy")
#     return df_out

# ============================================================
# Source tree utilities
# ============================================================
def get_source_categories(transport_type, medium, vehicle_type=None, drive=None):
    """
    Navigate CSV_TREE dynamically to find all applicable source entries.
    Returns a list of drive/fuel identifiers.
    """
    
    transport_node = CSV_TREE.get(str(transport_type).lower(), {})
    medium_node = transport_node.get(str(medium).lower(), {})

    if not medium_node:
        return []

    candidates = []
    
    # For road modes, dig deeper
    if str(medium).lower() == "road":
        # Handle case when vehicle_type is None or 'all'
        if vehicle_type is None or str(vehicle_type).lower() == "all":
            # Collect from all vehicle types
            for vt_key, vt_value in medium_node.items():
                if isinstance(vt_value, dict):
                    # Handle case when drive is None or 'all'
                    if drive is None or str(drive).lower() == "all":
                        # Get all drives for this vehicle type
                        for drive_values in vt_value.values():
                            if isinstance(drive_values, list):
                                candidates.extend(drive_values)
                    else:
                        # Get specific drive
                        drive_list = vt_value.get(str(drive).lower(), [])
                        if isinstance(drive_list, list):
                            candidates.extend(drive_list)
                elif isinstance(vt_value, list):
                    candidates.extend(vt_value)
        elif isinstance(vehicle_type, list):
            # Handle list of vehicle types
            for vt in vehicle_type:
                vt_node = medium_node.get(str(vt).lower(), {})
                if isinstance(vt_node, dict):
                    if drive is None or str(drive).lower() == "all":
                        # Get all drives for this vehicle type
                        for drive_values in vt_node.values():
                            if isinstance(drive_values, list):
                                candidates.extend(drive_values)
                    else:
                        # Get specific drive
                        drive_list = vt_node.get(str(drive).lower(), [])
                        if isinstance(drive_list, list):
                            candidates.extend(drive_list)
                elif isinstance(vt_node, list):
                    candidates.extend(vt_node)
        else:
            # Single vehicle type
            vehicle_node = medium_node.get(str(vehicle_type).lower(), {})
            if isinstance(vehicle_node, dict):
                if drive is None or str(drive).lower() == "all":
                    # Get all drives for this vehicle type
                    for drive_values in vehicle_node.values():
                        if isinstance(drive_values, list):
                            candidates.extend(drive_values)
                else:
                    # Get specific drive
                    candidates = vehicle_node.get(str(drive).lower(), [])
            elif isinstance(vehicle_node, list):
                candidates = vehicle_node
    else:
        # Non-road medium (might be flat list)
        if isinstance(medium_node, list):
            candidates = medium_node

    # Clean and deduplicate results
    cleaned = []
    for item in candidates:
        if isinstance(item, str):
            cleaned.append(item.lower())
        elif isinstance(item, (list, tuple)):
            cleaned.extend(str(v).lower() for v in item)

    # Deduplicate while preserving order
    return [c for c in dict.fromkeys(cleaned) if c]

def filter_source_dataframe(df, transport, medium, vehicle, drive):
    """
    Filter the source dataset using CSV_TREE hierarchy
    so we only get the relevant rows for a LEAP branch.
    """
    if df.empty:
        return df

    mask = pd.Series(True, index=df.index)

    def _apply(column, value):
        nonlocal mask
        if column not in df.columns:
            mask &= False
            return
        if isinstance(value, list):
            value = [str(v).lower() for v in value if v]
            if value and "all" not in value and "none" not in value:
                mask &= df[column].astype(str).str.lower().isin(value)
        else:
            if value and str(value).lower() != "all" and str(value).lower() != "none":
                mask &= df[column].astype(str).str.lower() == str(value).lower()
    
    _apply("Transport Type", transport)
    _apply("Medium", medium)
    _apply("Vehicle Type", vehicle)

    subset = df[mask].copy()
    if subset.empty:
        return subset

    if "Drive" not in subset.columns:
        return subset

    # match drives/fuels within CSV_TREE
    candidates = set(get_source_categories(transport, medium, vehicle, drive))
    if drive and str(drive).lower() != "all" and str(drive).lower() != "none":
        candidates.add(str(drive).lower())

    if not candidates:
        return subset

    subset["Drive"] = subset["Drive"].astype(str).str.lower()
    return subset[subset["Drive"].isin(candidates)].copy()


def process_measures_for_leap(df: pd.DataFrame, filtered_measure_config: dict, shortname: str) -> dict:
    """
    Applies all scaling and nonlinear conversions (e.g. Efficiency → Intensity/Fuel Economy)
    to prepare a dictionary of processed dataframes keyed by LEAP measure.
    """
    df_out = df.copy()
    processed = {}

    # Apply scaling
    for leap_measure, meta in filtered_measure_config.items():
        src = meta["source_mapping"]
        if src in CALCULATED_MEASURES:
            # Calculate measure if it's a calculated one
            df_out[src] = calculate_measures(df_out, src)
        elif src not in df_out.columns:
            continue
        
        df_out[leap_measure] = apply_scaling(df_out[src], leap_measure, shortname)
        processed[leap_measure] = df_out[["Date", leap_measure]].copy()

    # Apply nonlinear conversions
    if "Final Energy Intensity" in filtered_measure_config.keys():
        df_out = apply_special_conversions(df_out, "Final Energy Intensity")

        processed["Final Energy Intensity"] = df_out[["Date", "Final Energy Intensity"]].copy()

    if "Fuel Economy" in filtered_measure_config.keys():
        df_out = apply_special_conversions(df_out, "Fuel Economy")

        processed["Fuel Economy"] = df_out[["Date", "Fuel Economy"]].copy()
    if 'Final On-Road Fuel Economy' in filtered_measure_config.keys():
        df_out = apply_special_conversions(df_out, 'Final On-Road Fuel Economy')

        processed['Final On-Road Fuel Economy'] = df_out[['Date', 'Final On-Road Fuel Economy']].copy()

    return processed