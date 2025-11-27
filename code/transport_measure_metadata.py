"""Structured metadata for LEAP transport measures.

This module centralizes static dictionaries describing units, scaling
rules, weighting priorities, aggregation approaches, and helper lookups
used across the transport processing pipeline. Keeping the metadata
separate from procedural code simplifies maintenance and enables reuse
without importing heavier processing dependencies.
"""

# Unit + scaling lookup used for converting raw data from the source
# dataset (i.e. the 9th edition dataset) to its equivalent scaled to 1.
# i.e. stocks are measured in millions in our source data, so we need to
# multiply by 1e6 to get the actual number of stocks - thereby making it
# useful for LEAP and calculations. Not all measures here are used in the
# LEAP measure config, but they are included for completeness.
SOURCE_MEASURE_TO_UNIT = {
    "Energy": ("PJ", 1),
    "Stocks": ("Million_stocks", 1e6),
    "Sales": ("Million_vehicles_sold", 1e6),
    "New_vehicle_efficiency": ("Billion_km_per_pj", 1e9),
    "Efficiency": ("Billion_km_per_pj", 1e9),
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
    "Intensity": ("PJ_per_billion_passenger_or_freight_tonne_km", 1/1e9),
    "Gdp": ("Real_gdp_millions", 1e6),
    "Population": ("Population_thousands", 1e3),
    "Gdp_per_capita": ("Thousand_Gdp_per_capita", 1e3),
    "Stocks_per_thousand_capita": ("Stocks_per_thousand_capita", 1),
    "Average_age": ("Age", 1),
}

DEFAULT_WEIGHT_PRIORITY = ["Activity"]

# Used for aggregating measures that need weighting when being mapped to
# LEAP measures. If None then don't use a weight.
SOURCE_WEIGHT_PRIORITY = {
    "Efficiency": ["Activity", "Stocks", None],
    "Mileage": ["Stocks", "Activity", None],
    "Intensity": ["Activity", "Stocks", None],
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

    # Shares or rates (usually normalize to sum to 100)
    "Vehicle_sales_share": "share",
    "Supply_side_fuel_share": "share",
    "Demand_side_fuel_share": "share",
    "Turnover_rate": "share",
    "Activity_share_calc_transport_type": "share",
    "Activity_share_calc_fuel": "share",
    "Stock_share_calc_transport_type": "share",
    "Stock_share_calc_fuel": "share",
    "Stock_share_calc_vehicle_type": "share",
    "Sales": "sum",
    "Vehicle_sales_share_calc_transport_type": "share",
    "Vehicle_sales_share_calc_fuel": "share",
    "Vehicle_sales_share_calc_vehicle_type": "share",
    
    # Growth rates (apply later via derivative logic)
    "Activity_growth": "growth",
    "Efficiency_growth": "growth",
}

AGGREGATION_BASE_MEASURES = {
    "Stock_share_calc_transport_type": "Stocks",
    "Stock_share_calc_fuel": "Stocks",
    "Stock_share_calc_vehicle_type": "Stocks",
    "Sales_calc_vehicle_type": "Stocks",
    "Activity_share_calc_transport_type": "Activity",
    "Activity_share_calc_fuel": "Activity",
    "Vehicle_sales_share_calc_transport_type": "Stocks",
    "Vehicle_sales_share_calc_fuel": "Stocks",
    "Vehicle_sales_share_calc_vehicle_type": "Stocks",
}

CALCULATED_MEASURES = [
    "Stock_share_calc_transport_type",
    "Stock_share_calc_fuel",
    "Stock_share_calc_vehicle_type",
    "Sales_calc_vehicle_type",
    "Sales_calc_medium",
    "Activity_share_calc_transport_type",
    "Activity_share_calc_fuel",
    "Vehicle_sales_share_calc_transport_type",
    "Vehicle_sales_share_calc_fuel",
    "Vehicle_sales_share_calc_vehicle_type",
]

SHORTNAME_TO_ANALYSIS_TYPE = {
    "Transport type (road)": "Stock",
    "Vehicle type (road)": "Stock",
    "Technology (road)": "Stock",
    "Fuel (road)": "Stock",
    "Transport type (non-road)": "Intensity",
    "Vehicle type (non-road)": "Intensity",
    "Fuel (non-road)": "Intensity",
    "Others (level 1)": "Intensity",
    "Others (level 2)": "Intensity",
}

SHARE_MEASURES = ["Stock Share", "Device Share", "Sales Share", 'Activity Level',]

__all__ = [
    "SOURCE_MEASURE_TO_UNIT",
    "DEFAULT_WEIGHT_PRIORITY",
    "SOURCE_WEIGHT_PRIORITY",
    "AGGREGATION_RULES",
    "AGGREGATION_BASE_MEASURES",
    "CALCULATED_MEASURES",
    "SHORTNAME_TO_ANALYSIS_TYPE",
    "SHARE_MEASURES",
]
