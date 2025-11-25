"""Processing helpers for LEAP transport measure preparation.

This module groups the computational utilities used to transform source
transport data into LEAP-ready measures. It intentionally avoids the
static metadata structures so consumers can import only the behavior
they require.
"""

import pandas as pd

from basic_mappings import SOURCE_CSV_TREE
from transport_measure_catalog import (
    get_leap_measure,
    get_source_unit,
    get_weight_priority,
)
from transport_measure_metadata import (
    AGGREGATION_BASE_MEASURES,
    AGGREGATION_RULES,
    CALCULATED_MEASURES,
)


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
    
    if "inverse" in meta and meta["inverse"]:
        series = 1 / (series.replace(0, pd.NA)*src_scale)
        total_scale = factor
        return series * total_scale
    else:
        # total scaling = (source scale to base) * (LEAP-level adjustment)
        total_scale = src_scale * factor
        return series * total_scale


def aggregate_weighted(df, measure, group_cols, weight_col=None):
    """
    Perform weighted average aggregation using groupby on specified columns.

    Args:
        df: DataFrame containing data
        measure: Column to aggregate
        group_cols: List of columns to group by
        weight_col: Column to use as weights (defaults to priority list)

    Returns:
        Series with weighted average values
    """
    if df.empty or measure not in df.columns:
        return df

    # Find an appropriate weight column if not specified
    weight_col = next(
        (
            w
            for w in get_weight_priority(measure)
            if w in df.columns and (df[w].sum() > 0 and not df[w].isnull().all())
        ),
        None,
    )
    #if none then callculate unweighted mean
    # if measure == 'Intensity' or measure  == 'Final Energy Intensity':
    #     breakpoint()  # how to handle energy intensity for air

    if not weight_col or weight_col not in df.columns:
        df.loc[:, measure] = df[measure].mean()
        return df
    # breakpoint()
    # Create weighted values
    df_copy = df.copy()
    
    df_copy.loc[:, '_weighted_value'] = (
        df_copy[measure].fillna(0).infer_objects(copy=False)
        * df_copy[weight_col].fillna(0).infer_objects(copy=False)
    )
    df_copy.loc[:, '_weight'] = df_copy[weight_col].fillna(0).infer_objects(copy=False)
    # manual groupby with weighted calculation on the copy (easier to debug and safe)
    grouped = df_copy.groupby(group_cols)
    result = []
    
    for name, group in grouped:
        weight_sum = group['_weight'].sum()
        if weight_sum > 0:
            weighted_avg = group['_weighted_value'].sum() / weight_sum
        else:
            weighted_avg = group[measure].mean()
        
        # Create result for this group
        group_result = group.copy()
        group_result[measure] = weighted_avg
        result.append(group_result)
    
    df = pd.concat(result, ignore_index=True)
    # if all of result is 0 then raise a warning
    if (df[measure] == 0).all():
        # breakpoint()#to do. how to make this not be 0 if activity is all 0.
        print(
            f"[WARNING] Weighted aggregation of '{measure}' resulted in all zeros. Check weight column '{weight_col}' for validity."
        )
    # Clean up temporary columns (ignore if missing)
    df = df.drop(columns=['_weighted_value', '_weight'], errors='ignore')
    return df


def calculate_measures(df: pd.DataFrame, measure: str) -> pd.DataFrame:
    """
    Calculate and add specified measures to the DataFrame.
    Currently supports:
      - Stock_share_calc_transport_type
      - Stock_share_calc_fuel
      - Stock_share_calc_vehicle_type
      - Activity_share_calc_transport_type
      - Activity_share_calc_fuel
        - Vehicle_sales_share_calc_transport_type
        - Vehicle_sales_share_calc_fuel
        - Vehicle_sales_share_calc_vehicle_type
    The name at the end of the measure is the name of the column that will be grouped until, given the source columns [Transport
    Type, Medium, Vehicle Type, Drive, Fuel]
    So for Stock_share_calc_transport_type, it will be grouped until medium with the cols [Transport Type], for Stock_share_calc
    _fuel it will be grouped until fuel with the cols [Transport Type, Medium, Vehicle Type,Drive], and for Stock_share_calc_vehicle
    _type it will be grouped until vehicle type with the cols [Transport Type, Medium]. etc. > this way when we want the stock share
    for bevs within the car segment, then we get the cols for [Transport Type, Medium, Vehicle Type] and calculate the share of stocks
    for bevs within that group.
    The calc methods are specific to each measure and are defined within the function.
    """
    df_out = df.copy()
    source_cols = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"]
    for col in source_cols:
        #make col have no spaces and lower case for easier matching
        col_clean = col.replace(" ", "_").lower()
        if col_clean not in measure:
            continue
        group_cols = ['Date'] + source_cols[:source_cols.index(col)]
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

        # elif 'sales_calc' in measure.lower():
        #     # Calculate sales as the difference in stocks year-over-year. this deliberatly ignores turnover rates for simplicity. Note that this means that sales_calc measures should be calculated before vehicle_sales_share measures.
        #     df_out = df_out.sort_values(by=group_cols + ["Date"])
        #     # Group by all source columns including Date for proper sales calculation
        #     # This ensures we're tracking stock changes for each unique vehicle category
        #     all_group_cols = group_cols + [col for col in source_cols if col not in group_cols]
        #     df_out[measure] = df_out.groupby(all_group_cols)["Stocks"].diff().fillna(0)
        #     # Convert negative sales to 0 (can happen due to vehicle retirement or data anomalies)
        #     df_out[measure] = df_out[measure].clip(lower=0)

    elif 'vehicle_sales_share' in measure.lower():
        #we will just calcaulte sales share as the share of sales within the group cols.
        if 'Sales' not in df_out.columns:
            raise ValueError(f"Measure '{measure}' requires Sales to be calculated first.")

        df_out[measure] = df_out.groupby(group_cols)['Sales'].transform(lambda x: x / x.sum() * 100 if x.sum() != 0 else 0)
    # elif 'vehicle_sales_share' in measure.lower():
    #     # Calculate vehicle sales share - similar to stock share but using Sales column.
    #     df_out[measure] = df_out.groupby(group_cols)["Sales"].transform(lambda
    # elif other measures:
    #     df_out[measure] = df_out.groupby(group_cols)["Other Metric"].transform(lambda x: x / x.sum() * 100 if x.sum() != 0 else 0)

    # Add other calculations for the remaining measures here

    return df_out[measure]


def get_source_categories(transport_type, medium, vehicle_type=None, drive=None):
    """
    Navigate SOURCE_CSV_TREE dynamically to find all applicable source entries.
    Returns a list of drive/fuel identifiers.
    """

    transport_node = SOURCE_CSV_TREE.get(str(transport_type).lower(), {})
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


def filter_source_dataframe_by_categories(df, columns, categories):
    """
    Filter dataframe based on category hierarchy.

    Args:
        df_out: DataFrame to filter
        source_cols_for_grouping: List of grouping columns (will be populated by this function)
        category_hierarchy: List of categories [ttype, medium, vtype, drive, fuel]

    Returns:
        Filtered dataframe
    """
    category_to_column = {
        0: "Transport Type",
        1: "Medium",
        2: "Vehicle Type",
        3: "Drive",
        4: "Fuel"
    }
    filter_cols = [category_to_column[i] for i in range(len(columns))]
    for col, cat in zip(filter_cols, categories):
        if cat is not None:
            df = df[df[col] == cat]
    return df


def aggregate_measures(df_out, src, source_cols_for_grouping, ttype, medium, vtype, drive, fuel):
    # Check if we need to aggregate the data
    source_cols_for_grouping_no_date = source_cols_for_grouping.copy()
    source_cols_for_grouping_no_date.remove('Date')  # we will add date back in later
    if not (len(df_out) > 1 and src in AGGREGATION_RULES):
        breakpoint()
        raise ValueError(f"No aggregation rule defined for source measure: {src}.")
    
    agg_type = AGGREGATION_RULES.get(src)

    ####
    # #find the first category within ttype, medium, vtype, drive, fuel that is None, then filter so the df matches all the categories up to that point, minus 1. e.g. if medium is None, then dont filter at all. if drive is None, then filter for ttype and medium only. This allows for enough data to be able to aggregate shares of the level, weights and so on.
    # category_hierarchy = [ttype, medium, vtype, drive, fuel]
    # for i, category in enumerate(category_hierarchy):
    #     if category is None:
    #         source_cols_for_grouping_minus_one = source_cols_for_grouping[:i]
    #         break
    #     # If we reach here, it means the category is valid
    #     source_cols_for_grouping_minus_one.append(category)
    
    source_cols_for_grouping_minus_one_no_date = source_cols_for_grouping_no_date[:-1]
    source_cols_for_grouping_minus_one = source_cols_for_grouping[:-1]
    category_hierarchy_minus_one = [ttype, medium, vtype, drive, fuel][:len(source_cols_for_grouping_minus_one_no_date)]
    category_hierarchy = [ttype, medium, vtype, drive, fuel][:len(source_cols_for_grouping_minus_one_no_date) + 1]
    #now filter.
    df_filtered = filter_source_dataframe_by_categories(
        df_out,
        source_cols_for_grouping_minus_one_no_date,
        category_hierarchy_minus_one,
    )

    ####
    # if source_cols_for_grouping == ['Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive'] and drive == 'erev' and vtype == 'ht':
    #     breakpoint()#checking why erev heavy trucks are getting wrong aggregation
    if agg_type == "weighted":
        # For weighted average, we need to find appropriate weight column
        # breakpoint()#not sure how this works
        weight_col = next((w for w in get_weight_priority(src) if w in df_filtered.columns), None)
        
        # df_filtered2 = df_filtered.copy()
        if weight_col:
            # For weighted average aggregation
            df_filtered = aggregate_weighted(
                df_filtered,
                src,
                group_cols=source_cols_for_grouping_minus_one,
                weight_col=weight_col,
            )
            # #if any of the values are < 0.4 then try again since its kinda weird
            # if ((df_filtered[src] < 0.4).any() and (df_filtered['Medium'] =='road')).any():
            #     breakpoint()
            #     print(f"[WARNING] Weighted aggregation of '{src}' has values < 0.4, trying unweighted mean instead.")
            #     df_filtered2 = aggregate_weighted(
            #     df_filtered,
            #     src,
            #     group_cols=source_cols_for_grouping_minus_one,
            #     weight_col=weight_col,
            #     )
        else:
            raise ValueError(f"No suitable weight column found for weighted aggregation of '{src}'")
    elif agg_type == "sum":
        # Simple sum aggregation
        #drop the latest col from source_cols_for_grouping in case we have multiple categories within it and want to sum over them. e.g. where LPV corresponds to car,suv,lt.
        df_filtered_copy = df_filtered.copy()  # Ensure we have a clean copy
        df_filtered.loc[:, src] = df_filtered.groupby(source_cols_for_grouping)[src].transform('sum')

    elif agg_type == "share":
        #TEMP
        #lets try fil it with the base measure first then once we have that recorded for all rows we can calculate the share, instead of trying to do it all sequentially. e.g. if measure is stock share and base measure is stocks, then first fill in stocks for all rows, then afgter we've filled in all rows for all medium/ttype/etc rows we will calculate shares based on that.
        base_measure = AGGREGATION_BASE_MEASURES.get(src, None)
        if base_measure in df_filtered.columns:     
            #ignore the warning for the setting with copy since we are working on a filtered df anyway and the alternative results in FutureWarning: Setting an item of incompatible dtype is deprecated and will raise in a future error of pandas.
            df_filtered[src] = df_filtered[base_measure]
            #group and sum
            df_filtered = df_filtered.groupby(source_cols_for_grouping)[src].sum().reset_index()
            #now calculate share
        else:
            raise ValueError(
                f"Base measure '{base_measure}' not found in DataFrame for share calculation of '{src}'"
            )
            
        # # if len(source_cols_for_grouping_minus_one)>3:
        # #     #what happens when we try to do this for drives shares?
        # #     breakpoint()#MAYBE THIS SHOULD BE DONE USING .apply(
        # #     #     lambda x: x['_weighted_value'].sum() / x['_weight'].sum() if x['_weight'].sum() > 0 else 0
        # #     # )
        # # Convert to percentage share within each group
        # # breakpoint()#i think thisis way off in its scope. 
        # try:
        #     # For share calculation, use the original base measure before conversion
        #     base_measure = AGGREGATION_BASE_MEASURES.get(src, None)
        #     if base_measure in df_filtered.columns:
        #         df_filtered_copy = df_filtered[source_cols_for_grouping + [base_measure]].copy()
        #         #first sum by the group cols .. then we will calculate share within that group minus one col.
        #         #calc sum of measure
        #         df_filtered_copy = df_filtered_copy.groupby(source_cols_for_grouping).sum().reset_index()
                
        #         # Option 3: Replace NAs with 0 before calculation
        #         df_filtered_copy[src] = df_filtered_copy.groupby(
        #             source_cols_for_grouping_minus_one
        #         )[base_measure].transform(
        #             lambda x: (x.fillna(0) / x.fillna(0).sum() * 100) if x.fillna(0).sum() != 0 else x.fillna(0)
        #         )
        #         #drop src from original df_filtered if it exists
        #         if src in df_filtered.columns:
        #             df_filtered = df_filtered.drop(columns=[src])
        #         df_filtered_copy = df_filtered_copy[source_cols_for_grouping + [src]].copy()
        #         #since we lost the index from the groupby, we need to merge back to the original filtered df
        #         df_filtered = pd.merge(
        #             df_filtered,    df_filtered_copy,
        #             on=source_cols_for_grouping,
        #             how='left'
        #         )
        #     else:
        #         breakpoint()
        #         raise ValueError(
        #             f"Base measure '{base_measure}' not found in DataFrame for share calculation of '{src}'"
        #         )
        # except Exception as e:
        #     breakpoint()
    else:
        pass
    # ["passenger", "road", "lt", "hev"]
    #filter for the exact categories again to ensure no extra rows remain after aggregation
    df_filtered = filter_source_dataframe_by_categories(
        df_filtered,
        source_cols_for_grouping_no_date,
        category_hierarchy,
    )
    if len(df_filtered) == 0:
        breakpoint()
        raise ValueError(f"[ERROR] No data remaining after aggregation for source measure: {src}. Check filtering criteria.")
    
    df_filtered = df_filtered[source_cols_for_grouping + [src]].copy()
    #drop duplicates if any remain after aggregation
    df_filtered = df_filtered.drop_duplicates(
        subset=source_cols_for_grouping + [src],
        keep='first',
    )
    #double check there are no duplicates after aggregation. this would indicate a problem where we have multiple rows with the same values for the grouping columns after aggregation. I think its impossible but just in case.
    if df_filtered.duplicated(subset=source_cols_for_grouping).any():
        duplicated_dates = df_filtered[
            df_filtered.duplicated(
                subset=source_cols_for_grouping,
                keep=False,
            )
        ]
        print(f"[WARNING] Found {len(duplicated_dates)} rows with duplicate dates for {src}:")
        for date, group in duplicated_dates.groupby('Date'):
            print(f"  Date {date}: {len(group)} occurrences")
            print(group[source_cols_for_grouping + [src]])
        breakpoint()
        raise ValueError(f"[ERROR] Duplicate years found after aggregation for source measure: {src}.")
    
    return df_filtered


def process_measures_for_leap(
    df: pd.DataFrame,
    filtered_measure_config: dict,
    shortname: str,
    source_cols_for_grouping: list,
    ttype: str,
    medium: str,
    vtype: str,
    drive: str,
    fuel: str,
    src_tuple: tuple,
) -> dict:
    """
    Applies all scaling and nonlinear conversions (e.g. Efficiency → Intensity/Fuel Economy)
    to prepare a dictionary of processed dataframes keyed by LEAP measure.
    Note that this is done on a whole dataset that hasnt been filtered so we have access to all possible data that might be needed, e.g. for shares or weighted values. When the filtering is required we will use
        filter_source_dataframe_by_categories(df, columns, categories)

    """
    processed = {}
    print(f"Processing measures for LEAP branch: {shortname}")
    # Apply scaling
    for leap_measure, meta in filtered_measure_config.items():
        # if leap_measure == 'Final On-Road Fuel Economy':
        #     breakpoint()  # investigate why large values are occuring

        #todo want to create a method here for identifying if the src_tuple maps to a category which is mapped to multiple soruce categories. e.g. if src_tuple is ('freight','road','lcv','ice') then this maps to both ice_d, ice_g. Then we should calculate the measures using the aggregated data for both ice_d and ice_g combined rather than just ice_d or ice_g individually.
        def get_aggregated_categories(src_tuple):
            # This is a placeholder for the actual logic to find all relevant categories
            # For now, it just returns the original tuple
            return [src_tuple]
        # if ttype == 'Nonspecified transport' and leap_measure == 'Final Energy Intensity':
        #     breakpoint()  # investigate why 1000 is occuring
        # if drive == 'bev' and leap_measure == 'Stock Share':
        #     breakpoint()  # investigate why 1000 is occuring#why is the stock share ended up as >1
        
        df_out = df.copy()
        print('Recording measure:', leap_measure)
        src = meta["source_mapping"]
        if src in CALCULATED_MEASURES:
            try:
                # Calculate measure if it's a calculated one
                df_out.loc[:, src] = calculate_measures(df_out, src)
            except Exception as e:
                print(f"[ERROR] Exception while calculating {src}: {e}")
                breakpoint()

                df_out.loc[:, src] = calculate_measures(df_out, src)
        elif src not in df_out.columns:
            continue
        
        df_out = aggregate_measures(df_out, src, source_cols_for_grouping, ttype, medium, vtype, drive, fuel)
        
        # if source_cols_for_grouping == ['Date', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive'] and drive == 'erev' and vtype == 'ht':
        #     breakpoint()#checking why erev heavy trucks are getting wrong aggregation

        df_out[leap_measure] = apply_scaling(df_out.loc[:, src], leap_measure, shortname)
        processed[leap_measure] = df_out[["Date", leap_measure]].copy()
    return processed


__all__ = [
    "apply_scaling",
    "aggregate_weighted",
    "calculate_measures",
    "get_source_categories",
    "filter_source_dataframe_by_categories",
    "aggregate_measures",
    "process_measures_for_leap",
]
