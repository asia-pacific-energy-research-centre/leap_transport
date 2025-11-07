"""Pre-processing helpers for transport source data prior to LEAP mapping."""

import pandas as pd


def calculate_sales(df):
    # Calculate sales as the difference in stocks year-over-year. this deliberatly ignores turnover rates for simplicity.  Note
    #that this means that sales_calc measures should be calculated before vehicle_sales_share measures.
    group_cols = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"]
    df = df.sort_values(by=group_cols + ["Date", 'Scenario','Economy'])
    df['Sales'] = df.groupby(group_cols)["Stocks"].diff().fillna(0)
    # Convert negative sales to 0 (can happen due to vehicle retirement or data anomalies)
    df['Sales'] = df['Sales'].clip(lower=0)
    return df


def allocate_fuel_alternatives_energy_and_activity(df, economy):
    #note that when biofuel is referred to here it includes other low carbon fuels such as efuels
    #since this system assumes that vehicles that use biofuels (and other alternatives such as efuels) have a separate amount of
    # stocks and activity to their counterparts that use fossil fuels we need to split the energy and activity of the fossil fuel vehicles between the fossil fuel and biofuel variants based on the amount of biofuels used in those vehicles. This will provide a future use as well since we will probably use this function to allocate a ratio of biofuels use to the projections but that is quite hard within leap so we will do it here instead.
    #load in the source df with fuels mapped to
    src_by_fuel = pd.read_csv(r"../../data/bd dummy fuels 20_USA_NON_ROAD_DETAILED_model_output20250421.csv") #Date  EconomyScenario Transport Type  Vehicle Type    Drive   Medium  Fuel    Energy
    # 2022      20_USA  Target  passenger       suv     phev_d  road    17_electricity  0

    #filter for economy and scenario
    src_by_fuel = src_by_fuel[(src_by_fuel["Economy"] == economy) & (src_by_fuel["Scenario"] == 'Reference')]

    #Extract biofules and their counterparts. That is biodiesel/diesel, biogasoline/gasoline, biojet/jet fuel .. we can add more later if needed
    # breakpoint()#extract the fuels and put them through copilot:
    # fuels = src_by_fuel['Fuel'].unique()
    fuel_mappings = {
        '07_07_gas_diesel_oil': 'Diesel',
        '07_01_motor_gasoline': 'Gasoline',
        '07_x_jet_fuel': 'Jet fuel',
        '08_01_natural_gas': 'Natural gas',
        '16_05_biogasoline': 'Biogasoline',
        '16_06_biodiesel': 'Biodiesel',
        '16_07_bio_jet_kerosene': 'Biojet',
        '16_01_biogas': 'Biogas',
        '16_x_efuel': 'Efuel',
    }
    biofuel_fuel_map = {
        'Biodiesel': 'Diesel',
        'Biogasoline': 'Gasoline',
        'Biojet': 'Jet fuel',
        'Biogas': 'Natural gas',
        'Efuel-g': 'Gasoline',  #efuels repale multiple fuels. not sure how this will work yet
        'Efuel-d': 'Diesel',
        'Efuel-j': 'Jet fuel',
    }
    efuel_drive_to_fuel_mappings = {#since we reocrded efuels as a single fuel but they actually replace multiple fuels we need to map them based on drive type that the efuel is used in
        'phev_d': 'Efuel-d',
        'phev_g': 'Efuel-g',
        'ship_diesel': 'Efuel-d',
        'rail_diesel': 'Efuel-d',
        'ice_d': 'Efuel-d',
        'ice_g': 'Efuel-g',
        'air_jet_fuel': 'Efuel-j',
        'air_kerosene': 'Efuel-j',
        'air_av_gas': 'Efuel-j',
    }

    #extract only those that are within fuel_mappings.keys()
    src_by_fuel = src_by_fuel[src_by_fuel['Fuel'].isin(fuel_mappings.keys())]
    src_by_fuel['Mapped_Fuel'] = src_by_fuel['Fuel'].map(fuel_mappings)
    #map efuels based on drive type
    efuel_mask = src_by_fuel['Fuel'] == '16_x_efuel'
    src_by_fuel.loc[efuel_mask, 'Mapped_Fuel'] = src_by_fuel.loc[efuel_mask, 'Drive'].map(efuel_drive_to_fuel_mappings)
    
    #now create a sep df of low carbon fuels and a sep df of their counterparts
    biofuel_df = src_by_fuel[src_by_fuel['Mapped_Fuel'].isin(biofuel_fuel_map.keys())].copy()
    fossilfuel_df = src_by_fuel[src_by_fuel['Mapped_Fuel'].isin(biofuel_fuel_map.values())].copy()
    #pivot the biofuel df to have fuels as columns
    biofuel_pivot = biofuel_df.pivot(index=['Economy', 'Scenario','Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], columns='Mapped_Fuel', values='Energy').reset_index()
    #merge the fossil fuel df with the biofuel pivot to get the amount of biofuels used
    merged_df = pd.merge(fossilfuel_df, biofuel_pivot, on=['Economy', 'Scenario','Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], suffixes=('_fossil', '_bio'))
    
    #now we should have a df with columns: Date, Transport Type, Vehicle Type, Drive, Medium, Fuel (fossil), Energy (fossil), Biodiesel, Biogasoline, Biojet...etc
    #calculate total energy = fossil + lowcarbonfuels
    merged_df['Total_Energy'] = merged_df['Energy'].fillna(0) + merged_df[list(biofuel_fuel_map.keys())].sum(axis=1, skipna=True)
    #and then calc the share of each fuel and put it in the original df
    for biofuel, fossilfuel in biofuel_fuel_map.items():
        merged_df[f'{biofuel}_share'] = merged_df[biofuel].fillna(0) / merged_df['Total_Energy'].replace(0, pd.NA)
    #now apply these shares to the original df
    #fist we need to gett the oriignal df ready > we can do that by extracting every row which is for a fossilfuel with a biofuel counterpart and creating its counterpart:
    for biofuel, fossilfuel in biofuel_fuel_map.items():
        df_fossil = df[df['Fuel'] == fossilfuel].copy()
        df_bio = df_fossil.copy()
        df_bio['Fuel'] = biofuel
        # Activity-dependent variables that should be set to 0
        measure_cols_to_set_the_same_as_fossil = [
            'Efficiency', 
            'Occupancy_or_load', 
            'New_vehicle_efficiency', 
            'Turnover_rate', 
            'Activity_per_Stock', 
            'Mileage', 
            'Average_age', 
            'Intensity', 
            'Activity_efficiency_improvement', 
            'Non_road_intensity_improvement', 
            'Activity_growth',
            "Gdp_per_capita", # GDP per capita levels are same across combinations
            "Gdp",
            "Population", # Population levels
            "Age_distribution",
        ]
        measure_cols_to_divvy = [
            "Energy",  # Direct energy consumption
            "Stocks_old",  # Historical stock levels
            "Activity",  # Transport activity levels
            "Travel_km",  # Total travel kilometers
            "Stocks",  # Current stock levels
            "Surplus_stocks",  # Excess stock levels
            "Stocks_per_thousand_capita",  # Stock density (activity-related)
            "Vehicle_sales_share",  # Sales shares (activity-dependent)
            "Stock_turnover",  # Actual turnover (vs rate)
            "New_stocks_needed"  # New stock requirements
        ]
        
        df_fossil = df_fossil.rename(columns={col: f'Fossil_{col}' for col in measure_cols_to_divvy + measure_cols_to_set_the_same_as_fossil})
        #merge the two dfs
        merged_final = pd.merge(df_bio, df_fossil, on=['Economy', 'Scenario', 'Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], suffixes=('_bio', '_fossil'))
        #merge with merged_df to get the shares
        merged_final = pd.merge(merged_final, merged_df[['Economy', 'Scenario', 'Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'] + [f'{biofuel}_share']], on=['Economy', 'Scenario', 'Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'])
        #now calculate the values
        for col in measure_cols_to_divvy:
            merged_final[col] = merged_final[f'Fossil_{col}'] * merged_final[f'{biofuel}_share']
        for col in measure_cols_to_set_the_same_as_fossil:
            merged_final[col] = merged_final[f'Fossil_{col}']
        merged_final_ff_copy = merged_final.copy()
        merged_final_ff_copy['Fuel'] = fossilfuel
        merged_final['Fuel'] = biofuel
        #update the original df with the new biofuel rows
        biofuel_rows = merged_final[df.columns]
        merged_final_ff_copy = merged_final_ff_copy[df.columns.tolist() + [f'{biofuel}_share']]
        
        # breakpoint()#now we need to update the fossil fuel rows to be the remainder after biofuels have been allocated
        df = pd.concat([df, biofuel_rows], ignore_index=True)
        #merge on the shares to update the fossil fuel rows
        df = pd.merge(df, merged_final_ff_copy[['Economy', 'Scenario', 'Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium', f'{biofuel}_share']], on=['Economy', 'Scenario', 'Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], how='left')
        #however now we also need to update the fossil fuel rows to be the remainder after biofuels have been allocated
        df_fossil_update = df[df['Fuel'] == fossilfuel].copy()
        for col in measure_cols_to_divvy:
            df_fossil_update[col] = df_fossil_update[col] * (1 - df_fossil_update[f'{biofuel}_share'])
        # Only update columns that exist in df_fossil_update
        columns_to_update = [col for col in df.columns if col in df_fossil_update.columns]
        df.update(df_fossil_update[columns_to_update])
        df.update(df_fossil_update[df.columns])
        #drop the share column
        df = df.drop(columns=[f'{biofuel}_share'], errors='ignore')
    # breakpoint()#need to rename efuels to E-fuel in the df
    df.loc[df['Fuel'] == 'Efuel-g', 'Fuel'] = 'Efuel'
    df.loc[df['Fuel'] == 'Efuel-d', 'Fuel'] = 'Efuel'
    df.loc[df['Fuel'] == 'Efuel-j', 'Fuel'] = 'Efuel'
        
    return df


__all__ = [
    "calculate_sales",
    "allocate_fuel_alternatives_energy_and_activity",
]
