
def allocate_fuel_alternatives_energy_and_activity(df, economy):
    #since this system assumes that vehicles that use biofuels (and other alternatives such as efuels) have a separate amount of stocks and activity to their counterparts that use fossil fuels we need to split the energy and activity of the fossil fuel vehicles between the fossil fuel and biofuel variants based on the amount of biofuels used in those vehicles. This will provide a future use as well since we will probably use this function to allocate a ratio of biofuels use to the projections but that is quite hard within leap so we will do it here instead.
    #load in the source df with fuels mapped to
    src_by_fuel = pd.read_excel(r"../../data/bd dummy fuels 20_USA_NON_ROAD_DETAILED_model_output20250421.xlsx") #Date	Economy	Scenario	Transport Type	Vehicle Type	Drive	Medium	Fuel	Energy
    # 2022	20_USA	Target	passenger	suv	phev_d	road	17_electricity	0
    
    #filter for economy and scenario
    src_by_fuel = src_by_fuel[(src_by_fuel["Economy"] == economy) & (src_by_fuel["Scenario"] == 'Reference')]
    
    #Extract biofules and their counterparts. That is biodiesel/diesel, biogasoline/gasoline, biojet/jet fuel .. we can add more later if needed
    breakpoint()#extract the fuels and put them through copilot:
    fuels = src_by_fuel['Fuel'].unique()
    fuel_mappings = {
        '07_07_gas_diesel_oil': 'Diesel',
        '07_01_motor_gasoline': 'Gasoline', 
        '07_x_jet_fuel': 'Jet fuel',
        '16_05_biogasoline': 'Biogasoline',
        '16_06_biodiesel': 'Biodiesel',
        '16_07_bio_jet_kerosene': 'Biojet'
    }
    biofuel_fuel_map = {
        'Biodiesel': 'Diesel',
        'Biogasoline': 'Gasoline',
        'Biojet': 'Jet fuel'            
    }
    #extract only those that are within fuel_mappings.keys()
    src_by_fuel = src_by_fuel[src_by_fuel['Fuel'].isin(fuel_mappings.keys())]
    src_by_fuel['Mapped_Fuel'] = src_by_fuel['Fuel'].map(fuel_mappings)
    #now create a sep df of low carbon fuels and a sep df of their counterparts
    biofuel_df = src_by_fuel[src_by_fuel['Mapped_Fuel'].isin(biofuel_fuel_map.keys())].copy()
    fossilfuel_df = src_by_fuel[src_by_fuel['Mapped_Fuel'].isin(biofuel_fuel_map.values())].copy()
    #pivot the biofuel df to have fuels as columns
    biofuel_pivot = biofuel_df.pivot(index=['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], columns='Mapped_Fuel', values='Energy').reset_index()
    #merge the fossil fuel df with the biofuel pivot to get the amount of biofuels used
    merged_df = pd.merge(fossilfuel_df, biofuel_pivot, on=['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], suffixes=('_fossil', '_bio'))
    #now we should have a df with columns: Date, Transport Type, Vehicle Type, Drive, Medium, Fuel (fossil), Energy (fossil), Biodiesel, Biogasoline, Biojet...etc
    #calculate total energy = fossil + lowcarbonfuels
    merged_df['Total_Energy'] = merged_df['Energy'].fillna(0) + merged_df[list(biofuel_fuel_map.keys())].sum(axis=1, skipna=True)
    #and then calc the share of each fuel and put it in the original df
    for biofuel, fossilfuel in biofuel_fuel_map.items():
        merged_df[f'{biofuel}_share'] = merged_df[biofuel].fillna(0) / merged_df['Total_Energy'].replace(0, pd.NA)
    #now apply these shares to the original df
    #fist we need to gett the oriignal df ready > we can do that by extracting every row which is for a biofuel and merging it with its counterpart:
    for biofuel, fossilfuel in biofuel_fuel_map.items():
        df_bio = df[df['Fuel'] == biofuel].copy()
        df_fossil = df[df['Fuel'] == fossilfuel].copy()
        
        # Activity-dependent variables that should be set to 0
        measure_cols_to_set_the_same_as_fossil = [
            
            "Average_age",  # Fleet average age needs to be 0 since we have no activity therefore no stocks
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
        merged_final = pd.merge(df_bio, df_fossil, on=['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'], suffixes=('_bio', '_fossil'))
        #merge with merged_df to get the shares
        merged_final = pd.merge(merged_final, merged_df[['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'] + [f'{biofuel}_share']], on=['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'])
        #now calculate the values
        for col in measure_cols_to_divvy:
            merged_final[col] = merged_final[f'Fossil_{col}'] * merged_final[f'{biofuel}_share']
        for col in measure_cols_to_set_the_same_as_fossil:
            merged_final[col] = merged_final[f'Fossil_{col}']
        #update the original df
        df.update(merged_final[df.columns])
        
        #however now we also need to update the fossil fuel rows to be the remainder after biofuels have been allocated
        df_fossil_update = df[df['Fuel'] == fossilfuel].copy()
        df_fossil_update = pd.merge(df_fossil_update, merged_df[['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'] + [f'{biofuel}_share']], on=['Date', 'Transport Type', 'Vehicle Type', 'Drive', 'Medium'])
        for col in measure_cols_to_divvy:
            df_fossil_update[col] = df_fossil_update[col] * (1 - df_fossil_update[f'{biofuel}_share'])  
        df.update(df_fossil_update[df.columns])
        
    return df
    