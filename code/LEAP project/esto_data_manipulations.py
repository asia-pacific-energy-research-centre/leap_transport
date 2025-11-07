
def extract_esto_sector_fuels_for_leap_branches(leap_branch_list):
    """
    Does a backwards search on the ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP to find what keys have the leap branches in their values. this is mostly useful for the Others Levels 1 and 2 mappings where we have a many-to-one mapping.
    """
    leap_branch_to_esto_sector_fuel = {}
    for leap_branch in leap_branch_list:
        leap_branch_to_esto_sector_fuel[leap_branch] = []
        for esto_sector_fuel, leap_branches2 in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.items():
            if leap_branch in leap_branches2:
                leap_branch_to_esto_sector_fuel[leap_branch].append(esto_sector_fuel)
    return leap_branch_to_esto_sector_fuel

def extract_other_type_rows_from_esto_and_insert_into_transport_df(df, base_year, final_year, economy, TRANSPORT_ESTO_BALANCES_PATH = '../../data/all transport balances data.xlsx'):
    """Extract 'Other' shortname rows from ESTO and insert them into the transport dataframe."""
    
    #and insert the 'Other' shortname rows. These are those under the Other level 1 and level 2 in SHORTNAME_TO_LEAP_BRANCHES  and are basically rows that arent in this transport dataset because they were modelled separately. However to make it easy to use the same code to load them into LEAP we create rows for them here with activity levels equal to their enertgy use in the ESTO dataset and intensity=1. They will then have energy use = activity level * intensity = activity level = esto energy use. We can access their ESTO energy use from the ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP using extract_esto_sector_fuels_for_leap_branches(leap_branch_list) where leap_branch_list is the list of leap branches for the 'Other' shortnames
    
    #load esto dataset 
    esto_energy_use = pd.read_excel(TRANSPORT_ESTO_BALANCES_PATH)     
    other_shortnames = [sn for sn in SHORTNAME_TO_LEAP_BRANCHES.keys() if sn.startswith('Other')]
    other_leap_branches = []
    #extract the leap branches for these 'other' shortnames
    for sn in other_shortnames:
        other_leap_branches.extend(SHORTNAME_TO_LEAP_BRANCHES[sn])
    if len(other_leap_branches) > 0:
        other_rows = extract_esto_energy_use_for_leap_branches(other_leap_branches, esto_energy_use, economy, base_year, final_year)
        other_rows_df = pd.concat(other_rows, ignore_index=True)
        df = pd.concat([df, other_rows_df], ignore_index=True)
    return df

def extract_esto_energy_use_for_leap_branches(leap_branches, esto_energy_use,economy, base_year=2022, final_year=2060):
    #todo make sure this works with the validation def and this. 
    esto_sector_fuels_for_other = extract_esto_sector_fuels_for_leap_branches(leap_branches)
    other_rows = []
    for leap_branch, esto_rows in esto_sector_fuels_for_other.items():
        esto_rows_df_base_year = pd.DataFrame()#we will sum up all rows for this leap branch and insert their base year energy use into the transport df as activity level and energy use, with intensity =1
        esto_rows_df = pd.DataFrame()#we will sum up all rows for this leap branch and insert their values for all projected_years energy use into the transport df as activity level and energy use, with intensity =1
        if esto_rows == []:
            #this occurs if the leap branch is not mapped to any esto sector fuel. We should check if its in our list of leap branches we can skip, otehrwise warn
            LEAP_BRANCHES_TO_SKIP_IF_NO_ESTO_MAPPING = [
                ('Nonspecified transport',),
                ('Pipeline transport',) 
            ]
            if leap_branch not in LEAP_BRANCHES_TO_SKIP_IF_NO_ESTO_MAPPING:
                raise ValueError(f"Leap branch {leap_branch} has no ESTO mapping but is not in the skip list. If it is not feasible to create an esto mapping for this branch, please add it to the skip list, otherwise make sure it is mapped in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.")
            continue
        for (subsector, fuel, subfuel) in esto_rows:
            #create new rows for df using the ESTO data, filtered for the (subsector, fuel, subfuel) values, eg. ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline").
            # breakpoint()#check this works ok. worried that having only one col will cause issues
            esto_row_base_year = esto_energy_use[
                (esto_energy_use['sub1sectors'] == subsector) &
                (esto_energy_use['sub2sectors'] == 'x') &
                (esto_energy_use['fuels'] == fuel) &
                (esto_energy_use['subfuels'] == subfuel)
            ][base_year]
            esto_rows_df_base_year = pd.concat([esto_rows_df_base_year, esto_row_base_year], ignore_index=True)
            
            projected_years = [year for year in esto_energy_use.columns if isinstance(year, int) and year > base_year and year <= final_year]
            esto_row_projected_years = esto_energy_use[
                (esto_energy_use['sub1sectors'] == subsector) &
                (esto_energy_use['sub2sectors'] == 'x') &
                (esto_energy_use['fuels'] == fuel) &
                (esto_energy_use['subfuels'] == subfuel)
            ][projected_years]
            esto_rows_df = pd.concat([esto_rows_df, esto_row_projected_years], ignore_index=True)
        total_activity_level_base_year = esto_rows_df_base_year.sum().values[0]
        total_activity_levels_projected_years = esto_rows_df.sum().values
    
        #create new row in df with this activity level and intensity =1
        df_new_rows = {
            'Economy': economy,
            'Scenario': 'Reference',
            'Date': [base_year] + projected_years,
            'Transport Type': leap_branch[0],
            'Medium': leap_branch[1] if len(leap_branch) > 1 else None,#todo is it ok if we make these None? even if it doesnt amtch the way it is in ther rest of the df?
            'Vehicle Type': leap_branch[2] if len(leap_branch) > 2 else None,
            'Drive': leap_branch[3] if len(leap_branch) > 3 else None,
            'Fuel': leap_branch[4] if len(leap_branch) > 4 else None,
            'Activity': [total_activity_level_base_year] + list(total_activity_levels_projected_years),
            'Intensity': 1,
            'Energy' : [total_activity_level_base_year] + list(total_activity_levels_projected_years)
        }
        other_rows.append(pd.DataFrame(df_new_rows))
    return other_rows
#%%