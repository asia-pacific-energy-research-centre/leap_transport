#%%
import pandas as pd
  
EXPECTED_COLS_IN_SOURCE = [
    "Economy", "Date", "Medium", "Vehicle Type", "Transport Type", "Drive", "Scenario", "Efficiency", "Energy", "Mileage", "Stocks_old", "Activity", "Occupancy_or_load", "Intensity", "Activity_per_Stock", "Travel_km", "Stocks", "Activity_efficiency_improvement", "Average_age", "Gdp", "Gdp_per_capita", "New_vehicle_efficiency", "Population", "Surplus_stocks", "Stocks_per_thousand_capita", "Turnover_rate", "Age_distribution", "Unit", "Data_available", "Measure", "Vehicle_sales_share", "Stock_turnover", "New_stocks_needed", "Non_road_intensity_improvement", "Activity_growth"
]
SOURCE_CSV_TREE = {#this is the structure of the source csv file. Note that the fuels are not final fuels but rather the source fuel categories implied by each drive type. There are added fuels for low carobon fuels where applicable, such as efuels in most combustion engines, biofuels where applicable etc.
    "freight": {
        "air": {
            "all": {
                "air_av_gas": ["Aviation gasoline"],
                "air_diesel": ["Gas and diesel oil"],
                "air_electric": ["Electricity"],
                "air_fuel_oil": ["Fuel oil"],
                "air_gasoline": ["Motor gasoline"],
                "air_hydrogen": ["Hydrogen"],
                "air_jet_fuel": ["Kerosene type jet fuel"],
                "air_kerosene": ["Kerosene"],
                "air_lpg": ["LPG"]
            }
        },
        "rail": {
            "all": {
                "rail_coal": ["Other bituminous coal"],
                "rail_diesel": ["Gas and diesel oil"],
                "rail_electricity": ["Electricity"],
                "rail_fuel_oil": ["Fuel oil"],
                "rail_gasoline": ["Motor gasoline"],
                "rail_kerosene": ["Kerosene"],
                "rail_lpg": ["LPG"],
                "rail_natural_gas": ["Natural gas"]
            }
        },
        "road": {
            "ht": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lng": ["LNG"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            },
            "lcv": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            },
            "mt": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lng": ["LNG"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            }
        },
        "ship": {
            "all": {
                "ship_ammonia": ["Ammonia"],
                "ship_diesel": ["Gas and diesel oil"],
                "ship_electric": ["Electricity"],
                "ship_fuel_oil": ["Fuel oil"],
                "ship_gasoline": ["Motor gasoline"],
                "ship_hydrogen": ["Hydrogen"],
                "ship_kerosene": ["Kerosene"],
                "ship_lng": ["LNG"],
                "ship_lpg": ["LPG"],
                "ship_natural_gas": ["Natural gas"]
            }
        }
    },
    "passenger": {
        "air": {
            "all": {
                "air_av_gas": ["Aviation gasoline"],
                "air_diesel": ["Gas and diesel oil"],
                "air_electric": ["Electricity"],
                "air_fuel_oil": ["Fuel oil"],
                "air_gasoline": ["Motor gasoline"],
                "air_hydrogen": ["Hydrogen"],
                "air_jet_fuel": ["Kerosene type jet fuel"],
                "air_kerosene": ["Kerosene"],
                "air_lpg": ["LPG"]
            }
        },
        "rail": {
            "all": {
                "rail_coal": ["Other bituminous coal"],
                "rail_diesel": ["Gas and diesel oil"],
                "rail_electricity": ["Electricity"],
                "rail_fuel_oil": ["Fuel oil"],
                "rail_gasoline": ["Motor gasoline"],
                "rail_kerosene": ["Kerosene"],
                "rail_lpg": ["LPG"],
                "rail_natural_gas": ["Natural gas"]
            }
        },
        "road": {
            "2w": {
                "bev": ["Electricity"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"]
            },
            "bus": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            },
            "car": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            },
            "lt": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            },
            "suv": {
                "bev": ["Electricity"],
                "cng": ["Natural gas"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Gas and diesel oil"],
                "ice_g": ["Motor gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Gas and diesel oil"],
                "phev_g": ["Electricity", "Motor gasoline"]
            }
        },
        "ship": {
            "all": {
                "ship_ammonia": ["Ammonia"],
                "ship_diesel": ["Gas and diesel oil"],
                "ship_electric": ["Electricity"],
                "ship_fuel_oil": ["Fuel oil"],
                "ship_gasoline": ["Motor gasoline"],
                "ship_hydrogen": ["Hydrogen"],
                "ship_kerosene": ["Kerosene"],
                "ship_lng": ["LNG"],
                "ship_lpg": ["LPG"],
                "ship_natural_gas": ["Natural gas"]
            }
        }
    }
}


def add_fuel_column(df):
    """
    Add a 'Fuel' column to the dataframe based on the Drive type
    For drive types with multiple fuels, create duplicate rows with one fuel per row
    
    Parameters:
    df (pandas.DataFrame): DataFrame with columns for transport type, medium, vehicle type, and drive
    
    Returns:
    pandas.DataFrame: DataFrame with an additional 'Fuel' column and duplicate rows for multiple fuels
    """
    result_rows = []
    
    for _, row in df.iterrows():
        transport_type = row['Transport Type']
        medium = row['Medium']
        vehicle_type = row['Vehicle Type']
        drive = row['Drive']
        
        try:
            # Navigate the nested dictionary to find the fuels for this combination
            fuels = SOURCE_CSV_TREE[transport_type][medium][vehicle_type][drive]
            
            for fuel in fuels:
                new_row = row.copy()
                new_row['Fuel'] = fuel
                result_rows.append(new_row)
        except KeyError:
            # Handle the case where the combination doesn't exist in the tree
            raise ValueError(f"Combination not found in SOURCE_CSV_TREE: {transport_type}, {medium}, {vehicle_type}, {drive}")
    
    return pd.DataFrame(result_rows)
def convert_dict_tree_to_set_of_tuples(tree, path=()):
    """Convert nested dictionary tree to a set of tuples representing all paths, including intermediate branches."""
    tuples_set = set()
    for key, value in tree.items():
        current_path = path + (key,)
        # Add the current path as an intermediate branch
        tuples_set.add(current_path)
        
        if isinstance(value, dict):
            tuples_set.update(convert_dict_tree_to_set_of_tuples(value, current_path))
        elif isinstance(value, list):
            # For lists, create a tuple for each element in the list
            for item in value:
                tuples_set.add(current_path + (item,))
        else:
            tuples_set.add(current_path)
    return tuples_set

LEAP_STRUCTURE = {
    "Passenger road": {
        "Motorcycles": {
            "BEV": ["Electricity"],
            "ICE": ["Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
        },
        "Buses": {
            "BEV": ["Electricity"],
            "ICE": [
                "Gas and diesel oil",
                "Motor gasoline",
                "LPG",
                "Natural gas",
                "Biogasoline",
                "Biodiesel",
                "Biogas",
                "Efuel",
            ],
            "FCEV": ["Hydrogen"],
            "PHEV": [
                "Electricity",
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "Biodiesel",
                "Efuel",
            ],
            "EREV": [
                "Electricity",
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "Biodiesel",
                "Efuel",
            ],
        },
        "LPVs": {
            # Battery electric
            "BEV small": ["Electricity"],
            "BEV medium": ["Electricity"],
            "BEV large": ["Electricity"],
            # Internal combustion (size split)
            "ICE small": ["Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "ICE medium": [
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "LPG",
                "Natural gas",
                "Biodiesel",
                "Biogas",
                "Efuel",
            ],
            "ICE large": [
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "LPG",
                "Natural gas",
                "Biodiesel",
                "Biogas",
                "Efuel",
            ],
            # Plug-in hybrids
            "PHEV small": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "PHEV medium": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "PHEV large": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            # Hybrids (proxied to ICE)
            "HEV small": ["Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "HEV medium": ["Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "HEV large": ["Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "EREV small": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "EREV medium": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "EREV large": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
        },
    },
    #######################
    "Freight road": {
        "Trucks": {
            # Internal combustion
            "ICE heavy": [
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "LPG",
                "Natural gas",
                "LNG",
                "Biodiesel",
                "Biogas",
                "Efuel",
            ],
            "ICE medium": [
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "LPG",
                "Natural gas",
                "LNG",
                "Biodiesel",
                "Biogas",
                "Efuel",
            ],
            # Battery electric
            "BEV heavy": ["Electricity"],
            "BEV medium": ["Electricity"],
            # Extended-range EV (treated as PHEV)
            "EREV medium": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "EREV heavy": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            # Hydrogen
            "FCEV heavy": ["Hydrogen"],
            "FCEV medium": ["Hydrogen"],
            "PHEV heavy": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
            "PHEV medium": ["Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel"],
        },
        "LCVs": {
            "ICE": [
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "LPG",
                "Natural gas",
                "Biodiesel",
                "Biogas",
                "Efuel",
            ],
            "BEV": ["Electricity"],
            "PHEV": [
                "Electricity",
                "Motor gasoline",
                "Gas and diesel oil",
                "Biogasoline",
                "Biodiesel",
                "Efuel",
            ],
            "EREV": [
                "Electricity", "Motor gasoline", "Gas and diesel oil", "Biogasoline", "Biodiesel", "Efuel",
            ],
        },
    },
    #######################
    "Passenger non road": {
        "Rail": ["Electricity", "Gas and diesel oil", "Hydrogen", "Other bituminous coal", "Biodiesel", "Efuel"],
        "Air": ["Hydrogen", "Electricity", "Kerosene type jet fuel", "Aviation gasoline", "Bio jet kerosene", "Efuel"],
        "Shipping": [
            "Electricity",
            "Hydrogen",
            "Gas and diesel oil",
            "Fuel oil",
            "LNG",
            "Motor gasoline",
            "Ammonia",
            "Biodiesel",
            "Biogasoline",
            "Efuel",
        ],
    },
    "Freight non road": {
        "Rail": ["Electricity", "Gas and diesel oil", "Hydrogen", "Other bituminous coal", "Biodiesel", "Efuel"],
        "Air": ["Hydrogen", "Electricity", "Kerosene type jet fuel", "Aviation gasoline", "Bio jet kerosene", "Efuel"],
        "Shipping": [
            "Electricity",
            "Hydrogen",
            "Gas and diesel oil",
            "Fuel oil",
            "LNG",
            "Motor gasoline",
            "Ammonia",
            "Biodiesel",
            "Biogasoline",
            "Efuel",
        ],
    },
    "Nonspecified transport": [
        "Kerosene",
        "Fuel oil",
        "Gas and diesel oil",
        "LPG",
        "Motor gasoline",
        "Coke oven coke",
        "Other products",
        "Natural gas",
        "Electricity",
        
    ],
    "Pipeline transport": ["Fuel oil", "Gas and diesel oil", "Natural gas", "Electricity"],
}

######################
ESTO_TRANSPORT_SECTOR_TUPLES = {
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_02_aviation_gasoline"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_06_kerosene"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_x_jet_fuel"),

    ("15_02_road", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_02_road", "07_petroleum_products", "07_06_kerosene"),
    ("15_02_road", "07_petroleum_products", "07_07_gas_diesel_oil"),
    ("15_02_road", "07_petroleum_products", "07_08_fuel_oil"),
    ("15_02_road", "07_petroleum_products", "07_09_lpg"),
    ("15_02_road", "08_gas", "08_01_natural_gas"),
    ("15_02_road", "16_others", "16_05_biogasoline"),
    ("15_02_road", "16_others", "16_06_biodiesel"),
    ("15_02_road", "17_electricity", "x"),

    ("15_03_rail", "01_coal", "01_x_thermal_coal"),
    ("15_03_rail", "02_coal_products", "x"),
    ("15_03_rail", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_03_rail", "07_petroleum_products", "07_06_kerosene"),
    ("15_03_rail", "07_petroleum_products", "07_07_gas_diesel_oil"),
    ("15_03_rail", "07_petroleum_products", "07_08_fuel_oil"),
    ("15_03_rail", "07_petroleum_products", "07_09_lpg"),
    ("15_03_rail", "16_others", "16_06_biodiesel"),
    ("15_03_rail", "17_electricity", "x"),

    ("15_04_domestic_navigation", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_04_domestic_navigation", "07_petroleum_products", "07_06_kerosene"),
    ("15_04_domestic_navigation", "07_petroleum_products", "07_07_gas_diesel_oil"),
    ("15_04_domestic_navigation", "07_petroleum_products", "07_08_fuel_oil"),
    ("15_04_domestic_navigation", "07_petroleum_products", "07_09_lpg"),
    ("15_04_domestic_navigation", "08_gas", "08_01_natural_gas"),
    ("15_04_domestic_navigation", "16_others", "16_06_biodiesel"),
    ("15_04_domestic_navigation", "17_electricity", "x"),

    ("15_05_pipeline_transport", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_05_pipeline_transport", "07_petroleum_products", "07_07_gas_diesel_oil"),
    ("15_05_pipeline_transport", "07_petroleum_products", "07_08_fuel_oil"),
    ("15_05_pipeline_transport", "07_petroleum_products", "07_09_lpg"),
    ("15_05_pipeline_transport", "08_gas", "08_01_natural_gas"),
    ("15_05_pipeline_transport", "17_electricity", "x"),

    ("15_06_nonspecified_transport", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_07_gas_diesel_oil"),
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_08_fuel_oil"),
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_09_lpg"),
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_x_jet_fuel"),
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_x_other_petroleum_products"),
    ("15_06_nonspecified_transport", "02_coal_products", "x"),
    ("15_06_nonspecified_transport", "08_gas", "08_01_natural_gas"),
    ("15_06_nonspecified_transport", "17_electricity", "x"),
}

ALL_PATHS_LEAP = convert_dict_tree_to_set_of_tuples(LEAP_STRUCTURE, path=())
ALL_PATHS_SOURCE = convert_dict_tree_to_set_of_tuples(SOURCE_CSV_TREE, path=())

#%%
