
import pandas as pd

CSV_TREE = {
    "freight": {
        "air": {
            "all": {
                "air_av_gas": ["Aviation gasoline"],
                "air_diesel": ["Diesel"],
                "air_electric": ["Electricity"],
                "air_fuel_oil": ["Fuel oil"],
                "air_gasoline": ["Gasoline"],
                "air_hydrogen": ["Hydrogen"],
                "air_jet_fuel": ["Jet fuel"],
                "air_kerosene": ["Kerosene"],
                "air_lpg": ["LPG"]
            }
        },
        "rail": {
            "all": {
                "rail_coal": ["Coal"],
                "rail_diesel": ["Diesel"],
                "rail_electricity": ["Electricity"],
                "rail_fuel_oil": ["Fuel oil"],
                "rail_gasoline": ["Gasoline"],
                "rail_kerosene": ["Kerosene"],
                "rail_lpg": ["LPG"],
                "rail_natural_gas": ["Natural gas"]
            }
        },
        "road": {
            "ht": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lng": ["LNG"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            },
            "lcv": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            },
            "mt": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lng": ["LNG"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            }
        },
        "ship": {
            "all": {
                "ship_ammonia": ["Ammonia"],
                "ship_diesel": ["Diesel"],
                "ship_electric": ["Electricity"],
                "ship_fuel_oil": ["Fuel oil"],
                "ship_gasoline": ["Gasoline"],
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
                "air_diesel": ["Diesel"],
                "air_electric": ["Electricity"],
                "air_fuel_oil": ["Fuel oil"],
                "air_gasoline": ["Gasoline"],
                "air_hydrogen": ["Hydrogen"],
                "air_jet_fuel": ["Jet fuel"],
                "air_kerosene": ["Kerosene"],
                "air_lpg": ["LPG"]
            }
        },
        "rail": {
            "all": {
                "rail_coal": ["Coal"],
                "rail_diesel": ["Diesel"],
                "rail_electricity": ["Electricity"],
                "rail_fuel_oil": ["Fuel oil"],
                "rail_gasoline": ["Gasoline"],
                "rail_kerosene": ["Kerosene"],
                "rail_lpg": ["LPG"],
                "rail_natural_gas": ["Natural gas"]
            }
        },
        "road": {
            "2w": {
                "bev": ["Electricity"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"]
            },
            "bus": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            },
            "car": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            },
            "lt": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            },
            "suv": {
                "bev": ["Electricity"],
                "cng": ["CNG"],
                "fcev": ["Hydrogen"],
                "ice_d": ["Diesel"],
                "ice_g": ["Gasoline"],
                "lpg": ["LPG"],
                "phev_d": ["Electricity", "Diesel"],
                "phev_g": ["Electricity", "Gasoline"]
            }
        },
        "ship": {
            "all": {
                "ship_ammonia": ["Ammonia"],
                "ship_diesel": ["Diesel"],
                "ship_electric": ["Electricity"],
                "ship_fuel_oil": ["Fuel oil"],
                "ship_gasoline": ["Gasoline"],
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
            fuels = CSV_TREE[transport_type][medium][vehicle_type][drive]
            
            for fuel in fuels:
                new_row = row.copy()
                new_row['Fuel'] = fuel
                result_rows.append(new_row)
        except KeyError:
            # Handle the case where the combination doesn't exist in the tree
            raise ValueError(f"Combination not found in CSV_TREE: {transport_type}, {medium}, {vehicle_type}, {drive}")
    
    return pd.DataFrame(result_rows)

LEAP_STRUCTURE = {
    "Passenger road": {
        "Motorcycles": {
            "BEV": ["Electricity"],
            "ICE": ["Gasoline", "Diesel", "Biogasoline", "Biodiesel"],
        },
        "Buses": {
            "BEV": ["Electricity"],
            "ICE": ["Diesel", "Gasoline", "LPG", "CNG", "Biogasoline", "Biodiesel"],
            "FCEV": ["Hydrogen"],
        },
        "LPVs": {
            # Battery electric
            "BEV small": ["Electricity"],
            "BEV medium": ["Electricity"],
            "BEV large": ["Electricity"],
            # Internal combustion (size split)
            "ICE small": ["Gasoline", "Diesel", "Biogasoline", 'Biodiesel'],
            "ICE medium": ["Gasoline", "Diesel", "Biogasoline", "LPG", "CNG", "Biogasoline", "Biodiesel"],
            "ICE large": ["Gasoline", "Diesel", "Biogasoline", "LPG", "CNG", "Biogasoline", "Biodiesel"],
            # Plug-in hybrids
            "PHEV small": ["Electricity", "Gasoline", "Diesel", "Biogasoline"],
            "PHEV medium": ["Electricity", "Gasoline", "Diesel", "Biogasoline"],
            "PHEV large": ["Electricity", "Gasoline", "Diesel", "Biogasoline"],
            # Hybrids (proxied to ICE)
            "HEV small": ["Gasoline", "Diesel", "Biogasoline"],
            "HEV medium": ["Gasoline", "Diesel", "Biogasoline"],
            "HEV large": ["Gasoline", "Diesel", "Biogasoline"],
        },
    },
    #######################
    "Freight road": {
        "Trucks": {
            # Internal combustion
            "ICE heavy": ["Gasoline", "Diesel", "Biogasoline", "LPG", "CNG", "LNG", "Biodiesel"],
            "ICE medium": ["Gasoline", "Diesel", "Biogasoline", "LPG", "CNG", "LNG", "Biodiesel"],
            # Battery electric
            "BEV heavy": ["Electricity"],
            "BEV medium": ["Electricity"],
            # Extended-range EV (treated as PHEV)
            "EREV medium": ["Electricity", "Gasoline", "Diesel", "Biogasoline", "Biodiesel"],
            "EREV heavy": ["Electricity", "Gasoline", "Diesel", "Biogasoline", "Biodiesel"],
            # Hydrogen
            "FCEV heavy": ["Hydrogen"],
            "FCEV medium": ["Hydrogen"],
        },
        "LCVs": {
            "ICE": ["Gasoline", "Diesel", "Biogasoline", "LPG", "CNG", "Biogasoline", "Biodiesel"],
            "BEV": ["Electricity"],
            "PHEV": ["Electricity", "Gasoline", "Diesel", "Biogasoline"],
        },
        "Motorcycles": {
            "ICE": ["Gasoline", "Diesel", "Biogasoline"],
            "BEV": ["Electricity"],
        },
    },
    #######################
    "Passenger non road": {
        "Rail": ["Electricity", "Diesel", "Hydrogen", "Coal", 'Biodiesel'],
        "Air": ["Hydrogen", "Electricity", "Jet fuel", "Aviation gasoline"],
        "Shipping": ["Electricity", "Hydrogen", "Diesel", "Fuel oil", "LNG", "Gasoline", "Ammonia", 'Biodiesel'],
    },
    "Freight non road": {
        "Rail": ["Electricity", "Diesel", "Hydrogen", "Coal",'Biodiesel'],
        "Air": ["Hydrogen", "Electricity", "Jet fuel", "Aviation gasoline"],
        "Shipping": ["Electricity", "Hydrogen", "Diesel", "Fuel oil", "LNG", "Gasoline", "Ammonia",'Biodiesel'],
    },
    "Nonspecified transport": [
        "Kerosene",
        "Fuel oil",
        "Diesel",
        "LPG",
        "Gasoline",
        "Coal products",
        "Other petroleum products"
    ],
    'Pipeline transport': {
        "Fuel oil",
        "Diesel",
        'Natural gas',
        'Electricity',
    }
}

######################
ESTO_TRANSPORT_SECTOR_TUPLES = {
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_02_aviation_gasoline"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_06_kerosene"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_07_gas_diesel_oil"),
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_09_lpg"),
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
    ("15_06_nonspecified_transport", "08_gas", "08_01_natural_gas"),
    ("15_06_nonspecified_transport", "17_electricity", "x"),
}
