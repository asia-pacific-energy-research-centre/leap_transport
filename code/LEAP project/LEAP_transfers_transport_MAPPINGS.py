
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
            "ICE": ["Gasoline", "Diesel", "Ethanol"],
        },
        "Buses": {
            "BEV": ["Electricity"],
            "ICE": ["Diesel", "Gasoline", "LPG", "CNG", "Biogas"],
            "FCEV": ["Hydrogen"],
        },
        "LPVs": {
            # Battery electric
            "BEV small": ["Electricity"],
            "BEV medium": ["Electricity"],
            "BEV large": ["Electricity"],
            # Internal combustion (size split)
            "ICE small": ["Gasoline", "Diesel", "Ethanol"],
            "ICE medium": ["Gasoline", "Diesel", "Ethanol", "LPG", "CNG", "Biogas"],
            "ICE large": ["Gasoline", "Diesel", "Ethanol", "LPG", "CNG", "Biogas"],
            # Plug-in hybrids
            "PHEV small": ["Electricity", "Gasoline", "Diesel", "Ethanol"],
            "PHEV medium": ["Electricity", "Gasoline", "Diesel", "Ethanol"],
            "PHEV large": ["Electricity", "Gasoline", "Diesel", "Ethanol"],
            # Hybrids (proxied to ICE)
            "HEV small": ["Gasoline", "Diesel", "Ethanol"],
            "HEV medium": ["Gasoline", "Diesel", "Ethanol"],
            "HEV large": ["Gasoline", "Diesel", "Ethanol"],
        },
    },
    #######################
    "Freight road": {
        "Trucks": {
            # Internal combustion
            "ICE heavy": ["Gasoline", "Diesel", "Ethanol", "LPG", "CNG", "LNG", "Biogas"],
            "ICE medium": ["Gasoline", "Diesel", "Ethanol", "LPG", "CNG", "LNG", "Biogas"],
            # Battery electric
            "BEV heavy": ["Electricity"],
            "BEV medium": ["Electricity"],
            # Extended-range EV (treated as PHEV)
            "EREV medium": ["Electricity", "Gasoline", "Diesel", "Ethanol"],
            "EREV heavy": ["Electricity", "Gasoline", "Diesel", "Ethanol"],
            # Hydrogen
            "FCEV heavy": ["Hydrogen"],
            "FCEV medium": ["Hydrogen"],
        },
        "LCVs": {
            "ICE": ["Gasoline", "Diesel", "Ethanol", "LPG", "CNG", "Biogas"],
            "BEV": ["Electricity"],
            "PHEV": ["Electricity", "Gasoline", "Diesel", "Ethanol"],
        },
        "Motorcycles": {
            "ICE": ["Gasoline", "Diesel", "Ethanol"],
            "BEV": ["Electricity"],
        },
    },
    #######################
    "Non road": {
        "Passenger": {
            "Rail": ["Electricity", "Diesel", "Hydrogen", "Coal"],
            "Air": ["Hydrogen", "Electricity", "Jet fuel", "Aviation gasoline"],
            "Shipping": ["Electricity", "Hydrogen", "Diesel", "Fuel oil", "LNG", "Gasoline", "Ammonia"],
        },
        "Freight": {
            "Rail": ["Electricity", "Diesel", "Hydrogen", "Coal"],
            "Air": ["Hydrogen", "Electricity", "Jet fuel", "Aviation gasoline"],
            "Shipping": ["Electricity", "Hydrogen", "Diesel", "Fuel oil", "LNG", "Gasoline", "Ammonia"],
        }
    },
    "Nonspecified transport": [
        "Kerosene",
        "Fuel oil",
        "Diesel",
        "LPG",
        "Gasoline",
        "Coal products",
        "Other petroleum products",
    ]
}
SHORTNAME_TO_LEAP_BRANCHES = {
    # Mapping to names used for reference in LEAP
    'Vehicle type (road)': [
        ("Passenger road", "LPVs"),
        ("Passenger road", "Buses"),
        ("Passenger road", "Motorcycles"),
        ("Freight road", "Trucks"),
        ("Freight road", "LCVs"),
    ],

    'Technology (road)': [
        ("Passenger road", "LPVs", "BEV small"),
        ("Passenger road", "LPVs", "BEV medium"),
        ("Passenger road", "LPVs", "BEV large"),
        ("Passenger road", "LPVs", "ICE small"),
        ("Passenger road", "LPVs", "ICE medium"),
        ("Passenger road", "LPVs", "ICE large"),
        ("Passenger road", "LPVs", "PHEV small"),
        ("Passenger road", "LPVs", "PHEV medium"),
        ("Passenger road", "LPVs", "PHEV large"),
        ("Passenger road", "LPVs", "HEV small"),
        ("Passenger road", "LPVs", "HEV medium"),
        ("Passenger road", "LPVs", "HEV large"),
        ("Passenger road", "Buses", "BEV"),
        ("Passenger road", "Buses", "ICE"),
        ("Passenger road", "Buses", "FCEV"),
        ("Passenger road", "Motorcycles", "ICE"),
        ("Passenger road", "Motorcycles", "BEV"),
        ("Freight road", "Trucks", "ICE heavy"),
        ("Freight road", "Trucks", "ICE medium"),
        ("Freight road", "Trucks", "BEV heavy"),
        ("Freight road", "Trucks", "BEV medium"),
        ("Freight road", "Trucks", "EREV heavy"),
        ("Freight road", "Trucks", "EREV medium"),
        ("Freight road", "Trucks", "FCEV heavy"),
        ("Freight road", "Trucks", "FCEV medium"),
        ("Freight road", "LCVs", "ICE"),
        ("Freight road", "LCVs", "BEV"),
        ("Freight road", "LCVs", "PHEV"),
    ],

    'Fuel (road)': [
        ("Passenger road", "LPVs", "BEV small", "Electricity"),
        ("Passenger road", "LPVs", "BEV medium", "Electricity"),
        ("Passenger road", "LPVs", "BEV large", "Electricity"),
        ("Passenger road", "LPVs", "ICE small", "Gasoline"),
        ("Passenger road", "LPVs", "ICE small", "Diesel"),
        ("Passenger road", "LPVs", "ICE small", "Ethanol"),
        ("Passenger road", "LPVs", "ICE medium", "Gasoline"),
        ("Passenger road", "LPVs", "ICE medium", "Diesel"),
        ("Passenger road", "LPVs", "ICE medium", "Ethanol"),
        ("Passenger road", "LPVs", "ICE medium", "LPG"),
        ("Passenger road", "LPVs", "ICE medium", "CNG"),
        ("Passenger road", "LPVs", "ICE medium", "Biogas"),
        ("Passenger road", "LPVs", "ICE large", "Gasoline"),
        ("Passenger road", "LPVs", "ICE large", "Diesel"),
        ("Passenger road", "LPVs", "ICE large", "Ethanol"),
        ("Passenger road", "LPVs", "ICE large", "LPG"),
        ("Passenger road", "LPVs", "ICE large", "CNG"),
        ("Passenger road", "LPVs", "ICE large", "Biogas"),
        ("Passenger road", "LPVs", "PHEV small", "Electricity"),
        ("Passenger road", "LPVs", "PHEV small", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Diesel"),
        ("Passenger road", "LPVs", "PHEV small", "Ethanol"),
        ("Passenger road", "LPVs", "PHEV medium", "Electricity"),
        ("Passenger road", "LPVs", "PHEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Diesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Ethanol"),
        ("Passenger road", "LPVs", "PHEV large", "Electricity"),
        ("Passenger road", "LPVs", "PHEV large", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Diesel"),
        ("Passenger road", "LPVs", "PHEV large", "Ethanol"),
        ("Passenger road", "LPVs", "HEV small", "Gasoline"),
        ("Passenger road", "LPVs", "HEV small", "Diesel"),
        ("Passenger road", "LPVs", "HEV small", "Ethanol"),
        ("Passenger road", "LPVs", "HEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Diesel"),
        ("Passenger road", "LPVs", "HEV medium", "Ethanol"),
        ("Passenger road", "LPVs", "HEV large", "Gasoline"),
        ("Passenger road", "LPVs", "HEV large", "Diesel"),
        ("Passenger road", "LPVs", "HEV large", "Ethanol"),
        ("Passenger road", "Buses", "BEV", "Electricity"),
        ("Passenger road", "Buses", "ICE", "Diesel"),
        ("Passenger road", "Buses", "ICE", "Gasoline"),
        ("Passenger road", "Buses", "ICE", "LPG"),
        ("Passenger road", "Buses", "ICE", "CNG"),
        ("Passenger road", "Buses", "ICE", "Biogas"),
        ("Passenger road", "Buses", "FCEV", "Hydrogen"),
        ("Passenger road", "Motorcycles", "ICE", "Gasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Diesel"),
        ("Passenger road", "Motorcycles", "ICE", "Ethanol"),
        ("Passenger road", "Motorcycles", "BEV", "Electricity"),
        # Freight road fuels
        ("Freight road", "Trucks", "ICE heavy", "Gasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Diesel"),
        ("Freight road", "Trucks", "ICE heavy", "Ethanol"),
        ("Freight road", "Trucks", "ICE heavy", "LPG"),
        ("Freight road", "Trucks", "ICE heavy", "CNG"),
        ("Freight road", "Trucks", "ICE heavy", "LNG"),
        ("Freight road", "Trucks", "ICE heavy", "Biogas"),
        ("Freight road", "Trucks", "ICE medium", "Gasoline"),
        ("Freight road", "Trucks", "ICE medium", "Diesel"),
        ("Freight road", "Trucks", "ICE medium", "Ethanol"),
        ("Freight road", "Trucks", "ICE medium", "LPG"),
        ("Freight road", "Trucks", "ICE medium", "CNG"),
        ("Freight road", "Trucks", "ICE medium", "LNG"),
        ("Freight road", "Trucks", "ICE medium", "Biogas"),
        ("Freight road", "Trucks", "BEV heavy", "Electricity"),
        ("Freight road", "Trucks", "BEV medium", "Electricity"),
        ("Freight road", "Trucks", "EREV medium", "Gasoline"),
        ("Freight road", "Trucks", "EREV medium", "Diesel"),
        ("Freight road", "Trucks", "EREV medium", "Electricity"),
        ("Freight road", "Trucks", "EREV medium", "Ethanol"),
        ("Freight road", "Trucks", "EREV heavy", "Gasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Diesel"),
        ("Freight road", "Trucks", "EREV heavy", "Electricity"),
        ("Freight road", "Trucks", "EREV heavy", "Ethanol"),
        ("Freight road", "Trucks", "FCEV heavy", "Hydrogen"),
        ("Freight road", "Trucks", "FCEV medium", "Hydrogen"),
        ("Freight road", "LCVs", "ICE", "Gasoline"),
        ("Freight road", "LCVs", "ICE", "Diesel"),
        ("Freight road", "LCVs", "ICE", "Ethanol"),
        ("Freight road", "LCVs", "ICE", "LPG"),
        ("Freight road", "LCVs", "ICE", "CNG"),
        ("Freight road", "LCVs", "ICE", "Biogas"),
        ("Freight road", "LCVs", "BEV", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Gasoline"),
        ("Freight road", "LCVs", "PHEV", "Diesel"),
        ("Freight road", "LCVs", "PHEV", "Ethanol"),
    ],

    'Medium (road)': [
        ("Passenger road",),
        ("Freight road",),
    ],

    'Medium (non-road)': [
        ("Non road", "Passenger", "Air"),
        ("Non road", "Passenger", "Rail"),
        ("Non road", "Passenger", "Shipping"),
        ("Non road", "Freight", "Air"),
        ("Non road", "Freight", "Rail"),
        ("Non road", "Freight", "Shipping"),
    ],

    'Fuel (non-road)': [
        ("Non road", "Passenger", "Air", "Hydrogen"),
        ("Non road", "Passenger", "Air", "Electricity"),
        ("Non road", "Passenger", "Air", "Jet fuel"),
        ("Non road", "Passenger", "Air", "Aviation gasoline"),
        ("Non road", "Passenger", "Rail", "Electricity"),
        ("Non road", "Passenger", "Rail", "Diesel"),
        ("Non road", "Passenger", "Rail", "Hydrogen"),
        ("Non road", "Passenger", "Rail", "Coal"),
        ("Non road", "Passenger", "Shipping", "Electricity"),
        ("Non road", "Passenger", "Shipping", "Hydrogen"),
        ("Non road", "Passenger", "Shipping", "Diesel"),
        ("Non road", "Passenger", "Shipping", "Fuel oil"),
        ("Non road", "Passenger", "Shipping", "LNG"),
        ("Non road", "Passenger", "Shipping", "Gasoline"),
        ("Non road", "Passenger", "Shipping", "Ammonia"),
        ("Non road", "Freight", "Air", "Hydrogen"),
        ("Non road", "Freight", "Air", "Electricity"),
        ("Non road", "Freight", "Air", "Jet fuel"),
        ("Non road", "Freight", "Air", "Aviation gasoline"),
        ("Non road", "Freight", "Rail", "Electricity"),
        ("Non road", "Freight", "Rail", "Diesel"),
        ("Non road", "Freight", "Rail", "Hydrogen"),
        ("Non road", "Freight", "Rail", "Coal"),
        ("Non road", "Freight", "Shipping", "Electricity"),
        ("Non road", "Freight", "Shipping", "Hydrogen"),
        ("Non road", "Freight", "Shipping", "Diesel"),
        ("Non road", "Freight", "Shipping", "Fuel oil"),
        ("Non road", "Freight", "Shipping", "LNG"),
        ("Non road", "Freight", "Shipping", "Gasoline"),
        ("Non road", "Freight", "Shipping", "Ammonia"),
    ],
}
LEAP_BRANCH_TO_SOURCE_MAP = {
    #tuples map to: LEAP: (Transport Type, Medium, Vehicle Type, Drive, Fuel) : (Transport Type, Medium, Vehicle Type, Drive, Fuel)
    #for non-road mediums, the LEAP tuple omits vehicle type and drive since they don't apply
    #no one-to-many mappings to avoid complications

    # =========================
    # NON-ROAD: PASSENGER
    # =========================
    ("Non road", "Passenger", "Air", "Hydrogen"):        ("passenger", "air",  "all", "air_hydrogen", "Hydrogen"),
    ("Non road", "Passenger", "Air", "Electricity"):     ("passenger", "air",  "all", "air_electric", "Electricity"),
    ("Non road", "Passenger", "Air", "Jet fuel"):        ("passenger", "air",  "all", "air_jet_fuel", "Jet fuel"),
    ("Non road", "Passenger", "Air", "Aviation gasoline"): ("passenger", "air", "all", "air_av_gas", "Aviation gasoline"),

    ("Non road", "Passenger", "Rail", "Electricity"):    ("passenger", "rail", "all", "rail_electricity", "Electricity"),
    ("Non road", "Passenger", "Rail", "Diesel"):         ("passenger", "rail", "all", "rail_diesel", "Diesel"),
    ("Non road", "Passenger", "Rail", "Hydrogen"):       ("passenger", "rail", "all", "rail_electricity", "Electricity"),  # proxy
    ("Non road", "Passenger", "Rail", "Coal"):           ("passenger", "rail", "all", "rail_coal", "Coal"),

    ("Non road", "Passenger", "Shipping", "Electricity"):("passenger", "ship", "all", "ship_electric", "Electricity"),
    ("Non road", "Passenger", "Shipping", "Hydrogen"):   ("passenger", "ship", "all", "ship_hydrogen", "Hydrogen"),
    ("Non road", "Passenger", "Shipping", "Diesel"):     ("passenger", "ship", "all", "ship_diesel", "Diesel"),
    ("Non road", "Passenger", "Shipping", "Fuel oil"):   ("passenger", "ship", "all", "ship_fuel_oil", "Fuel oil"),
    ("Non road", "Passenger", "Shipping", "LNG"):        ("passenger", "ship", "all", "ship_lng", "LNG"),
    ("Non road", "Passenger", "Shipping", "Gasoline"):   ("passenger", "ship", "all", "ship_gasoline", "Gasoline"),
    ("Non road", "Passenger", "Shipping", "Ammonia"):    ("passenger", "ship", "all", "ship_ammonia", "Ammonia"),

    # =========================
    # NON-ROAD: FREIGHT
    # =========================
    ("Non road", "Freight", "Air", "Hydrogen"):          ("freight", "air",  "all", "air_hydrogen", "Hydrogen"),
    ("Non road", "Freight", "Air", "Electricity"):       ("freight", "air",  "all", "air_electric", "Electricity"),
    ("Non road", "Freight", "Air", "Jet fuel"):          ("freight", "air",  "all", "air_jet_fuel", "Jet fuel"),
    ("Non road", "Freight", "Air", "Aviation gasoline"): ("freight", "air",  "all", "air_av_gas", "Aviation gasoline"),

    ("Non road", "Freight", "Rail", "Electricity"):      ("freight", "rail", "all", "rail_electricity", "Electricity"),
    ("Non road", "Freight", "Rail", "Diesel"):           ("freight", "rail", "all", "rail_diesel", "Diesel"),
    ("Non road", "Freight", "Rail", "Hydrogen"):         ("freight", "rail", "all", "rail_electricity", "Electricity"),    # proxy
    ("Non road", "Freight", "Rail", "Coal"):             ("freight", "rail", "all", "rail_coal", "Coal"),

    ("Non road", "Freight", "Shipping", "Electricity"):  ("freight", "ship", "all", "ship_electric", "Electricity"),
    ("Non road", "Freight", "Shipping", "Hydrogen"):     ("freight", "ship", "all", "ship_hydrogen", "Hydrogen"),
    ("Non road", "Freight", "Shipping", "Diesel"):       ("freight", "ship", "all", "ship_diesel", "Diesel"),
    ("Non road", "Freight", "Shipping", "Fuel oil"):     ("freight", "ship", "all", "ship_fuel_oil", "Fuel oil"),
    ("Non road", "Freight", "Shipping", "LNG"):          ("freight", "ship", "all", "ship_lng", "LNG"),
    ("Non road", "Freight", "Shipping", "Gasoline"):     ("freight", "ship", "all", "ship_gasoline", "Gasoline"),
    ("Non road", "Freight", "Shipping", "Ammonia"):      ("freight", "ship", "all", "ship_ammonia", "Ammonia"),

    # =====================================================
    # ROAD: PASSENGER ROAD → LPVs
    # =====================================================
    ("Passenger road","LPVs","BEV small","Electricity"):   ("passenger","road","car","bev","Electricity"),
    ("Passenger road","LPVs","BEV medium","Electricity"):  ("passenger","road","suv","bev","Electricity"),
    ("Passenger road","LPVs","BEV large","Electricity"):   ("passenger","road","lt","bev","Electricity"),

    ("Passenger road","LPVs","ICE small","Gasoline"):      ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE small","Diesel"):        ("passenger","road","car","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE small","Ethanol"):       ("passenger","road","car","ice_g","Gasoline"),

    ("Passenger road","LPVs","ICE medium","Gasoline"):     ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE medium","Diesel"):       ("passenger","road","suv","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE medium","Ethanol"):      ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE medium","LPG"):          ("passenger","road","suv","lpg","LPG"),
    ("Passenger road","LPVs","ICE medium","CNG"):          ("passenger","road","suv","cng","CNG"),
    ("Passenger road","LPVs","ICE medium","Biogas"):       ("passenger","road","suv","cng","CNG"),

    ("Passenger road","LPVs","ICE large","Gasoline"):      ("passenger","road","lt","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE large","Diesel"):        ("passenger","road","lt","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE large","Ethanol"):       ("passenger","road","lt","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE large","LPG"):           ("passenger","road","lt","lpg","LPG"),
    ("Passenger road","LPVs","ICE large","CNG"):           ("passenger","road","lt","cng","CNG"),
    ("Passenger road","LPVs","ICE large","Biogas"):        ("passenger","road","lt","cng","CNG"),

    ("Passenger road","LPVs","PHEV small","Electricity"):  ("passenger","road","car","phev_g","Electricity"),
    ("Passenger road","LPVs","PHEV small","Gasoline"):     ("passenger","road","car","phev_g","Gasoline"),
    ("Passenger road","LPVs","PHEV small","Diesel"):       ("passenger","road","car","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV small","Ethanol"):      ("passenger","road","car","phev_g","Gasoline"),

    ("Passenger road","LPVs","PHEV medium","Electricity"): ("passenger","road","suv","phev_g","Electricity"),
    ("Passenger road","LPVs","PHEV medium","Gasoline"):    ("passenger","road","suv","phev_g","Gasoline"),
    ("Passenger road","LPVs","PHEV medium","Diesel"):      ("passenger","road","suv","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV medium","Ethanol"):     ("passenger","road","suv","phev_g","Gasoline"),

    ("Passenger road","LPVs","PHEV large","Electricity"):  ("passenger","road","lt","phev_g","Electricity"),
    ("Passenger road","LPVs","PHEV large","Gasoline"):     ("passenger","road","lt","phev_g","Gasoline"),
    ("Passenger road","LPVs","PHEV large","Diesel"):       ("passenger","road","lt","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV large","Ethanol"):      ("passenger","road","lt","phev_g","Gasoline"),

    ("Passenger road","LPVs","HEV small","Gasoline"):      ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV small","Diesel"):        ("passenger","road","car","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV small","Ethanol"):       ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV medium","Gasoline"):     ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV medium","Diesel"):       ("passenger","road","suv","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV medium","Ethanol"):      ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV large","Gasoline"):      ("passenger","road","lt","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV large","Diesel"):        ("passenger","road","lt","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV large","Ethanol"):       ("passenger","road","lt","ice_g","Gasoline"),

    # =========================
    # ROAD: PASSENGER ROAD → Buses
    # =========================
    ("Passenger road","Buses","BEV","Electricity"):        ("passenger","road","bus","bev","Electricity"),
    ("Passenger road","Buses","ICE","Diesel"):             ("passenger","road","bus","ice_d","Diesel"),
    ("Passenger road","Buses","ICE","Gasoline"):           ("passenger","road","bus","ice_g","Gasoline"),
    ("Passenger road","Buses","ICE","LPG"):                ("passenger","road","bus","lpg","LPG"),
    ("Passenger road","Buses","ICE","CNG"):                ("passenger","road","bus","cng","CNG"),
    ("Passenger road","Buses","ICE","Biogas"):             ("passenger","road","bus","cng","CNG"),
    ("Passenger road","Buses","FCEV","Hydrogen"):          ("passenger","road","bus","fcev","Hydrogen"),

    # =========================
    # ROAD: PASSENGER ROAD → Motorcycles
    # =========================
    ("Passenger road","Motorcycles","ICE","Gasoline"):     ("passenger","road","2w","ice_g","Gasoline"),
    ("Passenger road","Motorcycles","ICE","Diesel"):       ("passenger","road","2w","ice_d","Diesel"),
    ("Passenger road","Motorcycles","ICE","Ethanol"):      ("passenger","road","2w","ice_g","Gasoline"),
    ("Passenger road","Motorcycles","BEV","Electricity"):  ("passenger","road","2w","bev","Electricity"),

    # =========================
    # ROAD: FREIGHT ROAD → Trucks
    # =========================
    ("Freight road","Trucks","ICE heavy","Gasoline"):      ("freight","road","ht","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE heavy","Diesel"):        ("freight","road","ht","ice_d","Diesel"),
    ("Freight road","Trucks","ICE heavy","Ethanol"):       ("freight","road","ht","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE heavy","LPG"):           ("freight","road","ht","lpg","LPG"),
    ("Freight road","Trucks","ICE heavy","CNG"):           ("freight","road","ht","cng","CNG"),
    ("Freight road","Trucks","ICE heavy","LNG"):           ("freight","road","ht","lng","LNG"),
    ("Freight road","Trucks","ICE heavy","Biogas"):        ("freight","road","ht","cng","CNG"),

    ("Freight road","Trucks","ICE medium","Gasoline"):     ("freight","road","mt","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE medium","Diesel"):       ("freight","road","mt","ice_d","Diesel"),
    ("Freight road","Trucks","ICE medium","Ethanol"):      ("freight","road","mt","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE medium","LNG"):          ("freight","road","mt","lng","LNG"),
    ("Freight road","Trucks","ICE medium","CNG"):          ("freight","road","mt","cng","CNG"),
    ("Freight road","Trucks","ICE medium","LPG"):          ("freight","road","mt","lpg","LPG"),
    ("Freight road","Trucks","ICE medium","Biogas"):       ("freight","road","mt","cng","CNG"),

    ("Freight road","Trucks","BEV heavy","Electricity"):   ("freight","road","ht","bev","Electricity"),
    ("Freight road","Trucks","BEV medium","Electricity"):  ("freight","road","mt","bev","Electricity"),

    ("Freight road","Trucks","EREV medium","Gasoline"):    ("freight","road","mt","phev_g","Gasoline"),
    ("Freight road","Trucks","EREV medium","Electricity"): ("freight","road","mt","phev_g","Electricity"),
    ("Freight road","Trucks","EREV medium","Diesel"):      ("freight","road","mt","phev_d","Diesel"),
    ("Freight road","Trucks","EREV medium","Ethanol"):     ("freight","road","mt","phev_g","Gasoline"),

    ("Freight road","Trucks","EREV heavy","Gasoline"):     ("freight","road","ht","phev_g","Gasoline"),
    ("Freight road","Trucks","EREV heavy","Electricity"):  ("freight","road","ht","phev_g","Electricity"),
    ("Freight road","Trucks","EREV heavy","Diesel"):       ("freight","road","ht","phev_d","Diesel"),
    ("Freight road","Trucks","EREV heavy","Ethanol"):      ("freight","road","ht","phev_g","Gasoline"),

    ("Freight road","Trucks","FCEV heavy","Hydrogen"):     ("freight","road","ht","fcev","Hydrogen"),
    ("Freight road","Trucks","FCEV medium","Hydrogen"):    ("freight","road","mt","fcev","Hydrogen"),

    # =========================
    # ROAD: FREIGHT ROAD → LCVs
    # =========================
    ("Freight road","LCVs","ICE","Gasoline"):              ("freight","road","lcv","ice_g","Gasoline"),
    ("Freight road","LCVs","ICE","Diesel"):                ("freight","road","lcv","ice_d","Diesel"),
    ("Freight road","LCVs","ICE","Ethanol"):               ("freight","road","lcv","ice_g","Gasoline"),
    ("Freight road","LCVs","ICE","CNG"):                   ("freight","road","lcv","cng","CNG"),
    ("Freight road","LCVs","ICE","LPG"):                   ("freight","road","lcv","lpg","LPG"),
    ("Freight road","LCVs","ICE","Biogas"):                ("freight","road","lcv","cng","CNG"),
    ("Freight road","LCVs","BEV","Electricity"):           ("freight","road","lcv","bev","Electricity"),
    ("Freight road","LCVs","PHEV","Electricity"):          ("freight","road","lcv","phev_g","Electricity"),
    ("Freight road","LCVs","PHEV","Gasoline"):             ("freight","road","lcv","phev_g","Gasoline"),
    ("Freight road","LCVs","PHEV","Diesel"):               ("freight","road","lcv","phev_d","Diesel"),
    ("Freight road","LCVs","PHEV","Ethanol"):              ("freight","road","lcv","phev_g","Gasoline"),

    # =========================
    # AGGREGATE MAPPINGS
    # =========================
    ("Passenger road",):                                  ("passenger", "road"),
    ("Freight road",):                                    ("freight", "road"),
    ("Passenger road","LPVs"):                            ("passenger", "road", "car"),
    ("Passenger road","Buses"):                           ("passenger", "road", "bus"),
    ("Passenger road","Motorcycles"):                     ("passenger", "road", "2w"),
    ("Freight road","Trucks"):                            ("freight", "road","ht"),
    ("Freight road","LCVs"):                              ("freight", "road", "lcv"),
    ("Non road","Passenger","Air"):                       ("passenger", "air"),
    ("Non road","Passenger","Rail"):                      ("passenger", "rail"),
    ("Non road","Passenger","Shipping"):                  ("passenger", "ship"),
    ("Non road","Freight","Air"):                         ("freight", "air"),
    ("Non road","Freight","Rail"):                        ("freight", "rail"),
    ("Non road","Freight","Shipping"):                    ("freight", "ship"),
}
