#%%
import pandas as pd
from basic_mappings import ALL_PATHS_SOURCE, ALL_PATHS_LEAP
from branch_expression_mapping import LEAP_BRANCH_TO_EXPRESSION_MAPPING
SHORTNAME_TO_LEAP_BRANCHES = {
    # Mapping to names used for reference in LEAP
    "Transport type (road)": [
        ("Passenger road",),
        ("Freight road",),
    ],
    "Vehicle type (road)": [
        ("Passenger road", "LPVs"),
        ("Passenger road", "Buses"),
        ("Passenger road", "Motorcycles"),
        ("Freight road", "Trucks"),
        ("Freight road", "LCVs"),
    ],
    "Technology (road)": [
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
        ("Passenger road", "LPVs", "EREV small"),
        ("Passenger road", "LPVs", "EREV medium"),
        ("Passenger road", "LPVs", "EREV large"),
        ("Passenger road", "Buses", "BEV"),
        ("Passenger road", "Buses", "ICE"),
        ("Passenger road", "Buses", "FCEV"),
        ("Passenger road", "Buses", "PHEV"),
        ("Passenger road", "Buses", "EREV"),
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
        ("Freight road", "Trucks", "PHEV heavy"),
        ("Freight road", "Trucks", "PHEV medium"),
        ("Freight road", "LCVs", "ICE"),
        ("Freight road", "LCVs", "BEV"),
        ("Freight road", "LCVs", "PHEV"),
        ("Freight road", "LCVs", "EREV"),
    ],
    "Fuel (road)": [
        ("Passenger road", "LPVs", "BEV small", "Electricity"),
        ("Passenger road", "LPVs", "BEV medium", "Electricity"),
        ("Passenger road", "LPVs", "BEV large", "Electricity"),
        ("Passenger road", "LPVs", "ICE small", "Gasoline"),
        ("Passenger road", "LPVs", "ICE small", "Diesel"),
        ("Passenger road", "LPVs", "ICE small", "Efuel"),
        ("Passenger road", "LPVs", "ICE small", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE small", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE small", "Efuel"),
        ("Passenger road", "LPVs", "ICE medium", "Gasoline"),
        ("Passenger road", "LPVs", "ICE medium", "Diesel"),
        ("Passenger road", "LPVs", "ICE medium", "Efuel"),
        ("Passenger road", "LPVs", "ICE medium", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE medium", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE medium", "Efuel"),
        ("Passenger road", "LPVs", "ICE medium", "LPG"),
        ("Passenger road", "LPVs", "ICE medium", "CNG"),
        ("Passenger road", "LPVs", "ICE medium", "Biogas"),
        ("Passenger road", "LPVs", "ICE large", "Gasoline"),
        ("Passenger road", "LPVs", "ICE large", "Diesel"),
        ("Passenger road", "LPVs", "ICE large", "Efuel"),
        ("Passenger road", "LPVs", "ICE large", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE large", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE large", "Efuel"),
        ("Passenger road", "LPVs", "ICE large", "LPG"),
        ("Passenger road", "LPVs", "ICE large", "CNG"),
        ("Passenger road", "LPVs", "ICE large", "Biogas"),
        ("Passenger road", "LPVs", "PHEV small", "Electricity"),
        ("Passenger road", "LPVs", "PHEV small", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Diesel"),
        ("Passenger road", "LPVs", "PHEV small", "Efuel"),
        ("Passenger road", "LPVs", "PHEV small", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV small", "Efuel"),
        ("Passenger road", "LPVs", "PHEV medium", "Electricity"),
        ("Passenger road", "LPVs", "PHEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Diesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Efuel"),
        ("Passenger road", "LPVs", "PHEV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Efuel"),
        ("Passenger road", "LPVs", "PHEV large", "Electricity"),
        ("Passenger road", "LPVs", "PHEV large", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Diesel"),
        ("Passenger road", "LPVs", "PHEV large", "Efuel"),
        ("Passenger road", "LPVs", "PHEV large", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV large", "Efuel"),
        ("Passenger road", "LPVs", "HEV small", "Gasoline"),
        ("Passenger road", "LPVs", "HEV small", "Diesel"),
        ("Passenger road", "LPVs", "HEV small", "Efuel"),
        ("Passenger road", "LPVs", "HEV small", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV small", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV small", "Efuel"),
        ("Passenger road", "LPVs", "HEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Diesel"),
        ("Passenger road", "LPVs", "HEV medium", "Efuel"),
        ("Passenger road", "LPVs", "HEV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV medium", "Efuel"),
        ("Passenger road", "LPVs", "HEV large", "Gasoline"),
        ("Passenger road", "LPVs", "HEV large", "Diesel"),
        ("Passenger road", "LPVs", "HEV large", "Efuel"),
        ("Passenger road", "LPVs", "HEV large", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV large", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV large", "Efuel"),
        ("Passenger road", "LPVs", "EREV small", "Electricity"),
        ("Passenger road", "LPVs", "EREV small", "Gasoline"),
        ("Passenger road", "LPVs", "EREV small", "Diesel"),
        ("Passenger road", "LPVs", "EREV small", "Efuel"),
        ("Passenger road", "LPVs", "EREV small", "Biogasoline"),
        ("Passenger road", "LPVs", "EREV small", "Biodiesel"),
        ("Passenger road", "LPVs", "EREV small", "Efuel"),
        ("Passenger road", "LPVs", "EREV medium", "Electricity"),
        ("Passenger road", "LPVs", "EREV medium", "Gasoline"),
        ("Passenger road", "LPVs", "EREV medium", "Diesel"),
        ("Passenger road", "LPVs", "EREV medium", "Efuel"),
        ("Passenger road", "LPVs", "EREV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "EREV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "EREV medium", "Efuel"),
        ("Passenger road", "LPVs", "EREV large", "Electricity"),
        ("Passenger road", "LPVs", "EREV large", "Gasoline"),
        ("Passenger road", "LPVs", "EREV large", "Diesel"),
        ("Passenger road", "LPVs", "EREV large", "Efuel"),
        ("Passenger road", "LPVs", "EREV large", "Biogasoline"),
        ("Passenger road", "LPVs", "EREV large", "Biodiesel"),
        ("Passenger road", "LPVs", "EREV large", "Efuel"),
        # Bus fuels
        ("Passenger road", "Buses", "BEV", "Electricity"),
        ("Passenger road", "Buses", "ICE", "Diesel"),
        ("Passenger road", "Buses", "ICE", "Efuel"),
        ("Passenger road", "Buses", "ICE", "Biodiesel"),
        ("Passenger road", "Buses", "ICE", "Efuel"),
        ("Passenger road", "Buses", "ICE", "Biogasoline"),
        ("Passenger road", "Buses", "ICE", "Gasoline"),
        ("Passenger road", "Buses", "ICE", "LPG"),
        ("Passenger road", "Buses", "ICE", "CNG"),
        ("Passenger road", "Buses", "ICE", "Biogas"),
        ("Passenger road", "Buses", "FCEV", "Hydrogen"),
        ("Passenger road", "Buses", "PHEV", "Electricity"),
        ("Passenger road", "Buses", "PHEV", "Gasoline"),
        ("Passenger road", "Buses", "PHEV", "Diesel"),
        ("Passenger road", "Buses", "PHEV", "Efuel"),
        ("Passenger road", "Buses", "PHEV", "Biogasoline"),
        ("Passenger road", "Buses", "PHEV", "Biodiesel"),
        ("Passenger road", "Buses", "PHEV", "Efuel"),
        ("Passenger road", "Buses", "EREV", "Electricity"),
        ("Passenger road", "Buses", "EREV", "Gasoline"),
        ("Passenger road", "Buses", "EREV", "Diesel"),
        ("Passenger road", "Buses", "EREV", "Efuel"),
        ("Passenger road", "Buses", "EREV", "Biogasoline"),
        ("Passenger road", "Buses", "EREV", "Biodiesel"),
        ("Passenger road", "Buses", "EREV", "Efuel"),
        # Motorcycles fuels
        ("Passenger road", "Motorcycles", "ICE", "Gasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Diesel"),
        ("Passenger road", "Motorcycles", "ICE", "Efuel"),
        ("Passenger road", "Motorcycles", "ICE", "Biogasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Biodiesel"),
        ("Passenger road", "Motorcycles", "ICE", "Efuel"),
        ("Passenger road", "Motorcycles", "BEV", "Electricity"),
        # Freight road fuels
        ("Freight road", "Trucks", "ICE heavy", "Gasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Diesel"),
        ("Freight road", "Trucks", "ICE heavy", "Biogasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Biodiesel"),
        ("Freight road", "Trucks", "ICE heavy", "Efuel"),
        ("Freight road", "Trucks", "ICE heavy", "LPG"),
        ("Freight road", "Trucks", "ICE heavy", "CNG"),
        ("Freight road", "Trucks", "ICE heavy", "LNG"),
        ("Freight road", "Trucks", "ICE heavy", "Biogas"),
        ("Freight road", "Trucks", "ICE medium", "Gasoline"),
        ("Freight road", "Trucks", "ICE medium", "Diesel"),
        ("Freight road", "Trucks", "ICE medium", "Biogasoline"),
        ("Freight road", "Trucks", "ICE medium", "Biodiesel"),
        ("Freight road", "Trucks", "ICE medium", "Efuel"),
        ("Freight road", "Trucks", "ICE medium", "LPG"),
        ("Freight road", "Trucks", "ICE medium", "CNG"),
        ("Freight road", "Trucks", "ICE medium", "LNG"),
        ("Freight road", "Trucks", "ICE medium", "Biogas"),
        ("Freight road", "Trucks", "BEV heavy", "Electricity"),
        ("Freight road", "Trucks", "BEV medium", "Electricity"),
        ("Freight road", "Trucks", "EREV medium", "Gasoline"),
        ("Freight road", "Trucks", "EREV medium", "Diesel"),
        ("Freight road", "Trucks", "EREV medium", "Electricity"),
        ("Freight road", "Trucks", "EREV medium", "Biogasoline"),
        ("Freight road", "Trucks", "EREV medium", "Biodiesel"),
        ("Freight road", "Trucks", "EREV medium", "Efuel"),
        ("Freight road", "Trucks", "EREV heavy", "Gasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Diesel"),
        ("Freight road", "Trucks", "EREV heavy", "Electricity"),
        ("Freight road", "Trucks", "EREV heavy", "Biogasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Biodiesel"),
        ("Freight road", "Trucks", "EREV heavy", "Efuel"),
        ("Freight road", "Trucks", "FCEV heavy", "Hydrogen"),
        ("Freight road", "Trucks", "FCEV medium", "Hydrogen"),
        ("Freight road", "Trucks", "PHEV heavy", "Electricity"),
        ("Freight road", "Trucks", "PHEV heavy", "Gasoline"),
        ("Freight road", "Trucks", "PHEV heavy", "Diesel"),
        ("Freight road", "Trucks", "PHEV heavy", "Biogasoline"),
        ("Freight road", "Trucks", "PHEV heavy", "Biodiesel"),
        ("Freight road", "Trucks", "PHEV heavy", "Efuel"),
        ("Freight road", "Trucks", "PHEV medium", "Electricity"),
        ("Freight road", "Trucks", "PHEV medium", "Gasoline"),
        ("Freight road", "Trucks", "PHEV medium", "Diesel"),
        ("Freight road", "Trucks", "PHEV medium", "Biogasoline"),
        ("Freight road", "Trucks", "PHEV medium", "Biodiesel"),
        ("Freight road", "Trucks", "PHEV medium", "Efuel"),
        # LCVs fuels
        ("Freight road", "LCVs", "ICE", "Gasoline"),
        ("Freight road", "LCVs", "ICE", "Diesel"),
        ("Freight road", "LCVs", "ICE", "Efuel"),
        ("Freight road", "LCVs", "ICE", "Biogasoline"),
        ("Freight road", "LCVs", "ICE", "Biodiesel"),
        ("Freight road", "LCVs", "ICE", "Efuel"),
        ("Freight road", "LCVs", "ICE", "LPG"),
        ("Freight road", "LCVs", "ICE", "CNG"),
        ("Freight road", "LCVs", "ICE", "Biogas"),
        ("Freight road", "LCVs", "BEV", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Gasoline"),
        ("Freight road", "LCVs", "PHEV", "Diesel"),
        ("Freight road", "LCVs", "PHEV", "Efuel"),
        ("Freight road", "LCVs", "PHEV", "Biogasoline"),
        ("Freight road", "LCVs", "PHEV", "Biodiesel"),
        ("Freight road", "LCVs", "PHEV", "Efuel"),
        ("Freight road", "LCVs", "EREV", "Electricity"),
        ("Freight road", "LCVs", "EREV", "Gasoline"),
        ("Freight road", "LCVs", "EREV", "Diesel"),
        ("Freight road", "LCVs", "EREV", "Efuel"),
        ("Freight road", "LCVs", "EREV", "Biogasoline"),
        ("Freight road", "LCVs", "EREV", "Biodiesel"),
        ("Freight road", "LCVs", "EREV", "Efuel"),
    ],

    #################################################
    # NON-ROAD TRANSPORT TYPES
    #################################################
    "Transport type (non-road)": [
        ("Passenger non road",),
        ("Freight non road",),
    ],

    "Vehicle type (non-road)": [
        ("Passenger non road", "Air"),
        ("Passenger non road", "Rail"),
        ("Passenger non road", "Shipping"),
        ("Freight non road", "Air"),
        ("Freight non road", "Rail"),
        ("Freight non road", "Shipping"),
    ],

    "Fuel (non-road)": [
        ("Passenger non road", "Air", "Hydrogen"),
        ("Passenger non road", "Air", "Electricity"),
        ("Passenger non road", "Air", "Jet fuel"),
        ("Passenger non road", "Air", "Efuel"),
        ("Passenger non road", "Air", "Aviation gasoline"),
        ("Passenger non road", "Air", "Biojet"),
        
        ("Passenger non road", "Rail", "Electricity"),
        ("Passenger non road", "Rail", "Diesel"),
        ("Passenger non road", "Rail", "Efuel"),
        ("Passenger non road", "Rail", "Hydrogen"),
        ("Passenger non road", "Rail", "Coal"),
        ("Passenger non road", "Rail", "Biodiesel"),
        
        ("Passenger non road", "Shipping", "Electricity"),
        ("Passenger non road", "Shipping", "Hydrogen"),
        ("Passenger non road", "Shipping", "Diesel"),
        ("Passenger non road", "Shipping", "Efuel"),
        ("Passenger non road", "Shipping", "Fuel oil"),
        ("Passenger non road", "Shipping", "LNG"),
        ("Passenger non road", "Shipping", "Gasoline"),
        ("Passenger non road", "Shipping", "Ammonia"),
        ("Passenger non road", "Shipping", "Biodiesel"),
        ("Passenger non road", "Shipping", "Biogasoline"),
        
        ("Freight non road", "Air", "Hydrogen"),
        ("Freight non road", "Air", "Electricity"),
        ("Freight non road", "Air", "Jet fuel"),
        ("Freight non road", "Air", "Efuel"),
        ("Freight non road", "Air", "Aviation gasoline"),
        ("Freight non road", "Air", "Biojet"),
        
        ("Freight non road", "Rail", "Electricity"),
        ("Freight non road", "Rail", "Diesel"),
        ("Freight non road", "Rail", "Efuel"),
        ("Freight non road", "Rail", "Hydrogen"),
        ("Freight non road", "Rail", "Coal"),
        ("Freight non road", "Rail", "Biodiesel"),
        
        ("Freight non road", "Shipping", "Electricity"),
        ("Freight non road", "Shipping", "Hydrogen"),
        ("Freight non road", "Shipping", "Diesel"),
        ("Freight non road", "Shipping", "Efuel"),
        ("Freight non road", "Shipping", "Fuel oil"),
        ("Freight non road", "Shipping", "LNG"),
        ("Freight non road", "Shipping", "Gasoline"),
        ("Freight non road", "Shipping", "Ammonia"),
        ("Freight non road", "Shipping", "Biodiesel"),
        ("Freight non road", "Shipping", "Biogasoline"),
    ],

    "Others (level 1)": [
        ("Nonspecified transport",),
        ("Pipeline transport",)
    ],
    "Others (level 2)": [
        ("Nonspecified transport", "Kerosene"),
        ("Nonspecified transport", "Fuel oil"),
        ("Nonspecified transport", "Diesel"),
        ("Nonspecified transport", "LPG"),
        ("Nonspecified transport", "Gasoline"),
        ("Nonspecified transport", "Coal products"),
        ("Nonspecified transport", "Other petroleum products"),
        ("Nonspecified transport", "Natural gas"),
        ("Nonspecified transport", "Electricity"),
        ("Pipeline transport", "Fuel oil"),
        ("Pipeline transport", "Diesel"),
        ("Pipeline transport", "Natural gas"),
        ("Pipeline transport", "Electricity"),
    ]
}
ALL_LEAP_BRANCHES_TRANSPORT = [branch for branches in SHORTNAME_TO_LEAP_BRANCHES.values() for branch in branches]

LEAP_BRANCH_TO_SOURCE_MAP = {
    #tuples map to: LEAP: (Transport Type, Medium, Vehicle Type, Drive, Fuel): (Transport Type, Medium, Vehicle Type, Drive, Fuel)
    #for non-road mediums, the LEAP tuple omits vehicle type and drive since they don't apply
    #no one-to-many mappings to avoid complications

    #note that there are # proxy and # combination comments where we are using a newly created row that is created either as a proxy or a combination of rows in the source data. 
    # If the row is a combination then the first value in the tuple needs to begin with "Combined ". This helps the code avoid double counting energy use. 
    # =========================
    # NON-ROAD: PASSENGER
    # =========================
    ("Passenger non road", "Air", "Hydrogen"):        ("passenger", "air", "all", "air_hydrogen", "Hydrogen"),
    ("Passenger non road", "Air", "Electricity"):     ("passenger", "air", "all", "air_electric", "Electricity"),
    ("Passenger non road", "Air", "Jet fuel"):        ("passenger", "air", "all", "air_jet_fuel", "Jet fuel"),
    ("Passenger non road", "Air", "Efuel"):          ("passenger", "air", "all", "air_jet_fuel", "Efuel"),
    ("Passenger non road", "Air", "Aviation gasoline"): ("passenger", "air", "all", "air_av_gas", "Aviation gasoline"),
    ("Passenger non road", "Air", "Biojet"):         ("passenger", "air", "all", "air_jet_fuel", "Biojet"),

    ("Passenger non road", "Rail", "Electricity"):    ("passenger", "rail", "all", "rail_electricity", "Electricity"),
    ("Passenger non road", "Rail", "Diesel"):         ("passenger", "rail", "all", "rail_diesel", "Diesel"),
    ("Passenger non road", "Rail", "Efuel"):          ("passenger", "rail", "all", "rail_diesel", "Efuel"),
    ("Passenger non road", "Rail", "Hydrogen"):       ("passenger", "rail", "all", "rail_hydrogen", "Hydrogen"), # proxy
    ("Passenger non road", "Rail", "Coal"):           ("passenger", "rail", "all", "rail_coal", "Coal"),
    ("Passenger non road", "Rail", "Biodiesel"):      ("passenger", "rail", "all", "rail_diesel", "Biodiesel"),

    ("Passenger non road", "Shipping", "Electricity"):("passenger", "ship", "all", "ship_electric", "Electricity"),
    ("Passenger non road", "Shipping", "Hydrogen"):   ("passenger", "ship", "all", "ship_hydrogen", "Hydrogen"),
    ("Passenger non road", "Shipping", "Diesel"):     ("passenger", "ship", "all", "ship_diesel", "Diesel"),
    ("Passenger non road", "Shipping", "Efuel"):      ("passenger", "ship", "all", "ship_diesel", "Efuel"),
    ("Passenger non road", "Shipping", "Fuel oil"):   ("passenger", "ship", "all", "ship_fuel_oil", "Fuel oil"),
    ("Passenger non road", "Shipping", "LNG"):        ("passenger", "ship", "all", "ship_lng", "LNG"),
    ("Passenger non road", "Shipping", "Gasoline"):   ("passenger", "ship", "all", "ship_gasoline", "Gasoline"),
    ("Passenger non road", "Shipping", "Ammonia"):    ("passenger", "ship", "all", "ship_ammonia", "Ammonia"),
    ("Passenger non road", "Shipping", "Biogasoline"): ("passenger", "ship", "all", "ship_gasoline", "Biogasoline"),
    ("Passenger non road", "Shipping", "Biodiesel"):   ("passenger", "ship", "all", "ship_diesel", "Biodiesel"),

    # =========================
    # NON-ROAD: FREIGHT
    # =========================
    ("Freight non road", "Air", "Hydrogen"):          ("freight", "air", "all", "air_hydrogen", "Hydrogen"),
    ("Freight non road", "Air", "Electricity"):       ("freight", "air", "all", "air_electric", "Electricity"),
    ("Freight non road", "Air", "Jet fuel"):          ("freight", "air", "all", "air_jet_fuel", "Jet fuel"),
    ("Freight non road", "Air", "Efuel"):            ("freight", "air", "all", "air_jet_fuel", "Efuel"),
    ("Freight non road", "Air", "Aviation gasoline"): ("freight", "air", "all", "air_av_gas", "Aviation gasoline"),
    ("Freight non road", "Air", "Biojet"):           ("freight", "air", "all", "air_jet_fuel", "Biojet"),

    ("Freight non road", "Rail", "Electricity"):      ("freight", "rail", "all", "rail_electricity", "Electricity"),
    ("Freight non road", "Rail", "Diesel"):           ("freight", "rail", "all", "rail_diesel", "Diesel"),
    ("Freight non road", "Rail", "Efuel"):            ("freight", "rail", "all", "rail_diesel", "Efuel"),
    ("Freight non road", "Rail", "Hydrogen"):         ("freight", "rail", "all", "rail_hydrogen", "Hydrogen"), # proxy
    ("Freight non road", "Rail", "Coal"):             ("freight", "rail", "all", "rail_coal", "Coal"),
    ("Freight non road", "Rail", "Biodiesel"):       ("freight", "rail", "all", "rail_diesel", "Biodiesel"), 

    ("Freight non road", "Shipping", "Electricity"):  ("freight", "ship", "all", "ship_electric", "Electricity"),
    ("Freight non road", "Shipping", "Hydrogen"):     ("freight", "ship", "all", "ship_hydrogen", "Hydrogen"),
    ("Freight non road", "Shipping", "Diesel"):       ("freight", "ship", "all", "ship_diesel", "Diesel"),
    ("Freight non road", "Shipping", "Efuel"):        ("freight", "ship", "all", "ship_diesel", "Efuel"),
    ("Freight non road", "Shipping", "Fuel oil"):     ("freight", "ship", "all", "ship_fuel_oil", "Fuel oil"),
    ("Freight non road", "Shipping", "LNG"):          ("freight", "ship", "all", "ship_lng", "LNG"),
    ("Freight non road", "Shipping", "Gasoline"):     ("freight", "ship", "all", "ship_gasoline", "Gasoline"),
    ("Freight non road", "Shipping", "Ammonia"):      ("freight", "ship", "all", "ship_ammonia", "Ammonia"),
    ("Freight non road", "Shipping", "Biogasoline"):  ("freight", "ship", "all", "ship_gasoline", "Biogasoline"), 
    ("Freight non road", "Shipping", "Biodiesel"):    ("freight", "ship", "all", "ship_diesel", "Biodiesel"), 

    # =====================================================
    # ROAD: PASSENGER ROAD → LPVs
    # =====================================================
    ("Passenger road", "LPVs", "BEV small", "Electricity"):   ("passenger", "road", "car", "bev", "Electricity"),
    ("Passenger road", "LPVs", "BEV medium", "Electricity"):  ("passenger", "road", "suv", "bev", "Electricity"),
    ("Passenger road", "LPVs", "BEV large", "Electricity"):   ("passenger", "road", "lt", "bev", "Electricity"),

    ("Passenger road", "LPVs", "ICE small", "Gasoline"):      ("passenger", "road", "car", "ice_g", "Gasoline"),
    ("Passenger road", "LPVs", "ICE small", "Diesel"):        ("passenger", "road", "car", "ice_d", "Diesel"),
    ("Passenger road", "LPVs", "ICE small", "Efuel"):        ("passenger", "road", "car", "ice_d", "Efuel"),
    ("Passenger road", "LPVs", "ICE small", "Biogasoline"):       ("passenger", "road", "car", "ice_g", "Biogasoline"), #proxy?
    ("Passenger road", "LPVs", "ICE small", "Biodiesel"):       ("passenger", "road", "car", "ice_d", "Biodiesel"), #proxy?

    ("Passenger road", "LPVs", "ICE medium", "Gasoline"):     ("passenger", "road", "suv", "ice_g", "Gasoline"),
    ("Passenger road", "LPVs", "ICE medium", "Diesel"):       ("passenger", "road", "suv", "ice_d", "Diesel"),
    ("Passenger road", "LPVs", "ICE medium", "Efuel"):        ("passenger", "road", "suv", "ice_d", "Efuel"),
    ("Passenger road", "LPVs", "ICE medium", "Biodiesel"):       ("passenger", "road", "suv", "ice_d", "Biodiesel"),
    ("Passenger road", "LPVs", "ICE medium", "Biogasoline"):      ("passenger", "road", "suv", "ice_g", "Biogasoline"), 
    ("Passenger road", "LPVs", "ICE medium", "LPG"):          ("passenger", "road", "suv", "lpg", "LPG"),
    ("Passenger road", "LPVs", "ICE medium", "CNG"):          ("passenger", "road", "suv", "cng", "CNG"),
    ("Passenger road", "LPVs", "ICE medium", "Biogas"):       ("passenger", "road", "suv", "cng", "Biogas"), #proxy?

    ("Passenger road", "LPVs", "ICE large", "Gasoline"):      ("passenger", "road", "lt", "ice_g", "Gasoline"),
    ("Passenger road", "LPVs", "ICE large", "Diesel"):        ("passenger", "road", "lt", "ice_d", "Diesel"),
    ("Passenger road", "LPVs", "ICE large", "Efuel"):        ("passenger", "road", "lt", "ice_d", "Efuel"),
    ("Passenger road", "LPVs", "ICE large", "Biodiesel"):        ("passenger", "road", "lt", "ice_d", "Biodiesel"), 
    ("Passenger road", "LPVs", "ICE large", "Biogasoline"):       ("passenger", "road", "lt", "ice_g", "Biogasoline"), 
    ("Passenger road", "LPVs", "ICE large", "LPG"):           ("passenger", "road", "lt", "lpg", "LPG"),
    ("Passenger road", "LPVs", "ICE large", "CNG"):           ("passenger", "road", "lt", "cng", "CNG"),
    ("Passenger road", "LPVs", "ICE large", "Biogas"):        ("passenger", "road", "lt", "cng", "Biogas"), #proxy?

    ("Passenger road", "LPVs", "PHEV small", "Electricity"):  ("passenger", "road", "car", "phev_g", "Electricity"),
    ("Passenger road", "LPVs", "PHEV small", "Gasoline"):     ("passenger", "road", "car", "phev_g", "Gasoline"),
    ("Passenger road", "LPVs", "PHEV small", "Diesel"):       ("passenger", "road", "car", "phev_d", "Diesel"),
    ("Passenger road", "LPVs", "PHEV small", "Efuel"):       ("passenger", "road", "car", "phev_d", "Efuel"),
    ("Passenger road", "LPVs", "PHEV small", "Biodiesel"):       ("passenger", "road", "car", "phev_d", "Biodiesel"), 
    ("Passenger road", "LPVs", "PHEV small", "Biogasoline"):      ("passenger", "road", "car", "phev_g", "Biogasoline"), 

    ("Passenger road", "LPVs", "PHEV medium", "Electricity"): ("passenger", "road", "suv", "phev_g", "Electricity"),
    ("Passenger road", "LPVs", "PHEV medium", "Gasoline"):    ("passenger", "road", "suv", "phev_g", "Gasoline"),
    ("Passenger road", "LPVs", "PHEV medium", "Diesel"):      ("passenger", "road", "suv", "phev_d", "Diesel"),
    ("Passenger road", "LPVs", "PHEV medium", "Efuel"):      ("passenger", "road", "suv", "phev_d", "Efuel"),
    ("Passenger road", "LPVs", "PHEV medium", "Biodiesel"):      ("passenger", "road", "suv", "phev_d", "Biodiesel"), 
    ("Passenger road", "LPVs", "PHEV medium", "Biogasoline"):     ("passenger", "road", "suv", "phev_g", "Biogasoline"), 

    ("Passenger road", "LPVs", "PHEV large", "Electricity"):  ("passenger", "road", "lt", "phev_g", "Electricity"),
    ("Passenger road", "LPVs", "PHEV large", "Gasoline"):     ("passenger", "road", "lt", "phev_g", "Gasoline"),
    ("Passenger road", "LPVs", "PHEV large", "Diesel"):       ("passenger", "road", "lt", "phev_d", "Diesel"),
    ("Passenger road", "LPVs", "PHEV large", "Efuel"):       ("passenger", "road", "lt", "phev_d", "Efuel"),
    ("Passenger road", "LPVs", "PHEV large", "Biodiesel"):       ("passenger", "road", "lt", "phev_d", "Biodiesel"), 
    ("Passenger road", "LPVs", "PHEV large", "Biogasoline"):      ("passenger", "road", "lt", "phev_g", "Biogasoline"), 

    ("Passenger road", "LPVs", "HEV small", "Gasoline"):      ("passenger", "road", "car", "hev_g", "Gasoline"), #proxy?
    ("Passenger road", "LPVs", "HEV small", "Diesel"):        ("passenger", "road", "car", "hev_d", "Diesel"), #proxy?
    ("Passenger road", "LPVs", "HEV small", "Efuel"):        ("passenger", "road", "car", "hev_g", "Efuel"),#proxy?
    ("Passenger road", "LPVs", "HEV small", "Biodiesel"):        ("passenger", "road", "car", "hev_d", "Biodiesel"), #proxy?
    ("Passenger road", "LPVs", "HEV small", "Biogasoline"):       ("passenger", "road", "car", "hev_g", "Biogasoline"), #proxy?
    
    ("Passenger road", "LPVs", "HEV medium", "Gasoline"):     ("passenger", "road", "suv", "hev_g", "Gasoline"), #proxy?
    ("Passenger road", "LPVs", "HEV medium", "Diesel"):       ("passenger", "road", "suv", "hev_d", "Diesel"), #proxy?
    ("Passenger road", "LPVs", "HEV medium", "Efuel"):       ("passenger", "road", "suv", "hev_g", "Efuel"),#proxy?
    ("Passenger road", "LPVs", "HEV medium", "Biodiesel"):       ("passenger", "road", "suv", "hev_d", "Biodiesel"), #proxy?
    ("Passenger road", "LPVs", "HEV medium", "Biogasoline"):      ("passenger", "road", "suv", "hev_g", "Biogasoline"), #proxy?
    
    ("Passenger road", "LPVs", "HEV large", "Gasoline"):      ("passenger", "road", "lt", "hev_g", "Gasoline"), #proxy?
    ("Passenger road", "LPVs", "HEV large", "Diesel"):        ("passenger", "road", "lt", "hev_d", "Diesel"), #proxy?
    ("Passenger road", "LPVs", "HEV large", "Efuel"):        ("passenger", "road", "lt", "hev_g", "Efuel"),#proxy?
    ("Passenger road", "LPVs", "HEV large", "Biodiesel"):        ("passenger", "road", "lt", "hev_d", "Biodiesel"), #proxy?
     #proxy?
    ("Passenger road", "LPVs", "HEV large", "Biogasoline"):       ("passenger", "road", "lt", "hev_g", "Biogasoline"), #proxy?

    ("Passenger road", "LPVs", "EREV small", "Gasoline"):      ("passenger", "road", "car", "erev_g", "Gasoline"), #proxy?
    ("Passenger road", "LPVs", "EREV small", "Diesel"):        ("passenger", "road", "car", "erev_d", "Diesel"), #proxy?
    ("Passenger road", "LPVs", "EREV small", "Efuel"):        ("passenger", "road", "car", "erev_g", "Efuel"),#proxy?
    ("Passenger road", "LPVs", "EREV small", "Biodiesel"):        ("passenger", "road", "car", "erev_d", "Biodiesel"), #proxy?
    ("Passenger road", "LPVs", "EREV small", "Biogasoline"):       ("passenger", "road", "car", "erev_g", "Biogasoline"), #proxy?
    ("Passenger road", "LPVs", "EREV small", "Electricity"):   ("passenger", "road", "car", "erev_g", "Electricity"), #proxy?
    
    ("Passenger road", "LPVs", "EREV medium", "Gasoline"):     ("passenger", "road", "suv", "erev_g", "Gasoline"), #proxy?
    ("Passenger road", "LPVs", "EREV medium", "Diesel"):       ("passenger", "road", "suv", "erev_d", "Diesel"), #proxy?
    ("Passenger road", "LPVs", "EREV medium", "Efuel"):       ("passenger", "road", "suv", "erev_g", "Efuel"),#proxy?
    ("Passenger road", "LPVs", "EREV medium", "Biodiesel"):       ("passenger", "road", "suv", "erev_d", "Biodiesel"), #proxy?
    ("Passenger road", "LPVs", "EREV medium", "Biogasoline"):      ("passenger", "road", "suv", "erev_g", "Biogasoline"), #proxy?
    ("Passenger road", "LPVs", "EREV medium", "Electricity"):   ("passenger", "road", "suv", "erev_g", "Electricity"), #proxy?
    
    ("Passenger road", "LPVs", "EREV large", "Gasoline"):      ("passenger", "road", "lt", "erev_g", "Gasoline"), #proxy?
    ("Passenger road", "LPVs", "EREV large", "Diesel"):        ("passenger", "road", "lt", "erev_d", "Diesel"), #proxy?
    ("Passenger road", "LPVs", "EREV large", "Efuel"):        ("passenger", "road", "lt", "erev_g", "Efuel"),#proxy?
    ("Passenger road", "LPVs", "EREV large", "Biodiesel"):        ("passenger", "road", "lt", "erev_d", "Biodiesel"), #proxy?
    ("Passenger road", "LPVs", "EREV large", "Biogasoline"):       ("passenger", "road", "lt", "erev_g", "Biogasoline"), #proxy?
    ("Passenger road", "LPVs", "EREV large", "Electricity"):   ("passenger", "road", "lt", "erev_g", "Electricity"), #proxy?
    # =========================
    # ROAD: PASSENGER ROAD → Buses
    # =========================
    ("Passenger road", "Buses", "BEV", "Electricity"):        ("passenger", "road", "bus", "bev", "Electricity"),
    ("Passenger road", "Buses", "ICE", "Diesel"):             ("passenger", "road", "bus", "ice_d", "Diesel"),
    ("Passenger road", "Buses", "ICE", "Efuel"):             ("passenger", "road", "bus", "ice_d", "Efuel"),
    ("Passenger road", "Buses", "ICE", "Gasoline"):           ("passenger", "road", "bus", "ice_g", "Gasoline"),
    ("Passenger road", "Buses", "ICE", "LPG"):                ("passenger", "road", "bus", "lpg", "LPG"),
    ("Passenger road", "Buses", "ICE", "CNG"):                ("passenger", "road", "bus", "cng", "CNG"),
    ("Passenger road", "Buses", "ICE", "Biogas"):             ("passenger", "road", "bus", "cng", "Biogas"), #proxy?
    ("Passenger road", "Buses", "FCEV", "Hydrogen"):          ("passenger", "road", "bus", "fcev", "Hydrogen"),
    ("Passenger road", "Buses", "ICE", "Biodiesel"): ("passenger", "road", "bus", "ice_d", "Biodiesel"), 
    ("Passenger road", "Buses", "ICE", "Biogasoline"): ("passenger", "road", "bus", "ice_g", "Biogasoline"), 
    # PHEV buses
    ("Passenger road", "Buses", "PHEV", "Electricity"):       ("passenger", "road", "bus", "phev_g", "Electricity"),
    ("Passenger road", "Buses", "PHEV", "Gasoline"):          ("passenger", "road", "bus", "phev_g", "Gasoline"),
    ("Passenger road", "Buses", "PHEV", "Diesel"):            ("passenger", "road", "bus", "phev_d", "Diesel"),
    ("Passenger road", "Buses", "PHEV", "Efuel"):            ("passenger", "road", "bus", "phev_d", "Efuel"),
    ("Passenger road", "Buses", "PHEV", "Biodiesel"):         ("passenger", "road", "bus", "phev_d", "Biodiesel"), 
    ("Passenger road", "Buses", "PHEV", "Biogasoline"):       ("passenger", "road", "bus", "phev_g", "Biogasoline"), 

    # EREV buses  
    ("Passenger road", "Buses", "EREV", "Electricity"):       ("passenger", "road", "bus", "erev_g", "Electricity"), #proxy?
    ("Passenger road", "Buses", "EREV", "Gasoline"):          ("passenger", "road", "bus", "erev_g", "Gasoline"), #proxy?
    ("Passenger road", "Buses", "EREV", "Diesel"):            ("passenger", "road", "bus", "erev_d", "Diesel"), #proxy?
    ("Passenger road", "Buses", "EREV", "Efuel"):            ("passenger", "road", "bus", "erev_d", "Efuel"),#proxy?
    ("Passenger road", "Buses", "EREV", "Biodiesel"):         ("passenger", "road", "bus", "erev_d", "Biodiesel"), #proxy?
    ("Passenger road", "Buses", "EREV", "Biogasoline"):       ("passenger", "road", "bus", "erev_g", "Biogasoline"), #proxy?

    # =========================
    # ROAD: PASSENGER ROAD → Motorcycles
    # =========================
    ("Passenger road", "Motorcycles", "ICE", "Gasoline"):     ("passenger", "road", "2w", "ice_g", "Gasoline"),
    ("Passenger road", "Motorcycles", "ICE", "Diesel"):       ("passenger", "road", "2w", "ice_d", "Diesel"),
    ("Passenger road", "Motorcycles", "ICE", "Efuel"):       ("passenger", "road", "2w", "ice_d", "Efuel"),
    ("Passenger road", "Motorcycles", "ICE", "Biodiesel"):      ("passenger", "road", "2w", "ice_d", "Biodiesel"), 
    ("Passenger road", "Motorcycles", "ICE", "Biogasoline"):      ("passenger", "road", "2w", "ice_g", "Biogasoline"), 
    ("Passenger road", "Motorcycles", "BEV", "Electricity"):  ("passenger", "road", "2w", "bev", "Electricity"),

    # =========================
    # ROAD: FREIGHT ROAD → Trucks #todo we dont have phev trucks in here. need to create them
    # =========================
    ("Freight road", "Trucks", "ICE heavy", "Gasoline"):      ("freight", "road", "ht", "ice_g", "Gasoline"),
    ("Freight road", "Trucks", "ICE heavy", "Diesel"):        ("freight", "road", "ht", "ice_d", "Diesel"),
    ("Freight road", "Trucks", "ICE heavy", "Efuel"):        ("freight", "road", "ht", "ice_d", "Efuel"),
    ("Freight road", "Trucks", "ICE heavy", "Biodiesel"):        ("freight", "road", "ht", "ice_d", "Biodiesel"), 
    ("Freight road", "Trucks", "ICE heavy", "Biogasoline"):       ("freight", "road", "ht", "ice_g", "Biogasoline"), 
    ("Freight road", "Trucks", "ICE heavy", "LPG"):           ("freight", "road", "ht", "lpg", "LPG"),
    ("Freight road", "Trucks", "ICE heavy", "CNG"):           ("freight", "road", "ht", "cng", "CNG"),
    ("Freight road", "Trucks", "ICE heavy", "LNG"):           ("freight", "road", "ht", "lng", "LNG"),
    ("Freight road", "Trucks", "ICE heavy", "Biogas"):        ("freight", "road", "ht", "cng", "Biogas"), #proxy?

    ("Freight road", "Trucks", "ICE medium", "Gasoline"):     ("freight", "road", "mt", "ice_g", "Gasoline"),
    ("Freight road", "Trucks", "ICE medium", "Diesel"):       ("freight", "road", "mt", "ice_d", "Diesel"),
    ("Freight road", "Trucks", "ICE medium", "Efuel"):       ("freight", "road", "mt", "ice_d", "Efuel"),
    ("Freight road", "Trucks", "ICE medium", "Biodiesel"):       ("freight", "road", "mt", "ice_d", "Biodiesel"), 
    ("Freight road", "Trucks", "ICE medium", "Biogasoline"):      ("freight", "road", "mt", "ice_g", "Biogasoline"), 
    ("Freight road", "Trucks", "ICE medium", "LNG"):          ("freight", "road", "mt", "lng", "LNG"),
    ("Freight road", "Trucks", "ICE medium", "CNG"):          ("freight", "road", "mt", "cng", "CNG"),
    ("Freight road", "Trucks", "ICE medium", "LPG"):          ("freight", "road", "mt", "lpg", "LPG"),
    ("Freight road", "Trucks", "ICE medium", "Biogas"):       ("freight", "road", "mt", "cng", "Biogas"), #proxy?

    ("Freight road", "Trucks", "BEV heavy", "Electricity"):   ("freight", "road", "ht", "bev", "Electricity"),
    ("Freight road", "Trucks", "BEV medium", "Electricity"):  ("freight", "road", "mt", "bev", "Electricity"),

    ("Freight road", "Trucks", "EREV medium", "Gasoline"):    ("freight", "road", "mt", "erev_g", "Gasoline"), #proxy?
    ("Freight road", "Trucks", "EREV medium", "Electricity"): ("freight", "road", "mt", "erev_g", "Electricity"), #proxy?
    ("Freight road", "Trucks", "EREV medium", "Diesel"):      ("freight", "road", "mt", "erev_d", "Diesel"), #proxy?
    ("Freight road", "Trucks", "EREV medium", "Efuel"):      ("freight", "road", "mt", "erev_d", "Efuel"),#proxy?
    ("Freight road", "Trucks", "EREV medium", "Biodiesel"):      ("freight", "road", "mt", "erev_d", "Biodiesel"), #proxy?
    ("Freight road", "Trucks", "EREV medium", "Biogasoline"):     ("freight", "road", "mt", "erev_g", "Biogasoline"), #proxy?

    ("Freight road", "Trucks", "EREV heavy", "Gasoline"):     ("freight", "road", "ht", "erev_g", "Gasoline"), #proxy?
    ("Freight road", "Trucks", "EREV heavy", "Electricity"):  ("freight", "road", "ht", "erev_g", "Electricity"), #proxy?
    ("Freight road", "Trucks", "EREV heavy", "Diesel"):       ("freight", "road", "ht", "erev_d", "Diesel"), #proxy?
    ("Freight road", "Trucks", "EREV heavy", "Efuel"):       ("freight", "road", "ht", "erev_d", "Efuel"),#proxy?
    ("Freight road", "Trucks", "EREV heavy", "Biodiesel"):       ("freight", "road", "ht", "erev_d", "Biodiesel"), #proxy?
    ("Freight road", "Trucks", "EREV heavy", "Biogasoline"):      ("freight", "road", "ht", "erev_g", "Biogasoline"), #proxy?

    ("Freight road", "Trucks", "FCEV heavy", "Hydrogen"):     ("freight", "road", "ht", "fcev", "Hydrogen"),
    ("Freight road", "Trucks", "FCEV medium", "Hydrogen"):    ("freight", "road", "mt", "fcev", "Hydrogen"),
    
    # PHEV trucks (missing from the original mappings)
    ("Freight road", "Trucks", "PHEV medium", "Gasoline"):    ("freight", "road", "mt", "phev_g", "Gasoline"),
    ("Freight road", "Trucks", "PHEV medium", "Electricity"): ("freight", "road", "mt", "phev_g", "Electricity"),
    ("Freight road", "Trucks", "PHEV medium", "Diesel"):      ("freight", "road", "mt", "phev_d", "Diesel"),
    ("Freight road", "Trucks", "PHEV medium", "Efuel"):      ("freight", "road", "mt", "phev_d", "Efuel"),
    ("Freight road", "Trucks", "PHEV medium", "Biodiesel"):   ("freight", "road", "mt", "phev_d", "Biodiesel"), 
    ("Freight road", "Trucks", "PHEV medium", "Biogasoline"): ("freight", "road", "mt", "phev_g", "Biogasoline"), 

    ("Freight road", "Trucks", "PHEV heavy", "Gasoline"):     ("freight", "road", "ht", "phev_g", "Gasoline"),
    ("Freight road", "Trucks", "PHEV heavy", "Electricity"):  ("freight", "road", "ht", "phev_g", "Electricity"),
    ("Freight road", "Trucks", "PHEV heavy", "Diesel"):       ("freight", "road", "ht", "phev_d", "Diesel"),
    ("Freight road", "Trucks", "PHEV heavy", "Efuel"):       ("freight", "road", "ht", "phev_d", "Efuel"),
    ("Freight road", "Trucks", "PHEV heavy", "Biodiesel"):    ("freight", "road", "ht", "phev_d", "Biodiesel"), 
    ("Freight road", "Trucks", "PHEV heavy", "Biogasoline"):  ("freight", "road", "ht", "phev_g", "Biogasoline"), 

    # =========================
    # ROAD: FREIGHT ROAD → LCVs
    # =========================
    ("Freight road", "LCVs", "ICE", "Gasoline"):              ("freight", "road", "lcv", "ice_g", "Gasoline"),
    ("Freight road", "LCVs", "ICE", "Diesel"):                ("freight", "road", "lcv", "ice_d", "Diesel"),
    ("Freight road", "LCVs", "ICE", "Efuel"):                ("freight", "road", "lcv", "ice_d", "Efuel"),
    ("Freight road", "LCVs", "ICE", "Biodiesel"):                ("freight", "road", "lcv", "ice_d", "Biodiesel"), 
    ("Freight road", "LCVs", "ICE", "Biogasoline"):               ("freight", "road", "lcv", "ice_g", "Biogasoline"), 
    ("Freight road", "LCVs", "ICE", "CNG"):                   ("freight", "road", "lcv", "cng", "CNG"),
    ("Freight road", "LCVs", "ICE", "LPG"):                   ("freight", "road", "lcv", "lpg", "LPG"),
    ("Freight road", "LCVs", "ICE", "Biogas"):                ("freight", "road", "lcv", "cng", "Biogas"), #proxy?
    
    ("Freight road", "LCVs", "BEV", "Electricity"):           ("freight", "road", "lcv", "bev", "Electricity"),
    
    ("Freight road", "LCVs", "PHEV", "Electricity"):          ("freight", "road", "lcv", "phev_g", "Electricity"),
    ("Freight road", "LCVs", "PHEV", "Gasoline"):             ("freight", "road", "lcv", "phev_g", "Gasoline"),
    ("Freight road", "LCVs", "PHEV", "Diesel"):               ("freight", "road", "lcv", "phev_d", "Diesel"),
    ("Freight road", "LCVs", "PHEV", "Efuel"):               ("freight", "road", "lcv", "phev_d", "Efuel"),
    ("Freight road", "LCVs", "PHEV", "Biodiesel"):               ("freight", "road", "lcv", "phev_d", "Biodiesel"), 
    ("Freight road", "LCVs", "PHEV", "Biogasoline"):              ("freight", "road", "lcv", "phev_g", "Biogasoline"), 

    ("Freight road", "LCVs", "EREV", "Gasoline"):     ("freight", "road", "lcv", "erev_g", "Gasoline"), #proxy?
    ("Freight road", "LCVs", "EREV", "Electricity"):  ("freight", "road", "lcv", "erev_g", "Electricity"), #proxy?
    ("Freight road", "LCVs", "EREV", "Diesel"):       ("freight", "road", "lcv", "erev_d", "Diesel"), #proxy?
    ("Freight road", "LCVs", "EREV", "Efuel"):       ("freight", "road", "lcv", "erev_d", "Efuel"),#proxy?
    ("Freight road", "LCVs", "EREV", "Biodiesel"):       ("freight", "road", "lcv", "erev_d", "Biodiesel"), #proxy?
    ("Freight road", "LCVs", "EREV", "Biogasoline"):      ("freight", "road", "lcv", "erev_g", "Biogasoline"), #proxy?
    # =========================
    # TECHNOLOGY-LEVEL MAPPINGS
    ("Freight road", "LCVs", "BEV"):                       ("freight", "road", "lcv", "bev"),
    ("Freight road", "LCVs", "ICE"):                       ("Combined freight", "road", "lcv", "ice"), #combination
    ("Freight road", "LCVs", "PHEV"):                      ("Combined freight", "road", "lcv", "phev"), #combination
    ("Freight road", "LCVs", "EREV"):                      ("freight", "road", "lcv", "erev_d"), #proxy
    
    ("Freight road", "Trucks", "BEV heavy"):               ("freight", "road", "ht", "bev"),
    ("Freight road", "Trucks", "BEV medium"):              ("freight", "road", "mt", "bev"),
    ("Freight road", "Trucks", "EREV heavy"):              ("freight", "road", "ht", "erev_d"), #proxy
    ("Freight road", "Trucks", "EREV medium"):             ("freight", "road", "mt", "erev_d"), #proxy 
    ("Freight road", "Trucks", "FCEV heavy"):              ("freight", "road", "ht", "fcev"),
    ("Freight road", "Trucks", "FCEV medium"):             ("freight", "road", "mt", "fcev"),
    ("Freight road", "Trucks", "ICE heavy"):               ("Combined freight", "road", "ht", "ice"), #combination
    ("Freight road", "Trucks", "ICE medium"):              ("Combined freight", "road", "mt", "ice"), #combination
    ("Freight road", "Trucks", "PHEV heavy"):              ("Combined freight", "road", "ht", "phev"), #combination
    ("Freight road", "Trucks", "PHEV medium"):             ("Combined freight", "road", "mt", "phev"), #combination
    
    ("Passenger road", "Buses", "BEV"):                    ("passenger", "road", "bus", "bev"),
    ("Passenger road", "Buses", "FCEV"):                   ("passenger", "road", "bus", "fcev"),
    ("Passenger road", "Buses", "ICE"):                    ("Combined passenger", "road", "bus", "ice"), #combination
    ("Passenger road", "Buses", "PHEV"):                    ("Combined passenger", "road", "bus", "phev"), #combination
    ("Passenger road", "Buses", "EREV"):                    ("passenger", "road", "bus", "erev_d"), #proxy    
    
    ("Passenger road", "LPVs", "BEV large"):               ("passenger", "road", "lt", "bev"),
    ("Passenger road", "LPVs", "BEV medium"):              ("passenger", "road", "suv", "bev"),
    ("Passenger road", "LPVs", "BEV small"):               ("passenger", "road", "car", "bev"),
    ("Passenger road", "LPVs", "HEV large"):               ("passenger", "road", "lt", "hev"), #proxy
    ("Passenger road", "LPVs", "HEV medium"):              ("passenger", "road", "suv", "hev"), #proxy
    ("Passenger road", "LPVs", "HEV small"):               ("passenger", "road", "car", "hev"), #proxy
    ("Passenger road", "LPVs", "ICE large"):               ("Combined passenger", "road", "lt", "ice"), #combination
    ("Passenger road", "LPVs", "ICE medium"):              ("Combined passenger", "road", "suv", "ice"), #combination
    ("Passenger road", "LPVs", "ICE small"):               ("Combined passenger", "road", "car", "ice"), #combination
    ("Passenger road", "LPVs", "PHEV large"):              ("Combined passenger", "road", "lt", "phev"), #combination
    ("Passenger road", "LPVs", "PHEV medium"):             ("Combined passenger", "road", "suv", "phev"), #combination
    ("Passenger road", "LPVs", "PHEV small"):              ("Combined passenger", "road", "car", "phev"), #combination
    
    ("Passenger road", "Motorcycles", "BEV"):              ("passenger", "road", "2w", "bev"),
    ("Passenger road", "Motorcycles", "ICE"):              ("Combined passenger", "road", "2w", "ice"), #combination
    
    ("Passenger road", "LPVs", "EREV small"):             ("passenger", "road", "car", "erev_g"), #proxy
    ("Passenger road", "LPVs", "EREV medium"):            ("passenger", "road", "suv", "erev_g"), #proxy
    ("Passenger road", "LPVs", "EREV large"):             ("passenger", "road", "lt", "erev_g"), #proxy  

    # =========================
    # Other AGGREGATE MAPPINGS
    # =========================
    ("Passenger road",):                                  ("passenger", "road"),
    ("Freight road",):                                    ("freight", "road"),
    
    ("Passenger non road",):                              ("Combined passenger", "non road"), #combination
    ("Freight non road",):                                ("Combined freight", "non road"), #combination
    
    ("Passenger road", "LPVs"):                            ("Combined passenger", "road", "lpv"), #combination
    ("Passenger road", "Buses"):                           ("passenger", "road", "bus"),
    ("Passenger road", "Motorcycles"):                     ("passenger", "road", "2w"),
    ("Freight road", "Trucks"):                            ("Combined freight", "road", "truck"), #combination
    ("Freight road", "LCVs"):                              ("freight", "road", "lcv"),
    
    ("Passenger non road", "Air"):                       ("passenger", "air"),
    ("Passenger non road", "Rail"):                      ("passenger", "rail"),
    ("Passenger non road", "Shipping"):                  ("passenger", "ship"),
    ("Freight non road", "Air"):                         ("freight", "air"),
    ("Freight non road", "Rail"):                        ("freight", "rail"),
    ("Freight non road", "Shipping"):                    ("freight", "ship"),
    
    
    # =========================
    # Others Levels 1 and 2 > note that these values within the transport dataset are inserted within the script to make it easy to use within the system, but in fact they come from the esto dataset. 
    # =========================
    ("Nonspecified transport", "Kerosene"): ("Nonspecified transport", "Kerosene"),
    ("Nonspecified transport", "Fuel oil"): ("Nonspecified transport", "Fuel oil"),
    ("Nonspecified transport", "Diesel"): ("Nonspecified transport", "Diesel"),
    ("Nonspecified transport", "LPG"): ("Nonspecified transport", "LPG"),
    ("Nonspecified transport", "Gasoline"): ("Nonspecified transport", "Gasoline"),
    ("Nonspecified transport", "Coal products"): ("Nonspecified transport", "Coal products"),
    ("Nonspecified transport", "Other petroleum products"): ("Nonspecified transport", "Other petroleum products"),
    ("Nonspecified transport", "Natural gas"): ("Nonspecified transport", "Natural gas"),
    ("Nonspecified transport", "Electricity"): ("Nonspecified transport", "Electricity"),

    ("Pipeline transport", "Fuel oil"): ("Pipeline transport", "Fuel oil"),
    ("Pipeline transport", "Diesel"): ("Pipeline transport", "Diesel"),
    ("Pipeline transport", "Natural gas"): ("Pipeline transport", "Natural gas"),
    ("Pipeline transport", "Electricity"): ("Pipeline transport", "Electricity"),
    ("Nonspecified transport",): ("Nonspecified transport",),
    ("Pipeline transport",): ("Pipeline transport",)
}
#todo need to double check this and the ocbination set.
PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY = {#THESE SHOULD BE ALL PROXIES WITH #PROXY IN THE COMMENTS ABOVE

    # air
    # ("passenger", "air", "all", "air_biojet", "Biojet"): ("passenger", "air", "all", "air_jet_fuel", "Jet fuel"),
    # ("freight", "air", "all", "air_biojet", "Biojet"): ("freight", "air", "all", "air_jet_fuel", "Jet fuel"),

    # Freight rail
    # ("freight", "rail", "all", "rail_biodiesel", "Biodiesel"): ("freight", "rail", "all", "rail_diesel", "Diesel"),
    ("freight", "rail", "all", "rail_hydrogen", "Hydrogen"): ("freight", "rail", "all", "rail_electricity", "Electricity"),
    
    #LCVs
    ("freight", "road", "lcv", "cng", "Biogas"): ("freight", "road", "lcv", "cng", "CNG"),
    
    ("freight", "road", "lcv", "erev_g", "Gasoline"): ("freight", "road", "lcv", "phev_g", "Gasoline"),
    ("freight", "road", "lcv", "erev_g", "Electricity"):  ("freight", "road", "lcv", "phev_g", "Electricity"),
    ("freight", "road", "lcv", "erev_d", "Diesel"):       ("freight", "road", "lcv", "phev_d", "Diesel"),
    ("freight", "road", "lcv", "erev_d", "Efuel"):       ("freight", "road", "lcv", "phev_d", "Efuel"),
    ("freight", "road", "lcv", "erev_d", "Biodiesel"): ("freight", "road", "lcv", "phev_d", "Diesel"),
    ("freight", "road", "lcv", "erev_g", "Biogasoline"):   ("freight", "road", "lcv", "phev_g", "Gasoline"),
    
    ("freight", "road", "lcv", "erev_d"): ("Combined freight", "road", "lcv", "phev"),
    
    # Freight road heavy trucks
    ("freight", "road", "ht", "cng", "Biogas"): ("freight", "road", "ht", "cng", "CNG"),
    
    ("freight", "road", "ht", "erev_d", "Biodiesel"): ("freight", "road", "ht", "phev_d", "Diesel"),
    ("freight", "road", "ht", "erev_d", "Diesel"): ("freight", "road", "ht", "phev_d", "Diesel"),
    ("freight", "road", "ht", "erev_g", "Biogasoline"): ("freight", "road", "ht", "phev_g", "Gasoline"),
    ("freight", "road", "ht", "erev_g", "Electricity"): ("freight", "road", "ht", "phev_g", "Electricity"),
    ("freight", "road", "ht", "erev_g", "Gasoline"): ("freight", "road", "ht", "phev_g", "Gasoline"),
    ("freight", "road", "ht", "erev_d", "Efuel"): ("freight", "road", "ht", "phev_d", "Efuel"),
    
    ("freight", "road", "ht", "erev_d"): ("Combined freight", "road", "ht", "phev"),
    # Freight road medium trucks
    ("freight", "road", "mt", "cng", "Biogas"): ("freight", "road", "mt", "cng", "CNG"),
    
    ("freight", "road", "mt", "erev_d", "Biodiesel"): ("freight", "road", "mt", "phev_d", "Diesel"),
    ("freight", "road", "mt", "erev_d", "Diesel"): ("freight", "road", "mt", "phev_d", "Diesel"),
    ("freight", "road", "mt", "erev_g", "Biogasoline"): ("freight", "road", "mt", "phev_g", "Gasoline"),
    ("freight", "road", "mt", "erev_g", "Electricity"): ("freight", "road", "mt", "phev_g", "Electricity"),
    ("freight", "road", "mt", "erev_g", "Gasoline"): ("freight", "road", "mt", "phev_g", "Gasoline"),
    ("freight", "road", "mt", "erev_d", "Efuel"): ("freight", "road", "mt", "phev_d", "Efuel"),
    
    ("freight", "road", "mt", "erev_d"): ("Combined freight", "road", "mt", "phev"),
    
    # Passenger rail
    ("passenger", "rail", "all", "rail_hydrogen", "Hydrogen"): ("passenger", "rail", "all", "rail_electricity", "Electricity"),
    
    # Passenger road buses
    ("passenger", "road", "bus", "cng", "Biogas"): ("passenger", "road", "bus", "cng", "CNG"),
    
    ("passenger", "road", "bus", "erev_g", "Electricity"): ("passenger", "road", "bus", "phev_g", "Electricity"),
    ("passenger", "road", "bus", "erev_g", "Gasoline"): ("passenger", "road", "bus", "phev_g", "Gasoline"),
    ("passenger", "road", "bus", "erev_d", "Diesel"): ("passenger", "road", "bus", "phev_d", "Diesel"),
    ("passenger", "road", "bus", "erev_g", "Biogasoline"): ("passenger", "road", "bus", "phev_g", "Gasoline"),
    ("passenger", "road", "bus", "erev_d", "Biodiesel"): ("passenger", "road", "bus", "phev_d", "Diesel"),
    ("passenger", "road", "bus", "erev_d", "Efuel"): ("passenger", "road", "bus", "phev_d", "Efuel"),

    ("passenger", "road", "bus", "erev_d"): ("Combined passenger", "road", "bus", "phev"),
    
    # Passenger road cars
    ("passenger", "road", "car", "hev_d", "Biodiesel"): ("passenger", "road", "car", "ice_d", "Diesel"),
    ("passenger", "road", "car", "hev_d", "Diesel"): ("passenger", "road", "car", "ice_d", "Diesel"),
    ("passenger", "road", "car", "hev_g", "Biogasoline"): ("passenger", "road", "car", "ice_g", "Gasoline"),
    ("passenger", "road", "car", "hev_g", "Gasoline"): ("passenger", "road", "car", "ice_g", "Gasoline"),
    ("passenger", "road", "car", "hev_g", "Efuel"): ("passenger", "road", "car", "ice_g", "Efuel"),
    
    ("passenger", "road", "car", "erev_g", "Gasoline"): ("passenger", "road", "car", "phev_g", "Gasoline"),
    ("passenger", "road", "car", "erev_g", "Electricity"): ("passenger", "road", "car", "phev_g", "Electricity"),
    ("passenger", "road", "car", "erev_d", "Diesel"): ("passenger", "road", "car", "phev_d", "Diesel"),
    ("passenger", "road", "car", "erev_g", "Efuel"): ("passenger", "road", "car", "phev_d", "Efuel"),
    ("passenger", "road", "car", "erev_d", "Biodiesel"): ("passenger", "road", "car", "phev_d", "Diesel"),
    ("passenger", "road", "car", "erev_g", "Biogasoline"): ("passenger", "road", "car", "phev_g", "Gasoline"),
    
    ("passenger", "road", "car", "hev"): ("Combined passenger", "road", "car", "ice"),
    ("passenger", "road", "car", "erev_g"): ("Combined passenger", "road", "car", "phev"),
    
    # Passenger road light trucks
    ("passenger", "road", "lt", "cng", "Biogas"): ("passenger", "road", "lt", "cng", "CNG"),
    ("passenger", "road", "lt", "hev_d", "Biodiesel"): ("passenger", "road", "lt", "ice_d", "Diesel"),
    ("passenger", "road", "lt", "hev_d", "Diesel"): ("passenger", "road", "lt", "ice_d", "Diesel"),
    ("passenger", "road", "lt", "hev_g", "Biogasoline"): ("passenger", "road", "lt", "ice_g", "Gasoline"),
    ("passenger", "road", "lt", "hev_g", "Gasoline"): ("passenger", "road", "lt", "ice_g", "Gasoline"),
    ("passenger", "road", "lt", "hev_g", "Efuel"): ("passenger", "road", "lt", "ice_g", "Efuel"),
    
    ("passenger", "road", "lt", "erev_g", "Gasoline"): ("passenger", "road", "lt", "phev_g", "Gasoline"),
    ("passenger", "road", "lt", "erev_g", "Electricity"): ("passenger", "road", "lt", "phev_g", "Electricity"),
    ("passenger", "road", "lt", "erev_d", "Diesel"): ("passenger", "road", "lt", "phev_d", "Diesel"),
    ("passenger", "road", "lt", "erev_g", "Efuel"): ("passenger", "road", "lt", "phev_d", "Efuel"),
    ("passenger", "road", "lt", "erev_d", "Biodiesel"): ("passenger", "road", "lt", "phev_d", "Diesel"),
    ("passenger", "road", "lt", "erev_g", "Biogasoline"): ("passenger", "road", "lt", "phev_g", "Gasoline"),
    
    ("passenger", "road", "lt", "hev"): ("Combined passenger", "road", "lt", "ice"),
    ("passenger", "road", "lt", "erev_g"): ("Combined passenger", "road", "lt", "phev"),
    
    # Passenger road SUVs
    ("passenger", "road", "suv", "cng", "Biogas"): ("passenger", "road", "suv", "cng", "CNG"),
    ("passenger", "road", "suv", "hev_d", "Biodiesel"): ("passenger", "road", "suv", "ice_d", "Diesel"),
    ("passenger", "road", "suv", "hev_d", "Diesel"): ("passenger", "road", "suv", "ice_d", "Diesel"),
    ("passenger", "road", "suv", "hev_g", "Biogasoline"): ("passenger", "road", "suv", "ice_g", "Gasoline"),
    ("passenger", "road", "suv", "hev_g", "Gasoline"): ("passenger", "road", "suv", "ice_g", "Gasoline"),
    ("passenger", "road", "suv", "hev_g", "Efuel"): ("passenger", "road", "suv", "ice_g", "Efuel"),
    
    ("passenger", "road", "suv", "erev_g", "Gasoline"): ("passenger", "road", "suv", "phev_g", "Gasoline"),
    ("passenger", "road", "suv", "erev_g", "Electricity"): ("passenger", "road", "suv", "phev_g", "Electricity"),
    ("passenger", "road", "suv", "erev_d", "Diesel"): ("passenger", "road", "suv", "phev_d", "Diesel"),
    ("passenger", "road", "suv", "erev_g", "Efuel"): ("passenger", "road", "suv", "phev_d", "Efuel"),
    ("passenger", "road", "suv", "erev_d", "Biodiesel"): ("passenger", "road", "suv", "phev_d", "Diesel"),
    ("passenger", "road", "suv", "erev_g", "Biogasoline"): ("passenger", "road", "suv", "phev_g", "Gasoline"),
    
    ("passenger", "road", "suv", "hev"): ("Combined passenger", "road", "suv", "ice"),
    ("passenger", "road", "suv", "erev_g"): ("Combined passenger", "road", "suv", "phev"),
    
    # # Passenger shipping
    # ("passenger", "ship", "all", "ship_biodiesel", "Biodiesel"): ("passenger", "ship", "all", "ship_diesel", "Diesel"),
    # ("passenger", "ship", "all", "ship_biogasoline", "Biogasoline"): ("passenger", "ship", "all", "ship_gasoline", "Gasoline"),
}
COMBINATION_SOURCE_ROWS = {#THESE SHOULD BE ALL COMBINATIONS WITH #combination IN THE COMMENTS ABOVE
    ("Combined freight", "road", "truck"): [
        ("freight", "road", "ht", "ice_g", "Gasoline"),
        ("freight", "road", "ht", "ice_g", "Biogasoline"),
        ("freight", "road", "ht", "ice_g", "Efuel"),
        ("freight", "road", "ht", "ice_d", "Diesel"),
        ("freight", "road", "ht", "ice_d", "Biodiesel"),
        ("freight", "road", "ht", "bev", "Electricity"),
        ("freight", "road", "ht", "phev_g", "Gasoline"),
        ("freight", "road", "ht", "ice_g", "Biogasoline"),
        ("freight", "road", "ht", "ice_g", "Efuel"),
        ("freight", "road", "ht", "phev_d", "Diesel"),
        ("freight", "road", "ht", "phev_d", "Biodiesel"),
        ("freight", "road", "ht", "phev_d", "Efuel"),
        ("freight", "road", "ht", "fcev", "Hydrogen"),
        
        ("freight", "road", "mt", "ice_g", "Gasoline"),
        ("freight", "road", "mt", "ice_g", "Biogasoline"),
        ("freight", "road", "mt", "ice_g", "Efuel"),
        ("freight", "road", "mt", "ice_d", "Diesel"),
        ("freight", "road", "mt", "ice_d", "Biodiesel"),
        ("freight", "road", "mt", "ice_d", "Efuel"),
        ("freight", "road", "mt", "bev", "Electricity"),
        ("freight", "road", "mt", "phev_g", "Gasoline"),
        ("freight", "road", "mt", "ice_g", "Biogasoline"),
        ("freight", "road", "mt", "ice_g", "Efuel"),
        ("freight", "road", "mt", "phev_d", "Diesel"),
        ("freight", "road", "mt", "phev_d", "Biodiesel"),
        ("freight", "road", "mt", "phev_d", "Efuel"),
        ("freight", "road", "mt", "fcev", "Hydrogen"),
        
    ],
    ("Combined freight", "road", "ht", "ice"): [
        ("freight", "road", "ht", "ice_g", "Gasoline"),
        ("freight", "road", "ht", "ice_g", "Biogasoline"),
        ("freight", "road", "ht", "ice_g", "Efuel"),
        ("freight", "road", "ht", "ice_d", "Diesel"),
        ("freight", "road", "ht", "ice_d", "Biodiesel"),
        ("freight", "road", "ht", "ice_d", "Efuel"),
    ],
    ("Combined freight", "road", "ht", "phev"): [
        ("freight", "road", "ht", "phev_g", "Gasoline"),
        ("freight", "road", "ht", "phev_g", "Biogasoline"),
        ("freight", "road", "ht", "phev_g", "Efuel"),
        ("freight", "road", "ht", "phev_d", "Diesel"),
        ("freight", "road", "ht", "phev_d", "Biodiesel"),
        ("freight", "road", "ht", "phev_d", "Efuel"),
    ],
    ("Combined freight", "road", "lcv", "ice"): [
        ("freight", "road", "lcv", "ice_g", "Gasoline"),
        ("freight", "road", "lcv", "ice_g", "Biogasoline"),
        ("freight", "road", "lcv", "ice_g", "Efuel"),
        ("freight", "road", "lcv", "ice_d", "Diesel"),
        ("freight", "road", "lcv", "ice_d", "Biodiesel"),
        ("freight", "road", "lcv", "ice_d", "Efuel"),
    ],
    ("Combined freight", "road", "lcv", "phev"): [
        ("freight", "road", "lcv", "phev_g", "Gasoline"),
        ("freight", "road", "lcv", "phev_g", "Biogasoline"),
        ("freight", "road", "lcv", "phev_g", "Efuel"),
        ("freight", "road", "lcv", "phev_d", "Diesel"),
        ("freight", "road", "lcv", "phev_d", "Biodiesel"),
        ("freight", "road", "lcv", "phev_d", "Efuel"),
    ],
    ("Combined freight", "road", "mt", "ice"): [
        ("freight", "road", "mt", "ice_g", "Gasoline"),
        ("freight", "road", "mt", "ice_g", "Biogasoline"),
        ("freight", "road", "mt", "ice_g", "Efuel"),
        ("freight", "road", "mt", "ice_d", "Diesel"),
        ("freight", "road", "mt", "ice_d", "Biodiesel"),
        ("freight", "road", "mt", "ice_d", "Efuel"),
    ],
    ("Combined freight", "road", "mt", "phev"): [
        ("freight", "road", "mt", "phev_g", "Gasoline"),
        ("freight", "road", "mt", "phev_g", "Biogasoline"),
        ("freight", "road", "mt", "phev_g", "Efuel"),
        ("freight", "road", "mt", "phev_d", "Diesel"),
        ("freight", "road", "mt", "phev_d", "Biodiesel"),
        ("freight", "road", "mt", "phev_d", "Efuel"),
    ],
    ("Combined passenger", "road", "lpv"): [
        ("passenger", "road", "car", "ice_g", "Gasoline"),
        ("passenger", "road", "car", "ice_g", "Biogasoline"),
        ("passenger", "road", "car", "ice_g", "Efuel"),
        ("passenger", "road", "car", "ice_d", "Diesel"),
        ("passenger", "road", "car", "ice_d", "Biodiesel"),
        ("passenger", "road", "car", "ice_d", "Efuel"),
        ("passenger", "road", "car", "bev", "Electricity"),
        ("passenger", "road", "car", "phev_g", "Gasoline"),
        ("passenger", "road", "car", "phev_g", "Biogasoline"),
        ("passenger", "road", "car", "phev_g", "Efuel"),
        ("passenger", "road", "car", "phev_d", "Diesel"),
        ("passenger", "road", "car", "phev_d", "Biodiesel"),
        ("passenger", "road", "car", "phev_d", "Efuel"),
        ("passenger", "road", "car", "cng", "CNG"),
        ("passenger", "road", "car", "lpg", "LPG"),
        ("passenger", "road", "car", "fcev", "Hydrogen"),
        
        ("passenger", "road", "suv", "ice_g", "Gasoline"),
        ("passenger", "road", "suv", "ice_g", "Biogasoline"),
        ("passenger", "road", "suv", "ice_g", "Efuel"),
        ("passenger", "road", "suv", "ice_d", "Diesel"),
        ("passenger", "road", "suv", "ice_d", "Biodiesel"),
        ("passenger", "road", "suv", "ice_d", "Efuel"),
        ("passenger", "road", "suv", "bev", "Electricity"),
        ("passenger", "road", "suv", "phev_g", "Gasoline"),
        ("passenger", "road", "suv", "phev_g", "Biogasoline"),
        ("passenger", "road", "suv", "phev_g", "Efuel"),
        ("passenger", "road", "suv", "phev_d", "Diesel"),
        ("passenger", "road", "suv", "phev_d", "Biodiesel"),
        ("passenger", "road", "suv", "phev_d", "Efuel"),
        ("passenger", "road", "suv", "cng", "CNG"),
        ("passenger", "road", "suv", "lpg", "LPG"),
        ("passenger", "road", "suv", "fcev", "Hydrogen"),
        
        ("passenger", "road", "lt", "ice_g", "Gasoline"),
        ("passenger", "road", "lt", "ice_g", "Biogasoline"),
        ("passenger", "road", "lt", "ice_g", "Efuel"),
        ("passenger", "road", "lt", "ice_d", "Diesel"),
        ("passenger", "road", "lt", "ice_d", "Biodiesel"),
        ("passenger", "road", "lt", "ice_d", "Efuel"),
        ("passenger", "road", "lt", "bev", "Electricity"),
        ("passenger", "road", "lt", "phev_g", "Gasoline"),
        ("passenger", "road", "lt", "phev_g", "Biogasoline"),
        ("passenger", "road", "lt", "phev_g", "Efuel"),
        ("passenger", "road", "lt", "phev_d", "Diesel"),
        ("passenger", "road", "lt", "phev_d", "Biodiesel"),
        ("passenger", "road", "lt", "phev_d", "Efuel"),
        ("passenger", "road", "lt", "cng", "CNG"),
        ("passenger", "road", "lt", "lpg", "LPG"),
        ("passenger", "road", "lt", "fcev", "Hydrogen"),
    ],
    ("Combined passenger", "road", "car", "ice"): [
        ("passenger", "road", "car", "ice_g", "Gasoline"),
        ("passenger", "road", "car", "ice_g", "Biogasoline"),
        ("passenger", "road", "car", "ice_g", "Efuel"),
        ("passenger", "road", "car", "ice_d", "Diesel"),
        ("passenger", "road", "car", "ice_d", "Biodiesel"),
        ("passenger", "road", "car", "ice_d", "Efuel"),
    ],
    ("Combined passenger", "road", "car", "phev"): [
        ("passenger", "road", "car", "phev_g", "Gasoline"),
        ("passenger", "road", "car", "phev_g", "Biogasoline"),
        ("passenger", "road", "car", "phev_g", "Efuel"),
        ("passenger", "road", "car", "phev_d", "Diesel"),
        ("passenger", "road", "car", "phev_d", "Biodiesel"),
        ("passenger", "road", "car", "phev_d", "Efuel"),
    ],
    ("Combined passenger", "road", "lt", "ice"): [
        ("passenger", "road", "lt", "ice_g", "Gasoline"),
        ("passenger", "road", "lt", "ice_g", "Biogasoline"),
        ("passenger", "road", "lt", "ice_g", "Efuel"),
        ("passenger", "road", "lt", "ice_d", "Diesel"),
        ("passenger", "road", "lt", "ice_d", "Biodiesel"),
        ("passenger", "road", "lt", "ice_d", "Efuel"),
    ],
    ("Combined passenger", "road", "lt", "phev"): [
        ("passenger", "road", "lt", "phev_g", "Gasoline"),
        ("passenger", "road", "lt", "phev_g", "Biogasoline"),
        ("passenger", "road", "lt", "phev_g", "Efuel"),
        ("passenger", "road", "lt", "phev_d", "Diesel"),
        ("passenger", "road", "lt", "phev_d", "Biodiesel"),
        ("passenger", "road", "lt", "phev_d", "Efuel"),
    ],
    ("Combined passenger", "road", "suv", "phev"): [
        ("passenger", "road", "suv", "phev_g", "Gasoline"),
        ("passenger", "road", "suv", "phev_g", "Biogasoline"),
        ("passenger", "road", "suv", "phev_g", "Efuel"),
        ("passenger", "road", "suv", "phev_d", "Diesel"),
        ("passenger", "road", "suv", "phev_d", "Biodiesel"),
        ("passenger", "road", "suv", "phev_d", "Efuel"),
    ],
    ("Combined passenger", "road", "suv", "ice"): [
        ("passenger", "road", "suv", "ice_g", "Gasoline"),
        ("passenger", "road", "suv", "ice_g", "Biogasoline"),
        ("passenger", "road", "suv", "ice_g", "Efuel"),
        ("passenger", "road", "suv", "ice_d", "Diesel"),
        ("passenger", "road", "suv", "ice_d", "Biodiesel"),
        ("passenger", "road", "suv", "ice_d", "Efuel"),
        ("freight", "road", "ht", "fcev", "Hydrogen"),
    ],
    ("Combined passenger", "road", "2w", "ice"): [
        ("passenger", "road", "2w", "ice_g", "Gasoline"),
        ("passenger", "road", "2w", "ice_g", "Biogasoline"),
        ("passenger", "road", "2w", "ice_g", "Efuel"),
        ("passenger", "road", "2w", "ice_d", "Diesel"),
        ("passenger", "road", "2w", "ice_d", "Biodiesel"),
        ("passenger", "road", "2w", "ice_d", "Efuel"),
    ],
    ("Combined passenger", "road", "bus", "ice"): [
        ("passenger", "road", "bus", "ice_g", "Gasoline"),
        ("passenger", "road", "bus", "ice_g", "Biogasoline"),
        ("passenger", "road", "bus", "ice_g", "Efuel"),
        ("passenger", "road", "bus", "ice_d", "Diesel"),
        ("passenger", "road", "bus", "ice_d", "Biodiesel"),
        ("passenger", "road", "bus", "ice_d", "Efuel"),
    ],
    ("Combined passenger", "road", "bus", "phev"): [
        ("passenger", "road", "bus", "phev_g", "Gasoline"),
        ("passenger", "road", "bus", "phev_g", "Biogasoline"),
        ("passenger", "road", "bus", "phev_g", "Efuel"),
        ("passenger", "road", "bus", "phev_d", "Diesel"),
        ("passenger", "road", "bus", "phev_d", "Biodiesel"),
        ("passenger", "road", "bus", "phev_d", "Efuel"),
    ],
    #non road:
    ("Combined passenger", "non road"): 
        [tup for tup in ALL_PATHS_SOURCE if len(tup) == 5 and tup[1] != "road" and tup[0] == "passenger"],
    ("Combined freight", "non road"):
        [tup for tup in ALL_PATHS_SOURCE if len(tup) == 5 and tup[1] != "road" and tup[0] == "freight"],
}

#%%
def check_LEAP_BRANCH_TO_SOURCE_MAP_for_missing_proxies_and_combinations(LEAP_BRANCH_TO_SOURCE_MAP, PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY, COMBINATION_SOURCE_ROWS):
    
    # Get all source values from LEAP_BRANCH_TO_SOURCE_MAP
    all_source_values = set(LEAP_BRANCH_TO_SOURCE_MAP.values())
    
    # Convert ALL_PATHS_SOURCE to DataFrame with 5 columns. where a tuple has less than 5 elements, fill with None
    all_paths_df = pd.DataFrame(columns=["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"])
    for src in ALL_PATHS_SOURCE:
        row = list(src) + [None] * (5 - len(src))
        all_paths_df = pd.concat([all_paths_df, pd.DataFrame([row], columns=all_paths_df.columns)], ignore_index=True)
    
    # Check if all source values exist in ALL_PATHS_SOURCE, where they dont, check if they are in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY, if they dont, raise error
    for source_value in all_source_values:
        #ignore any with Nonspecified transport or Pipeline transport since those come from the esto dataset and are not mapped here
        if source_value[0] in ["Nonspecified transport", "Pipeline transport"]:
            continue
        if source_value not in ALL_PATHS_SOURCE:
            #extract all the source tuples from the PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY and COMBINATION_SOURCE_ROWS
            extracted_sources = [
                src for src in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY.keys()
            ] + [src for src in COMBINATION_SOURCE_ROWS.keys()
            ]
            matches = [src for src in extracted_sources if src == source_value]
            #if fuel is one of the fuel mixing ones that we set in allocate_fuel_alternatives_energy_and_activity (e.g. Efuel, Biojet, Biogasoline, Biodiesel) then we can ignore it here since those are handled separately
            if source_value[-1] in ["Efuel", "Biojet", "Biogasoline", "Biodiesel", "Biogas"]:
                continue
            if len(matches) == 0:
                breakpoint()
                print('WARNING: Source value from LEAP_BRANCH_TO_SOURCE_MAP is missing in ALL_PATHS_SOURCE and not accounted for in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY or COMBINATION_SOURCE_ROWS:\n', source_value)
                # raise ValueError(f"Source value {source_value} from LEAP_BRANCH_TO_SOURCE_MAP is missing in ALL_PATHS_SOURCE and not accounted for in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY or COMBINATION_SOURCE_ROWS.")

            elif len(matches) > 1:
                breakpoint()
                raise ValueError(f"Source value {source_value} from LEAP_BRANCH_TO_SOURCE_MAP has multiple matches in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY or COMBINATION_SOURCE_ROWS, please check for duplicates:\n Matches: {matches}")
            else:
                pass  # It's accounted for in the proxies or combinations
    #and also check for none in the other direction
    # Check that all keys in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY and COMBINATION_SOURCE_ROWS exist in LEAP_BRANCH_TO_SOURCE_MAP values
    for proxy_dict, dict_name in [(PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY, "PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY"), (COMBINATION_SOURCE_ROWS, "COMBINATION_SOURCE_ROWS")]:
        for key in proxy_dict.keys():
            if key not in all_source_values:
                raise ValueError(f"Key {key} from {dict_name} is not present in LEAP_BRANCH_TO_SOURCE_MAP values.")
           
def create_new_source_rows_based_on_proxies_with_no_activity(source_df):
    #todo since we dont handle biofuels and other mixed fuels in the soruce df we should handle them differently too.. pehraps by inserting the energy use from the esto dataset?
    #we need to occasionally create new sets of source rows based on proxy mappings, e.g. rail hydrogen maps to rail electricity in LEAP. we want to keep the same effiicnecy but not the same activity/stocks and so on. So we will create these new rows using the existing rows as a base and just changing the fuel/drive type as needed.
    #this will be used within prepare_input_data()
    new_df_rows = pd.DataFrame()  # Initialize an empty DataFrame to hold new 
    for new_src_tuple, src_tuple in PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY.items():
        source_cols = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"][:len(src_tuple)]
        # Build a boolean mask across all source columns, then take a copy to avoid SettingWithCopyWarning
        mask = pd.Series(True, index=source_df.index)
        for i, col in enumerate(source_cols):
            mask &= source_df[col] == src_tuple[i]
        matching_rows = source_df.loc[mask].copy()
        if len(matching_rows) == 0:
            breakpoint()
            raise ValueError(f"No matching rows found in source_df for source tuple: {src_tuple}")
        # set up a copy to modify. we will set its cols to the new source tuple values and copy over the value cols where needed, otherwise set to 0
        for i, col in enumerate(source_cols):
            matching_rows.loc[:, col] = new_src_tuple[i]
                
        # Parameters/factors that should be kept (not activity-dependent)
        measure_cols_to_keep = [
            "Efficiency", # Vehicle efficiency is a technical parameter
            "Occupancy_or_load", # Load factor is a parameter
            "Activity_efficiency_improvement", # Efficiency improvement rate is a parameter
            "New_vehicle_efficiency", # New vehicle efficiency is a parameter
            "Turnover_rate", # Fleet turnover rate is a parameter
            "Age_distribution", # Age distribution pattern can be kept
            "Non_road_intensity_improvement", # Efficiency improvement parameter
            "Activity_growth", # Growth rate parameter
            "Intensity", # Energy intensity (activity-dependent)
            "Gdp", # GDP levels
            "Gdp_per_capita", # GDP per capita levels
            "Activity_per_Stock", # Activity per vehicle
            "Mileage", # Annual mileage per vehicle
        ]
        
        # Activity-dependent variables that should be set to 0
        measure_cols_to_set_to_0 = [
            "Average_age", # Fleet average age needs to be 0 since we have no activity therefore no stocks
            "Energy", # Direct energy consumption
            "Stocks_old", # Historical stock levels
            "Activity", # Transport activity levels
            "Travel_km", # Total travel kilometers
            "Stocks", # Current stock levels
            "Surplus_stocks", # Excess stock levels
            "Stocks_per_thousand_capita", # Stock density (activity-related)
            "Vehicle_sales_share", # Sales shares (activity-dependent)
            "Stock_turnover", # Actual turnover (vs rate)
            "New_stocks_needed"  # New stock requirements
        ]
        #now set the cols to 0 where needed
        for col in measure_cols_to_set_to_0:
            if col in matching_rows.columns:
                matching_rows.loc[:, col] = 0
        new_df_rows = pd.concat([new_df_rows, matching_rows], ignore_index=True)
    return new_df_rows

def create_new_source_rows_based_on_combinations(source_df):
    #this one will instead set up new source rows based on combinations of existing source rows, e.g. ICE medium in passenger road is a combination of gasoline and diesel ICE medium vehicles. We will create new rows for these combinations by aggregating the relevant rows. This will require additon of most variables except some which will need to have weigthed averages applied. Where the sum of weigths is 0 we will set the weighted average to jsut be the simple average to avoid NaNs.
    WEIGHTED_AVERAGE_COLS_WITH_WEIGHTS = {
        "Efficiency": "Activity",
        "Occupancy_or_load": "Activity",
        "New_vehicle_efficiency": "New_stocks_needed",
        "Turnover_rate": "Stocks",
        "Activity_per_Stock": "Stocks",
        "Mileage": "Stocks",
        "Average_age": "Stocks",
        "Intensity": "Activity",
        "Activity_efficiency_improvement": "Activity", # Efficiency improvement rate is a parameter,
        "Non_road_intensity_improvement": "Activity", # Efficiency improvement parameter
        "Activity_growth": "Activity", # Growth rate parameter
    }
    COLS_TO_SUM = [
        "Travel_km", # Total travel kilometers
        "Energy", # Direct energy consumption
        "Stocks_old", # Historical stock levels
        "Activity", # Transport activity levels
        "Stocks", # Current stock levels
        "Surplus_stocks", # Excess stock levels
        "Stocks_per_thousand_capita", # Vehicle owenrship
        "Vehicle_sales_share", # Sales shares 
        "Stock_turnover", # Actual turnover (vs rate)
        "New_stocks_needed"  # New stock requirements
    ]
    COLS_TO_LEAVE_AS_IS = [#just take the first value since they are the same across combinations
        "Gdp_per_capita", # GDP per capita levels are same across combinations
        "GDP",
        "Population", # Population levels
    ]
    
    COLS_TO_SET_TO_NA = [#these are jsut too difificult to aggregate meaningfully for nwo
        "Age_distribution", # Age distribution pattern is complex to aggregate
    ]
    
    new_df_rows = pd.DataFrame()  # Initialize an empty DataFrame to hold new rows
    rows_to_remove = pd.DataFrame()  # Keep track of rows that will be removed after combination
    for new_src_tuple, source_tuples_list in COMBINATION_SOURCE_ROWS.items():
        # Find all rows that match any of the source tuples in the combination
        matching_rows_list = []
        
        for src_tuple in source_tuples_list:
            source_cols = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"][:len(src_tuple)]
            # Build a boolean mask across all source columns, then take a copy to avoid SettingWithCopyWarning
            mask = pd.Series(True, index=source_df.index)
            for i, col in enumerate(source_cols):
                mask &= source_df[col] == src_tuple[i]
            matching_rows = source_df.loc[mask].copy()
            if not matching_rows.empty:
                matching_rows_list.append(matching_rows)
        
        if not matching_rows_list:
            continue  # Skip if no matching rows found
            
        # Concatenate all matching rows for this combination
        all_matching_rows = pd.concat(matching_rows_list, ignore_index=True)
        
        # Create a new combined row
        new_rows = all_matching_rows.copy()  # Start with first row as template
        # breakpoint()#Should we create a new version of all rows since we want to remove the old ones? yes i thnk so since it means we then get a set of all of them when pointing to their children later on.
        
        # Set the new tuple values
        source_cols = ["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"]#[:len(new_src_tuple)]
        for i, col in enumerate(source_cols):
            if i<len(new_src_tuple):
                new_rows[col] = new_src_tuple[i]
            else:
                pass
                # new_rows[col] = pd.NA  # leave it as is if not specified in the new source tuple
        
        # Apply aggregation rules
        
        #add date to source_cols
        source_cols = source_cols + ["Date"]
        new_rows_final = new_rows[source_cols].copy()  # Start with source columns only
        #drop duplicates to avoid issues when merging back later
        new_rows_final = new_rows_final.drop_duplicates().reset_index(drop=True)
        
        for col in all_matching_rows.columns:
            #create new df from new rows
            new_rows_copy = new_rows[source_cols + [col]].copy()
            if col in COLS_TO_SUM:
                #group and sum by all source cols to avoid duplications
                new_rows_copy= new_rows_copy.groupby(source_cols).sum().reset_index()
                
            elif col in WEIGHTED_AVERAGE_COLS_WITH_WEIGHTS:
                #create new df from new rows
                weight_col = WEIGHTED_AVERAGE_COLS_WITH_WEIGHTS[col]
                if weight_col in all_matching_rows.columns:
                    new_rows_copy = new_rows[source_cols + [col, weight_col]].copy()
                    weights = new_rows_copy[weight_col].copy()
                    # Calculate weighted average, use simple average if sum of weights is 0
                    if weights.sum() > 0:
                        # Group by source cols and calculate weighted average
                        grouped = new_rows_copy.groupby(source_cols)
                        
                        def weighted_avg(group):
                            weights = group[weight_col]
                            values = group[col]
                            if weights.sum() > 0:
                                return (values * weights).sum() / weights.sum()
                            else:
                                return values.mean()
                        
                        new_rows_copy = grouped.apply(weighted_avg, include_groups=False).reset_index()
                        new_rows_copy.columns = source_cols + [col]
                    else:
                        # Group by source cols and calculate simple average
                        grouped = new_rows_copy.groupby(source_cols)
                        new_rows_copy = grouped[col].mean().reset_index()
                        new_rows_copy.columns = source_cols + [col]
                else:
                    # Group by source cols and calculate simple average
                    grouped = new_rows_copy.groupby(source_cols)
                    new_rows_copy = grouped[col].mean().reset_index()
                    new_rows_copy.columns = source_cols + [col]
            elif col in COLS_TO_LEAVE_AS_IS:
                #drop duplicates to avoid issues when merging back later
                new_rows_copy= new_rows_copy.groupby(source_cols).first().reset_index()
                pass
            elif col in COLS_TO_SET_TO_NA:
                new_rows_copy[col] = pd.NA
                #drop duplicates to avoid issues when merging back later
                new_rows_copy= new_rows_copy.groupby(source_cols).first().reset_index()
            else:
                continue  # Dont need to change other columns
            
            
            #merge back into new_rows_final
            new_rows_final = pd.merge(new_rows_final, new_rows_copy, on=source_cols, how='left')
            #check for duplicates
            if new_rows_final.duplicated(subset=source_cols).any():
                breakpoint()#we have duplicates here which is not expected
                raise ValueError("Duplicates found in new combined rows after merging column: " + col)
        # if new_src_tuple == ("freight", "non road"):
        #     breakpoint()#check the new row here. and rows to rmeove
        #im trying to work out what to do with the rows to remove..the problem is that in some cases we do need to remove them such as when we are setting up new values from combinations with their drive types specified.. this leaves us with double ups when were summing by vehiclle type or even medium later on. but in when we are setting new values for mediums, we dont need to worry since they wont get included in any summing..
        
        
        new_df_rows = pd.concat([new_df_rows, new_rows_final], ignore_index=True)
        #check for duplicates in new_df_rows
        if new_df_rows.duplicated(subset=source_cols).any():
            breakpoint()#we have duplicates here which is not expected
            raise ValueError("Duplicates found in new combined rows after adding to new_df_rows for source tuple: " + str(new_src_tuple))
        #i decided that the rows that we could remove are nto required to be removed since they are required by some mappings and cannot be mistaken wit
        # rows_to_remove = pd.concat([rows_to_remove, all_matching_rows], ignore_index=True)#we have to remove tehse rows snuce they are now combined into one row BUT MASKE SURE OT REMOVE THEM ONLY AFTER THE PROXY ROWS HAVE BEEN ADDED TO AVOID ISSUES WHERE A PROXY ROW THATS BEING RELIED UPON IS ALSO PART OF A COMBINATION AND THEREFORE GETS REMOVED BEFORE IT CAN BE USED TO CREATE THE PROXY ROW
    
    return new_df_rows #, rows_to_remove

#%%
ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP = {
    #Please note that we may be missing some LEAP branches here if there is no corresponding fuel use in the ESTO dataset. They are basically where data will appear in the projections but dont exist in the historical data. Use validate_branch_mapping and identify_missing_esto_mappings to find and  missing branches. An example would be biogasoline in ships. 
    # ------------------------------------------------------------
    # 15_01_domestic_air_transport → Passenger non-road Air
    # ------------------------------------------------------------
    
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Nonspecified transport", "Gasoline")
    ],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_02_aviation_gasoline"): [
        ("Passenger non road", "Air", "Aviation gasoline"),
        ("Freight non road", "Air", "Aviation gasoline")
    ],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_x_jet_fuel"): [
        ("Passenger non road", "Air", "Jet fuel"),
        ("Freight non road", "Air", "Jet fuel"),
    ],

    # ------------------------------------------------------------
    # 15_02_road → Passenger road + Freight road
    # ------------------------------------------------------------
    ("15_02_road", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Passenger road", "LPVs", "ICE small", "Gasoline"),
        ("Passenger road", "LPVs", "ICE medium", "Gasoline"),
        ("Passenger road", "LPVs", "ICE large", "Gasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Gasoline"),
        ("Freight road", "LCVs", "ICE", "Gasoline"),
        ("Passenger road", "LPVs", "HEV small", "Gasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "HEV large", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Gasoline"),
        ("Passenger road", "LPVs", "EREV small", "Gasoline"),
        ("Passenger road", "LPVs", "EREV medium", "Gasoline"),
        ("Passenger road", "LPVs", "EREV large", "Gasoline"),
        ("Passenger road", "Buses", "ICE", "Gasoline"),
        ("Freight road", "Trucks", "ICE medium", "Gasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Gasoline"),
        ("Freight road", "Trucks", "EREV medium", "Gasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Gasoline"),
        ("Freight road", "LCVs", "PHEV", "Gasoline"),
        ("Freight road", "LCVs", "EREV", "Gasoline"),
        ("Passenger road", "Buses", "EREV", "Gasoline"),
        ("Passenger road", "Buses", "PHEV", "Gasoline"),
        ("Freight road", "Trucks", "PHEV heavy", "Gasoline"),
        ("Freight road", "Trucks", "PHEV medium", "Gasoline"),
    ],
    ("15_02_road", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_02_road", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger road", "LPVs", "ICE small", "Diesel"),
        ("Passenger road", "LPVs", "ICE medium", "Diesel"),
        ("Passenger road", "LPVs", "ICE large", "Diesel"),
        ("Passenger road", "Buses", "ICE", "Diesel"),
        ("Freight road", "LCVs", "ICE", "Diesel"),
        ("Freight road", "Trucks", "ICE medium", "Diesel"),
        ("Freight road", "Trucks", "ICE heavy", "Diesel"),
        ("Passenger road", "LPVs", "HEV small", "Diesel"),
        ("Passenger road", "LPVs", "HEV medium", "Diesel"),
        ("Passenger road", "LPVs", "HEV large", "Diesel"),
        ("Passenger road", "LPVs", "PHEV small", "Diesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Diesel"),
        ("Passenger road", "LPVs", "PHEV large", "Diesel"),
        ("Passenger road", "LPVs", "EREV small", "Diesel"),
        ("Passenger road", "LPVs", "EREV medium", "Diesel"),
        ("Passenger road", "LPVs", "EREV large", "Diesel"),
        ("Passenger road", "Motorcycles", "ICE", "Diesel"),
        ("Freight road", "Trucks", "EREV medium", "Diesel"),
        ("Freight road", "Trucks", "EREV heavy", "Diesel"),
        ("Freight road", "LCVs", "PHEV", "Diesel"),
        ("Freight road", "LCVs", "EREV", "Diesel"),
        ("Passenger road", "Buses", "EREV", "Diesel"),
        ("Passenger road", "Buses", "PHEV", "Diesel"),
        ("Freight road", "Trucks", "PHEV heavy", "Diesel"),
        ("Freight road", "Trucks", "PHEV medium", "Diesel"),
    ],
    ("15_02_road", "07_petroleum_products", "07_08_fuel_oil"): [("Nonspecified transport", "Fuel oil")],
    ("15_02_road", "07_petroleum_products", "07_09_lpg"): [
        ("Passenger road", "LPVs", "ICE medium", "LPG"),
        ("Passenger road", "LPVs", "ICE large", "LPG"),
        ("Passenger road", "Buses", "ICE", "LPG"),
        ("Freight road", "LCVs", "ICE", "LPG"),
        ("Freight road", "Trucks", "ICE medium", "LPG"),
        ("Freight road", "Trucks", "ICE heavy", "LPG")
    ],
    ("15_02_road", "08_gas", "08_01_natural_gas"): [
        ("Passenger road", "LPVs", "ICE medium", "CNG"),
        ("Passenger road", "LPVs", "ICE large", "CNG"),
        ("Passenger road", "Buses", "ICE", "CNG"),
        ("Freight road", "LCVs", "ICE", "CNG"),
        ("Freight road", "Trucks", "ICE medium", "CNG"),
        ("Freight road", "Trucks", "ICE heavy", "CNG")
    ],
    ("15_02_road", "16_others", "16_05_biogasoline"): [
        ("Passenger road", "LPVs", "ICE medium", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE large", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE small", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV small", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV large", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Biogasoline"),
        ("Passenger road", "LPVs", "EREV small", "Biogasoline"),
        ("Passenger road", "LPVs", "EREV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "EREV large", "Biogasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Biogasoline"),
        ("Freight road", "LCVs", "ICE", "Biogasoline"),
        ("Freight road", "LCVs", "PHEV", "Biogasoline"),
        ("Freight road", "LCVs", "EREV", "Biogasoline"),
        ("Freight road", "Trucks", "ICE medium", "Biogasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Biogasoline"),
        ("Freight road", "Trucks", "EREV medium", "Biogasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Biogasoline"),
        ("Passenger road", "Buses", "ICE", "Biogasoline"),
        ("Passenger road", "Buses", "EREV", "Biogasoline"),
        ("Passenger road", "Buses", "PHEV", "Biogasoline"),
        ("Freight road", "Trucks", "PHEV heavy", "Biogasoline"),
        ("Freight road", "Trucks", "PHEV medium", "Biogasoline"),
    ],
    ("15_02_road", "16_others", "16_06_biodiesel"): [
        ("Passenger road", "Buses", "ICE", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE small", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE medium", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE large", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV small", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV large", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV small", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV large", "Biodiesel"),
        ("Passenger road", "LPVs", "EREV small", "Biodiesel"),
        ("Passenger road", "LPVs", "EREV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "EREV large", "Biodiesel"),
        ("Passenger road", "Motorcycles", "ICE", "Biodiesel"),
        ("Freight road", "LCVs", "ICE", "Biodiesel"),
        ("Freight road", "LCVs", "PHEV", "Biodiesel"),
        ("Freight road", "LCVs", "EREV", "Biodiesel"),
        ("Freight road", "Trucks", "ICE medium", "Biodiesel"),
        ("Freight road", "Trucks", "EREV medium", "Biodiesel"),
        ("Freight road", "Trucks", "EREV heavy", "Biodiesel"),
        ("Freight road", "Trucks", "ICE heavy", "Biodiesel"),
        ("Passenger road", "Buses", "EREV", "Biodiesel"),
        ("Passenger road", "Buses", "PHEV", "Biodiesel"),
        ("Freight road", "Trucks", "PHEV heavy", "Biodiesel"),
        ("Freight road", "Trucks", "PHEV medium", "Biodiesel"),
    ],
    ("15_02_road", "17_electricity", "x"): [
        ("Passenger road", "LPVs", "BEV small", "Electricity"),
        ("Passenger road", "LPVs", "BEV medium", "Electricity"),
        ("Passenger road", "LPVs", "BEV large", "Electricity"),
        ("Passenger road", "Buses", "BEV", "Electricity"),
        ("Passenger road", "Motorcycles", "BEV", "Electricity"),
        ("Freight road", "LCVs", "BEV", "Electricity"),
        ("Freight road", "Trucks", "BEV heavy", "Electricity"),
        ("Passenger road", "LPVs", "PHEV small", "Electricity"),
        ("Passenger road", "LPVs", "PHEV medium", "Electricity"),
        ("Passenger road", "LPVs", "PHEV large", "Electricity"),
        ("Passenger road", "LPVs", "EREV small", "Electricity"),
        ("Passenger road", "LPVs", "EREV medium", "Electricity"),
        ("Passenger road", "LPVs", "EREV large", "Electricity"),
        ("Freight road", "Trucks", "BEV medium", "Electricity"),
        ("Freight road", "Trucks", "EREV medium", "Electricity"),
        ("Freight road", "Trucks", "EREV heavy", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Electricity"),
        ("Freight road", "LCVs", "EREV", "Electricity"),
        ("Passenger road", "Buses", "EREV", "Electricity"),
        ("Passenger road", "Buses", "PHEV", "Electricity"),
        ("Freight road", "Trucks", "PHEV heavy", "Electricity"),
        ("Freight road", "Trucks", "PHEV medium", "Electricity")
    ],

    # ------------------------------------------------------------
    # 15_03_rail → Passenger/Freight non-road Rail
    # ------------------------------------------------------------
    ("15_03_rail", "01_coal", "01_x_thermal_coal"): [
        ("Passenger non road", "Rail", "Coal"),
        ("Freight non road", "Rail", "Coal")
    ],
    ("15_03_rail", "02_coal_products", "x"): [
        ("Nonspecified transport", "Coal products"),
    ],
    ("15_03_rail", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Nonspecified transport", "Gasoline"),
    ],
    ("15_03_rail", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_03_rail", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger non road", "Rail", "Diesel"),
        ("Freight non road", "Rail", "Diesel"),
    ],
    ("15_03_rail", "07_petroleum_products", "07_08_fuel_oil"): [
        ("Nonspecified transport", "Fuel oil")
    ],
    ("15_03_rail", "07_petroleum_products", "07_09_lpg"): [
        ("Nonspecified transport", "LPG")
    ],
    ("15_03_rail", "16_others", "16_06_biodiesel"): [
        ("Passenger non road", "Rail", "Biodiesel"),
        ("Freight non road", "Rail", "Biodiesel")
    ],
    ("15_03_rail", "17_electricity", "x"): [
        ("Passenger non road", "Rail", "Electricity"),
        ("Freight non road", "Rail", "Electricity")
    ],

    # ------------------------------------------------------------
    # 15_04_domestic_navigation → Passenger/Freight non-road Shipping
    # ------------------------------------------------------------
    ("15_04_domestic_navigation", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Passenger non road", "Shipping", "Gasoline"),
        ("Freight non road", "Shipping", "Gasoline")
    ],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger non road", "Shipping", "Diesel"),
        ("Freight non road", "Shipping", "Diesel"),
    ],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_08_fuel_oil"): [
        ("Passenger non road", "Shipping", "Fuel oil"),
        ("Freight non road", "Shipping", "Fuel oil")
    ],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_09_lpg"): [
        ("Nonspecified transport", "LPG")
    ],
    ("15_04_domestic_navigation", "08_gas", "08_01_natural_gas"): [
        ("Passenger non road", "Shipping", "LNG"),
        ("Freight non road", "Shipping", "LNG")
    ],
    ("15_04_domestic_navigation", "16_others", "16_06_biodiesel"): [
        ("Passenger non road", "Shipping", "Biodiesel"),
        ("Freight non road", "Shipping", "Biodiesel")
    ],
    ("15_04_domestic_navigation", "17_electricity", "x"): [
        ("Passenger non road", "Shipping", "Electricity"),
        ("Freight non road", "Shipping", "Electricity")
    ],

    # ------------------------------------------------------------
    # 15_05_pipeline_transport → lump into Nonspecified
    # ------------------------------------------------------------
    ("15_05_pipeline_transport", "07_petroleum_products", "07_01_motor_gasoline"): [("Nonspecified transport", "Gasoline")],
    ("15_05_pipeline_transport", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Pipeline transport", "Diesel"),
    ],
    ("15_05_pipeline_transport", "07_petroleum_products", "07_08_fuel_oil"): [("Pipeline transport", "Fuel oil")],
    ("15_05_pipeline_transport", "07_petroleum_products", "07_09_lpg"): [("Nonspecified transport", "LPG")],
    ("15_05_pipeline_transport", "08_gas", "08_01_natural_gas"): [("Pipeline transport", "Natural gas")],
    ("15_05_pipeline_transport", "17_electricity", "x"): [("Pipeline transport", "Electricity")],

    # ------------------------------------------------------------
    # 15_06_nonspecified_transport → direct mapping to Nonspecified
    # ------------------------------------------------------------
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_01_motor_gasoline"): [("Nonspecified transport", "Gasoline")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Nonspecified transport", "Diesel"),
    ],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_08_fuel_oil"): [("Nonspecified transport", "Fuel oil")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_09_lpg"): [("Nonspecified transport", "LPG")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_x_jet_fuel"): [("Nonspecified transport", "Kerosene")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_x_other_petroleum_products"): [("Nonspecified transport", "Other petroleum products")],
    ("15_06_nonspecified_transport", "08_gas", "08_01_natural_gas"): [("Nonspecified transport", "Natural gas")],
    ("15_06_nonspecified_transport", "17_electricity", "x"): [("Nonspecified transport", "Electricity")],
}
#%%

# FUELS WITHOUT ESTO MAPPINGS (Need special handling):
# These branch tuples cannot be mapped because ESTO data doesn't include these fuel types:
# Complete this validation code by adding the missing branches to the unmappable set:

# ------------------------------------------------------------
# FUELS WITHOUT ESTO MAPPINGS (Cannot be mapped)
# ------------------------------------------------------------

UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT = {
    # Hydrogen branches (No hydrogen fuel category in ESTO):
    ("Passenger non road", "Air", "Hydrogen"),
    ("Freight non road", "Air", "Hydrogen"), 
    ("Passenger non road", "Rail", "Hydrogen"),
    ("Freight non road", "Rail", "Hydrogen"),
    ("Passenger non road", "Shipping", "Hydrogen"),
    ("Freight non road", "Shipping", "Hydrogen"),
    ("Passenger road", "Buses", "FCEV", "Hydrogen"),
    ("Freight road", "Trucks", "FCEV medium", "Hydrogen"),
    ("Freight road", "Trucks", "FCEV heavy", "Hydrogen"),
    
    # Ammonia branches (No ammonia fuel category in ESTO):
    ("Passenger non road", "Shipping", "Ammonia"),
    ("Freight non road", "Shipping", "Ammonia"),
    
    # Biogasline in ships: #these occur because ships use like no gasoline as it is.
    ("Passenger non road", "Shipping", "Biogasoline"),
    ("Freight non road", "Shipping", "Biogasoline"),
    
    # Electric aircraft (No electric aircraft category in current ESTO):
    ("Passenger non road", "Air", "Electricity"),
    ("Freight non road", "Air", "Electricity"),
    # Biojet branches (No biojet fuel category in ESTO):
    ("Passenger non road", "Air", "Biojet"),
    ("Freight non road", "Air", "Biojet"),
    # LNG trucks (No LNG subcategory in standard ESTO road transport):
    ("Freight road", "Trucks", "ICE medium", "LNG"),
    ("Freight road", "Trucks", "ICE heavy", "LNG"),
    
    # Biogas branches (No separate biogas category - would map to CNG but creates ambiguity):
    ("Passenger road", "LPVs", "ICE medium", "Biogas"),
    ("Passenger road", "LPVs", "ICE large", "Biogas"),
    ("Passenger road", "Buses", "ICE", "Biogas"),
    ("Freight road", "LCVs", "ICE", "Biogas"),
    ("Freight road", "Trucks", "ICE medium", "Biogas"),
    ("Freight road", "Trucks", "ICE heavy", "Biogas"),
    
    #all efuel branches (no efuel category in ESTO)
    # All Efuel branches for passenger road
    ("Passenger road", "LPVs", "ICE small", "Efuel"),
    ("Passenger road", "LPVs", "ICE medium", "Efuel"),
    ("Passenger road", "LPVs", "ICE large", "Efuel"),
    ("Passenger road", "LPVs", "PHEV small", "Efuel"),
    ("Passenger road", "LPVs", "PHEV medium", "Efuel"),
    ("Passenger road", "LPVs", "PHEV large", "Efuel"),
    ("Passenger road", "LPVs", "EREV small", "Efuel"),
    ("Passenger road", "LPVs", "EREV medium", "Efuel"),
    ("Passenger road", "LPVs", "EREV large", "Efuel"),
    ("Passenger road", "LPVs", "HEV small", "Efuel"),
    ("Passenger road", "LPVs", "HEV medium", "Efuel"),
    ("Passenger road", "LPVs", "HEV large", "Efuel"),
    ("Passenger road", "Buses", "ICE", "Efuel"),
    ("Passenger road", "Buses", "PHEV", "Efuel"),
    ("Passenger road", "Buses", "EREV", "Efuel"),
    ("Passenger road", "Motorcycles", "ICE", "Efuel"),
    
    # All Efuel branches for freight road
    ("Freight road", "Trucks", "ICE heavy", "Efuel"),
    ("Freight road", "Trucks", "ICE medium", "Efuel"),
    ("Freight road", "Trucks", "EREV heavy", "Efuel"),
    ("Freight road", "Trucks", "EREV medium", "Efuel"),
    ("Freight road", "Trucks", "PHEV heavy", "Efuel"),
    ("Freight road", "Trucks", "PHEV medium", "Efuel"),
    ("Freight road", "LCVs", "ICE", "Efuel"),
    ("Freight road", "LCVs", "PHEV", "Efuel"),
    ("Freight road", "LCVs", "EREV", "Efuel"),
    
    # All Efuel branches for non-road transport
    ("Passenger non road", "Air", "Efuel"),
    ("Freight non road", "Air", "Efuel"),
    ("Passenger non road", "Rail", "Efuel"),
    ("Freight non road", "Rail", "Efuel"),
    ("Passenger non road", "Shipping", "Efuel"),
    ("Freight non road", "Shipping", "Efuel"),
    
    # Others that are just the lower level branches that have no fuel specified:
    ("Nonspecified transport",),
    ("Pipeline transport",),
    ("Freight non road",),
    ("Freight non road", "Air"),
    ("Freight non road", "Rail"),
    ("Freight non road", "Shipping"),
    ("Freight road",),
    ("Freight road", "LCVs"),
    ("Freight road", "LCVs", "BEV"),
    ("Freight road", "LCVs", "ICE"),
    ("Freight road", "LCVs", "PHEV"),
    ("Freight road", "LCVs", "EREV"),
    ("Freight road", "Trucks"),
    ("Freight road", "Trucks", "BEV heavy"),
    ("Freight road", "Trucks", "BEV medium"),
    ("Freight road", "Trucks", "EREV heavy"),
    ("Freight road", "Trucks", "EREV medium"),
    ("Freight road", "Trucks", "FCEV heavy"),
    ("Freight road", "Trucks", "FCEV medium"),
    ("Freight road", "Trucks", "ICE heavy"),
    ("Freight road", "Trucks", "ICE medium"),
    ("Freight road", "Trucks", "PHEV heavy"),
    ("Freight road", "Trucks", "PHEV medium"),
    ("Passenger non road",),
    ("Passenger non road", "Air"),
    ("Passenger non road", "Rail"),
    ("Passenger non road", "Shipping"),
    ("Passenger road",),
    ("Passenger road", "Buses"),
    ("Passenger road", "Buses", "BEV"),
    ("Passenger road", "Buses", "FCEV"),
    ("Passenger road", "Buses", "ICE"),
    ("Passenger road", "Buses", "EREV"),
    ("Passenger road", "Buses", "PHEV"),
    ("Passenger road", "LPVs"),
    ("Passenger road", "LPVs", "BEV large"),
    ("Passenger road", "LPVs", "BEV medium"),
    ("Passenger road", "LPVs", "BEV small"),
    ("Passenger road", "LPVs", "HEV large"),
    ("Passenger road", "LPVs", "HEV medium"),
    ("Passenger road", "LPVs", "HEV small"),
    ("Passenger road", "LPVs", "ICE large"),
    ("Passenger road", "LPVs", "ICE medium"),
    ("Passenger road", "LPVs", "ICE small"),
    ("Passenger road", "LPVs", "PHEV large"),
    ("Passenger road", "LPVs", "PHEV medium"),
    ("Passenger road", "LPVs", "PHEV small"),
    ("Passenger road", "LPVs", "EREV large"),
    ("Passenger road", "LPVs", "EREV medium"),
    ("Passenger road", "LPVs", "EREV small"),
    ("Passenger road", "Motorcycles"),
    ("Passenger road", "Motorcycles", "BEV"),
    ("Passenger road", "Motorcycles", "ICE"),
}

def identify_missing_esto_mappings_for_leap_branches(
    esto_to_leap_mapping,
    unmappable_branches,
    all_leap_branches,
):
    """
    Check that every LEAP branch is either mapped to an ESTO key or marked as unmappable.

    Parameters
    ----------
    esto_to_leap_mapping: Mapping of ESTO keys to lists of LEAP branch tuples.
    unmappable_branches: Iterable of branch tuples that should be excluded from coverage checks.
    all_leap_branches: Iterable of all branch tuples that should be represented in the mapping.

    Returns
    -------
    dict containing the missing branches (if any) and a summary of coverage.
    """
    all_leap_set = set(all_leap_branches)
    mapped_leap_branches = {
        leap_branch for leap_branches_list in esto_to_leap_mapping.values() for leap_branch in leap_branches_list
    }
    unmappable_set = set(unmappable_branches)

    missing_branches = all_leap_set - mapped_leap_branches - unmappable_set

    total_branches = len(all_leap_set)
    mapped_count = len(mapped_leap_branches)
    unmappable_count = len(unmappable_set)
    missing_count = len(missing_branches)

    summary = {
        "total_branches": total_branches,
        "mapped_count": mapped_count,
        "unmappable_count": unmappable_count,
        "missing_count": missing_count,
        "coverage_percent": (mapped_count + unmappable_count) / total_branches * 100 if total_branches else 0,
    }

    if missing_branches:
        missing_list = "\n  ".join(str(branch) for branch in sorted(missing_branches))
        raise ValueError(
            "Found LEAP branches without ESTO mappings that are not marked as unmappable.\n"
            "Add them to the ESTO to LEAP mapping or the unmappable list.\n"
            f"Missing branches ({missing_count}):\n  {missing_list}"
        )

    print(f"All LEAP branches are covered ({summary['coverage_percent']:.1f}% mapped or marked unmappable).")
    return {"missing_branches": missing_branches, "summary": summary}

#%%
# ============================================================
# LEAP_MEASURE_CONFIG
# Final version — hierarchical, cleaned, and LEAP-ready
# ============================================================
# ============================================================
# LEAP_MEASURE_CONFIG — Final structured version
# ============================================================
PJ_PER_TONNE_KM_TO_GJ_PER_TONNE_KM = 1e6
# Convert from km/PJ to MJ/100km. 
# 1 PJ = 1e15 J = 1e9 MJ
# So km/PJ becomes km/(1e9 MJ) = km/MJ * 1e-9
# To get MJ/100km: (1e9 MJ/PJ) * (100km/km) / (km/PJ) = 1e11 / (km/PJ)
KM_PER_PJ_TO_MJ_PER_100KM = 1e11#have to make sure the order is correct - this needs to be timesed by the value after inversion
LEAP_MEASURE_CONFIG = {
    # ============================================================
    # TRANSPORT TYPE (NON-ROAD)
    # ============================================================
    # All measures for transport type (non-road):
    # Activity Level
    "Transport type (non-road)": {
        "Activity Level": {
            "source_mapping": "Activity",
            "factor": 1,
            "unit": "Passenger_km_or_freight_tonne_km",
            "LEAP_units": "Passenger-km$Tonne-km",
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
    },

    # ============================================================
    # VEHICLE TYPE (NON-ROAD)
    # ============================================================
    # All measures for vehicle type (non-road):
    # Activity Level
    "Vehicle type (non-road)": {
        "Activity Level": {
            "source_mapping": "Activity_share_calc_fuel",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": 'of Tonne-km$of Passenger-km'#handled through define_value_based_on_src_tuple
        }
    },

    # ============================================================
    # FUEL (NON-ROAD)
    # ============================================================
    # All measures for fuel (non-road):
    # Activity Level
    # Final Energy Intensity 
    "Fuel (non-road)": {
        "Activity Level": {
            "source_mapping": "Activity_share_calc_fuel",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": 'of Tonne-km$of Passenger-km' #handled through define_value_based_on_src_tuple
        },
        "Final Energy Intensity": {
            "source_mapping": "Intensity",
            #need to convert from PJ pr tonne km to GJ per tonne km for LEAP
            "factor": PJ_PER_TONNE_KM_TO_GJ_PER_TONNE_KM,
            "unit": "GJ_per_tonne_km",
            "LEAP_units": "Gigajoule",
            "LEAP_Scale": None,
            "LEAP_Per": "Passenger-km$Tonne-km"
        }
    },

    ##################################################################
    # ============================================================
    # ROAD
    # ============================================================
    ##################################################################

    # ============================================================
    # TRANSPORT TYPE (ROAD)
    # ============================================================
    # All measures for transport type (road):
    # Activity Level
    "Transport type (road)": {
        # "Activity Level": {
        #     "source_mapping": "Activity",
        #     "factor": 1,
        #     "unit": "Billion_passenger_km_or_freight_tonne_km",
        #     "LEAP_units": "Passenger-km$Tonne-km", #ISSUE: how will we find this and match it up with the (assumed) calculations for vehicle km based on mileage and stocks in the rest of the branch? LEAP doesnt seem to have an occupancy or load variable for transport type level? Perhaps i will have to calculate this outside of leap and ensure that the sum of vehicle km from the vehicle type level matches the activity level at the transport type level?
        #     "LEAP_Scale": None,
        #     "LEAP_Per": None
        # },
        "Stock": {
            "source_mapping": "Stocks",
            "factor": 1,
            "unit": "stocks",
            "LEAP_units": "Device",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Sales": {
            "source_mapping": "Sales",
            "factor": 1,
            "unit": "stocks_per_year",
            "LEAP_units": "Device",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        
    },

    # ============================================================
    # VEHICLE TYPE (ROAD)
    # ============================================================
    # All measures for vehicle type (road):
    # Sales Share
    # Stock
    # Stock Share
    "Vehicle type (road)": {
        "Stock": {
            "source_mapping": "Stocks",
            "factor": 1,
            "unit": "stocks",
            "LEAP_units": "Device",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Stock Share": {
            "source_mapping": "Stock_share_calc_vehicle_type",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        },
        "Sales Share": {
            "source_mapping": "Vehicle_sales_share_calc_vehicle_type",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        }
    },

    # ============================================================
    # TECHNOLOGY (ROAD)
    # ============================================================
    # All measures for technology (road):
    # First Sales Year
    # Fraction of Scrapped Replaced
    # Max Scrappage Fraction
    # Sales Share
    # Scrappage
    # Stock Share
    "Technology (road)": {
        "First Sales Year": {
            "source_mapping": None,
            "factor": 1,
            "unit": "year",
            "LEAP_units": "Years",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Fraction of Scrapped Replaced": {
            "source_mapping": None,
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Percent",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        },
        "Max Scrappage Fraction": {
            "source_mapping": None,
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Percent",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        },
        "Sales Share": {
            "source_mapping": "Vehicle_sales_share_calc_fuel",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        },
        "Scrappage": {
            "source_mapping": "Scrappage",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Vehicle",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Stock Share": {
            "source_mapping": "Stock_share_calc_fuel",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        }
    },

    # ============================================================
    # FUEL (ROAD)
    # ============================================================
    # All measures for fuel (road):
    # Average Mileage
    # Device Share
    # Final On-Road Fuel Economy
    # Final On-Road Mileage
    # Fuel Economy
    # Fuel Economy Correction Factor
    # Mileage
    # Mileage Correction Factor
    "Fuel (road)": {
        "Average Mileage": {
            "source_mapping": "Mileage",
            "factor": 1,
            "unit": "km_per_stock",
            "LEAP_units": "Kilometer",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Device Share": {
            "source_mapping": "Stock_share_calc_fuel",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": None
        },
        "Final On-Road Fuel Economy": {
            "source_mapping": "Efficiency",
            'factor': KM_PER_PJ_TO_MJ_PER_100KM,
            'inverse': True,
            "unit": "MJ_per_100km",
            "LEAP_units": "MJ/100 km",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Final On-Road Mileage": {
            "source_mapping": "Mileage",
            "factor": 1,
            "unit": "km_per_stock",
            "LEAP_units": "Kilometer",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Fuel Economy": {
            "source_mapping": "Efficiency",
            "factor": KM_PER_PJ_TO_MJ_PER_100KM,
            "inverse": True,
            "unit": "MJ_per_100km",
            "LEAP_units": "MJ/100 km",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Fuel Economy Correction Factor": {
            "source_mapping": None,
            "factor": 1,
            "unit": "factor",
            "LEAP_units": "Percent",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Mileage": {
            "source_mapping": "Mileage",
            "factor": 1,
            "unit": "km_per_stock",
            "LEAP_units": "Kilometer",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Mileage Correction Factor": {
            "source_mapping": None,
            "factor": 1,
            "unit": "factor",
            "LEAP_units": "Percent",
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
    },
    
    # ============================================================
    # Others (level 1 and 2) - corresponding to the nonspecified and pipeline transport sectors
    # ============================================================
    #NOTE THE SINCE ACTIVITY IS IRRELEVANT WE WILL CALCAULTE IT SO THAT ENERGY = ACTIVITY * INTENSITY HOLDS TRUE. PERHAPS WE CAN JUST SET IT TO THE DESIRED ENERGY AND INTENSITY TO 1?
    "Others (level 1)": {
        "Activity Level": {
            "source_mapping": "Activity",
            "factor": 1,
            "unit": "Unspecified Unit",
            "LEAP_units": "Unspecified Unit",
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
    },
    "Others (level 2)": {
        # "Activity Level": {
        #     "source_mapping": "Activity",
        #     "factor": 1,
        #     "unit": "Unspecified Unit",
        #     "LEAP_units": "Unspecified Unit",
        #     "LEAP_Scale": None,
        #     "LEAP_Per": None
        # },
        "Activity Level": {
            "source_mapping": "Activity",
            "factor": 1,
            "unit": "%",
            "LEAP_units": "Share",
            "LEAP_Scale": "%",
            "LEAP_Per": None#'of Tonne-km$of Passenger-km' #handled through define_value_based_on_src_tuple
        },
            
        "Final Energy Intensity": {
            "source_mapping": "Intensity",
            "factor": 1e9,#counteract the 1e-9 for intensity in SOURCE_MEASURE_TO_UNIT for Intensity since these are based on using 1 as intensity and setting activity to desired energy.
            "unit": "Unspecified Unit",
            "LEAP_units": "Petajoule",
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
    }
}
#%%

# breakpoint()#how can we include a dict which will define the default split if we are hacing to use SPLIT_EQUALLY here? for example if its for device share of phevs, shifting the share to 50/50 elec/gasoline makes more sense than among the other fuels.. stock share can be spit into the basic fuels too. so on. 
# measure_branch_tuple = (measure,) + row['branch_tuple_minus_one_level']#do we drop one level here?
#("Stock Share", "Passenger road", "LPVs")
# default_splits_dict = {
#     ("Device Share", "Freight road", "LCVs", "PHEV"): {"Electricity": 50.0, "Diesel": 50.0},
#     ("Device Share", "Freight road", "Trucks", "PHEV heavy"): {"Electricity": 50.0, "Diesel": 50.0},#etc.

#     [("Stock Share", "Passenger road", "LPVs"), ("Stock Share", "Passenger road", "Buses"), ("Stock Share", "Passenger road", "Motorcycles")] : {"LPVs": 100, "Buses": 50.0},

#%%
def create_share_type_branch_settings_dict(LEAP_MEASURE_CONFIG=LEAP_MEASURE_CONFIG, LEAP_BRANCH_TO_EXPRESSION_MAPPING=LEAP_BRANCH_TO_EXPRESSION_MAPPING):
    share_type_measures = []#LEAP_MEASURE_CONFIG
    for short_name in LEAP_MEASURE_CONFIG:
        for measure in LEAP_MEASURE_CONFIG[short_name].keys():
            if measure not in share_type_measures:
                if LEAP_MEASURE_CONFIG[short_name][measure].get('LEAP_units', None) == 'Share':
                    share_type_measures.append((measure))
    #find all entries in the mapping which are share type measures and group them by their branch minus last level. then we can use this to set default values for splits when needed
    share_type_branch_dict = {}
    for key, value in LEAP_BRANCH_TO_EXPRESSION_MAPPING.items():
        if key[0] in share_type_measures:
            measure = key[0]
            branch_minus_one_level = (measure,) + key[1:-1]
            last_level = key[-1]
            if branch_minus_one_level not in share_type_branch_dict:
                share_type_branch_dict[branch_minus_one_level] = {last_level:0}
            else:
                share_type_branch_dict[branch_minus_one_level][last_level] = 0
    # Print the dictionary in a format that can be copy/pasted
    print("DEFAULT_BRANCH_SHARE_SETTINGS_DICT = {")
    for branch, keys in sorted(share_type_branch_dict.items()):
        print(f"    {branch}: {{")
        for key in sorted(keys):
            print(f"        \"{key}\": 0,")
        print("    },")
    print("}")
# %%
# create_share_type_branch_settings_dict()#run this and then put it through chatgpt to get some reasonable default values.
DEFAULT_BRANCH_SHARE_SETTINGS_DICT = {
    # ======================
    # DEVICE SHARE (fuels)
    # ======================
    ("Device Share", "Freight road", "LCVs", "BEV"): {
        "Electricity": 100,
    },
    ("Device Share", "Freight road", "LCVs", "EREV"): {
        "Biodiesel": 1,
        "Biogasoline": 0,
        "Diesel": 4,
        "Efuel": 0,
        "Electricity": 20,
        "Gasoline": 75,
    },
    ("Device Share", "Freight road", "LCVs", "ICE"): {
        "Biodiesel": 0,
        "Biogas": 0,
        "Biogasoline": 0,
        "CNG": 3,
        "Diesel": 80,
        "Efuel": 0,
        "Gasoline": 15,
        "LPG": 2,
    },
    ("Device Share", "Freight road", "LCVs", "PHEV"): {
        "Biodiesel": 0,
        "Biogasoline": 0,
        "Diesel": 5,
        "Efuel": 0,
        "Electricity": 35,
        "Gasoline": 60,
    },
    ("Device Share", "Freight road", "Trucks", "BEV heavy"): {
        "Electricity": 100,
    },
    ("Device Share", "Freight road", "Trucks", "BEV medium"): {
        "Electricity": 100,
    },
    ("Device Share", "Freight road", "Trucks", "EREV heavy"): {
        "Biodiesel": 2,
        "Biogasoline": 0,
        "Diesel": 78,
        "Efuel": 0,
        "Electricity": 20,
        "Gasoline": 0,
    },
    ("Device Share", "Freight road", "Trucks", "EREV medium"): {
        "Biodiesel": 1,
        "Biogasoline": 0,
        "Diesel": 69,
        "Efuel": 0,
        "Electricity": 20,
        "Gasoline": 10,
    },
    ("Device Share", "Freight road", "Trucks", "FCEV heavy"): {
        "Hydrogen": 100,
    },
    ("Device Share", "Freight road", "Trucks", "FCEV medium"): {
        "Hydrogen": 100,
    },
    ("Device Share", "Freight road", "Trucks", "ICE heavy"): {
        "Biodiesel": 2,
        "Biogas": 0,
        "Biogasoline": 1,
        "CNG": 4,
        "Diesel": 85,
        "Efuel": 0,
        "Gasoline": 2,
        "LNG": 5,
        "LPG": 1,
    },
    ("Device Share", "Freight road", "Trucks", "ICE medium"): {
        "Biodiesel": 1,
        "Biogas": 0,
        "Biogasoline": 1,
        "CNG": 8,
        "Diesel": 75,
        "Efuel": 0,
        "Gasoline": 10,
        "LNG": 2,
        "LPG": 3,
    },
    ("Device Share", "Freight road", "Trucks", "PHEV heavy"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 60,
        "Efuel": 0,
        "Electricity": 35,
        "Gasoline": 3,
    },
    ("Device Share", "Freight road", "Trucks", "PHEV medium"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 40,
        "Efuel": 0,
        "Electricity": 18,
        "Gasoline": 40,
    },

    ("Device Share", "Passenger road", "Buses", "BEV"): {
        "Electricity": 100,
    },
    ("Device Share", "Passenger road", "Buses", "EREV"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 70,
        "Efuel": 0,
        "Electricity": 25,
        "Gasoline": 3,
    },
    ("Device Share", "Passenger road", "Buses", "FCEV"): {
        "Hydrogen": 100,
    },
    ("Device Share", "Passenger road", "Buses", "ICE"): {
        "Biodiesel": 2,
        "Biogas": 1,
        "Biogasoline": 1,
        "CNG": 20,
        "Diesel": 65,
        "Efuel": 0,
        "Gasoline": 3,
        "LPG": 8,
    },
    ("Device Share", "Passenger road", "Buses", "PHEV"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 60,
        "Efuel": 0,
        "Electricity": 35,
        "Gasoline": 3,
    },

    ("Device Share", "Passenger road", "LPVs", "BEV large"): {
        "Electricity": 100,
    },
    ("Device Share", "Passenger road", "LPVs", "BEV medium"): {
        "Electricity": 100,
    },
    ("Device Share", "Passenger road", "LPVs", "BEV small"): {
        "Electricity": 100,
    },
    ("Device Share", "Passenger road", "LPVs", "EREV large"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 8,
        "Efuel": 0,
        "Electricity": 30,
        "Gasoline": 60,
    },
    ("Device Share", "Passenger road", "LPVs", "EREV medium"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 8,
        "Efuel": 0,
        "Electricity": 30,
        "Gasoline": 60,
    },
    ("Device Share", "Passenger road", "LPVs", "EREV small"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 8,
        "Efuel": 0,
        "Electricity": 30,
        "Gasoline": 60,
    },
    ("Device Share", "Passenger road", "LPVs", "HEV large"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 9,
        "Efuel": 0,
        "Gasoline": 89,
    },
    ("Device Share", "Passenger road", "LPVs", "HEV medium"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 9,
        "Efuel": 0,
        "Gasoline": 89,
    },
    ("Device Share", "Passenger road", "LPVs", "HEV small"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 9,
        "Efuel": 0,
        "Gasoline": 89,
    },
    ("Device Share", "Passenger road", "LPVs", "ICE large"): {
        "Biodiesel": 1,
        "Biogas": 0,
        "Biogasoline": 1,
        "CNG": 3,
        "Diesel": 40,
        "Efuel": 0,
        "Gasoline": 50,
        "LPG": 5,
    },
    ("Device Share", "Passenger road", "LPVs", "ICE medium"): {
        "Biodiesel": 1,
        "Biogas": 0,
        "Biogasoline": 1,
        "CNG": 3,
        "Diesel": 30,
        "Efuel": 0,
        "Gasoline": 60,
        "LPG": 5,
    },
    ("Device Share", "Passenger road", "LPVs", "ICE small"): {
        "Biodiesel": 1,
        "Biogasoline": 2,
        "Diesel": 7,
        "Efuel": 0,
        "Gasoline": 90,
    },
    ("Device Share", "Passenger road", "LPVs", "PHEV large"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 8,
        "Efuel": 0,
        "Electricity": 35,
        "Gasoline": 55,
    },
    ("Device Share", "Passenger road", "LPVs", "PHEV medium"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 8,
        "Efuel": 0,
        "Electricity": 35,
        "Gasoline": 55,
    },
    ("Device Share", "Passenger road", "LPVs", "PHEV small"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 8,
        "Efuel": 0,
        "Electricity": 35,
        "Gasoline": 55,
    },

    ("Device Share", "Passenger road", "Motorcycles", "BEV"): {
        "Electricity": 100,
    },
    ("Device Share", "Passenger road", "Motorcycles", "ICE"): {
        "Biodiesel": 1,
        "Biogasoline": 1,
        "Diesel": 3,
        "Efuel": 0,
        "Gasoline": 95,
    },

    # ======================
    # SALES SHARE (tech / segments)
    # ======================
    ("Sales Share", "Freight road"): {
        "LCVs": 30,
        "Trucks": 70,
    },
    ("Sales Share", "Freight road", "LCVs"): {
        "BEV": 5,
        "EREV": 1,
        "ICE": 88,
        "PHEV": 6,
    },
    ("Sales Share", "Freight road", "Trucks"): {
        "BEV heavy": 2,
        "BEV medium": 6,
        "EREV heavy": 1,
        "EREV medium": 3,
        "FCEV heavy": 1,
        "FCEV medium": 2,
        "ICE heavy": 37,
        "ICE medium": 40,
        "PHEV heavy": 3,
        "PHEV medium": 5,
    },

    ("Sales Share", "Passenger road"): {
        "Buses": 9,
        "LPVs": 68,
        "Motorcycles": 23,
    },
    ("Sales Share", "Passenger road", "Buses"): {
        "BEV": 10,
        "EREV": 2,
        "FCEV": 5,
        "ICE": 75,
        "PHEV": 8,
    },
    ("Sales Share", "Passenger road", "LPVs"): {
        "BEV large": 4,
        "BEV medium": 7,
        "BEV small": 9,
        "EREV large": 1,
        "EREV medium": 1,
        "EREV small": 0,
        "HEV large": 2,
        "HEV medium": 6,
        "HEV small": 10,
        "ICE large": 10,
        "ICE medium": 18,
        "ICE small": 22,
        "PHEV large": 3,
        "PHEV medium": 4,
        "PHEV small": 3,
    },
    ("Sales Share", "Passenger road", "Motorcycles"): {
        "BEV": 20,
        "ICE": 80,
    },

    # ======================
    # STOCK SHARE (tech / segments)
    # ======================
    ("Stock Share", "Freight road"): {
        "LCVs": 25,
        "Trucks": 75,
    },
    ("Stock Share", "Freight road", "LCVs"): {
        "BEV": 1,
        "EREV": 0,
        "ICE": 97,
        "PHEV": 2,
    },
    ("Stock Share", "Freight road", "Trucks"): {
        "BEV heavy": 1,
        "BEV medium": 4,
        "EREV heavy": 0,
        "EREV medium": 1,
        "FCEV heavy": 0,
        "FCEV medium": 1,
        "ICE heavy": 45,
        "ICE medium": 45,
        "PHEV heavy": 1,
        "PHEV medium": 2,
    },

    ("Stock Share", "Passenger road"): {
        "Buses": 10,
        "LPVs": 70,
        "Motorcycles": 20,
    },
    ("Stock Share", "Passenger road", "Buses"): {
        "BEV": 4,
        "EREV": 1,
        "FCEV": 2,
        "ICE": 90,
        "PHEV": 3,
    },
    ("Stock Share", "Passenger road", "LPVs"): {
        "BEV large": 2,
        "BEV medium": 3,
        "BEV small": 4,
        "EREV large": 0,
        "EREV medium": 0,
        "EREV small": 0,
        "HEV large": 1,
        "HEV medium": 4,
        "HEV small": 8,
        "ICE large": 10,
        "ICE medium": 25,
        "ICE small": 35,
        "PHEV large": 2,
        "PHEV medium": 3,
        "PHEV small": 3,
    },
    ("Stock Share", "Passenger road", "Motorcycles"): {
        "BEV": 5,
        "ICE": 95,
    },
}


#%%
def _get_most_detailed_branch_set(branches):
    """Return only the branch tuples that are not prefixes of longer tuples."""

    branch_list = list(branches)
    most_detailed = set()

    for branch in branch_list:
        if not any(
            other != branch
            and len(other) > len(branch)
            and other[: len(branch)] == branch
            for other in branch_list
        ):
            most_detailed.add(branch)

    return most_detailed


def validate_branch_combinations_across_mappings(max_examples: int = 5):
    """Validate that detailed branch tuples appear across all mapping dictionaries.

    The validation compares the most detailed branch combinations defined in:

    * ``LEAP_BRANCH_TO_EXPRESSION_MAPPING`` (ignoring the measure prefix),
    * ``LEAP_BRANCH_TO_SOURCE_MAP``,
    * ``ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP`` values,
    * ``SHORTNAME_TO_LEAP_BRANCHES`` values, and
    * ``ALL_PATHS_LEAP`` generated from ``basic_mappings``.

    Branches listed in ``UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT`` are excluded
    when checking coverage inside ``ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP``
    because they are known to have no ESTO equivalent.
    """

    expression_branches = _get_most_detailed_branch_set(
        [key[1:] for key in LEAP_BRANCH_TO_EXPRESSION_MAPPING if len(key) > 1]
    )
    shortname_branches = _get_most_detailed_branch_set(
        branch
        for branches in SHORTNAME_TO_LEAP_BRANCHES.values()
        for branch in branches
    )
    source_branches = _get_most_detailed_branch_set(LEAP_BRANCH_TO_SOURCE_MAP.keys())
    esto_branches = _get_most_detailed_branch_set(
        branch
        for branches in ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP.values()
        for branch in branches
    )
    leap_structure_branches = _get_most_detailed_branch_set(ALL_PATHS_LEAP)

    mapping_sets = {
        "LEAP_BRANCH_TO_EXPRESSION_MAPPING": expression_branches,
        "LEAP_BRANCH_TO_SOURCE_MAP": source_branches,
        "ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP": esto_branches,
        "SHORTNAME_TO_LEAP_BRANCHES": shortname_branches,
        "ALL_PATHS_LEAP": leap_structure_branches,
    }

    missing_summary = {}

    for source_name, branches in mapping_sets.items():
        for target_name, target_branches in mapping_sets.items():
            if source_name == target_name:
                continue

            if target_name == "ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP":
                branches_to_check = {
                    branch
                    for branch in branches
                    if branch not in UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT
                }
            else:
                branches_to_check = set(branches)

            missing = branches_to_check - target_branches
            if missing:
                missing_summary.setdefault(source_name, {}).setdefault(
                    target_name, set()
                ).update(missing)

    print("\n=== Branch combination coverage check ===")
    if not missing_summary:
        print("All branch combinations are consistent across mappings.")
    else:
        for source_name, target_map in missing_summary.items():
            print(f"\n{source_name} discrepancies:")
            for target_name, missing in target_map.items():
                print(f"  Missing in {target_name}: {len(missing)} branches")
                for branch in sorted(missing)[:max_examples]:
                    print(f"    - {branch}")

    return missing_summary

#%%
identify_missing_esto_mappings_for_leap_branches(
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
    [branch for branches in SHORTNAME_TO_LEAP_BRANCHES.values() for branch in branches],
)
#%%
validate_branch_combinations_across_mappings()
#%%
check_LEAP_BRANCH_TO_SOURCE_MAP_for_missing_proxies_and_combinations(LEAP_BRANCH_TO_SOURCE_MAP, PROXIED_SOURCE_ROWS_WITH_NO_ACTIVITY, COMBINATION_SOURCE_ROWS)
#%%
