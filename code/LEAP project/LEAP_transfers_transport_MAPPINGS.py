#%%
import pandas as pd

SHORTNAME_TO_LEAP_BRANCHES = {
    # Mapping to names used for reference in LEAP
    'Transport type (road)': [
        ("Passenger road",),
        ("Freight road",),
    ],
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
        ("Passenger road", "LPVs", "ICE small", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE small", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE medium", "Gasoline"),
        ("Passenger road", "LPVs", "ICE medium", "Diesel"),
        ("Passenger road", "LPVs", "ICE medium", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE medium", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE medium", "LPG"),
        ("Passenger road", "LPVs", "ICE medium", "CNG"),
        ("Passenger road", "LPVs", "ICE medium", "Biogas"),
        ("Passenger road", "LPVs", "ICE large", "Gasoline"),
        ("Passenger road", "LPVs", "ICE large", "Diesel"),
        ("Passenger road", "LPVs", "ICE large", "Biogasoline"),
        ("Passenger road", "LPVs", "ICE large", "Biodiesel"),
        ("Passenger road", "LPVs", "ICE large", "LPG"),
        ("Passenger road", "LPVs", "ICE large", "CNG"),
        ("Passenger road", "LPVs", "ICE large", "Biogas"),
        ("Passenger road", "LPVs", "PHEV small", "Electricity"),
        ("Passenger road", "LPVs", "PHEV small", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Diesel"),
        ("Passenger road", "LPVs", "PHEV small", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV small", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Electricity"),
        ("Passenger road", "LPVs", "PHEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Diesel"),
        ("Passenger road", "LPVs", "PHEV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "PHEV large", "Electricity"),
        ("Passenger road", "LPVs", "PHEV large", "Gasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Diesel"),
        ("Passenger road", "LPVs", "PHEV large", "Biogasoline"),
        ("Passenger road", "LPVs", "PHEV large", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV small", "Gasoline"),
        ("Passenger road", "LPVs", "HEV small", "Diesel"),
        ("Passenger road", "LPVs", "HEV small", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV small", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV medium", "Gasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Diesel"),
        ("Passenger road", "LPVs", "HEV medium", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV medium", "Biodiesel"),
        ("Passenger road", "LPVs", "HEV large", "Gasoline"),
        ("Passenger road", "LPVs", "HEV large", "Diesel"),
        ("Passenger road", "LPVs", "HEV large", "Biogasoline"),
        ("Passenger road", "LPVs", "HEV large", "Biodiesel"),
        ("Passenger road", "Buses", "BEV", "Electricity"),
        ("Passenger road", "Buses", "ICE", "Diesel"),
        ("Passenger road", "Buses", "ICE", "Gasoline"),
        ("Passenger road", "Buses", "ICE", "LPG"),
        ("Passenger road", "Buses", "ICE", "CNG"),
        ("Passenger road", "Buses", "ICE", "Biogas"),
        ("Passenger road", "Buses", "FCEV", "Hydrogen"),
        ("Passenger road", "Motorcycles", "ICE", "Gasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Diesel"),
        ("Passenger road", "Motorcycles", "ICE", "Biogasoline"),
        ("Passenger road", "Motorcycles", "ICE", "Biodiesel"),
        ("Passenger road", "Motorcycles", "BEV", "Electricity"),
        # Freight road fuels
        ("Freight road", "Trucks", "ICE heavy", "Gasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Diesel"),
        ("Freight road", "Trucks", "ICE heavy", "Biogasoline"),
        ("Freight road", "Trucks", "ICE heavy", "Biodiesel"),
        ("Freight road", "Trucks", "ICE heavy", "LPG"),
        ("Freight road", "Trucks", "ICE heavy", "CNG"),
        ("Freight road", "Trucks", "ICE heavy", "LNG"),
        ("Freight road", "Trucks", "ICE heavy", "Biogas"),
        ("Freight road", "Trucks", "ICE medium", "Gasoline"),
        ("Freight road", "Trucks", "ICE medium", "Diesel"),
        ("Freight road", "Trucks", "ICE medium", "Biogasoline"),
        ("Freight road", "Trucks", "ICE medium", "Biodiesel"),
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
        ("Freight road", "Trucks", "EREV heavy", "Gasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Diesel"),
        ("Freight road", "Trucks", "EREV heavy", "Electricity"),
        ("Freight road", "Trucks", "EREV heavy", "Biogasoline"),
        ("Freight road", "Trucks", "EREV heavy", "Biodiesel"),
        ("Freight road", "Trucks", "FCEV heavy", "Hydrogen"),
        ("Freight road", "Trucks", "FCEV medium", "Hydrogen"),
        ("Freight road", "LCVs", "ICE", "Gasoline"),
        ("Freight road", "LCVs", "ICE", "Diesel"),
        ("Freight road", "LCVs", "ICE", "Biogasoline"),
        ("Freight road", "LCVs", "ICE", "Biodiesel"),
        ("Freight road", "LCVs", "ICE", "LPG"),
        ("Freight road", "LCVs", "ICE", "CNG"),
        ("Freight road", "LCVs", "ICE", "Biogas"),
        ("Freight road", "LCVs", "BEV", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Electricity"),
        ("Freight road", "LCVs", "PHEV", "Gasoline"),
        ("Freight road", "LCVs", "PHEV", "Diesel"),
        ("Freight road", "LCVs", "PHEV", "Biogasoline"),
        ("Freight road", "LCVs", "PHEV", "Biodiesel"),
    ],

    #################################################
    # NON-ROAD TRANSPORT TYPES
    #################################################
    'Transport type (non-road)': [
        ("Passenger non road",),
        ("Freight non road",),
    ],

    'Vehicle type (non-road)': [
        ("Passenger non road", "Air"),
        ("Passenger non road", "Rail"),
        ("Passenger non road", "Shipping"),
        ("Freight non road", "Air"),
        ("Freight non road", "Rail"),
        ("Freight non road", "Shipping"),
    ],

    'Fuel (non-road)': [
        ("Passenger non road", "Air", "Hydrogen"),
        ("Passenger non road", "Air", "Electricity"),
        ("Passenger non road", "Air", "Jet fuel"),
        ("Passenger non road", "Air", "Aviation gasoline"),
        
        ("Passenger non road", "Rail", "Electricity"),
        ("Passenger non road", "Rail", "Diesel"),
        ("Passenger non road", "Rail", "Hydrogen"),
        ("Passenger non road", "Rail", "Coal"),
        ("Passenger non road", "Rail", "Biodiesel"),
        
        ("Passenger non road", "Shipping", "Electricity"),
        ("Passenger non road", "Shipping", "Hydrogen"),
        ("Passenger non road", "Shipping", "Diesel"),
        ("Passenger non road", "Shipping", "Fuel oil"),
        ("Passenger non road", "Shipping", "LNG"),
        ("Passenger non road", "Shipping", "Gasoline"),
        ("Passenger non road", "Shipping", "Ammonia"),
        ("Passenger non road", "Shipping", "Biodiesel"),
        
        ("Freight non road", "Air", "Hydrogen"),
        ("Freight non road", "Air", "Electricity"),
        ("Freight non road", "Air", "Jet fuel"),
        ("Freight non road", "Air", "Aviation gasoline"),
        
        ("Freight non road", "Rail", "Electricity"),
        ("Freight non road", "Rail", "Diesel"),
        ("Freight non road", "Rail", "Hydrogen"),
        ("Freight non road", "Rail", "Coal"),
        ("Freight non road", "Rail", "Biodiesel"),
        
        ("Freight non road", "Shipping", "Electricity"),
        ("Freight non road", "Shipping", "Hydrogen"),
        ("Freight non road", "Shipping", "Diesel"),
        ("Freight non road", "Shipping", "Fuel oil"),
        ("Freight non road", "Shipping", "LNG"),
        ("Freight non road", "Shipping", "Gasoline"),
        ("Freight non road", "Shipping", "Ammonia"),
        ("Freight non road", "Shipping", "Biodiesel"),
    ],

    'Others (level 1)': [
        ("Nonspecified transport",),
        ('Pipeline transport',)
    ],
    'Others (level 2)': [
        ("Nonspecified transport", "Kerosene"),
        ("Nonspecified transport", "Fuel oil"),
        ("Nonspecified transport", "Diesel"),
        ("Nonspecified transport", "LPG"),
        ("Nonspecified transport", "Gasoline"),
        ("Nonspecified transport", "Coal products"),
        ("Nonspecified transport", "Other petroleum products"),
        ("Pipeline transport", "Fuel oil"),
        ("Pipeline transport", "Diesel"),
        ("Pipeline transport", "Natural gas"),
        ("Pipeline transport", "Electricity"),
    ]
}

LEAP_BRANCH_TO_SOURCE_MAP = {
    #tuples map to: LEAP: (Transport Type, Medium, Vehicle Type, Drive, Fuel) : (Transport Type, Medium, Vehicle Type, Drive, Fuel)
    #for non-road mediums, the LEAP tuple omits vehicle type and drive since they don't apply
    #no one-to-many mappings to avoid complications

    # =========================
    # NON-ROAD: PASSENGER
    # =========================
    ("Passenger non road", "Air", "Hydrogen"):        ("passenger", "air",  "all", "air_hydrogen", "Hydrogen"),
    ("Passenger non road", "Air", "Electricity"):     ("passenger", "air",  "all", "air_electric", "Electricity"),
    ("Passenger non road", "Air", "Jet fuel"):        ("passenger", "air",  "all", "air_jet_fuel", "Jet fuel"),
    ("Passenger non road", "Air", "Aviation gasoline"): ("passenger", "air", "all", "air_av_gas", "Aviation gasoline"),

    ("Passenger non road", "Rail", "Electricity"):    ("passenger", "rail", "all", "rail_electricity", "Electricity"),
    ("Passenger non road", "Rail", "Diesel"):         ("passenger", "rail", "all", "rail_diesel", "Diesel"),
    ("Passenger non road", "Rail", "Hydrogen"):       ("passenger", "rail", "all", "rail_electricity", "Electricity"),  # proxy
    ("Passenger non road", "Rail", "Coal"):           ("passenger", "rail", "all", "rail_coal", "Coal"),

    ("Passenger non road", "Shipping", "Electricity"):("passenger", "ship", "all", "ship_electric", "Electricity"),
    ("Passenger non road", "Shipping", "Hydrogen"):   ("passenger", "ship", "all", "ship_hydrogen", "Hydrogen"),
    ("Passenger non road", "Shipping", "Diesel"):     ("passenger", "ship", "all", "ship_diesel", "Diesel"),
    ("Passenger non road", "Shipping", "Fuel oil"):   ("passenger", "ship", "all", "ship_fuel_oil", "Fuel oil"),
    ("Passenger non road", "Shipping", "LNG"):        ("passenger", "ship", "all", "ship_lng", "LNG"),
    ("Passenger non road", "Shipping", "Gasoline"):   ("passenger", "ship", "all", "ship_gasoline", "Gasoline"),
    ("Passenger non road", "Shipping", "Ammonia"):    ("passenger", "ship", "all", "ship_ammonia", "Ammonia"),

    # =========================
    # NON-ROAD: FREIGHT
    # =========================
    ("Freight non road", "Air", "Hydrogen"):          ("freight", "air",  "all", "air_hydrogen", "Hydrogen"),
    ("Freight non road", "Air", "Electricity"):       ("freight", "air",  "all", "air_electric", "Electricity"),
    ("Freight non road", "Air", "Jet fuel"):          ("freight", "air",  "all", "air_jet_fuel", "Jet fuel"),
    ("Freight non road", "Air", "Aviation gasoline"): ("freight", "air",  "all", "air_av_gas", "Aviation gasoline"),

    ("Freight non road", "Rail", "Electricity"):      ("freight", "rail", "all", "rail_electricity", "Electricity"),
    ("Freight non road", "Rail", "Diesel"):           ("freight", "rail", "all", "rail_diesel", "Diesel"),
    ("Freight non road", "Rail", "Hydrogen"):         ("freight", "rail", "all", "rail_electricity", "Electricity"),    # proxy
    ("Freight non road", "Rail", "Coal"):             ("freight", "rail", "all", "rail_coal", "Coal"),

    ("Freight non road", "Shipping", "Electricity"):  ("freight", "ship", "all", "ship_electric", "Electricity"),
    ("Freight non road", "Shipping", "Hydrogen"):     ("freight", "ship", "all", "ship_hydrogen", "Hydrogen"),
    ("Freight non road", "Shipping", "Diesel"):       ("freight", "ship", "all", "ship_diesel", "Diesel"),
    ("Freight non road", "Shipping", "Fuel oil"):     ("freight", "ship", "all", "ship_fuel_oil", "Fuel oil"),
    ("Freight non road", "Shipping", "LNG"):          ("freight", "ship", "all", "ship_lng", "LNG"),
    ("Freight non road", "Shipping", "Gasoline"):     ("freight", "ship", "all", "ship_gasoline", "Gasoline"),
    ("Freight non road", "Shipping", "Ammonia"):      ("freight", "ship", "all", "ship_ammonia", "Ammonia"),
    # =====================================================
    # ROAD: PASSENGER ROAD → LPVs
    # =====================================================
    ("Passenger road","LPVs","BEV small","Electricity"):   ("passenger","road","car","bev","Electricity"),
    ("Passenger road","LPVs","BEV medium","Electricity"):  ("passenger","road","suv","bev","Electricity"),
    ("Passenger road","LPVs","BEV large","Electricity"):   ("passenger","road","lt","bev","Electricity"),

    ("Passenger road","LPVs","ICE small","Gasoline"):      ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE small","Diesel"):        ("passenger","road","car","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE small","Biogasoline"):       ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE small","Biodiesel"):       ("passenger","road","car","ice_d","Diesel"),

    ("Passenger road","LPVs","ICE medium","Gasoline"):     ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE medium","Diesel"):       ("passenger","road","suv","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE medium","Biodiesel"):       ("passenger","road","suv","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE medium","Biogasoline"):      ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE medium","LPG"):          ("passenger","road","suv","lpg","LPG"),
    ("Passenger road","LPVs","ICE medium","CNG"):          ("passenger","road","suv","cng","CNG"),
    ("Passenger road","LPVs","ICE medium","Biogas"):       ("passenger","road","suv","cng","CNG"),

    ("Passenger road","LPVs","ICE large","Gasoline"):      ("passenger","road","lt","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE large","Diesel"):        ("passenger","road","lt","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE large","Biodiesel"):        ("passenger","road","lt","ice_d","Diesel"),
    ("Passenger road","LPVs","ICE large","Biogasoline"):       ("passenger","road","lt","ice_g","Gasoline"),
    ("Passenger road","LPVs","ICE large","LPG"):           ("passenger","road","lt","lpg","LPG"),
    ("Passenger road","LPVs","ICE large","CNG"):           ("passenger","road","lt","cng","CNG"),
    ("Passenger road","LPVs","ICE large","Biogas"):        ("passenger","road","lt","cng","CNG"),

    ("Passenger road","LPVs","PHEV small","Electricity"):  ("passenger","road","car","phev_g","Electricity"),
    ("Passenger road","LPVs","PHEV small","Gasoline"):     ("passenger","road","car","phev_g","Gasoline"),
    ("Passenger road","LPVs","PHEV small","Diesel"):       ("passenger","road","car","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV small","Biodiesel"):       ("passenger","road","car","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV small","Biogasoline"):      ("passenger","road","car","phev_g","Gasoline"),

    ("Passenger road","LPVs","PHEV medium","Electricity"): ("passenger","road","suv","phev_g","Electricity"),
    ("Passenger road","LPVs","PHEV medium","Gasoline"):    ("passenger","road","suv","phev_g","Gasoline"),
    ("Passenger road","LPVs","PHEV medium","Diesel"):      ("passenger","road","suv","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV medium","Biodiesel"):      ("passenger","road","suv","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV medium","Biogasoline"):     ("passenger","road","suv","phev_g","Gasoline"),

    ("Passenger road","LPVs","PHEV large","Electricity"):  ("passenger","road","lt","phev_g","Electricity"),
    ("Passenger road","LPVs","PHEV large","Gasoline"):     ("passenger","road","lt","phev_g","Gasoline"),
    ("Passenger road","LPVs","PHEV large","Diesel"):       ("passenger","road","lt","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV large","Biodiesel"):       ("passenger","road","lt","phev_d","Diesel"),
    ("Passenger road","LPVs","PHEV large","Biogasoline"):      ("passenger","road","lt","phev_g","Gasoline"),

    ("Passenger road","LPVs","HEV small","Gasoline"):      ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV small","Diesel"):        ("passenger","road","car","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV small","Biodiesel"):        ("passenger","road","car","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV small","Biogasoline"):       ("passenger","road","car","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV medium","Gasoline"):     ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV medium","Diesel"):       ("passenger","road","suv","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV medium","Biodiesel"):       ("passenger","road","suv","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV medium","Biogasoline"):      ("passenger","road","suv","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV large","Gasoline"):      ("passenger","road","lt","ice_g","Gasoline"),
    ("Passenger road","LPVs","HEV large","Diesel"):        ("passenger","road","lt","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV large","Biodiesel"):        ("passenger","road","lt","ice_d","Diesel"),
    ("Passenger road","LPVs","HEV large","Biogasoline"):       ("passenger","road","lt","ice_g","Gasoline"),

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
    ("Passenger road","Motorcycles","ICE","Biodiesel"):       ("passenger","road","2w","ice_d","Diesel"),
    ("Passenger road","Motorcycles","ICE","Biogasoline"):      ("passenger","road","2w","ice_g","Gasoline"),
    ("Passenger road","Motorcycles","BEV","Electricity"):  ("passenger","road","2w","bev","Electricity"),

    # =========================
    # ROAD: FREIGHT ROAD → Trucks
    # =========================
    ("Freight road","Trucks","ICE heavy","Gasoline"):      ("freight","road","ht","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE heavy","Diesel"):        ("freight","road","ht","ice_d","Diesel"),
    ("Freight road","Trucks","ICE heavy","Biodiesel"):        ("freight","road","ht","ice_d","Diesel"),
    ("Freight road","Trucks","ICE heavy","Biogasoline"):       ("freight","road","ht","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE heavy","LPG"):           ("freight","road","ht","lpg","LPG"),
    ("Freight road","Trucks","ICE heavy","CNG"):           ("freight","road","ht","cng","CNG"),
    ("Freight road","Trucks","ICE heavy","LNG"):           ("freight","road","ht","lng","LNG"),
    ("Freight road","Trucks","ICE heavy","Biogas"):        ("freight","road","ht","cng","CNG"),

    ("Freight road","Trucks","ICE medium","Gasoline"):     ("freight","road","mt","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE medium","Diesel"):       ("freight","road","mt","ice_d","Diesel"),
    ("Freight road","Trucks","ICE medium","Biodiesel"):       ("freight","road","mt","ice_d","Diesel"),
    ("Freight road","Trucks","ICE medium","Biogasoline"):      ("freight","road","mt","ice_g","Gasoline"),
    ("Freight road","Trucks","ICE medium","LNG"):          ("freight","road","mt","lng","LNG"),
    ("Freight road","Trucks","ICE medium","CNG"):          ("freight","road","mt","cng","CNG"),
    ("Freight road","Trucks","ICE medium","LPG"):          ("freight","road","mt","lpg","LPG"),
    ("Freight road","Trucks","ICE medium","Biogas"):       ("freight","road","mt","cng","CNG"),

    ("Freight road","Trucks","BEV heavy","Electricity"):   ("freight","road","ht","bev","Electricity"),
    ("Freight road","Trucks","BEV medium","Electricity"):  ("freight","road","mt","bev","Electricity"),

    ("Freight road","Trucks","EREV medium","Gasoline"):    ("freight","road","mt","phev_g","Gasoline"),
    ("Freight road","Trucks","EREV medium","Electricity"): ("freight","road","mt","phev_g","Electricity"),
    ("Freight road","Trucks","EREV medium","Diesel"):      ("freight","road","mt","phev_d","Diesel"),
    ("Freight road","Trucks","EREV medium","Biodiesel"):      ("freight","road","mt","phev_d","Diesel"),
    ("Freight road","Trucks","EREV medium","Biogasoline"):     ("freight","road","mt","phev_g","Gasoline"),

    ("Freight road","Trucks","EREV heavy","Gasoline"):     ("freight","road","ht","phev_g","Gasoline"),
    ("Freight road","Trucks","EREV heavy","Electricity"):  ("freight","road","ht","phev_g","Electricity"),
    ("Freight road","Trucks","EREV heavy","Diesel"):       ("freight","road","ht","phev_d","Diesel"),
    ("Freight road","Trucks","EREV heavy","Biodiesel"):       ("freight","road","ht","phev_d","Diesel"),
    ("Freight road","Trucks","EREV heavy","Biogasoline"):      ("freight","road","ht","phev_g","Gasoline"),

    ("Freight road","Trucks","FCEV heavy","Hydrogen"):     ("freight","road","ht","fcev","Hydrogen"),
    ("Freight road","Trucks","FCEV medium","Hydrogen"):    ("freight","road","mt","fcev","Hydrogen"),

    # =========================
    # ROAD: FREIGHT ROAD → LCVs
    # =========================
    ("Freight road","LCVs","ICE","Gasoline"):              ("freight","road","lcv","ice_g","Gasoline"),
    ("Freight road","LCVs","ICE","Diesel"):                ("freight","road","lcv","ice_d","Diesel"),
    ("Freight road","LCVs","ICE","Biodiesel"):                ("freight","road","lcv","ice_d","Diesel"),
    ("Freight road","LCVs","ICE","Biogasoline"):               ("freight","road","lcv","ice_g","Gasoline"),
    ("Freight road","LCVs","ICE","CNG"):                   ("freight","road","lcv","cng","CNG"),
    ("Freight road","LCVs","ICE","LPG"):                   ("freight","road","lcv","lpg","LPG"),
    ("Freight road","LCVs","ICE","Biogas"):                ("freight","road","lcv","cng","CNG"),
    ("Freight road","LCVs","BEV","Electricity"):           ("freight","road","lcv","bev","Electricity"),
    ("Freight road","LCVs","PHEV","Electricity"):          ("freight","road","lcv","phev_g","Electricity"),
    ("Freight road","LCVs","PHEV","Gasoline"):             ("freight","road","lcv","phev_g","Gasoline"),
    ("Freight road","LCVs","PHEV","Diesel"):               ("freight","road","lcv","phev_d","Diesel"),
    ("Freight road","LCVs","PHEV","Biodiesel"):               ("freight","road","lcv","phev_d","Diesel"),
    ("Freight road","LCVs","PHEV","Biogasoline"):              ("freight","road","lcv","phev_g","Gasoline"),
    
    # =========================
    # TECHNOLOGY-LEVEL MAPPINGS #note that these are a bit meh, we might benefit from some sort of aggregteion of all ICE drives into one, PHEV drives into one,etc. within the Source data.
    # =========================
    ('Passenger road', 'LPVs', 'ICE medium'):              ("passenger", "road", "car", 'ice_g'),
    ('Passenger road', 'LPVs', 'HEV small'):               ("passenger", "road", "car", 'ice_g'),
    ('Passenger road', 'LPVs', 'PHEV large'):              ("passenger", "road", "lt", 'phev_g'),
    ('Passenger road', 'LPVs', 'HEV large'):               ("passenger", "road", "lt", 'ice_g'),
    ('Passenger road', 'LPVs', 'PHEV medium'):             ("passenger", "road", "suv", 'phev_g'),
    ('Freight road', 'Trucks', 'ICE heavy'):               ("freight", "road", "ht", 'ice_d'),
    ('Freight road', 'LCVs', 'ICE'):                       ("freight", "road", "ice_d"),
    ('Passenger road', 'LPVs', 'BEV small'):               ("passenger", "road", "car", 'bev'),
    ('Freight road', 'Trucks', 'FCEV medium'):             ("freight", "road", "mt", 'fcev'),
    ('Freight road', 'Trucks', 'BEV heavy'):               ("freight", "road", "ht", 'bev'),
    ('Passenger road', 'LPVs', 'PHEV small'):              ("passenger", "road", "car", 'phev_g'),
    ('Passenger road', 'Buses', 'ICE'):                    ("passenger", "road", "bus", 'ice_g'),
    ('Freight road', 'LCVs', 'PHEV'):                      ("freight", "road", "lcv", 'phev_g'),
    ('Passenger road', 'LPVs', 'HEV medium'):              ("passenger", "road", "suv", 'hev_g'),
    ('Freight road', 'Trucks', 'ICE medium'):              ("freight", "road", "mt", 'ice_d'),
    ('Passenger road', 'LPVs', 'BEV large'):               ("passenger", "road", "lt", 'bev'),
    ('Passenger road', 'LPVs', 'ICE small'):               ("passenger", "road", "car", 'ice_g'),
    ('Passenger road', 'Motorcycles', 'ICE'):              ("passenger", "road", "2w", 'ice_g'),
    ('Passenger road', 'LPVs', 'BEV medium'):              ("passenger", "road", "suv", 'bev'),
    ('Freight road', 'LCVs', 'BEV'):                       ("freight", "road", "lcv", 'bev'),
    ('Passenger road', 'Buses', 'FCEV'):                   ("passenger", "road", "bus", 'fcev'),
    ('Passenger road', 'Buses', 'BEV'):                    ("passenger", "road", "bus", 'bev'),
    ('Freight road', 'Trucks', 'EREV medium'):             ("freight", "road", "mt", 'erev'),
    ('Freight road', 'Trucks', 'FCEV heavy'):              ("freight", "road", "ht", 'fcev'),
    ('Freight road', 'Trucks', 'BEV medium'):              ("freight", "road", "mt", 'bev'),
    ('Passenger road', 'Motorcycles', 'BEV'):              ("passenger", "road", "2w", 'bev'),
    ('Freight road', 'Trucks', 'EREV heavy'):              ("freight", "road", "ht", 'erev'),
    ('Passenger road', 'LPVs', 'ICE large'):               ("passenger", "road", "lt", 'ice_g'),

    # =========================
    # Other AGGREGATE MAPPINGS
    # =========================
    ("Passenger road",):                                  ("passenger", "road"),
    ("Freight road",):                                    ("freight", "road"),
    
    ("Passenger non road",):                              ("passenger", "non road"),
    ("Freight non road",):                                ("freight", "non road"),
    
    ("Passenger road","LPVs"):                            ("passenger", "road", "car"),
    ("Passenger road","Buses"):                           ("passenger", "road", "bus"),
    ("Passenger road","Motorcycles"):                     ("passenger", "road", "2w"),
    ("Freight road","Trucks"):                            ("freight", "road","ht"),
    ("Freight road","LCVs"):                              ("freight", "road", "lcv"),
    
    ("Passenger non road", "Air"):                       ("passenger", "air"),
    ("Passenger non road","Rail"):                      ("passenger", "rail"),
    ("Passenger non road","Shipping"):                  ("passenger", "ship"),
    ("Freight non road","Air"):                         ("freight", "air"),
    ("Freight non road","Rail"):                        ("freight", "rail"),
    ("Freight non road","Shipping"):                    ("freight", "ship"),
    
    
    # =========================
    # Others Levels 1 and 2
    # =========================
    ("Nonspecified transport", "Kerosene"): None,
    ("Nonspecified transport", "Fuel oil"): None,
    ("Nonspecified transport", "Diesel"): None,
    ("Nonspecified transport", "LPG"): None,
    ("Nonspecified transport", "Gasoline"): None,
    ("Nonspecified transport", "Coal products"): None,
    ("Nonspecified transport", "Other petroleum products"): None,
    ("Pipeline transport", "Fuel oil"): None,
    ("Pipeline transport", "Diesel"): None,
    ("Pipeline transport", "Natural gas"): None,
    ("Pipeline transport", "Electricity"): None,
    ("Nonspecified transport",): None,
    ('Pipeline transport',): None
}

ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP = {

    # ------------------------------------------------------------
    # 15_01_domestic_air_transport → Passenger non-road Air
    # ------------------------------------------------------------
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Passenger non road", "Air", "Gasoline")
    ],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_02_aviation_gasoline"): [
        ("Passenger non road", "Air", "Aviation gasoline")
    ],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger non road", "Air", "Diesel")
    ],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_09_lpg"): [
        ("Passenger non road", "Air", "LPG")
    ],
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_x_jet_fuel"): [
        ("Passenger non road", "Air", "Jet fuel")
    ],

    # ------------------------------------------------------------
    # 15_02_road → Passenger road + Freight road
    # ------------------------------------------------------------
    ("15_02_road", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Passenger road","LPVs","ICE small","Gasoline"),
        ("Passenger road","LPVs","ICE medium","Gasoline"),
        ("Passenger road","LPVs","ICE large","Gasoline"),
        ("Passenger road","Motorcycles","ICE","Gasoline"),
        ("Freight road","LCVs","ICE","Gasoline")
    ],
    ("15_02_road", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_02_road", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger road","LPVs","ICE small","Diesel"),
        ("Passenger road","LPVs","ICE medium","Diesel"),
        ("Passenger road","LPVs","ICE large","Diesel"),
        ("Passenger road","Buses","ICE","Diesel"),
        ("Freight road","LCVs","ICE","Diesel"),
        ("Freight road","Trucks","ICE medium","Diesel"),
        ("Freight road","Trucks","ICE heavy","Diesel")
    ],
    ("15_02_road", "07_petroleum_products", "07_08_fuel_oil"): [("Nonspecified transport", "Fuel oil")],
    ("15_02_road", "07_petroleum_products", "07_09_lpg"): [
        ("Passenger road","LPVs","ICE medium","LPG"),
        ("Passenger road","LPVs","ICE large","LPG"),
        ("Passenger road","Buses","ICE","LPG")
    ],
    ("15_02_road", "08_gas", "08_01_natural_gas"): [
        ("Passenger road","LPVs","ICE medium","CNG"),
        ("Passenger road","LPVs","ICE large","CNG"),
        ("Passenger road","Buses","ICE","CNG"),
        ("Freight road","LCVs","ICE","CNG"),
        ("Freight road","Trucks","ICE medium","CNG"),
        ("Freight road","Trucks","ICE heavy","CNG")
    ],
    ("15_02_road", "16_others", "16_05_biogasoline"): [
        ("Passenger road","LPVs","ICE medium","Biogasoline"),
        ("Passenger road","LPVs","ICE large","Biogasoline")
    ],
    ("15_02_road", "16_others", "16_06_biodiesel"): [
        ("Passenger road","Buses","ICE","Biodiesel"),
        ("Freight road","LCVs","ICE","Biodiesel"),
        ("Freight road","Trucks","ICE heavy","Biodiesel")
    ],
    ("15_02_road", "17_electricity", "x"): [
        ("Passenger road","LPVs","BEV small","Electricity"),
        ("Passenger road","LPVs","BEV medium","Electricity"),
        ("Passenger road","LPVs","BEV large","Electricity"),
        ("Passenger road","Buses","BEV","Electricity"),
        ("Passenger road","Motorcycles","BEV","Electricity"),
        ("Freight road","LCVs","BEV","Electricity"),
        ("Freight road","Trucks","BEV heavy","Electricity")
    ],

    # ------------------------------------------------------------
    # 15_03_rail → Passenger/Freight non-road Rail
    # ------------------------------------------------------------
    ("15_03_rail", "01_coal", "01_x_thermal_coal"): [
        ("Passenger non road","Rail","Coal"),
        ("Freight non road","Rail","Coal")
    ],
    ("15_03_rail", "02_coal_products", "x"): [
        ("Nonspecified transport", "Coal products"),
    ],
    ("15_03_rail", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Nonspecified transport", "Gasoline"),
    ],
    ("15_03_rail", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_03_rail", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger non road","Rail","Diesel"),
        ("Freight non road","Rail","Diesel")
    ],
    ("15_03_rail", "07_petroleum_products", "07_08_fuel_oil"): [
        ("Nonspecified transport", "Fuel oil")
    ],
    ("15_03_rail", "07_petroleum_products", "07_09_lpg"): [
        ("Nonspecified transport", "LPG")
    ],
    ("15_03_rail", "16_others", "16_06_biodiesel"): [
        ("Passenger non road","Rail","Biodiesel"),
        ("Freight non road","Rail","Biodiesel")
    ],
    ("15_03_rail", "17_electricity", "x"): [
        ("Passenger non road","Rail","Electricity"),
        ("Freight non road","Rail","Electricity")
    ],

    # ------------------------------------------------------------
    # 15_04_domestic_navigation → Passenger/Freight non-road Shipping
    # ------------------------------------------------------------
    ("15_04_domestic_navigation", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Passenger non road","Shipping","Gasoline"),
        ("Freight non road","Shipping","Gasoline")
    ],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_06_kerosene"): [("Nonspecified transport", "Kerosene")],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger non road","Shipping","Diesel"),
        ("Freight non road","Shipping","Diesel")
    ],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_08_fuel_oil"): [
        ("Passenger non road","Shipping","Fuel oil"),
        ("Freight non road","Shipping","Fuel oil")
    ],
    ("15_04_domestic_navigation", "07_petroleum_products", "07_09_lpg"): [
        ("Nonspecified transport", "LPG")
    ],
    ("15_04_domestic_navigation", "08_gas", "08_01_natural_gas"): [
        ("Passenger non road","Shipping","LNG"),
        ("Freight non road","Shipping","LNG")
    ],
    ("15_04_domestic_navigation", "16_others", "16_06_biodiesel"): [
        ("Passenger non road","Shipping","Biodiesel"),
        ("Freight non road","Shipping","Biodiesel")
    ],
    ("15_04_domestic_navigation", "17_electricity", "x"): [
        ("Passenger non road","Shipping","Electricity"),
        ("Freight non road","Shipping","Electricity")
    ],

    # ------------------------------------------------------------
    # 15_05_pipeline_transport → lump into Nonspecified
    # ------------------------------------------------------------
    ("15_05_pipeline_transport", "07_petroleum_products", "07_01_motor_gasoline"): [("Nonspecified transport", "Gasoline")],
    ("15_05_pipeline_transport", "07_petroleum_products", "07_07_gas_diesel_oil"): [("Pipeline transport", "Diesel")],
    ("15_05_pipeline_transport", "07_petroleum_products", "07_08_fuel_oil"): [("Pipeline transport", "Fuel oil")],
    ("15_05_pipeline_transport", "07_petroleum_products", "07_09_lpg"): [("Nonspecified transport", "LPG")],
    ("15_05_pipeline_transport", "08_gas", "08_01_natural_gas"): [("Pipeline transport", "Natural gas")],
    ("15_05_pipeline_transport", "17_electricity", "x"): [("Pipeline transport", "Electricity")],

    # ------------------------------------------------------------
    # 15_06_nonspecified_transport → direct mapping to Nonspecified
    # ------------------------------------------------------------
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_01_motor_gasoline"): [("Nonspecified transport", "Gasoline")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_07_gas_diesel_oil"): [("Nonspecified transport", "Diesel")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_08_fuel_oil"): [("Nonspecified transport", "Fuel oil")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_09_lpg"): [("Nonspecified transport", "LPG")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_x_jet_fuel"): [("Nonspecified transport", "Kerosene")],
    ("15_06_nonspecified_transport", "07_petroleum_products", "07_x_other_petroleum_products"): [("Nonspecified transport", "Other petroleum products")],
    ("15_06_nonspecified_transport", "08_gas", "08_01_natural_gas"): [("Nonspecified transport", "Gasoline")],
    ("15_06_nonspecified_transport", "17_electricity", "x"): [("Nonspecified transport", "Gasoline")],
}
#%%


# ============================================================
# LEAP_MEASURE_CONFIG
# Final version — hierarchical, cleaned, and LEAP-ready
# ============================================================
# ============================================================
# LEAP_MEASURE_CONFIG — Final structured version
# ============================================================

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
            "source_mapping": "Activity",
            "factor": 1,
            "unit": "Passenger_km_or_freight_tonne_km",
            "LEAP_units": "Passenger-km$Tonne-km",
            "LEAP_Scale": None,
            "LEAP_Per": None
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
            "source_mapping": "Activity",
            "factor": 1,
            "unit": "Passenger_km_or_freight_tonne_km",
            "LEAP_units": "Passenger-km$Tonne-km",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Final Energy Intensity": {
            "source_mapping": "Efficiency",
            "factor": 1e-9,
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
        "Activity Level": {
            "source_mapping": "Activity",
            "factor": 1,
            "unit": "Billion_passenger_km_or_freight_tonne_km",
            "LEAP_units": "Passenger-km$Tonne-km",#ISSUE: how will we find this and match it up with the (assumed) calculations for vehicle km based on mileage and stocks in the rest of the branch? LEAP doesnt seem to have an occupancy or load variable for transport type level? Perhaps i will have to calculate this outside of leap and ensure that the sum of vehicle km from the vehicle type level matches the activity level at the transport type level?
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
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
            "source_mapping": "Vehicle_sales_share",
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
            "factor": 0.1,
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
            "factor": 0.1,
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
    'Others (level 1)': {
        "Activity Level": {
            "source_mapping": None,
            "factor": 1,
            "unit": "Unspecified Unit",
            "LEAP_units": "Unspecified Unit",
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
    },
    'Others (level 2)': {
        "Activity Level": {
            "source_mapping": None,
            "factor": 1,
            "unit": "Unspecified Unit",
            "LEAP_units": "Unspecified Unit",
            "LEAP_Scale": None,
            "LEAP_Per": None
        },
        "Final Energy Intensity": {
            "source_mapping": None,
            "factor": 1,
            "unit": "Unspecified Unit",
            "LEAP_units": "Unspecified Unit",
            "LEAP_Scale": None,
            "LEAP_Per": None
        }
    }
}
