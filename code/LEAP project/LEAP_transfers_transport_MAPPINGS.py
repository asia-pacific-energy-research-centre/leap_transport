CSV_TREE = {
  "freight": {
    "air": {
      "all": [
        "air_av_gas",
        "air_diesel",
        "air_electric",
        "air_fuel_oil",
        "air_gasoline",
        "air_hydrogen",
        "air_jet_fuel",
        "air_kerosene",
        "air_lpg"
      ]
    },
    "rail": {
      "all": [
        "rail_coal",
        "rail_diesel",
        "rail_electricity",
        "rail_fuel_oil",
        "rail_gasoline",
        "rail_kerosene",
        "rail_lpg",
        "rail_natural_gas"
      ]
    },
    "road": {
      "ht": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lng",
        "lpg",
        "phev_d",
        "phev_g"
      ],
      "lcv": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lpg",
        "phev_d",
        "phev_g"
      ],
      "mt": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lng",
        "lpg",
        "phev_d",
        "phev_g"
      ]
    },
    "ship": {
      "all": [
        "ship_ammonia",
        "ship_diesel",
        "ship_electric",
        "ship_fuel_oil",
        "ship_gasoline",
        "ship_hydrogen",
        "ship_kerosene",
        "ship_lng",
        "ship_lpg",
        "ship_natural_gas"
      ]
    }
  },
  "passenger": {
    "air": {
      "all": [
        "air_av_gas",
        "air_diesel",
        "air_electric",
        "air_fuel_oil",
        "air_gasoline",
        "air_hydrogen",
        "air_jet_fuel",
        "air_kerosene",
        "air_lpg"
      ]
    },
    "rail": {
      "all": [
        "rail_coal",
        "rail_diesel",
        "rail_electricity",
        "rail_fuel_oil",
        "rail_gasoline",
        "rail_kerosene",
        "rail_lpg",
        "rail_natural_gas"
      ]
    },
    "road": {
      "2w": [
        "bev",
        "ice_d",
        "ice_g"
      ],
      "bus": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lpg",
        "phev_d",
        "phev_g"
      ],
      "car": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lpg",
        "phev_d",
        "phev_g"
      ],
      "lt": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lpg",
        "phev_d",
        "phev_g"
      ],
      "suv": [
        "bev",
        "cng",
        "fcev",
        "ice_d",
        "ice_g",
        "lpg",
        "phev_d",
        "phev_g"
      ]
    },
    "ship": {
      "all": [
        "ship_ammonia",
        "ship_diesel",
        "ship_electric",
        "ship_fuel_oil",
        "ship_gasoline",
        "ship_hydrogen",
        "ship_kerosene",
        "ship_lng",
        "ship_lpg",
        "ship_natural_gas"
      ]
    }
  }
}
LEAP_STRUCTURE_PASSENGER = {
    "Air": ["Hydrogen", "Electricity", "Jet fuel", "Aviation gasoline"],
    "Rail": ["Electricity", "Diesel", "Hydrogen", "Coal"],
    "Shipping": ["Electricity", "Hydrogen", "Diesel", "Fuel oil", "LNG", "Gasoline", "Ammonia"],
    "Road": {
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
        "Buses": {
            "BEV": ["Electricity"],
            "ICE": ["Diesel", "Gasoline", "LPG", "CNG", "Biogas"],
            "FCEV": ["Hydrogen"],
        },
        "Motorcycles": {
            "BEV": ["Electricity"],
            "ICE": ["Gasoline", "Diesel", "Ethanol"],
        },
    },
}

LEAP_STRUCTURE_FREIGHT = {
    "Air": ["Hydrogen", "Electricity", "Jet fuel", "Aviation gasoline"],
    "Rail": ["Electricity", "Diesel", "Hydrogen", "Coal"],
    "Shipping": ["Electricity", "Hydrogen", "Diesel", "Fuel oil", "LNG", "Gasoline", "Ammonia"],
    "Road": {
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
}

LEAP_STRUCTURE_NONSPECIFIED = {
    "Nonspecified": [
        "Kerosene",
        "Fuel oil",
        "Diesel",
        "LPG",
        "Gasoline",
        "Coal products",
        "Other petroleum products",
    ]
}
LEAP_STRUCTURE = {
    "Passenger": LEAP_STRUCTURE_PASSENGER,
    "Freight": LEAP_STRUCTURE_FREIGHT,
    "Non-specified": LEAP_STRUCTURE_NONSPECIFIED,
}

LEAP_TO_SOURCE_MAP = {
    #tuples map to: LEAP: (Transport Type, Medium, Vehicle Type, Drive, Fuel) : (Transport Type, Medium, Vehicle Type, Drive)
    #but for non road medums, the LEAP tuple omits vehicle type and drive since they dont apply (and in the source data are just "all" and "all")
    # =========================
    # NON-ROAD: PASSENGER
    # =========================
    ("Passenger", "Air", "Hydrogen"):        ("passenger", "air",  "all", "air_hydrogen"),
    ("Passenger", "Air", "Electricity"):     ("passenger", "air",  "all", "air_electric"),
    ("Passenger", "Air", "Jet fuel"):        ("passenger", "air",  "all", "air_jet_fuel"),
    ("Passenger", "Air", "Aviation gasoline"): ("passenger", "air", "all", "air_av_gas"),

    ("Passenger", "Rail", "Electricity"):    ("passenger", "rail", "all", "rail_electricity"),
    ("Passenger", "Rail", "Diesel"):         ("passenger", "rail", "all", "rail_diesel"),
    ("Passenger", "Rail", "Hydrogen"):       ("passenger", "rail", "all", "rail_electricity"),  # proxy
    ("Passenger", "Rail", "Coal"):           ("passenger", "rail", "all", "rail_coal"),

    ("Passenger", "Shipping", "Electricity"):("passenger", "ship", "all", "ship_electric"),
    ("Passenger", "Shipping", "Hydrogen"):   ("passenger", "ship", "all", "ship_hydrogen"),
    ("Passenger", "Shipping", "Diesel"):     ("passenger", "ship", "all", "ship_diesel"),
    ("Passenger", "Shipping", "Fuel oil"):   ("passenger", "ship", "all", "ship_fuel_oil"),
    ("Passenger", "Shipping", "LNG"):        ("passenger", "ship", "all", "ship_lng"),
    ("Passenger", "Shipping", "Gasoline"):   ("passenger", "ship", "all", "ship_gasoline"),
    ("Passenger", "Shipping", "Ammonia"):    ("passenger", "ship", "all", "ship_ammonia"),

    # =========================
    # NON-ROAD: FREIGHT
    # =========================
    ("Freight", "Air", "Hydrogen"):          ("freight", "air",  "all", "air_hydrogen"),
    ("Freight", "Air", "Electricity"):       ("freight", "air",  "all", "air_electric"),
    ("Freight", "Air", "Jet fuel"):          ("freight", "air",  "all", "air_jet_fuel"),
    ("Freight", "Air", "Aviation gasoline"): ("freight", "air",  "all", "air_av_gas"),

    ("Freight", "Rail", "Electricity"):      ("freight", "rail", "all", "rail_electricity"),
    ("Freight", "Rail", "Diesel"):           ("freight", "rail", "all", "rail_diesel"),
    ("Freight", "Rail", "Hydrogen"):         ("freight", "rail", "all", "rail_electricity"),    # proxy
    ("Freight", "Rail", "Coal"):             ("freight", "rail", "all", "rail_coal"),

    ("Freight", "Shipping", "Electricity"):  ("freight", "ship", "all", "ship_electric"),
    ("Freight", "Shipping", "Hydrogen"):     ("freight", "ship", "all", "ship_hydrogen"),
    ("Freight", "Shipping", "Diesel"):       ("freight", "ship", "all", "ship_diesel"),
    ("Freight", "Shipping", "Fuel oil"):     ("freight", "ship", "all", "ship_fuel_oil"),
    ("Freight", "Shipping", "LNG"):          ("freight", "ship", "all", "ship_lng"),
    ("Freight", "Shipping", "Gasoline"):     ("freight", "ship", "all", "ship_gasoline"),
    ("Freight", "Shipping", "Ammonia"):      ("freight", "ship", "all", "ship_ammonia"),

    # =====================================================
    # ROAD: PASSENGER → LPVs (small→car, medium→suv, large→lt)
    # =====================================================
    # BEV
    ("Passenger","Road","LPVs","BEV small","Electricity"):   ("passenger","road","car","bev"),
    ("Passenger","Road","LPVs","BEV medium","Electricity"):  ("passenger","road","suv","bev"),
    ("Passenger","Road","LPVs","BEV large","Electricity"):   ("passenger","road","lt","bev"),

    # ICE small (Gasoline, Diesel, Ethanol)
    ("Passenger","Road","LPVs","ICE small","Gasoline"):      ("passenger","road","car","ice_g"),
    ("Passenger","Road","LPVs","ICE small","Diesel"):        ("passenger","road","car","ice_d"),
    ("Passenger","Road","LPVs","ICE small","Ethanol"):       ("passenger","road","car","ice_g"),  # proxy

    # ICE medium (+ LPG, CNG, Biogas)
    ("Passenger","Road","LPVs","ICE medium","Gasoline"):     ("passenger","road","suv","ice_g"),
    ("Passenger","Road","LPVs","ICE medium","Diesel"):       ("passenger","road","suv","ice_d"),
    ("Passenger","Road","LPVs","ICE medium","Ethanol"):      ("passenger","road","suv","ice_g"),  # proxy
    ("Passenger","Road","LPVs","ICE medium","LPG"):          ("passenger","road","suv","lpg"),
    ("Passenger","Road","LPVs","ICE medium","CNG"):          ("passenger","road","suv","cng"),
    ("Passenger","Road","LPVs","ICE medium","Biogas"):       ("passenger","road","suv","cng"),    # proxy

    # ICE large (+ LPG, CNG, Biogas)
    ("Passenger","Road","LPVs","ICE large","Gasoline"):      ("passenger","road","lt","ice_g"),
    ("Passenger","Road","LPVs","ICE large","Diesel"):        ("passenger","road","lt","ice_d"),
    ("Passenger","Road","LPVs","ICE large","Ethanol"):       ("passenger","road","lt","ice_g"),   # proxy
    ("Passenger","Road","LPVs","ICE large","LPG"):           ("passenger","road","lt","lpg"),
    ("Passenger","Road","LPVs","ICE large","CNG"):           ("passenger","road","lt","cng"),
    ("Passenger","Road","LPVs","ICE large","Biogas"):        ("passenger","road","lt","cng"),     # proxy

    # PHEV small/medium/large (Electricity + Gasoline/Diesel/Ethanol)
    ("Passenger","Road","LPVs","PHEV small","Electricity"):  ("passenger","road","car","phev_g"),
    ("Passenger","Road","LPVs","PHEV small","Gasoline"):     ("passenger","road","car","phev_g"),
    ("Passenger","Road","LPVs","PHEV small","Diesel"):       ("passenger","road","car","phev_d"),
    ("Passenger","Road","LPVs","PHEV small","Ethanol"):      ("passenger","road","car","phev_g"), # proxy

    ("Passenger","Road","LPVs","PHEV medium","Electricity"): ("passenger","road","suv","phev_g"),
    ("Passenger","Road","LPVs","PHEV medium","Gasoline"):    ("passenger","road","suv","phev_g"),
    ("Passenger","Road","LPVs","PHEV medium","Diesel"):      ("passenger","road","suv","phev_d"),
    ("Passenger","Road","LPVs","PHEV medium","Ethanol"):     ("passenger","road","suv","phev_g"), # proxy

    ("Passenger","Road","LPVs","PHEV large","Electricity"):  ("passenger","road","lt","phev_g"),
    ("Passenger","Road","LPVs","PHEV large","Gasoline"):     ("passenger","road","lt","phev_g"),
    ("Passenger","Road","LPVs","PHEV large","Diesel"):       ("passenger","road","lt","phev_d"),
    ("Passenger","Road","LPVs","PHEV large","Ethanol"):      ("passenger","road","lt","phev_g"),  # proxy

    # HEV small/medium/large (Gasoline/Diesel/Ethanol → proxied to ICE)
    ("Passenger","Road","LPVs","HEV small","Gasoline"):      ("passenger","road","car","ice_g"),
    ("Passenger","Road","LPVs","HEV small","Diesel"):        ("passenger","road","car","ice_d"),
    ("Passenger","Road","LPVs","HEV small","Ethanol"):       ("passenger","road","car","ice_g"),  # proxy

    ("Passenger","Road","LPVs","HEV medium","Gasoline"):     ("passenger","road","suv","ice_g"),
    ("Passenger","Road","LPVs","HEV medium","Diesel"):       ("passenger","road","suv","ice_d"),
    ("Passenger","Road","LPVs","HEV medium","Ethanol"):      ("passenger","road","suv","ice_g"),  # proxy

    ("Passenger","Road","LPVs","HEV large","Gasoline"):      ("passenger","road","lt","ice_g"),
    ("Passenger","Road","LPVs","HEV large","Diesel"):        ("passenger","road","lt","ice_d"),
    ("Passenger","Road","LPVs","HEV large","Ethanol"):       ("passenger","road","lt","ice_g"),   # proxy

    # =========================
    # ROAD: PASSENGER → Buses
    # =========================
    ("Passenger","Road","Buses","BEV","Electricity"):        ("passenger","road","bus","bev"),
    ("Passenger","Road","Buses","ICE","Diesel"):             ("passenger","road","bus","ice_d"),
    ("Passenger","Road","Buses","ICE","Gasoline"):           ("passenger","road","bus","ice_g"),
    ("Passenger","Road","Buses","ICE","LPG"):                ("passenger","road","bus","lpg"),
    ("Passenger","Road","Buses","ICE","CNG"):                ("passenger","road","bus","cng"),
    ("Passenger","Road","Buses","ICE","Biogas"):             ("passenger","road","bus","cng"),     # proxy
    ("Passenger","Road","Buses","FCEV","Hydrogen"):          ("passenger","road","bus","fcev"),

    # =========================
    # ROAD: PASSENGER → Motorcycles
    # =========================
    ("Passenger","Road","Motorcycles","ICE","Gasoline"):     ("passenger","road","2w","ice_g"),
    ("Passenger","Road","Motorcycles","ICE","Diesel"):       ("passenger","road","2w","ice_d"),
    ("Passenger","Road","Motorcycles","ICE","Ethanol"):      ("passenger","road","2w","ice_g"),    # proxy
    ("Passenger","Road","Motorcycles","BEV","Electricity"):  ("passenger","road","2w","bev"),

    # =====================================
    # ROAD: FREIGHT → Trucks (medium/heavy)
    # =====================================
    # ICE heavy
    ("Freight","Road","Trucks","ICE heavy","Gasoline"):      ("freight","road","ht","ice_g"),
    ("Freight","Road","Trucks","ICE heavy","Diesel"):        ("freight","road","ht","ice_d"),
    ("Freight","Road","Trucks","ICE heavy","Ethanol"):       ("freight","road","ht","ice_g"),      # proxy
    ("Freight","Road","Trucks","ICE heavy","LPG"):           ("freight","road","ht","lpg"),
    ("Freight","Road","Trucks","ICE heavy","CNG"):           ("freight","road","ht","cng"),
    ("Freight","Road","Trucks","ICE heavy","LNG"):           ("freight","road","ht","lng"),
    ("Freight","Road","Trucks","ICE heavy","Biogas"):        ("freight","road","ht","cng"),        # proxy

    # ICE medium
    ("Freight","Road","Trucks","ICE medium","Gasoline"):     ("freight","road","mt","ice_g"),
    ("Freight","Road","Trucks","ICE medium","Diesel"):       ("freight","road","mt","ice_d"),
    ("Freight","Road","Trucks","ICE medium","Ethanol"):      ("freight","road","mt","ice_g"),      # proxy
    ("Freight","Road","Trucks","ICE medium","LNG"):          ("freight","road","mt","lng"),
    ("Freight","Road","Trucks","ICE medium","CNG"):          ("freight","road","mt","cng"),
    ("Freight","Road","Trucks","ICE medium","LPG"):          ("freight","road","mt","lpg"),
    ("Freight","Road","Trucks","ICE medium","Biogas"):       ("freight","road","mt","cng"),        # proxy

    # BEV
    ("Freight","Road","Trucks","BEV heavy","Electricity"):   ("freight","road","ht","bev"),
    ("Freight","Road","Trucks","BEV medium","Electricity"):  ("freight","road","mt","bev"),

    # EREV (map to PHEV)
    ("Freight","Road","Trucks","EREV medium","Gasoline"):    ("freight","road","mt","phev_g"),
    ("Freight","Road","Trucks","EREV medium","Electricity"): ("freight","road","mt","phev_g"),     # electric portion
    ("Freight","Road","Trucks","EREV medium","Diesel"):      ("freight","road","mt","phev_d"),
    ("Freight","Road","Trucks","EREV medium","Ethanol"):     ("freight","road","mt","phev_g"),     # proxy

    ("Freight","Road","Trucks","EREV heavy","Gasoline"):     ("freight","road","ht","phev_g"),
    ("Freight","Road","Trucks","EREV heavy","Electricity"):  ("freight","road","ht","phev_g"),
    ("Freight","Road","Trucks","EREV heavy","Diesel"):       ("freight","road","ht","phev_d"),
    ("Freight","Road","Trucks","EREV heavy","Ethanol"):      ("freight","road","ht","phev_g"),     # proxy

    # FCEV
    ("Freight","Road","Trucks","FCEV heavy","Hydrogen"):     ("freight","road","ht","fcev"),
    ("Freight","Road","Trucks","FCEV medium","Hydrogen"):    ("freight","road","mt","fcev"),

    # =========================
    # ROAD: FREIGHT → LCVs
    # =========================
    ("Freight","Road","LCVs","ICE","Gasoline"):              ("freight","road","lcv","ice_g"),
    ("Freight","Road","LCVs","ICE","Diesel"):                ("freight","road","lcv","ice_d"),
    ("Freight","Road","LCVs","ICE","Ethanol"):               ("freight","road","lcv","ice_g"),     # proxy
    ("Freight","Road","LCVs","ICE","CNG"):                   ("freight","road","lcv","cng"),
    ("Freight","Road","LCVs","ICE","LPG"):                   ("freight","road","lcv","lpg"),
    ("Freight","Road","LCVs","ICE","Biogas"):                ("freight","road","lcv","cng"),       # proxy

    ("Freight","Road","LCVs","BEV","Electricity"):           ("freight","road","lcv","bev"),

    ("Freight","Road","LCVs","PHEV","Electricity"):          ("freight","road","lcv","phev_g"),
    ("Freight","Road","LCVs","PHEV","Gasoline"):             ("freight","road","lcv","phev_g"),
    ("Freight","Road","LCVs","PHEV","Diesel"):               ("freight","road","lcv","phev_d"),
    ("Freight","Road","LCVs","PHEV","Ethanol"):              ("freight","road","lcv","phev_g"),    # proxy
}

# ------------------------------------------------------------