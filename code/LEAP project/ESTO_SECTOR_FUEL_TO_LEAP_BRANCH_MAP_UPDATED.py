# ============================================================
# ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP (Updated)
# ============================================================
# Auto-generated on 2025-10-23
# This file extends ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP with valid unmapped LEAP branches
# identified during validation. Original key ordering and comments are preserved.
# Skipped (unmappable) branches are listed at the end for manual handling.
# ============================================================

ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP = {

    # ------------------------------------------------------------
    # 15_01_domestic_air_transport → Passenger non-road Air
    # ------------------------------------------------------------
    ("15_01_domestic_air_transport", "07_petroleum_products", "07_x_jet_fuel"): [
        ("Passenger non road", "Air", "Jet fuel"),
        ("Freight non road", "Air", "Jet fuel"),  # Added auto-mapped
    ],

    # ------------------------------------------------------------
    # 15_02_road → Passenger road + Freight road
    # ------------------------------------------------------------
    ("15_02_road", "07_petroleum_products", "07_01_motor_gasoline"): [
        ("Passenger road","LPVs","ICE small","Gasoline"),
        ("Passenger road","LPVs","ICE medium","Gasoline"),
        ("Passenger road","LPVs","ICE large","Gasoline"),
        ("Passenger road","Motorcycles","ICE","Gasoline"),
        ("Freight road","LCVs","ICE","Gasoline"),
        ("Passenger road","LPVs","PHEV medium","Gasoline"),  # Added auto-mapped
    ],
    ("15_02_road", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger road","LPVs","ICE small","Diesel"),
        ("Passenger road","LPVs","ICE medium","Diesel"),
        ("Passenger road","LPVs","ICE large","Diesel"),
        ("Passenger road","Buses","ICE","Diesel"),
        ("Freight road","LCVs","ICE","Diesel"),
        ("Freight road","Trucks","ICE medium","Diesel"),
        ("Freight road","Trucks","ICE heavy","Diesel"),
    ],
    ("15_02_road", "17_electricity", "x"): [
        ("Passenger road","LPVs","BEV small","Electricity"),
        ("Passenger road","LPVs","BEV medium","Electricity"),
        ("Passenger road","LPVs","BEV large","Electricity"),
        ("Passenger road","Buses","BEV","Electricity"),
        ("Passenger road","Motorcycles","BEV","Electricity"),
        ("Freight road","LCVs","BEV","Electricity"),
        ("Freight road","Trucks","BEV heavy","Electricity"),
    ],

    # ------------------------------------------------------------
    # 15_03_rail → Passenger/Freight non-road Rail
    # ------------------------------------------------------------
    ("15_03_rail", "07_petroleum_products", "07_07_gas_diesel_oil"): [
        ("Passenger non road","Rail","Diesel"),
        ("Freight non road","Rail","Diesel"),
    ],
    ("15_03_rail", "17_electricity", "x"): [
        ("Passenger non road","Rail","Electricity"),
        ("Freight non road","Rail","Electricity"),
    ],

    # ------------------------------------------------------------
    # 15_04_domestic_navigation → Passenger/Freight non-road Shipping
    # ------------------------------------------------------------
    ("15_04_domestic_navigation", "07_petroleum_products", "07_08_fuel_oil"): [
        ("Passenger non road","Shipping","Fuel oil"),
        ("Freight non road","Shipping","Fuel oil"),
    ],
    ("15_04_domestic_navigation", "08_gas", "08_01_natural_gas"): [
        ("Passenger non road","Shipping","LNG"),
        ("Freight non road","Shipping","LNG"),
    ],
    ("15_04_domestic_navigation", "17_electricity", "x"): [
        ("Passenger non road","Shipping","Electricity"),
        ("Freight non road","Shipping","Electricity"),
    ],
}


# ============================================================
# ⚠️ Skipped (manual mapping required)
# ============================================================
#   ('Freight road', 'LCVs', 'PHEV')
#   ('Passenger road', 'Motorcycles', 'ICE')
#   ('Passenger road', 'LPVs', 'HEV large')
#   ('Passenger road', 'LPVs', 'HEV medium', 'Ethanol')
#   ('Freight road', 'Trucks', 'ICE heavy')
#   ('Passenger road', 'Buses', 'BEV')
#   ('Freight non road', 'Shipping', 'Ammonia')
#   ('Freight road', 'Trucks', 'FCEV heavy', 'Hydrogen')
