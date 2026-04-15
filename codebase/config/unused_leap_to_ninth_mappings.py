
# #these are unsued but likely to be useful at some point in time. 
# #they map from the overly detailed transprot categories in the ninth edition to the more general categories in the leap.
# #note that the ninth categories follow a levelling system where the first two digits are the main category, the next two are the subcategory, the next two are the subsubcategory, the next two are the subsubsubcategory, and the last two are the specific technology.

#%%
mapping_tuple = {
    '15_01_01_passenger': ('Passenger non road', 'Air'),
    '15_01_02_freight': ('Freight non road', 'Air'),
    '15_01_domestic_air_transport': ('Transport non road', 'Air'),
    '15_02_01_01_01': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_02': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_03': ('Passenger road', 'Motorcycles', 'BEV'),
    '15_02_01_01_04': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_05': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_06': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_07': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_08': ('Passenger road', 'Motorcycles', 'BEV'),
    '15_02_01_01_09': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_01_two_wheeler': ('Passenger road', 'Motorcycles', 'ICE'),
    '15_02_01_02_01': ('Passenger road', 'LPVs', 'ICE medium'),
    '15_02_01_02_02': ('Passenger road', 'LPVs', 'ICE medium'),
    '15_02_01_02_03': ('Passenger road', 'LPVs', 'BEV medium'),
    '15_02_01_02_04': ('Passenger road', 'LPVs', 'ICE medium'),
    '15_02_01_02_05': ('Passenger road', 'LPVs', 'PHEV medium'),
    '15_02_01_02_06': ('Passenger road', 'LPVs', 'PHEV medium'),
    '15_02_01_02_07': ('Passenger road', 'LPVs', 'ICE medium'),
    '15_02_01_02_08': ('Passenger road', 'LPVs', 'BEV medium'),
    '15_02_01_02_09': ('Passenger road', 'LPVs', 'ICE medium'),
    '15_02_01_02_car': ('Passenger road', 'LPVs', 'ICE medium'),
    '15_02_01_03_01': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_03_02': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_03_03': ('Passenger road', 'LPVs', 'BEV large'),
    '15_02_01_03_04': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_03_05': ('Passenger road', 'LPVs', 'PHEV large'),
    '15_02_01_03_06': ('Passenger road', 'LPVs', 'PHEV large'),
    '15_02_01_03_07': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_03_08': ('Passenger road', 'LPVs', 'BEV large'),
    '15_02_01_03_09': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_03_sports_utility_vehicle': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_04_01': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_04_02': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_04_03': ('Passenger road', 'LPVs', 'BEV large'),
    '15_02_01_04_04': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_04_05': ('Passenger road', 'LPVs', 'PHEV large'),
    '15_02_01_04_06': ('Passenger road', 'LPVs', 'PHEV large'),
    '15_02_01_04_07': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_04_08': ('Passenger road', 'LPVs', 'BEV large'),
    '15_02_01_04_09': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_04_light_truck': ('Passenger road', 'LPVs', 'ICE large'),
    '15_02_01_05_01': ('Passenger road', 'Buses', 'ICE'),
    '15_02_01_05_02': ('Passenger road', 'Buses', 'ICE'),
    '15_02_01_05_03': ('Passenger road', 'Buses', 'BEV'),
    '15_02_01_05_04': ('Passenger road', 'Buses', 'ICE'),
    '15_02_01_05_05': ('Passenger road', 'Buses', 'PHEV'),
    '15_02_01_05_06': ('Passenger road', 'Buses', 'PHEV'),
    '15_02_01_05_07': ('Passenger road', 'Buses', 'ICE'),
    '15_02_01_05_08': ('Passenger road', 'Buses', 'FCEV'),
    '15_02_01_05_09': ('Passenger road', 'Buses', 'ICE'),
    '15_02_01_05_bus': ('Passenger road', 'Buses', 'ICE'),
    '15_02_01_passenger': ('Passenger road',),
    '15_02_02_01_01': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_01_02': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_01_03': ('Freight road', 'LCVs', 'BEV'),
    '15_02_02_01_04': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_01_05': ('Freight road', 'LCVs', 'PHEV'),
    '15_02_02_01_06': ('Freight road', 'LCVs', 'PHEV'),
    '15_02_02_01_07': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_01_08': ('Freight road', 'LCVs', 'BEV'),
    '15_02_02_01_09': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_01_two_wheeler_freight': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_02_01': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_02_02': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_02_03': ('Freight road', 'LCVs', 'BEV'),
    '15_02_02_02_04': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_02_05': ('Freight road', 'LCVs', 'PHEV'),
    '15_02_02_02_06': ('Freight road', 'LCVs', 'PHEV'),
    '15_02_02_02_07': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_02_08': ('Freight road', 'LCVs', 'BEV'),
    '15_02_02_02_09': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_02_light_commercial_vehicle': ('Freight road', 'LCVs', 'ICE'),
    '15_02_02_03_01': ('Freight road', 'Trucks', 'ICE medium'),
    '15_02_02_03_02': ('Freight road', 'Trucks', 'ICE medium'),
    '15_02_02_03_03': ('Freight road', 'Trucks', 'BEV medium'),
    '15_02_02_03_04': ('Freight road', 'Trucks', 'ICE medium'),
    '15_02_02_03_05': ('Freight road', 'Trucks', 'PHEV medium'),
    '15_02_02_03_06': ('Freight road', 'Trucks', 'PHEV medium'),
    '15_02_02_03_07': ('Freight road', 'Trucks', 'ICE medium'),
    '15_02_02_03_08': ('Freight road', 'Trucks', 'FCEV medium'),
    '15_02_02_03_09': ('Freight road', 'Trucks', 'ICE medium'),
    '15_02_02_03_medium_truck': ('Freight road', 'Trucks', 'ICE medium'),
    '15_02_02_04_01': ('Freight road', 'Trucks', 'ICE heavy'),
    '15_02_02_04_02': ('Freight road', 'Trucks', 'ICE heavy'),
    '15_02_02_04_03': ('Freight road', 'Trucks', 'BEV heavy'),
    '15_02_02_04_04': ('Freight road', 'Trucks', 'ICE heavy'),
    '15_02_02_04_05': ('Freight road', 'Trucks', 'PHEV heavy'),
    '15_02_02_04_06': ('Freight road', 'Trucks', 'PHEV heavy'),
    '15_02_02_04_07': ('Freight road', 'Trucks', 'ICE heavy'),
    '15_02_02_04_08': ('Freight road', 'Trucks', 'FCEV heavy'),
    '15_02_02_04_09': ('Freight road', 'Trucks', 'ICE heavy'),
    '15_02_02_04_heavy_truck': ('Freight road', 'Trucks', 'ICE heavy'),
    '15_02_02_freight': ('Freight road',),
    '15_02_road': ('Road',),
    '15_03_01_passenger': ('Passenger non road', 'Rail'),
    '15_03_02_freight': ('Freight non road', 'Rail'),
    '15_03_rail': ('Transport non road', 'Rail'),
    '15_04_01_passenger': ('Passenger non road', 'Shipping'),
    '15_04_02_freight': ('Freight non road', 'Shipping'),
    '15_04_domestic_navigation': ('Transport non road', 'Shipping'),
    '15_05_pipeline_transport': ('Pipeline transport',),
    '15_06_nonspecified_transport': ('Nonspecified transport',),
}

#%%
#%%
leap_to_ninth_lowest_named = {
    'BEV small': (),
    'EREV': (),
    'EREV heavy': (),
    'EREV large': (),
    'EREV medium': (),
    'EREV small': (),
    'HEV large': (),
    'HEV medium': (),
    'HEV small': (),
    'ICE small': (),
    'PHEV small': (),
    'Air': (
        '15_01_01_passenger',
        '15_01_02_freight',
    ),
    'Freight non road': (
        '15_01_02_freight',
        '15_03_02_freight',
        '15_04_02_freight',
    ),
    'International transport': (
        '04_international_marine_bunkers',
        '05_international_aviation_bunkers',
    ),
    'Nonspecified transport': (
        '15_06_nonspecified_transport',
    ),
    'Passenger non road': (
        '15_01_01_passenger',
        '15_03_01_passenger',
        '15_04_01_passenger',
    ),
    'Pipeline transport': (
        '15_05_pipeline_transport',
    ),
    'Rail': (
        '15_03_01_passenger',
        '15_03_02_freight',
    ),
    'Road': (
        '15_02_01_passenger',
        '15_02_02_freight',
    ),
    'Shipping': (
        '15_04_01_passenger',
        '15_04_02_freight',
    ),
    'Transport non road': (
        '15_01_01_passenger',
        '15_01_02_freight',
        '15_03_01_passenger',
        '15_03_02_freight',
        '15_04_01_passenger',
        '15_04_02_freight',
        '15_05_pipeline_transport',
        '15_06_nonspecified_transport',
    ),
    'BEV': (
        '15_02_01_01_03_battery_ev',
        '15_02_01_02_03_battery_ev',
        '15_02_01_03_03_battery_ev',
        '15_02_01_04_03_battery_ev',
        '15_02_01_05_03_battery_ev',
        '15_02_02_01_03_battery_ev',
        '15_02_02_02_03_battery_ev',
        '15_02_02_03_03_battery_ev',
        '15_02_02_04_03_battery_ev',
    ),
    'BEV heavy': (
        '15_02_02_04_03_battery_ev',
    ),
    'BEV large': (
        '15_02_01_03_03_battery_ev',
        '15_02_01_04_03_battery_ev',
    ),
    'BEV medium': (
        '15_02_01_02_03_battery_ev',
        '15_02_02_03_03_battery_ev',
    ),
    'FCEV': (
        '15_02_01_01_08_fuel_cell_ev',
        '15_02_01_02_08_fuel_cell_ev',
        '15_02_01_03_08_fuel_cell_ev',
        '15_02_01_04_08_fuel_cell_ev',
        '15_02_01_05_08_fuel_cell_ev',
        '15_02_02_01_08_fuel_cell_ev',
        '15_02_02_02_08_fuel_cell_ev',
        '15_02_02_03_08_fuel_cell_ev',
        '15_02_02_04_08_fuel_cell_ev',
    ),
    'FCEV heavy': (
        '15_02_02_04_08_fuel_cell_ev',
    ),
    'FCEV medium': (
        '15_02_02_03_08_fuel_cell_ev',
    ),
    'ICE': (
        '15_02_01_01_01_diesel_engine',
        '15_02_01_01_02_gasoline_engine',
        '15_02_01_01_04_compressed_natual_gas',
        '15_02_01_01_07_liquified_petroleum_gas',
        '15_02_01_01_09_lng',
        '15_02_01_02_01_diesel_engine',
        '15_02_01_02_02_gasoline_engine',
        '15_02_01_02_04_compressed_natual_gas',
        '15_02_01_02_07_liquified_petroleum_gas',
        '15_02_01_02_09_lng',
        '15_02_01_03_01_diesel_engine',
        '15_02_01_03_02_gasoline_engine',
        '15_02_01_03_04_compressed_natual_gas',
        '15_02_01_03_07_liquified_petroleum_gas',
        '15_02_01_03_09_lng',
        '15_02_01_04_01_diesel_engine',
        '15_02_01_04_02_gasoline_engine',
        '15_02_01_04_04_compressed_natual_gas',
        '15_02_01_04_07_liquified_petroleum_gas',
        '15_02_01_04_09_lng',
        '15_02_01_05_01_diesel_engine',
        '15_02_01_05_02_gasoline_engine',
        '15_02_01_05_04_compressed_natual_gas',
        '15_02_01_05_07_liquified_petroleum_gas',
        '15_02_01_05_09_lng',
        '15_02_02_01_01_diesel_engine',
        '15_02_02_01_02_gasoline_engine',
        '15_02_02_01_04_compressed_natual_gas',
        '15_02_02_01_07_liquified_petroleum_gas',
        '15_02_02_01_09_lng',
        '15_02_02_02_01_diesel_engine',
        '15_02_02_02_02_gasoline_engine',
        '15_02_02_02_04_compressed_natual_gas',
        '15_02_02_02_07_liquified_petroleum_gas',
        '15_02_02_02_09_lng',
        '15_02_02_03_01_diesel_engine',
        '15_02_02_03_02_gasoline_engine',
        '15_02_02_03_04_compressed_natual_gas',
        '15_02_02_03_07_liquified_petroleum_gas',
        '15_02_02_03_09_lng',
        '15_02_02_04_01_diesel_engine',
        '15_02_02_04_02_gasoline_engine',
        '15_02_02_04_04_compressed_natual_gas',
        '15_02_02_04_07_liquified_petroleum_gas',
        '15_02_02_04_09_lng',
    ),
    'ICE heavy': (
        '15_02_02_04_01_diesel_engine',
        '15_02_02_04_02_gasoline_engine',
        '15_02_02_04_04_compressed_natual_gas',
        '15_02_02_04_07_liquified_petroleum_gas',
        '15_02_02_04_09_lng',
    ),
    'ICE large': (
        '15_02_01_03_01_diesel_engine',
        '15_02_01_03_02_gasoline_engine',
        '15_02_01_03_04_compressed_natual_gas',
        '15_02_01_03_07_liquified_petroleum_gas',
        '15_02_01_03_09_lng',
        '15_02_01_04_01_diesel_engine',
        '15_02_01_04_02_gasoline_engine',
        '15_02_01_04_04_compressed_natual_gas',
        '15_02_01_04_07_liquified_petroleum_gas',
        '15_02_01_04_09_lng',
    ),
    'ICE medium': (
        '15_02_01_02_01_diesel_engine',
        '15_02_01_02_02_gasoline_engine',
        '15_02_01_02_04_compressed_natual_gas',
        '15_02_01_02_07_liquified_petroleum_gas',
        '15_02_01_02_09_lng',
        '15_02_02_03_01_diesel_engine',
        '15_02_02_03_02_gasoline_engine',
        '15_02_02_03_04_compressed_natual_gas',
        '15_02_02_03_07_liquified_petroleum_gas',
        '15_02_02_03_09_lng',
    ),
    'Buses': (
        '15_02_01_05_01_diesel_engine',
        '15_02_01_05_02_gasoline_engine',
        '15_02_01_05_03_battery_ev',
        '15_02_01_05_04_compressed_natual_gas',
        '15_02_01_05_05_plugin_hybrid_ev_gasoline',
        '15_02_01_05_06_plugin_hybrid_ev_diesel',
        '15_02_01_05_07_liquified_petroleum_gas',
        '15_02_01_05_08_fuel_cell_ev',
        '15_02_01_05_09_lng',
    ),
    'Freight road': (
        '15_02_02_01_01_diesel_engine',
        '15_02_02_01_02_gasoline_engine',
        '15_02_02_01_03_battery_ev',
        '15_02_02_01_04_compressed_natual_gas',
        '15_02_02_01_05_plugin_hybrid_ev_gasoline',
        '15_02_02_01_06_plugin_hybrid_ev_diesel',
        '15_02_02_01_07_liquified_petroleum_gas',
        '15_02_02_01_08_fuel_cell_ev',
        '15_02_02_01_09_lng',
        '15_02_02_02_01_diesel_engine',
        '15_02_02_02_02_gasoline_engine',
        '15_02_02_02_03_battery_ev',
        '15_02_02_02_04_compressed_natual_gas',
        '15_02_02_02_05_plugin_hybrid_ev_gasoline',
        '15_02_02_02_06_plugin_hybrid_ev_diesel',
        '15_02_02_02_07_liquified_petroleum_gas',
        '15_02_02_02_08_fuel_cell_ev',
        '15_02_02_02_09_lng',
        '15_02_02_03_01_diesel_engine',
        '15_02_02_03_02_gasoline_engine',
        '15_02_02_03_03_battery_ev',
        '15_02_02_03_04_compressed_natual_gas',
        '15_02_02_03_05_plugin_hybrid_ev_gasoline',
        '15_02_02_03_06_plugin_hybrid_ev_diesel',
        '15_02_02_03_07_liquified_petroleum_gas',
        '15_02_02_03_08_fuel_cell_ev',
        '15_02_02_03_09_lng',
        '15_02_02_04_01_diesel_engine',
        '15_02_02_04_02_gasoline_engine',
        '15_02_02_04_03_battery_ev',
        '15_02_02_04_04_compressed_natual_gas',
        '15_02_02_04_05_plugin_hybrid_ev_gasoline',
        '15_02_02_04_06_plugin_hybrid_ev_diesel',
        '15_02_02_04_07_liquified_petroleum_gas',
        '15_02_02_04_08_fuel_cell_ev',
        '15_02_02_04_09_lng',
    ),
    'LCVs': (
        '15_02_02_01_01_diesel_engine',
        '15_02_02_01_02_gasoline_engine',
        '15_02_02_01_03_battery_ev',
        '15_02_02_01_04_compressed_natual_gas',
        '15_02_02_01_05_plugin_hybrid_ev_gasoline',
        '15_02_02_01_06_plugin_hybrid_ev_diesel',
        '15_02_02_01_07_liquified_petroleum_gas',
        '15_02_02_01_08_fuel_cell_ev',
        '15_02_02_01_09_lng',
        '15_02_02_02_01_diesel_engine',
        '15_02_02_02_02_gasoline_engine',
        '15_02_02_02_03_battery_ev',
        '15_02_02_02_04_compressed_natual_gas',
        '15_02_02_02_05_plugin_hybrid_ev_gasoline',
        '15_02_02_02_06_plugin_hybrid_ev_diesel',
        '15_02_02_02_07_liquified_petroleum_gas',
        '15_02_02_02_08_fuel_cell_ev',
        '15_02_02_02_09_lng',
    ),
    'LPVs': (
        '15_02_01_02_01_diesel_engine',
        '15_02_01_02_02_gasoline_engine',
        '15_02_01_02_03_battery_ev',
        '15_02_01_02_04_compressed_natual_gas',
        '15_02_01_02_05_plugin_hybrid_ev_gasoline',
        '15_02_01_02_06_plugin_hybrid_ev_diesel',
        '15_02_01_02_07_liquified_petroleum_gas',
        '15_02_01_02_08_fuel_cell_ev',
        '15_02_01_02_09_lng',
        '15_02_01_03_01_diesel_engine',
        '15_02_01_03_02_gasoline_engine',
        '15_02_01_03_03_battery_ev',
        '15_02_01_03_04_compressed_natual_gas',
        '15_02_01_03_05_plugin_hybrid_ev_gasoline',
        '15_02_01_03_06_plugin_hybrid_ev_diesel',
        '15_02_01_03_07_liquified_petroleum_gas',
        '15_02_01_03_08_fuel_cell_ev',
        '15_02_01_03_09_lng',
        '15_02_01_04_01_diesel_engine',
        '15_02_01_04_02_gasoline_engine',
        '15_02_01_04_03_battery_ev',
        '15_02_01_04_04_compressed_natual_gas',
        '15_02_01_04_05_plugin_hybrid_ev_gasoline',
        '15_02_01_04_06_plugin_hybrid_ev_diesel',
        '15_02_01_04_07_liquified_petroleum_gas',
        '15_02_01_04_08_fuel_cell_ev',
        '15_02_01_04_09_lng',
    ),
    'Motorcycles': (
        '15_02_01_01_01_diesel_engine',
        '15_02_01_01_02_gasoline_engine',
        '15_02_01_01_03_battery_ev',
        '15_02_01_01_04_compressed_natual_gas',
        '15_02_01_01_05_plugin_hybrid_ev_gasoline',
        '15_02_01_01_06_plugin_hybrid_ev_diesel',
        '15_02_01_01_07_liquified_petroleum_gas',
        '15_02_01_01_08_fuel_cell_ev',
        '15_02_01_01_09_lng',
    ),
    'PHEV': (
        '15_02_01_01_05_plugin_hybrid_ev_gasoline',
        '15_02_01_01_06_plugin_hybrid_ev_diesel',
        '15_02_01_02_05_plugin_hybrid_ev_gasoline',
        '15_02_01_02_06_plugin_hybrid_ev_diesel',
        '15_02_01_03_05_plugin_hybrid_ev_gasoline',
        '15_02_01_03_06_plugin_hybrid_ev_diesel',
        '15_02_01_04_05_plugin_hybrid_ev_gasoline',
        '15_02_01_04_06_plugin_hybrid_ev_diesel',
        '15_02_01_05_05_plugin_hybrid_ev_gasoline',
        '15_02_01_05_06_plugin_hybrid_ev_diesel',
        '15_02_02_01_05_plugin_hybrid_ev_gasoline',
        '15_02_02_01_06_plugin_hybrid_ev_diesel',
        '15_02_02_02_05_plugin_hybrid_ev_gasoline',
        '15_02_02_02_06_plugin_hybrid_ev_diesel',
        '15_02_02_03_05_plugin_hybrid_ev_gasoline',
        '15_02_02_03_06_plugin_hybrid_ev_diesel',
        '15_02_02_04_05_plugin_hybrid_ev_gasoline',
        '15_02_02_04_06_plugin_hybrid_ev_diesel',
    ),
    'PHEV heavy': (
        '15_02_02_04_05_plugin_hybrid_ev_gasoline',
        '15_02_02_04_06_plugin_hybrid_ev_diesel',
    ),
    'PHEV large': (
        '15_02_01_03_05_plugin_hybrid_ev_gasoline',
        '15_02_01_03_06_plugin_hybrid_ev_diesel',
        '15_02_01_04_05_plugin_hybrid_ev_gasoline',
        '15_02_01_04_06_plugin_hybrid_ev_diesel',
    ),
    'PHEV medium': (
        '15_02_01_02_05_plugin_hybrid_ev_gasoline',
        '15_02_01_02_06_plugin_hybrid_ev_diesel',
        '15_02_02_03_05_plugin_hybrid_ev_gasoline',
        '15_02_02_03_06_plugin_hybrid_ev_diesel',
    ),
    'Passenger road': (
        '15_02_01_01_01_diesel_engine',
        '15_02_01_01_02_gasoline_engine',
        '15_02_01_01_03_battery_ev',
        '15_02_01_01_04_compressed_natual_gas',
        '15_02_01_01_05_plugin_hybrid_ev_gasoline',
        '15_02_01_01_06_plugin_hybrid_ev_diesel',
        '15_02_01_01_07_liquified_petroleum_gas',
        '15_02_01_01_08_fuel_cell_ev',
        '15_02_01_01_09_lng',
        '15_02_01_02_01_diesel_engine',
        '15_02_01_02_02_gasoline_engine',
        '15_02_01_02_03_battery_ev',
        '15_02_01_02_04_compressed_natual_gas',
        '15_02_01_02_05_plugin_hybrid_ev_gasoline',
        '15_02_01_02_06_plugin_hybrid_ev_diesel',
        '15_02_01_02_07_liquified_petroleum_gas',
        '15_02_01_02_08_fuel_cell_ev',
        '15_02_01_02_09_lng',
        '15_02_01_03_01_diesel_engine',
        '15_02_01_03_02_gasoline_engine',
        '15_02_01_03_03_battery_ev',
        '15_02_01_03_04_compressed_natual_gas',
        '15_02_01_03_05_plugin_hybrid_ev_gasoline',
        '15_02_01_03_06_plugin_hybrid_ev_diesel',
        '15_02_01_03_07_liquified_petroleum_gas',
        '15_02_01_03_08_fuel_cell_ev',
        '15_02_01_03_09_lng',
        '15_02_01_04_01_diesel_engine',
        '15_02_01_04_02_gasoline_engine',
        '15_02_01_04_03_battery_ev',
        '15_02_01_04_04_compressed_natual_gas',
        '15_02_01_04_05_plugin_hybrid_ev_gasoline',
        '15_02_01_04_06_plugin_hybrid_ev_diesel',
        '15_02_01_04_07_liquified_petroleum_gas',
        '15_02_01_04_08_fuel_cell_ev',
        '15_02_01_04_09_lng',
        '15_02_01_05_01_diesel_engine',
        '15_02_01_05_02_gasoline_engine',
        '15_02_01_05_03_battery_ev',
        '15_02_01_05_04_compressed_natual_gas',
        '15_02_01_05_05_plugin_hybrid_ev_gasoline',
        '15_02_01_05_06_plugin_hybrid_ev_diesel',
        '15_02_01_05_07_liquified_petroleum_gas',
        '15_02_01_05_08_fuel_cell_ev',
        '15_02_01_05_09_lng',
    ),
    'Trucks': (
        '15_02_02_03_01_diesel_engine',
        '15_02_02_03_02_gasoline_engine',
        '15_02_02_03_03_battery_ev',
        '15_02_02_03_04_compressed_natual_gas',
        '15_02_02_03_05_plugin_hybrid_ev_gasoline',
        '15_02_02_03_06_plugin_hybrid_ev_diesel',
        '15_02_02_03_07_liquified_petroleum_gas',
        '15_02_02_03_08_fuel_cell_ev',
        '15_02_02_03_09_lng',
        '15_02_02_04_01_diesel_engine',
        '15_02_02_04_02_gasoline_engine',
        '15_02_02_04_03_battery_ev',
        '15_02_02_04_04_compressed_natual_gas',
        '15_02_02_04_05_plugin_hybrid_ev_gasoline',
        '15_02_02_04_06_plugin_hybrid_ev_diesel',
        '15_02_02_04_07_liquified_petroleum_gas',
        '15_02_02_04_08_fuel_cell_ev',
        '15_02_02_04_09_lng',
    ),
}


def export_leap_to_ninth_lowest_named_csv(output_path="config/leap_to_ninth_lowest_named.csv"):
    import csv
    from pathlib import Path

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(("leap_category", "ninth_code"))
        for leap_category, ninth_codes in leap_to_ninth_lowest_named.items():
            if not ninth_codes:
                writer.writerow((leap_category, ""))
                continue
            for ninth_code in ninth_codes:
                writer.writerow((leap_category, ninth_code))

    return output_path


# if __name__ == "__main__":
#     print(export_leap_to_ninth_lowest_named_csv())

#%%

#%%
#%%
# AI, please map the below 9th categories tjo the leap categories which are provided below that.
# # 9th categories:
# sub1sectors

# 15_01_domestic_air_transport
# 15_02_road
# 15_03_rail
# 15_04_domestic_navigation
# 15_05_pipeline_transport
# 15_06_nonspecified_transport

# sub2sectors

# 15_01_01_passenger
# 15_01_02_freight
# 15_02_01_passenger
# 15_02_02_freight
# 15_03_01_passenger
# 15_03_02_freight
# 15_04_01_passenger
# 15_04_02_freight

# sub3sectors

# 15_02_01_01_two_wheeler
# 15_02_01_02_car
# 15_02_01_03_sports_utility_vehicle
# 15_02_01_04_light_truck
# 15_02_01_05_bus
# 15_02_02_01_two_wheeler_freight
# 15_02_02_02_light_commercial_vehicle
# 15_02_02_03_medium_truck
# 15_02_02_04_heavy_truck
# sub4sectors

# 15_02_01_01_01_diesel_engine
# 15_02_01_01_02_gasoline_engine
# 15_02_01_01_03_battery_ev
# 15_02_01_01_04_compressed_natual_gas
# 15_02_01_01_05_plugin_hybrid_ev_gasoline
# 15_02_01_01_06_plugin_hybrid_ev_diesel
# 15_02_01_01_07_liquified_petroleum_gas
# 15_02_01_01_08_fuel_cell_ev
# 15_02_01_01_09_lng
# 15_02_01_02_01_diesel_engine
# 15_02_01_02_02_gasoline_engine
# 15_02_01_02_03_battery_ev
# 15_02_01_02_04_compressed_natual_gas
# 15_02_01_02_05_plugin_hybrid_ev_gasoline
# 15_02_01_02_06_plugin_hybrid_ev_diesel
# 15_02_01_02_07_liquified_petroleum_gas
# 15_02_01_02_08_fuel_cell_ev
# 15_02_01_02_09_lng
# 15_02_01_03_01_diesel_engine
# 15_02_01_03_02_gasoline_engine
# 15_02_01_03_03_battery_ev
# 15_02_01_03_04_compressed_natual_gas
# 15_02_01_03_05_plugin_hybrid_ev_gasoline
# 15_02_01_03_06_plugin_hybrid_ev_diesel
# 15_02_01_03_07_liquified_petroleum_gas
# 15_02_01_03_08_fuel_cell_ev
# 15_02_01_03_09_lng
# 15_02_01_04_01_diesel_engine
# 15_02_01_04_02_gasoline_engine
# 15_02_01_04_03_battery_ev
# 15_02_01_04_04_compressed_natual_gas
# 15_02_01_04_05_plugin_hybrid_ev_gasoline
# 15_02_01_04_06_plugin_hybrid_ev_diesel
# 15_02_01_04_07_liquified_petroleum_gas
# 15_02_01_04_08_fuel_cell_ev
# 15_02_01_04_09_lng
# 15_02_01_05_01_diesel_engine
# 15_02_01_05_02_gasoline_engine
# 15_02_01_05_03_battery_ev
# 15_02_01_05_04_compressed_natual_gas
# 15_02_01_05_05_plugin_hybrid_ev_gasoline
# 15_02_01_05_06_plugin_hybrid_ev_diesel
# 15_02_01_05_07_liquified_petroleum_gas
# 15_02_01_05_08_fuel_cell_ev
# 15_02_01_05_09_lng
# 15_02_02_01_01_diesel_engine
# 15_02_02_01_02_gasoline_engine
# 15_02_02_01_03_battery_ev
# 15_02_02_01_04_compressed_natual_gas
# 15_02_02_01_05_plugin_hybrid_ev_gasoline
# 15_02_02_01_06_plugin_hybrid_ev_diesel
# 15_02_02_01_07_liquified_petroleum_gas
# 15_02_02_01_08_fuel_cell_ev
# 15_02_02_01_09_lng
# 15_02_02_02_01_diesel_engine
# 15_02_02_02_02_gasoline_engine
# 15_02_02_02_03_battery_ev
# 15_02_02_02_04_compressed_natual_gas
# 15_02_02_02_05_plugin_hybrid_ev_gasoline
# 15_02_02_02_06_plugin_hybrid_ev_diesel
# 15_02_02_02_07_liquified_petroleum_gas
# 15_02_02_02_08_fuel_cell_ev
# 15_02_02_02_09_lng
# 15_02_02_03_01_diesel_engine
# 15_02_02_03_02_gasoline_engine
# 15_02_02_03_03_battery_ev
# 15_02_02_03_04_compressed_natual_gas
# 15_02_02_03_05_plugin_hybrid_ev_gasoline
# 15_02_02_03_06_plugin_hybrid_ev_diesel
# 15_02_02_03_07_liquified_petroleum_gas
# 15_02_02_03_08_fuel_cell_ev
# 15_02_02_03_09_lng
# 15_02_02_04_01_diesel_engine
# 15_02_02_04_02_gasoline_engine
# 15_02_02_04_03_battery_ev
# 15_02_02_04_04_compressed_natual_gas
# 15_02_02_04_05_plugin_hybrid_ev_gasoline
# 15_02_02_04_06_plugin_hybrid_ev_diesel
# 15_02_02_04_07_liquified_petroleum_gas
# 15_02_02_04_08_fuel_cell_ev
# 15_02_02_04_09_lng

# # leap categories:
    
# Transport non road
#   Freight non road
#     Air
#     Rail
#     Shipping
#   Passenger non road
#     Rail
#     Shipping
#     Air
#   International transport
#     Air
#     Shipping
#   Pipeline transport
#   Nonspecified transport
# Freight road
#   Trucks
#     ICE heavy
#     ICE medium
#     BEV heavy
#     BEV medium
#     EREV heavy
#     EREV medium
#     FCEV heavy
#     FCEV medium
#     PHEV heavy
#     PHEV medium
#   LCVs
#     ICE
#     BEV
#     PHEV
#     EREV
# Passenger road
#   Motorcycles
#     BEV
#     ICE
#   Buses
#     BEV
#     ICE
#     FCEV
#     PHEV
#     EREV
#   LPVs
#     BEV small
#     BEV medium
#     BEV large
#     ICE medium
#     ICE large
#     ICE small
#     PHEV small
#     PHEV medium
#     PHEV large
#     HEV small
#     HEV medium
#     HEV large
#     EREV small
#     EREV medium
#     EREV large


#%%