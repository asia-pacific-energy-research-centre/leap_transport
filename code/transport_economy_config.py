"""
Configuration for transport LEAP runs, keyed by economy code.

Use `get_transport_run_config("<economy>")` to fetch all file paths and
defaults for that economy in one place.
"""
DEFAULT_TRANSPORT_ECONOMY = "12_NZ"
DEFAULT_TRANSPORT_SCENARIO = "Target"
# Mapping of economy code -> config values used by MAIN_leap_import.py
TRANSPORT_ECONOMY_CONFIGS = {
    "12_NZ": {
        'Target':{
            "transport_model_path": "../data/12_NZ_NON_ROAD_DETAILED_model_output20250226 regular.csv",
            "transport_region": "New Zealand",
            "transport_base_year": 2022,
            "transport_final_year": 2060,
            "transport_model_name": "NZ transport",
            "transport_export_path": "../results/NZ_transport_leap_export_Target.xlsx",
            "transport_import_path": "../data/import_files/DEFAULT_transport_leap_import_Target.xlsx",
            "transport_esto_balances_path": "../data/all transport balances data.xlsx",
            "transport_fuels_path": "../data/12_NZ_NON_ROAD_DETAILED_model_output20250226 fuels.csv",
            "survival_profile_path": "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
            "vintage_profile_path": "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
            "passenger_sales_output": "../results/passenger_sales_NZ_Target.csv",
            "freight_sales_output": "../results/freight_sales_NZ_Target.csv"},
        'Reference': {
            "transport_model_path": "../data/12_NZ_NON_ROAD_DETAILED_model_output20250226 regular.csv",
            "transport_region": "New Zealand",
            "transport_base_year": 2022,
            "transport_final_year": 2060,
            "transport_model_name": "NZ transport",
            "transport_export_path": "../results/NZ_transport_leap_export_Target.xlsx",
            "transport_import_path": "../data/import_files/DEFAULT_transport_leap_import_Target.xlsx",
            "transport_esto_balances_path": "../data/all transport balances data.xlsx",
            "transport_fuels_path": "../data/12_NZ_NON_ROAD_DETAILED_model_output20250226 fuels.csv",
            "survival_profile_path": "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
            "vintage_profile_path": "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
            "passenger_sales_output": "../results/passenger_sales_NZ_Reference.csv",
            "freight_sales_output": "../results/freight_sales_NZ_Reference.csv",
        }  
    },
    "20_USA": {
        'Target':{
            "transport_model_path": "../data/USA transport file.xlsx",
            "transport_region": "United States of America",
            "transport_base_year": 2022,
            "transport_final_year": 2060,
            "transport_model_name": "USA transport",
            "transport_export_path": "../results/USA_transport_leap_export_Target.xlsx",
            "transport_import_path": "../data/import_files/DEFAULT_transport_leap_import_Target.xlsx",
            "transport_esto_balances_path": "../data/all transport balances data.xlsx",
            "transport_fuels_path": "../data/USA fuels model output.csv",
            "survival_profile_path": "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
            "vintage_profile_path": "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
            "passenger_sales_output": "../results/passenger_sales_USA_Target.csv",
            "freight_sales_output": "../results/freight_sales_USA_Target.csv",
        },
        'Reference': {
            "transport_model_path": "../data/USA transport file.xlsx",
            "transport_region": "United States of America",
            "transport_base_year": 2022,
            "transport_final_year": 2060,
            "transport_model_name": "USA transport",
            "transport_export_path": "../results/USA_transport_leap_export_Reference.xlsx",
            "transport_import_path": "../data/import_files/DEFAULT_transport_leap_import_Reference.xlsx",
            "transport_esto_balances_path": "../data/all transport balances data.xlsx",
            "transport_fuels_path": "../data/USA fuels model output.csv",
            "survival_profile_path": "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
            "vintage_profile_path": "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
            "passenger_sales_output": "../results/passenger_sales_USA_Reference.csv",
            "freight_sales_output": "../results/freight_sales_USA_Reference.csv",
        }  
    },
}


def get_transport_run_config(economy: str, scenario: str) -> dict:
    """Return config for the requested economy and scenario, or raise if missing."""
    try:
        return TRANSPORT_ECONOMY_CONFIGS[economy][scenario]
    except KeyError as exc:
        raise KeyError(
            f"Transport economy '{economy}' with scenario '{scenario}' is not configured. Add it to TRANSPORT_ECONOMY_CONFIGS."
        ) from exc


def load_transport_run_config(economy: str | None = None, scenario: str | None = None) -> tuple[str, str, object]:
    """
    Load and return the selected transport economy configuration as a SimpleNamespace.

    Parameters
    ----------
    economy : str | None, optional
        Identifier of the economy to load, falling back to the default if not provided.
    scenario : str | None, optional
        Scenario name to load, falling back to the default if omitted.

    Returns
    -------
    tuple[str, str, object]
        A tuple of (economy_code, scenario_code, SimpleNamespace), where the namespace
        exposes configuration fields with attribute-style access rather than dictionary
        indexing for improved readability and autocomplete support.
 
    Return (economy_code, config) for the requested economy (defaulting to DEFAULT_TRANSPORT_ECONOMY).

    Config is a SimpleNamespace for convenient attribute access, e.g. cfg.transport_model_path.
    """
    from types import SimpleNamespace

    selected_economy = economy or DEFAULT_TRANSPORT_ECONOMY
    selected_scenario = scenario or DEFAULT_TRANSPORT_SCENARIO
    cfg = get_transport_run_config(selected_economy, selected_scenario)
    return selected_economy, selected_scenario, SimpleNamespace(**cfg)