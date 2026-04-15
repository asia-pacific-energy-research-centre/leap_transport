"""
Configuration for transport LEAP runs, keyed by economy code.

Use `get_transport_run_config("<economy>")` to fetch all file paths and
defaults for that economy in one place.
"""

from pathlib import Path

DEFAULT_TRANSPORT_ECONOMY = "12_NZ"
DEFAULT_TRANSPORT_SCENARIO = "Target"

COMMON_CONFIG = {
    "transport_import_path": "data/import_files/DEFAULT_transport_leap_import_TGT_REF_CA.xlsx",
    # Pre-trump merged energy data (higher variability); swap to 20251106 files when needed.
    "transport_esto_balances_path": "data/merged_file_energy_ALL_20250814_pretrump.csv",
    "survival_profile_path": "data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    "vintage_profile_path": "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    "transport_final_year": 2060,
}

# Economy metadata for 9th-edition transport runs.
# NOTE: Region strings must match LEAP region names used by your model.
ECONOMY_METADATA = {
    "01_AUS": {"region": "Australia", "short": "AUS", "base_year": 2022, "file": "01_AUS_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
    "02_BD": {"region": "Brunei Darussalam", "short": "BD", "base_year": 2022, "file": "02_BD_NON_ROAD_DETAILED_model_output_with_fuels20250123.csv"},
    "03_CDA": {"region": "Canada", "short": "CDA", "base_year": 2022, "file": "03_CDA_NON_ROAD_DETAILED_model_output_with_fuels20250225.csv"},
    "04_CHL": {"region": "Chile", "short": "CHL", "base_year": 2022, "file": "04_CHL_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
    "05_PRC": {"region": "People's Republic of China", "short": "PRC", "base_year": 2022, "file": "05_PRC_NON_ROAD_DETAILED_model_output_with_fuels20250225.csv"},
    "06_HKC": {"region": "Hong Kong, China", "short": "HKC", "base_year": 2022, "file": "06_HKC_NON_ROAD_DETAILED_model_output_with_fuels20250225.csv"},
    "07_INA": {"region": "Indonesia", "short": "INA", "base_year": 2022, "file": "07_INA_NON_ROAD_DETAILED_model_output_with_fuels20250123.csv"},
    "08_JPN": {"region": "Japan", "short": "JPN", "base_year": 2022, "file": "08_JPN_NON_ROAD_DETAILED_model_output_with_fuels20250415.csv"},
    "09_ROK": {"region": "Republic of Korea", "short": "ROK", "base_year": 2022, "file": "09_ROK_NON_ROAD_DETAILED_model_output_with_fuels20250123.csv"},
    "10_MAS": {"region": "Malaysia", "short": "MAS", "base_year": 2022, "file": "10_MAS_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
    "11_MEX": {"region": "Mexico", "short": "MEX", "base_year": 2022, "file": "11_MEX_NON_ROAD_DETAILED_model_output_with_fuels20250225.csv"},
    "12_NZ": {"region": "New Zealand", "short": "NZ", "base_year": 2022, "file": "12_NZ_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
    "13_PNG": {"region": "Papua New Guinea", "short": "PNG", "base_year": 2022, "file": "13_PNG_NON_ROAD_DETAILED_model_output_with_fuels20250421.csv"},
    "14_PE": {"region": "Peru", "short": "PE", "base_year": 2022, "file": "14_PE_NON_ROAD_DETAILED_model_output_with_fuels20250417.csv"},
    "15_PHL": {"region": "Philippines", "short": "PHL", "base_year": 2022, "file": "15_PHL_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
    "16_RUS": {"region": "Russia", "short": "RUS", "base_year": 2021, "file": "16_RUS_NON_ROAD_DETAILED_model_output_with_fuels20250331.csv"},
    "17_SGP": {"region": "Singapore", "short": "SGP", "base_year": 2022, "file": "17_SGP_NON_ROAD_DETAILED_model_output_with_fuels20250227.csv"},
    "18_CT": {"region": "Chinese Taipei", "short": "CT", "base_year": 2022, "file": "18_CT_NON_ROAD_DETAILED_model_output_with_fuels20250123.csv"},
    "19_THA": {"region": "Thailand", "short": "THA", "base_year": 2022, "file": "19_THA_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
    "20_USA": {"region": "United States of America", "short": "USA", "base_year": 2022, "file": "20_USA_NON_ROAD_DETAILED_model_output_with_fuels20250225.csv"},
    "21_VN": {"region": "Viet Nam", "short": "VN", "base_year": 2022, "file": "21_VN_NON_ROAD_DETAILED_model_output_with_fuels20250226.csv"},
}


# Mapping of economy code -> scenario -> config values used by transport_workflow_pipeline.py
TRANSPORT_ECONOMY_CONFIGS = {}
for economy_code, meta in ECONOMY_METADATA.items():
    scenario_cfg = {}
    raw_model_file = meta["file"].replace("_model_output_with_fuels", "_model_output")
    for scenario in ("Target", "Reference"):
        scenario_cfg[scenario] = {
            **COMMON_CONFIG,
            "transport_model_path": f"data/transport_data_9th/model_output_detailed_2/{raw_model_file}",
            "transport_region": meta["region"],
            "transport_base_year": meta["base_year"],
            "transport_model_name": f"{meta['short']} transport",
            "transport_export_path": f"results/{economy_code}_transport_leap_export_{scenario}.xlsx",
            "transport_fuels_path": f"data/transport_data_9th/model_output_with_fuels/{meta['file']}",
            "passenger_sales_output": f"results/passenger_sales_{economy_code}_{scenario}.csv",
            "freight_sales_output": f"results/freight_sales_{economy_code}_{scenario}.csv",
        }
    TRANSPORT_ECONOMY_CONFIGS[economy_code] = scenario_cfg


def get_transport_run_config(economy: str, scenario: str) -> dict:
    """Return config for the requested economy and scenario, or raise if missing."""
    try:
        return TRANSPORT_ECONOMY_CONFIGS[economy][scenario]
    except KeyError as exc:
        raise KeyError(
            f"Transport economy '{economy}' with scenario '{scenario}' is not configured. Add it to TRANSPORT_ECONOMY_CONFIGS."
        ) from exc


def _strip_known_economy_suffix(stem: str) -> str:
    """
    Remove a trailing economy-code token from a filename stem when present.

    Examples:
    - vehicle_survival_modified_20_USA -> vehicle_survival_modified
    - vintage_modelled_from_survival_00_APEC -> vintage_modelled_from_survival
    """
    parts = str(stem).split("_")
    if len(parts) >= 3 and parts[-2].isdigit() and len(parts[-2]) == 2:
        return "_".join(parts[:-2])
    if len(parts) >= 2 and parts[-2] == "00" and parts[-1].upper() == "APEC":
        return "_".join(parts[:-2])
    return str(stem)


def resolve_lifecycle_profile_path_for_economy(base_path: str, economy: str) -> str:
    """
    Prefer economy-specific lifecycle profile files when available.

    If `<stem>_<economy>.xlsx` exists, return it; otherwise return base_path.
    """
    from functions.path_utils import resolve_str

    resolved_base = resolve_str(base_path)
    if resolved_base is None:
        return base_path

    base_obj = Path(resolved_base)
    stem_without_suffix = _strip_known_economy_suffix(base_obj.stem)
    candidate_abs = base_obj.with_name(f"{stem_without_suffix}_{economy}{base_obj.suffix}")
    if candidate_abs.exists():
        return str(candidate_abs)
    return base_path


def apply_lifecycle_profile_overrides_for_economy(cfg: dict, economy: str) -> dict:
    """Return cfg with lifecycle profile paths swapped to economy-specific files when available."""
    out = dict(cfg)
    for key in ("survival_profile_path", "vintage_profile_path"):
        value = out.get(key)
        if isinstance(value, str) and value.strip():
            out[key] = resolve_lifecycle_profile_path_for_economy(value, economy)
    return out


def list_transport_run_configs(scenario: str | None = None) -> list[tuple[str, str]]:
    """Return sorted (economy, scenario) pairs available in TRANSPORT_ECONOMY_CONFIGS."""
    selected_scenario = scenario or DEFAULT_TRANSPORT_SCENARIO
    configs = []
    for economy in sorted(TRANSPORT_ECONOMY_CONFIGS):
        if selected_scenario in TRANSPORT_ECONOMY_CONFIGS[economy]:
            configs.append((economy, selected_scenario))
    return configs


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
    from functions.path_utils import resolve_str

    selected_economy = economy or DEFAULT_TRANSPORT_ECONOMY
    selected_scenario = scenario or DEFAULT_TRANSPORT_SCENARIO
    cfg = get_transport_run_config(selected_economy, selected_scenario)
    cfg = apply_lifecycle_profile_overrides_for_economy(cfg, selected_economy)
    cfg = {
        key: resolve_str(value)
        if isinstance(value, str) and (key.endswith("_path") or key.endswith("_output"))
        else value
        for key, value in cfg.items()
    }
    return selected_economy, selected_scenario, SimpleNamespace(**cfg)
