#%%
"""Utility helpers for exporting historical transport datasets."""
from pathlib import Path
from typing import Dict, Iterable, Sequence

import pandas as pd

DEFAULT_MERGED_DATA = Path("../../data/merged_file_energy_ALL_20250814.csv")
DEFAULT_TRANSPORT_BALANCES = Path("../../data/all transport balances data.xlsx")
DEFAULT_APEC_OUTPUT = Path("../../data/TRANSPORT_all_APPLICABLE_historical_sectors_fuels_9th_outlook.xlsx")
DEFAULT_NON_APEC_OUTPUT = Path("../../data/TRANSPORT_all_NONAPEC_historical_energy_use.xlsx")

REFERENCE_SCENARIO = "reference"
APEC_ECONOMY = "00_APEC"
TRANSPORT_SECTOR = "15_transport_sector"

ALL_ECONOMY_IDS = [
    "01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA",
    "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE",
    "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN",
]

BASE_YEAR_TO_ECONOMY: Dict[int, Iterable[str]] = {
    2021: ["16_RUS"],
    2022: [
        "01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA",
        "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE",
        "15_PHL", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN",
    ],
}


#%%
def export_apec_historical_sector_fuels(
    transport_balances: pd.DataFrame,
    base_year: int,
    output_path: Path = DEFAULT_APEC_OUTPUT,
    scenario: str = REFERENCE_SCENARIO,
    economy: str = APEC_ECONOMY,
    sector: str = TRANSPORT_SECTOR,
) -> pd.DataFrame:
    """Filter the transport balances data for APEC totals and export unique sector-fuel rows."""
    filtered = transport_balances[
        (transport_balances["scenarios"] == scenario.lower())
        & (transport_balances["economy"] == economy)
        & (transport_balances["sectors"] == sector)
        & (transport_balances["subtotal_layout"] == False)
    ]

    non_zero = filtered[filtered[base_year] != 0]
    unique_rows = non_zero[
        [
            "sectors",
            "sub1sectors",
            "sub2sectors",
            "sub3sectors",
            "sub4sectors",
            "fuels",
            "subfuels",
        ]
    ].drop_duplicates()

    output_path = Path(output_path)
    unique_rows.to_excel(output_path, index=False)
    return unique_rows


#%%
def export_non_apec_historical_energy_use(
    merged_energy: pd.DataFrame,
    base_year_to_economy: Dict[int, Sequence[str]],
    output_path: Path = DEFAULT_NON_APEC_OUTPUT,
    scenario: str = REFERENCE_SCENARIO,
    sector: str = TRANSPORT_SECTOR,
) -> pd.DataFrame:
    """Export non-APEC transport energy rows for each base year/economy pair."""
    rows = []
    for base_year, economies in base_year_to_economy.items():
        base_year_str = str(base_year)
        filtered = merged_energy[
            (merged_energy["scenarios"] == scenario.lower())
            & (merged_energy["sectors"] == sector)
            & (merged_energy["subtotal_layout"] == False)
            & (merged_energy[base_year_str] != 0)
            & (merged_energy["economy"].isin(economies))
        ]
        rows.append(filtered)

    if rows:
        export_df = pd.concat(rows, ignore_index=True)
    else:
        export_df = pd.DataFrame(columns=merged_energy.columns)

    output_path = Path(output_path)
    export_df.to_excel(output_path, index=False)
    return export_df


#%%
def main(
    merged_data_path: Path = DEFAULT_MERGED_DATA,
    transport_balances_path: Path = DEFAULT_TRANSPORT_BALANCES,
    apec_output_path: Path = DEFAULT_APEC_OUTPUT,
    non_apec_output_path: Path = DEFAULT_NON_APEC_OUTPUT,
    base_year: int = 2021,
) -> None:
    """Generate both APEC-wide and non-APEC historical exports using the default datasets."""
    merged_energy = pd.read_csv(merged_data_path)
    transport_balances = pd.read_excel(transport_balances_path)

    export_apec_historical_sector_fuels(transport_balances, base_year, apec_output_path)
    export_non_apec_historical_energy_use(
        merged_energy,
        BASE_YEAR_TO_ECONOMY,
        output_path=non_apec_output_path,
    )


if __name__ == "__main__":
    main()
