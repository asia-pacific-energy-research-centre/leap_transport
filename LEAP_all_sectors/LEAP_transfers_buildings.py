"""Example loader using the generic sector pipeline for buildings."""

from __future__ import annotations

import pandas as pd

from sector_config import SectorConfig
from sector_processor import SectorProcessor

from LEAP_transfers_buildings_config import (
    BUILDINGS_EXPRESSION_MAPPING,
    BUILDINGS_LEAP_BRANCH_TO_SOURCE_MAP,
    BUILDINGS_MEASURE_CONFIG,
    BUILDINGS_SHORTNAME_TO_LEAP_BRANCHES,
    DEFAULT_BUILDINGS_EXPRESSION_MODE,
)


def prepare_buildings_input_data(
    excel_path: str,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
):
    """Load a simplified building dataset and apply basic filtering."""

    df = pd.read_excel(excel_path)
    if 'Economy' in df.columns:
        df = df[df['Economy'] == economy]
    if 'Scenario' in df.columns:
        df = df[df['Scenario'] == scenario]
    df = df[(df['Date'] >= base_year) & (df['Date'] <= final_year)]

    expected_columns = {
        'Date',
        'Building Type',
        'Major Use',
        'Fuel',
        'Activity',
        'Intensity',
        'Energy',
    }
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for buildings loader: {sorted(missing)}")
    return df


def buildings_measures_processor(
    df: pd.DataFrame,
    filtered_measure_config: dict,
    shortname: str,
    group_columns,
    leap_tuple,
    src_tuple,
):
    """Create simple measure tables without additional aggregation."""

    filtered = df.copy()
    for column, value in zip(['Building Type', 'Major Use', 'Fuel'], src_tuple):
        if column not in filtered.columns:
            raise KeyError(f"Source column '{column}' missing from dataset")
        filtered = filtered[filtered[column] == value]

    if filtered.empty:
        return {}

    applicable_group_columns = [col for col in group_columns if col in filtered.columns]
    if not applicable_group_columns:
        raise ValueError("No valid grouping columns available for buildings processor")

    grouped = filtered.groupby(applicable_group_columns, as_index=False)
    measures = {}
    for measure, metadata in filtered_measure_config.items():
        source = metadata.get('source_mapping')
        if source is None or source not in df.columns:
            continue
        df_measure = grouped[source].sum().rename(columns={source: measure})
        measures[measure] = df_measure
    return measures


BUILDINGS_SECTOR_CONFIG = SectorConfig(
    name="buildings",
    leap_root=r"Demand\\Buildings",
    leap_branch_to_source_map=BUILDINGS_LEAP_BRANCH_TO_SOURCE_MAP,
    shortname_to_leap_branches=BUILDINGS_SHORTNAME_TO_LEAP_BRANCHES,
    measure_config=BUILDINGS_MEASURE_CONFIG,
    expression_mapping=BUILDINGS_EXPRESSION_MAPPING,
    measures_processor=buildings_measures_processor,
    source_dimension_order=["Building Type", "Major Use", "Fuel"],
    grouping_base_columns=("Scenario", "Economy"),
    log_extra_columns=["Economy"],
    default_expression_mode=DEFAULT_BUILDINGS_EXPRESSION_MODE,
)


def load_buildings_into_leap(
    excel_path: str,
    economy: str,
    scenario: str,
    base_year: int = 2020,
    final_year: int = 2050,
    save_log: bool = True,
    log_filename: str = "../../results/buildings_leap_data_log.xlsx",
    SET_VARS_IN_LEAP_USING_COM: bool = True,
    create_import_files: bool = False,
    import_filename: str = "../../results/buildings_leap_import.xlsx",
):
    """Run the building sector loader using the shared processor."""

    processor = SectorProcessor(
        BUILDINGS_SECTOR_CONFIG,
        set_vars_in_leap_using_com=SET_VARS_IN_LEAP_USING_COM,
    )
    loader_kwargs = {
        'excel_path': excel_path,
        'economy': economy,
        'scenario': scenario,
        'base_year': base_year,
        'final_year': final_year,
    }
    return processor.load_into_leap(
        prepare_buildings_input_data,
        loader_kwargs=loader_kwargs,
        save_log=save_log,
        log_filename=log_filename,
        create_import_files=create_import_files,
        import_filename=import_filename,
    )


if __name__ == "__main__":
    load_buildings_into_leap(
        excel_path="../../data/example_buildings.xlsx",
        economy="01_APEC",
        scenario="Reference",
    )
