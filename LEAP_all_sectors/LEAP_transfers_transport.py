#%%
# ============================================================
# LEAP_transfers_transport_loader.py
# ============================================================
# Main logic for processing and loading transport data into LEAP.
# Depends on LEAP_transfers_transport_core.py and mappings/config files.
# ============================================================

import pandas as pd

from sector_config import SectorConfig
from sector_processor import SectorProcessor

from LEAP_transfers_transport_core import (
    analyze_data_quality,
    extract_other_type_rows_from_esto_and_insert_into_transport_df,
    normalize_sales_shares,
    validate_shares,
)
from LEAP_transfers_transport_MAPPINGS import (
    ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
    LEAP_BRANCH_TO_SOURCE_MAP,
    LEAP_MEASURE_CONFIG,
    SHORTNAME_TO_LEAP_BRANCHES,
    UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
)
from LEAP_BRANCH_TO_EXPRESSION_MAPPING import ALL_YEARS, LEAP_BRANCH_TO_EXPRESSION_MAPPING
from LEAP_tranposrt_measures_config import calculate_sales, list_all_measures, process_measures_for_leap
from LEAP_transfers_transport_excel import create_leap_import_file
from basic_mappings import ESTO_TRANSPORT_SECTOR_TUPLES, add_fuel_column
from LEAP_mappings_validation import (
    validate_all_mappings_with_measures,
    validate_final_energy_use_for_base_year_equals_esto_totals,
)

# ------------------------------------------------------------
# Modular process functions
# ------------------------------------------------------------
def prepare_input_data(
    excel_path,
    economy,
    base_year,
    final_year,
    TRANSPORT_ESTO_BALANCES_PATH='../../data/all transport balances data.xlsx',
):
    """Load and preprocess transport data for a specific economy."""

    print(f"\n=== Loading Transport Data for {economy} ===")
    df = pd.read_excel(excel_path)
    df = df[df["Economy"] == economy]
    df = df[(df["Date"] >= base_year) & (df["Date"] <= final_year)]
    df = add_fuel_column(df)
    df.loc[df["Medium"] != "road", ["Stocks", 'Vehicle_sales_share']] = 0
    df = calculate_sales(df)
    df = analyze_data_quality(df)

    # Create a non-road medium that aggregates all non-road activity
    non_road_df = df[df["Medium"] != "road"].copy()
    non_road_df["Medium"] = "non road"
    non_road_df = (
        non_road_df[
            [
                "Scenario",
                'Date',
                'Transport Type',
                'Medium',
                'Vehicle Type',
                'Drive',
                'Fuel',
                'Activity',
            ]
        ]
        .groupby([
            "Scenario",
            'Date',
            'Transport Type',
            'Medium',
            'Vehicle Type',
            'Drive',
            'Fuel',
        ])['Activity']
        .sum()
        .reset_index()
    )
    df = pd.concat([df, non_road_df], ignore_index=True)

    df = extract_other_type_rows_from_esto_and_insert_into_transport_df(
        df, base_year, final_year, economy, TRANSPORT_ESTO_BALANCES_PATH
    )
    return df


def validate_shares_with_logging(
    df,
    *,
    tolerance: float = 0.01,
    auto_correct: bool = True,
    road_only: bool = True,
    report_path: str = "../../results/share_validation_report.csv",
):
    """Validate share columns and persist the diagnostic report."""

    df, report = validate_shares(df, tolerance=tolerance, auto_correct=auto_correct, road_only=road_only)
    if report is not None and not report.empty:
        report.to_csv(report_path, index=False)
    return df, report

        
        





def transport_measures_processor(
    df: pd.DataFrame,
    filtered_measure_config: dict,
    shortname: str,
    group_columns,
    leap_tuple,
    src_tuple,
):
    """Adapter that reuses the transport-specific measure processor."""

    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    return process_measures_for_leap(
        df,
        filtered_measure_config,
        shortname,
        list(group_columns),
        ttype,
        medium,
        vtype,
        drive,
        fuel,
    )


def transport_metadata_resolver(meta_values, leap_tuple, src_tuple):
    """Resolve placeholder metadata values based on the source tuple."""

    resolved = dict(meta_values)
    ttype, medium, vtype, drive, fuel = tuple(list(src_tuple) + [None] * (5 - len(src_tuple)))[:5]
    for key in ('LEAP_units', 'LEAP_Scale', 'LEAP_Per'):
        value = resolved.get(key)
        if isinstance(value, str) and '$' in value:
            if value == 'Passenger-km$Tonne-km':
                if ttype == 'passenger':
                    resolved[key] = 'Passenger-km'
                elif ttype == 'freight':
                    resolved[key] = 'Tonne-km'
                else:
                    raise ValueError(
                        f"Unexpected transport type '{ttype}' when resolving metadata placeholder"
                    )
            else:
                raise ValueError(f"Unknown metadata placeholder: {value}")
    return resolved


TRANSPORT_SECTOR_CONFIG = SectorConfig(
    name="transport",
    leap_root=r"Demand\Transport",
    leap_branch_to_source_map=LEAP_BRANCH_TO_SOURCE_MAP,
    shortname_to_leap_branches=SHORTNAME_TO_LEAP_BRANCHES,
    measure_config=LEAP_MEASURE_CONFIG,
    expression_mapping=LEAP_BRANCH_TO_EXPRESSION_MAPPING,
    measures_processor=transport_measures_processor,
    source_dimension_order=["Transport Type", "Medium", "Vehicle Type", "Drive", "Fuel"],
    share_normalisers=[normalize_sales_shares],
    validators=[validate_shares_with_logging],
    grouping_base_columns=("Scenario",),
    metadata_resolver=transport_metadata_resolver,
    log_extra_columns=["Economy"],
    default_expression_mode=("Data", ALL_YEARS),
)


def load_transport_into_leap_v3(
    excel_path,
    economy,
    scenario,
    diagnose_method='first_of_each_length',
    base_year=2022,
    final_year=2060,
    save_log=True,
    log_filename="../../results/leap_data_log.xlsx",
    SET_VARS_IN_LEAP_USING_COM=True,
    create_import_files=False,
    import_filename="../../results/leap_import.xlsx",
    TRANSPORT_ESTO_BALANCES_PATH='../../data/all transport balances data.xlsx',
):
    """Main orchestrator for LEAP transport data loading using the generic pipeline."""

    validate_all_mappings_with_measures(
        ESTO_SECTOR_FUEL_TO_LEAP_BRANCH_MAP,
        LEAP_BRANCH_TO_EXPRESSION_MAPPING,
        LEAP_BRANCH_TO_SOURCE_MAP,
        SHORTNAME_TO_LEAP_BRANCHES,
        LEAP_MEASURE_CONFIG,
        ESTO_TRANSPORT_SECTOR_TUPLES,
        UNMAPPABLE_BRANCHES_NO_ESTO_EQUIVALENT,
        EXAMPLE_SAMPLE_SIZE=1000,
    )

    processor = SectorProcessor(
        TRANSPORT_SECTOR_CONFIG,
        set_vars_in_leap_using_com=SET_VARS_IN_LEAP_USING_COM,
    )

    loader_kwargs = {
        'excel_path': excel_path,
        'economy': economy,
        'base_year': base_year,
        'final_year': final_year,
        'TRANSPORT_ESTO_BALANCES_PATH': TRANSPORT_ESTO_BALANCES_PATH,
    }

    result = processor.load_into_leap(
        prepare_input_data,
        loader_kwargs=loader_kwargs,
        diagnose_method=diagnose_method,
        save_log=save_log,
        log_filename=log_filename,
        create_import_files=create_import_files,
        import_filename=import_filename,
        import_builder=lambda log_df, path: create_leap_import_file(
            log_df,
            path,
            scenario=scenario,
            base_year=base_year,
            final_year=final_year,
        ),
    )

    validate_final_energy_use_for_base_year_equals_esto_totals(
        economy,
        scenario,
        base_year,
        final_year,
        result.import_dataframe,
        TRANSPORT_ESTO_BALANCES_PATH,
    )
    print("\n=== Transport data successfully filled into LEAP. ===\n")
    return result
