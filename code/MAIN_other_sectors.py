#%%
from traitlets import Tuple
from LEAP_core import (
    fill_branches_from_export_file,
    create_branches_from_export_file,
    connect_to_leap
)
from LEAP_excel_io import (
    copy_energy_spreadsheet_into_leap_import_file,
)
# Connect to LEAP
L = connect_to_leap()

# Define parameters
leap_export_filename = '../results/leap_balances_export_file.xlsx'
energy_spreadsheet_filename = '../data/merged_file_energy_ALL_20250814.csv'
ECONOMY = '20_USA'
BASE_YEAR = 2022
SUBTOTAL_COLUMN = 'subtotal_layout'
SCENARIO = "reference"
ROOT = r"Key Assumptions\Energy Balances"
REGION = "Region 1"
DROP_ZERO_BRANCHES = True
sheet_name = "Energy_Balances"
variable_col_value="Activity Level"#turns out that if u are doing key assumptions, u need to specify the variable col value as "Activity Level" even if it is some other measure, like energy.
units = "PJ"
filters_dict = {
    "sectors": ["15_transport_sector"]
}
#%%
# Copy energy spreadsheet into LEAP import file
copy_energy_spreadsheet_into_leap_import_file(
    leap_export_filename=leap_export_filename,
    energy_spreadsheet_filename=energy_spreadsheet_filename,
    ECONOMY=ECONOMY,
    BASE_YEAR=BASE_YEAR,
    SUBTOTAL_COLUMN=SUBTOTAL_COLUMN,
    SCENARIO=SCENARIO,
    ROOT=ROOT,
    REGION=REGION,
    DROP_ZERO_BRANCHES=DROP_ZERO_BRANCHES,
    sheet_name=sheet_name,
    variable_col_value=variable_col_value,
    units=units,
    filters_dict=filters_dict,
)

# Create branches from export file
create_branches_from_export_file(
    L,
    leap_export_filename,
    sheet_name=sheet_name,
    branch_path_col="Branch Path",
    branch_root=ROOT,
    scenario=SCENARIO,
    region=REGION,
    branch_type_mapping=None,
    default_branch_type=(BRANCH_KEY_ASSUMPTION_CATEGORY, BRANCH_KEY_ASSUMPTION_CATEGORY, BRANCH_KEY_ASSUMPTION_BRANCH),
    RAISE_ERROR_ON_FAILED_BRANCH_CREATION=True,
)
#%%
# Fill branches with data from export file
fill_branches_from_export_file(
    L,
    leap_export_filename,
    sheet_name=sheet_name,
    scenario=SCENARIO,
    region=REGION,
    RAISE_ERROR_ON_FAILED_SET=True,
)

#%%
# Dummy energy reconciliation workflow (turn on by setting run_demo=True)
run_demo = False

if run_demo:
    import pandas as pd
    from energy_use_reconciliation import build_branch_rules_from_mapping, reconcile_energy_use

    # Small example showing how to reconcile ESTO totals with a LEAP export.
    example_branch_path = f"{ROOT}\\Example branch"
    demo_export = pd.DataFrame(
        {
            "Branch Path": [example_branch_path, example_branch_path],
            "Variable": ["Activity Level", "Final Energy Intensity"],
            BASE_YEAR: [1.0, 1.0],
        }
    )

    example_mapping = {
        ("example_sector", "example_fuel"): [("Energy Balances", "Example branch")]
    }
    demo_rules = build_branch_rules_from_mapping(
        example_mapping,
        unmappable_branches=[],
        all_leap_branches=[("Energy Balances", "Example branch")],
        analysis_type_lookup=lambda branch: "Intensity",
        root="Key Assumptions",
    )

    demo_esto_totals = {("example_sector", "example_fuel"): 1.1}
    adjusted_demo_df, demo_summary = reconcile_energy_use(
        export_df=demo_export,
        base_year=BASE_YEAR,
        branch_mapping_rules=demo_rules,
        esto_energy_totals=demo_esto_totals,
        apply_adjustments_to_future_years=False,
    )

    demo_summary.to_csv("../intermediate_data/dummy_energy_reconciliation_summary.csv", index=False)
    adjusted_demo_df.to_csv("../intermediate_data/dummy_energy_reconciliation_export.csv", index=False)

