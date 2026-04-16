import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
CODE_DIR = REPO_ROOT / "codebase"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from functions.leap_utilities_functions import (
    join_and_check_import_structure_matches_export_structure,
)


class LeapUtilitiesFunctionsTests(unittest.TestCase):
    def test_structure_check_handles_export_suffixed_level_columns(self):
        region = "United States of America"
        branch_path = r"Demand\Transport"
        base_rows = [
            {
                "BranchID": 10,
                "VariableID": 20,
                "ScenarioID": 1,
                "RegionID": 1,
                "Branch Path": branch_path,
                "Variable": "Total Activity",
                "Scenario": "Current Accounts",
                "Region": region,
                "Scale": "Million",
                "Units": "Vehicle-km",
                "Per...": pd.NA,
                "Expression": "1",
                2022: 1.0,
                "Level 1": "Demand",
                "Level 2": "Transport",
            },
            {
                "BranchID": 10,
                "VariableID": 20,
                "ScenarioID": 4,
                "RegionID": 1,
                "Branch Path": branch_path,
                "Variable": "Total Activity",
                "Scenario": "Reference",
                "Region": region,
                "Scale": "Million",
                "Units": "Vehicle-km",
                "Per...": pd.NA,
                "Expression": "2",
                2022: 2.0,
                "Level 1": "Demand",
                "Level 2": "Transport",
            },
        ]
        import_df = pd.DataFrame(base_rows)
        export_df = import_df.drop(
            columns=["BranchID", "VariableID", "ScenarioID", "RegionID"]
        ).copy()
        export_df_for_viewing = export_df.copy()

        with tempfile.TemporaryDirectory() as tmp:
            import_path = Path(tmp) / "template.xlsx"
            with pd.ExcelWriter(import_path, engine="openpyxl") as writer:
                import_df.to_excel(
                    writer,
                    sheet_name="Export",
                    index=False,
                    startrow=2,
                )

            leap_df, viewing_df = join_and_check_import_structure_matches_export_structure(
                import_path,
                export_df,
                export_df_for_viewing,
                scenario="Reference",
                region=region,
                STRICT_CHECKS=True,
            )

        self.assertIn("Level 1", leap_df.columns)
        self.assertIn("Level 2", leap_df.columns)
        self.assertNotIn("Level 1_export", leap_df.columns)
        self.assertEqual(set(leap_df["Level 1"]), {"Demand"})
        self.assertEqual(set(viewing_df["BranchID"]), {10})


if __name__ == "__main__":
    unittest.main()
