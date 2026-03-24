import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
CODE_DIR = REPO_ROOT / "codebase"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

import lifecycle_profile_editor as lpe
import sales_curve_estimate as sce


class SalesLifecyclePairTests(unittest.TestCase):
    def test_missing_date_column_raises_keyerror(self):
        df = pd.DataFrame({"not_date": [1]})
        with self.assertRaisesRegex(KeyError, "Expected a 'Date' column"):
            sce.estimate_passenger_sales_from_dataframe(
                df=df,
                survival_curves={"LPV": pd.Series([1.0], index=pd.Index([0], dtype=int))},
                vintage_profiles={"LPV": pd.Series([1.0], index=pd.Index([0], dtype=int))},
                economy="20_USA",
                scenario="Reference",
                base_year=2022,
                final_year=2022,
                esto_energy_path="dummy.csv",
            )

    def test_cumulative_to_annual_conversion_consistent_between_modules(self):
        cumulative = {0: 100.0, 1: 90.0, 2: 81.0}

        annual_from_lifecycle_pct = lpe.convert_cumulative_survival_to_annual(cumulative)
        annual_from_sales = sce._convert_cumulative_survival_to_annual(pd.Series(cumulative))

        ages = sorted(cumulative.keys())
        lifecycle_vals = np.array([annual_from_lifecycle_pct[a] / 100.0 for a in ages], dtype=float)
        sales_vals = annual_from_sales.reindex(ages).to_numpy(dtype=float)

        np.testing.assert_allclose(lifecycle_vals, sales_vals, atol=1e-12)
        self.assertAlmostEqual(annual_from_lifecycle_pct[max(ages)], 0.0, places=12)

    def test_convert_result_to_dataframe_keeps_vehicle_share_column(self):
        years = pd.Index([2020, 2021], dtype=int)
        result = {
            "M_envelope": pd.Series([1.0, 1.1], index=years),
            "adjusted_vehicle_ownership": pd.Series([1000.0, 1100.0], index=years),
            "passenger_total_sales": pd.Series([10.0, 12.0], index=years),
            "passenger_total_retirements": pd.Series([9.0, 11.0], index=years),
            "target_stocks": {"LPV": pd.Series([100.0, 110.0], index=years)},
            "sales": {"LPV": pd.Series([10.0, 12.0], index=years)},
            "retirements": {"LPV": pd.Series([9.0, 11.0], index=years)},
            "passenger_shares": {"LPV": pd.Series([1.0, 1.0], index=years)},
            "vehicle_shares": pd.DataFrame({"LPV": [1.0, 1.0]}, index=years),
        }

        out = sce.convert_result_to_dataframe(result)
        result_df = out["result_dataframe"]

        self.assertIn("vehicle_share", result_df.columns)
        self.assertTrue((result_df["vehicle_share"] == 1.0).all())

    def test_compute_base_capacity_index_rejects_zero_population(self):
        with self.assertRaisesRegex(ValueError, "base_population"):
            sce.compute_base_capacity_index(
                base_stocks={"LPV": 10.0},
                base_population=0.0,
                weights={"LPV": 1.0},
            )

    def test_compute_sales_rejects_non_contiguous_age_grid(self):
        target_stock = pd.Series([100.0, 100.0], index=pd.Index([2022, 2023], dtype=int))
        survival = pd.Series([0.9, 0.8], index=pd.Index([0, 2], dtype=int))
        vintage = pd.Series([0.5, 0.5], index=pd.Index([0, 2], dtype=int))

        with self.assertRaisesRegex(ValueError, "contiguous"):
            sce.compute_sales_from_stock_targets(
                target_stock=target_stock,
                survival_curve=survival,
                vintage_profile=vintage,
            )

    def test_drive_family_aggregation_groups_expected_types(self):
        years = pd.Index([2030, 2031], dtype=int)
        per_drive = pd.DataFrame(
            {
                "ice_g": [10.0, 12.0],
                "ice_d": [5.0, 6.0],
                "hev": [2.0, 3.0],
                "phev_g": [4.0, 4.0],
                "bev": [7.0, 8.0],
                "fcev": [1.0, 1.0],
                "mystery_drive": [9.0, 9.0],
            },
            index=years,
        )

        grouped = sce._aggregate_drive_frame_by_family(per_drive)

        self.assertEqual(list(grouped.columns), ["ICE", "HEV", "PHEV", "EV", "Other"])
        np.testing.assert_allclose(grouped["ICE"].to_numpy(dtype=float), np.array([15.0, 18.0]))
        np.testing.assert_allclose(grouped["HEV"].to_numpy(dtype=float), np.array([2.0, 3.0]))
        np.testing.assert_allclose(grouped["PHEV"].to_numpy(dtype=float), np.array([4.0, 4.0]))
        np.testing.assert_allclose(grouped["EV"].to_numpy(dtype=float), np.array([8.0, 9.0]))
        np.testing.assert_allclose(grouped["Other"].to_numpy(dtype=float), np.array([9.0, 9.0]))


if __name__ == "__main__":
    unittest.main()
