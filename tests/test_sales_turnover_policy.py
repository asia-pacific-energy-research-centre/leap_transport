import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
CODE_DIR = REPO_ROOT / "codebase"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

import sales_curve_estimate as sce
import sales_workflow as sce_policy


class SalesTurnoverPolicyTests(unittest.TestCase):
    def test_policy_module_matches_legacy_when_no_policy(self):
        years = pd.Index([2022, 2023, 2024, 2025], dtype=int)
        target_stock = pd.Series([100.0, 102.0, 104.0, 106.0], index=years)
        survival = pd.Series([0.92, 0.85, 0.0], index=pd.Index([0, 1, 2], dtype=int))
        vintage = pd.Series([0.0, 0.55, 0.45], index=pd.Index([0, 1, 2], dtype=int))

        legacy_sales, _, legacy_retirements = sce.compute_sales_from_stock_targets(
            target_stock=target_stock,
            survival_curve=survival,
            vintage_profile=vintage,
            return_retirements=True,
        )
        policy_sales, _, policy_retirements = sce_policy.compute_sales_from_stock_targets(
            target_stock=target_stock,
            survival_curve=survival,
            vintage_profile=vintage,
            turnover_policy=None,
            return_retirements=True,
        )

        np.testing.assert_allclose(policy_sales.values, legacy_sales.values, atol=1e-12)
        np.testing.assert_allclose(policy_retirements.values, legacy_retirements.values, atol=1e-12)

    def test_additional_retirement_policy_increases_sales_and_retirements(self):
        years = pd.Index([2022, 2023, 2024, 2025, 2026], dtype=int)
        target_stock = pd.Series([100.0, 100.0, 100.0, 100.0, 100.0], index=years)
        survival = pd.Series([0.95, 0.90, 0.80, 0.0], index=pd.Index([0, 1, 2, 3], dtype=int))
        vintage = pd.Series([0.0, 0.40, 0.35, 0.25], index=pd.Index([0, 1, 2, 3], dtype=int))

        legacy_sales, _, legacy_retirements = sce.compute_sales_from_stock_targets(
            target_stock=target_stock,
            survival_curve=survival,
            vintage_profile=vintage,
            return_retirements=True,
        )
        policy_sales, cohorts, policy_retirements = sce_policy.compute_sales_from_stock_targets(
            target_stock=target_stock,
            survival_curve=survival,
            vintage_profile=vintage,
            turnover_policy={
                "additional_retirement_rate": {2024: 0.15, 2025: 0.20, 2026: 0.20},
            },
            return_retirements=True,
        )

        self.assertGreater(float(policy_sales.sum()), float(legacy_sales.sum()))
        self.assertGreater(float(policy_retirements.sum()), float(legacy_retirements.sum()))
        self.assertGreater(float(policy_sales.loc[2025]), float(legacy_sales.loc[2025]))
        np.testing.assert_allclose(
            cohorts.sum(axis=1).to_numpy(dtype=float),
            target_stock.to_numpy(dtype=float),
            atol=1e-10,
        )

    def test_passenger_wrapper_supports_turnover_policy(self):
        years = pd.Index([2022, 2023, 2024, 2025, 2026], dtype=int)
        population = pd.Series(1000.0, index=years)
        energy = pd.Series(10.0, index=years)
        base_stocks = {"LPV": 100.0}
        survival_curves = {
            "LPV": pd.Series([0.95, 0.90, 0.80, 0.70, 0.0], index=pd.Index([0, 1, 2, 3, 4], dtype=int))
        }
        vintage_profiles = {
            "LPV": pd.Series([0.0, 0.35, 0.25, 0.20, 0.20], index=pd.Index([0, 1, 2, 3, 4], dtype=int))
        }

        baseline = sce_policy.build_passenger_sales_for_economy(
            years=years,
            population=population,
            energy_use_passenger=energy,
            base_stocks=base_stocks,
            survival_curves=survival_curves,
            vintage_profiles=vintage_profiles,
            weights={"LPV": 1.0},
            M_sat=0.1,
            saturated=True,
            plot=False,
        )
        policy_result = sce_policy.build_passenger_sales_for_economy(
            years=years,
            population=population,
            energy_use_passenger=energy,
            base_stocks=base_stocks,
            survival_curves=survival_curves,
            vintage_profiles=vintage_profiles,
            weights={"LPV": 1.0},
            M_sat=0.1,
            saturated=True,
            turnover_policies={"LPV": {"additional_retirement_rate": 0.12}},
            plot=False,
        )

        self.assertTrue(policy_result["policy_enabled"])
        self.assertEqual(policy_result["unused_turnover_policy_keys"], [])
        self.assertGreater(
            float(policy_result["passenger_total_sales"].sum()),
            float(baseline["passenger_total_sales"].sum()),
        )
        np.testing.assert_allclose(
            policy_result["target_stocks"]["LPV"].to_numpy(dtype=float),
            baseline["target_stocks"]["LPV"].to_numpy(dtype=float),
            atol=1e-12,
        )

    def test_drive_policy_is_converted_to_vehicle_bucket_policy(self):
        years = pd.Index([2022, 2023], dtype=int)
        df = pd.DataFrame(
            {
                "Date": [2022, 2022, 2022, 2023, 2023, 2023],
                "Transport Type": ["passenger"] * 6,
                "Medium": ["road"] * 6,
                "Vehicle Type": ["car"] * 6,
                "Drive": ["ice_g", "ice_g", "bev", "ice_g", "ice_g", "bev"],
                "Fuel": ["Motor gasoline", "Efuel", "Electricity", "Motor gasoline", "Efuel", "Electricity"],
                "Stocks": [64.0, 16.0, 20.0, 48.0, 12.0, 40.0],
            }
        )
        drive_policy = {
            "ICE": {
                "drives": ["ice_d", "ice_g"],
                "additional_retirement_rate": {2022: 0.10, 2023: 0.20},
            }
        }

        policies, diagnostics = sce_policy.derive_vehicle_turnover_policies_from_drive_policy(
            df=df,
            years=years,
            drive_turnover_policy=drive_policy,
            vehicle_type_map={"car": "LPV"},
            transport_type="passenger",
            medium="road",
            stocks_col="Stocks",
        )

        self.assertIn("LPV", policies)
        rate = policies["LPV"]["additional_retirement_rate"].reindex(years).astype(float)
        # ICE stock share: 80/100 in 2022, 60/100 in 2023.
        np.testing.assert_allclose(rate.values, np.array([0.08, 0.12]), atol=1e-12)
        self.assertIn("ice_d", diagnostics["unused_policy_drives"])
        self.assertFalse(diagnostics["contributions_long"].empty)
        self.assertIn("all_drive_stock_shares_long", diagnostics)
        all_drive_diag = diagnostics["all_drive_stock_shares_long"]
        self.assertFalse(all_drive_diag.empty)
        self.assertIn("bev", set(all_drive_diag["drive"].astype(str)))

    def test_initial_fleet_age_shift_vintage_derivation_reports_average_age_change(self):
        vintage_profiles = {
            "LPV": pd.Series([0.10, 0.25, 0.30, 0.20, 0.15], index=pd.Index([0, 1, 2, 3, 4], dtype=int)),
            "MC": pd.Series([0.20, 0.30, 0.25, 0.15, 0.10], index=pd.Index([0, 1, 2, 3, 4], dtype=int)),
        }

        shifted_profiles, diagnostics = sce_policy.derive_initial_fleet_age_shift_vintage_profiles(
            vintage_profiles=vintage_profiles,
            initial_fleet_age_shift_years={"LPV": 2.0, "MC": 0.0},
        )

        self.assertIn("LPV", shifted_profiles)
        self.assertNotIn("MC", shifted_profiles)
        self.assertIn("implied_average_age_delta_years", diagnostics.columns)

        lpv_diag = diagnostics.loc[diagnostics["vehicle_bucket"] == "LPV"].iloc[0]
        self.assertGreater(float(lpv_diag["implied_average_age_delta_years"]), 0.0)

    def test_passenger_wrapper_supports_analysis_initial_fleet_age_shift_override(self):
        years = pd.Index([2022, 2023, 2024, 2025, 2026], dtype=int)
        population = pd.Series(1000.0, index=years)
        energy = pd.Series(10.0, index=years)
        base_stocks = {"LPV": 100.0}
        survival_curves = {
            "LPV": pd.Series([0.95, 0.90, 0.80, 0.70, 0.0], index=pd.Index([0, 1, 2, 3, 4], dtype=int))
        }
        vintage_profiles = {
            "LPV": pd.Series([0.0, 0.35, 0.25, 0.20, 0.20], index=pd.Index([0, 1, 2, 3, 4], dtype=int))
        }

        baseline = sce_policy.build_passenger_sales_for_economy(
            years=years,
            population=population,
            energy_use_passenger=energy,
            base_stocks=base_stocks,
            survival_curves=survival_curves,
            vintage_profiles=vintage_profiles,
            weights={"LPV": 1.0},
            M_sat=0.1,
            saturated=True,
            plot=False,
        )
        shifted = sce_policy.build_passenger_sales_for_economy(
            years=years,
            population=population,
            energy_use_passenger=energy,
            base_stocks=base_stocks,
            survival_curves=survival_curves,
            vintage_profiles=vintage_profiles,
            weights={"LPV": 1.0},
            M_sat=0.1,
            saturated=True,
            analysis_initial_fleet_age_shift_years=2.0,
            plot=False,
        )

        self.assertTrue(shifted["policy_enabled"])
        self.assertIn("analysis_initial_fleet_age_shift_diagnostics", shifted)
        self.assertFalse(shifted["analysis_initial_fleet_age_shift_diagnostics"].empty)
        self.assertIn("shifted_vintage_profiles_from_initial_age_shift", shifted)
        self.assertGreater(
            float(shifted["passenger_total_sales"].sum()),
            float(baseline["passenger_total_sales"].sum()),
        )
        self.assertGreater(
            float(shifted["passenger_total_retirements"].sum()),
            float(baseline["passenger_total_retirements"].sum()),
        )

    def test_subtract_turnover_policies_preserves_non_drive_terms(self):
        years = pd.Index([2030, 2031], dtype=int)
        total = {
            "LPV": {
                "additional_retirement_rate": {2030: 0.12, 2031: 0.14},
                "survival_multipliers_by_age": {0: 1.05, 1: 1.10},
            }
        }
        drive_only = {
            "LPV": {
                "additional_retirement_rate": {2030: 0.02, 2031: 0.03},
            }
        }

        out = sce_policy._subtract_turnover_policies(total, drive_only, years)

        self.assertIn("LPV", out)
        self.assertIn("survival_multipliers_by_age", out["LPV"])
        remaining = pd.Series(out["LPV"]["additional_retirement_rate"]).reindex(years).astype(float)
        np.testing.assert_allclose(remaining.values, np.array([0.10, 0.11]), atol=1e-12)


if __name__ == "__main__":
    unittest.main()
