import sys
import unittest
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
CODE_DIR = REPO_ROOT / "codebase"
FUNCTIONS_DIR = CODE_DIR / "functions"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))
if str(FUNCTIONS_DIR) not in sys.path:
    sys.path.insert(0, str(FUNCTIONS_DIR))

from config.branch_mappings import LEAP_MEASURE_CONFIG
from functions.mappings_validation import calculate_energy_use_for_intensity_analysis_branch
from functions.transport_branch_paths import (
    TRANSPORT_ROOT,
    build_transport_branch_path,
    extract_transport_branch_tuple,
    is_non_road_transport_branch_path,
    is_pipeline_or_nonspecified_branch_path,
)
from results_analysis.transport_pre_recon_vs_raw_disaggregated import _pre_metrics_for_branch


class TransportBranchPathTests(unittest.TestCase):
    def test_build_path_routes_non_road_branches_under_container(self):
        self.assertEqual(
            build_transport_branch_path(("Passenger non road", "Air", "Electricity")),
            r"Demand\Transport non road\Passenger non road\Air\Electricity",
        )
        self.assertEqual(
            build_transport_branch_path(("Freight non road", "Rail", "Biodiesel")),
            r"Demand\Transport non road\Freight non road\Rail\Biodiesel",
        )
        self.assertEqual(
            build_transport_branch_path(("Pipeline transport", "Natural gas")),
            r"Demand\Transport non road\Pipeline transport\Natural gas",
        )
        self.assertEqual(
            build_transport_branch_path(("Nonspecified transport", "Fuel oil")),
            r"Demand\Transport non road\Nonspecified transport\Fuel oil",
        )
        self.assertEqual(
            build_transport_branch_path(("International transport", "Air", "Hydrogen")),
            r"Demand\Transport non road\International transport\Air\Hydrogen",
        )

    def test_build_path_keeps_road_branches_at_root(self):
        self.assertEqual(
            build_transport_branch_path(("Passenger road", "LPVs", "BEV small", "Electricity")),
            r"Demand\Passenger road\LPVs\BEV small\Electricity",
        )
        self.assertEqual(
            build_transport_branch_path(("Freight road", "Trucks", "ICE heavy", "Gas and diesel oil")),
            r"Demand\Freight road\Trucks\ICE heavy\Gas and diesel oil",
        )

    def test_extract_branch_tuple_supports_old_and_new_non_road_paths(self):
        new_path = r"Demand\Transport non road\Passenger non road\Shipping\Ammonia"
        old_path = r"Demand\Passenger non road\Shipping\Ammonia"
        expected = ("Passenger non road", "Shipping", "Ammonia")

        self.assertEqual(extract_transport_branch_tuple(new_path), expected)
        self.assertEqual(extract_transport_branch_tuple(old_path), expected)
        self.assertTrue(is_non_road_transport_branch_path(new_path))
        self.assertTrue(is_non_road_transport_branch_path(old_path))

    def test_extract_branch_tuple_supports_new_and_old_international_paths(self):
        new_path = r"Demand\Transport non road\International transport\Shipping\Fuel oil"
        old_path = r"Demand\International transport\Shipping\Fuel oil"
        expected = ("International transport", "Shipping", "Fuel oil")

        self.assertEqual(extract_transport_branch_tuple(new_path), expected)
        self.assertEqual(extract_transport_branch_tuple(old_path), expected)
        self.assertTrue(is_non_road_transport_branch_path(new_path))

    def test_pipeline_and_nonspecified_classification_supports_old_and_new_paths(self):
        pipeline_new = r"Demand\Transport non road\Pipeline transport\Electricity"
        pipeline_old = r"Demand\Pipeline transport\Electricity"
        nonspecified_new = r"Demand\Transport non road\Nonspecified transport\Kerosene"
        nonspecified_old = r"Demand\Nonspecified transport\Kerosene"

        for path in (pipeline_new, pipeline_old, nonspecified_new, nonspecified_old):
            self.assertTrue(is_pipeline_or_nonspecified_branch_path(path))

    def test_pre_metrics_non_road_new_root_uses_two_share_structure(self):
        branch_tuple = ("Passenger non road", "Air", "Electricity")
        branch_path = build_transport_branch_path(branch_tuple, root=TRANSPORT_ROOT)
        mode_path = build_transport_branch_path(branch_tuple[:-1], root=TRANSPORT_ROOT)
        parent_path = build_transport_branch_path(branch_tuple[:-2], root=TRANSPORT_ROOT)
        lookup = {
            (parent_path, "Activity Level", 2022): 200_000_000_000.0,
            (mode_path, "Activity Level", 2022): 50.0,
            (branch_path, "Activity Level", 2022): 40.0,
            (branch_path, "Final Energy Intensity", 2022): 2.5,
        }

        metrics = _pre_metrics_for_branch(
            lookup=lookup,
            device_share_parent_sum_lookup={},
            device_share_parent_count_lookup={},
            leap_branch_tuple=branch_tuple,
            analysis_type="Intensity",
            date=2022,
        )

        self.assertEqual(metrics["pre_effective_activity"], 40_000_000_000.0)
        self.assertEqual(metrics["pre_intensity"], 2500.0)

    def test_validation_intensity_energy_handles_new_non_road_root(self):
        branch_tuple = ("Passenger non road", "Air", "Electricity")
        branch_path = build_transport_branch_path(branch_tuple, root=TRANSPORT_ROOT)
        mode_path = build_transport_branch_path(branch_tuple[:-1], root=TRANSPORT_ROOT)
        parent_path = build_transport_branch_path(branch_tuple[:-2], root=TRANSPORT_ROOT)
        export_df = pd.DataFrame(
            {
                "Branch Path": [parent_path, mode_path, branch_path, branch_path],
                "Variable": [
                    "Activity Level",
                    "Activity Level",
                    "Activity Level",
                    "Final Energy Intensity",
                ],
                2022: [1_000_000_000.0, 25.0, 40.0, 1_000_000.0],
            }
        )

        activity_scale = LEAP_MEASURE_CONFIG["Fuel (non-road)"]["Activity Level"]["factor"]
        intensity_scale = LEAP_MEASURE_CONFIG["Fuel (non-road)"]["Final Energy Intensity"]["factor"]
        expected_energy = (
            (1_000_000_000.0 * 25.0 * 40.0 / 10000.0) / activity_scale
        ) * (1_000_000.0 / intensity_scale)

        result = calculate_energy_use_for_intensity_analysis_branch(
            branch_path=branch_path,
            branch_tuple=branch_tuple,
            export_df=export_df,
            BASE_YEAR=2022,
        )

        self.assertAlmostEqual(result, expected_energy, places=12)

    def test_validation_intensity_energy_handles_new_pipeline_root(self):
        branch_tuple = ("Pipeline transport", "Electricity")
        branch_path = build_transport_branch_path(branch_tuple, root=TRANSPORT_ROOT)
        parent_path = build_transport_branch_path(branch_tuple[:-1], root=TRANSPORT_ROOT)
        export_df = pd.DataFrame(
            {
                "Branch Path": [parent_path, branch_path, branch_path],
                "Variable": [
                    "Activity Level",
                    "Activity Level",
                    "Final Energy Intensity",
                ],
                2022: [500_000_000.0, 20.0, 1.5],
            }
        )

        activity_scale = LEAP_MEASURE_CONFIG["Others (level 2)"]["Activity Level"]["factor"]
        expected_energy = ((500_000_000.0 * 20.0 / 100.0) / activity_scale) * 1.5

        result = calculate_energy_use_for_intensity_analysis_branch(
            branch_path=branch_path,
            branch_tuple=branch_tuple,
            export_df=export_df,
            BASE_YEAR=2022,
        )

        self.assertAlmostEqual(result, expected_energy, places=12)


if __name__ == "__main__":
    unittest.main()
