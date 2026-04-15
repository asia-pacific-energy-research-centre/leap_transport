# File Guide

This guide maps the active files in the current transport toolkit.

Important: the active pipeline lives under `codebase/`. The old `code/` path is not used in this repo.

## Core entrypoints

- `codebase/transport_workflow.py`
  - Thin orchestrator and runtime switch surface.
  - Runs domestic workflow, optional international workflow, and optional dashboard step.
- `codebase/functions/transport_workflow_pipeline.py`
  - Main domestic engine (input prep, mapping, LEAP export generation, reconciliation).
- `codebase/lifecycle_profile_workflow.py`
  - Upstream lifecycle generation wrapper (survival -> vintage profiles).
- `codebase/functions/international_transport_pipeline.py`
  - International bunker export workflow.

## Config and mapping layer

- `codebase/config/transport_economy_config.py`
  - Economy/scenario file paths, years, and output destinations.
- `codebase/config/basic_mappings.py`
  - Required source schema and core mapping helpers.
- `codebase/config/branch_mappings.py`
  - LEAP branch tuples and source-to-LEAP mapping definitions.
- `codebase/config/branch_expression_mapping.py`
  - Expression templates and measure-to-expression behavior.
- `codebase/config/measure_catalog.py`
- `codebase/config/measure_metadata.py`

## Processing and validation modules

- `codebase/functions/preprocessing.py`
  - Fuel allocation, stock/share normalization, and pre-export transformations.
- `codebase/functions/measure_processing.py`
  - Per-measure extraction/writes for each branch mapping.
- `codebase/functions/mappings_validation.py`
  - Mapping integrity checks and share normalization checks.
- `codebase/functions/esto_data.py`
  - ESTO “other” row insertion support.
- `codebase/functions/merged_energy_io.py`
  - Merged-energy data loading.

## Reconciliation and historical output

- `codebase/functions/energy_use_reconciliation_road.py`
  - Reconciliation math used by domestic pipeline.
- `codebase/functions/historical_exports.py`
  - Historical-context export helpers.

## Sales and lifecycle logic

- `codebase/sales_workflow.py`
  - Passenger/freight policy-aware sales wrappers.
- `codebase/functions/sales_curve_estimate.py`
  - Core survival/vintage sales estimation logic.
- `codebase/functions/lifecycle_profile_editor.py`
  - Lifecycle profile editing and vintage derivation.

## Diagnostics and dashboard

- `codebase/results_analysis/results_dashboard_workflow.py`
  - Dashboard entrypoint.
- `codebase/results_analysis/leap_series_analysis_workflow.py`
  - Series comparison orchestration.
- `codebase/results_analysis/leap_series_comparison.py`
  - Long-table + charts pipeline.
- `codebase/results_analysis/transport_pre_recon_vs_raw_disaggregated.py`
  - Pre-recon vs raw comparison table generator used by checkpoint-audit workflows.

## Runtime directories

- `data/`: input files, templates, lifecycle inputs.
- `intermediate_data/`: checkpoints used by reruns/reconciliation.
- `results/`: exports, reconciliation artifacts, diagnostics, archives.
- `data/errors/`: debug/error CSVs emitted during failed validations.

## High-level docs to read first

1. `README.md`
2. `docs/PROCESS_FLOW.md` (now includes onboarding + runbook content)
3. `docs/TRANSPORT_WORKFLOW_SWITCHES.md`
4. `docs/TROUBLESHOOTING.md`
5. `docs/LIFECYCLE_WORKFLOW.md`

## Deep architecture docs

- `docs/for ai/SYSTEM_ARCHITECTURE.md`
- `docs/for ai/MODULE_RELATIONSHIPS.md`
- `docs/for ai/CHANGE_IMPACT_MATRIX.md`
