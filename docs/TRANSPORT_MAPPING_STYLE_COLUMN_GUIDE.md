# Transport Mapping-Style Column Guide

This guide explains the key columns in files like:

- `results/checkpoint_audit/transport_mapping_style_2022_compare_20_USA_Target.csv`
- `results/checkpoint_audit/transport_pre_recon_vs_raw_disaggregated_*.csv`

Scope: these columns compare **pre-reconciliation LEAP-side values** vs **raw source values** after mapping.

## Stock Columns

- `pre_effective_stock`:
  LEAP-side fuel-level effective stock before reconciliation.
  Formula: `pre_parent_stock * pre_mode_stock_share * pre_device_share / 10000`.

- `pre_effective_stock_scaled`:
  Optional scaled version of `pre_effective_stock` (for testing stock magnitude factors).
  Usually equals `pre_effective_stock` when scale factor is `1.0`.

- `pre_parent_stock`:
  Parent stock used in effective stock calculation (usually at higher branch level, e.g. mode parent).

- `pre_mode_stock_share`:
  Stock share (%) at mode/technology parent level.

- `pre_device_share`:
  Device share (%) for the fuel branch.

- `pre_device_share_sibling_sum`:
  Sum of `Device Share` across siblings (same parent) in the post-normalized pre-recon checkpoint.
  Expected near `100` for standard share families.

- `pre_device_share_sibling_count`:
  Number of sibling rows included in `pre_device_share_sibling_sum`.

- `pre_norm_device_share_sibling_sum`:
  Sibling `Device Share` sum from the **pre-normalization** long checkpoint.
  Useful for catching oversum/undersum before normalization.

- `pre_norm_device_share_sibling_count`:
  Number of sibling rows used for `pre_norm_device_share_sibling_sum`.

- `raw_stocks`:
  Raw source stocks for the mapped source tuple (converted to absolute stocks where applicable).

- `stock_pct_diff`:
  `(pre_effective_stock - raw_stocks) / raw_stocks`.

- `stock_pct_diff_scaled`:
  `(pre_effective_stock_scaled - raw_stocks) / raw_stocks`.

- `stock_cause_flag`:
  Heuristic label for likely stock mismatch cause (for fast triage).

- `raw_stocks_reverted`:
  Optional stock-family total where alternative fuels are collapsed back to
  their fossil counterpart for comparison clarity.
  Example collapse: `Biodiesel -> Gas and diesel oil`, `Biogasoline -> Motor gasoline`.

- `pre_effective_stock_reverted`:
  Optional LEAP-side stock-family total using the same collapsed fuel family as
  `raw_stocks_reverted`.

- `pre_effective_stock_scaled_reverted`:
  Optional scaled version of `pre_effective_stock_reverted`.

- `stock_pct_diff_reverted`:
  `(pre_effective_stock_reverted - raw_stocks_reverted) / raw_stocks_reverted`.

- `stock_pct_diff_scaled_reverted`:
  `(pre_effective_stock_scaled_reverted - raw_stocks_reverted) / raw_stocks_reverted`.

- `stock_cause_flag_reverted`:
  Heuristic stock mismatch label using reverted-family stocks.

## Mileage / Efficiency / Activity / Intensity Columns

- `pre_mileage`:
  Pre-recon mileage normalized to raw-like units.

- `raw_mileage`:
  Raw weighted mileage for mapped source tuple.

- `mileage_pct_diff`:
  `(pre_mileage - raw_mileage) / raw_mileage`.

- `pre_efficiency`:
  Pre-recon fuel economy in `MJ/100km`.

- `raw_efficiency`:
  Raw weighted efficiency for mapped source tuple, converted from source
  `Billion_km_per_pj` to `MJ/100km` before comparison.

- `efficiency_pct_diff`:
  `(pre_efficiency - raw_efficiency) / raw_efficiency`.

- `pre_effective_activity`:
  Pre-recon effective activity (mainly for intensity-style branches), in
  absolute passenger-km / tonne-km style units.

- `raw_activity`:
  Raw activity for mapped source tuple, scaled from source
  `Billion_passenger_km_or_freight_tonne_km` to absolute units.

- `activity_pct_diff`:
  `(pre_effective_activity - raw_activity) / raw_activity`.

- `pre_intensity`:
  Pre-recon intensity normalized to raw-like units.

- `raw_intensity`:
  Raw weighted intensity for mapped source tuple.

- `intensity_pct_diff`:
  `(pre_intensity - raw_intensity) / raw_intensity`.

- `any_metric_over_5pct`:
  `True` if any checked metric abs pct diff exceeds `5%`.

## Mapping Relationship Columns

- `leap_branches_per_source_tuple`:
  Number of LEAP branches that map to the same source tuple in the output table.

- `source_tuples_per_leap_branch`:
  Number of source tuples mapped to the same LEAP branch in the output table.

- `mapping_relationship_type`:
  Cardinality class derived from the two counts:
  - `one_to_one`
  - `many_leap_to_one_source`
  - `one_leap_to_many_source`
  - `many_to_many`

## Quick Interpretation Tips

- Large `stock_pct_diff_scaled` with normal `mileage_pct_diff` and `efficiency_pct_diff`:
  usually a stock/share allocation issue, not unit conversion of mileage/efficiency.

- If fuel-split logic intentionally creates alternative-fuel branches, inspect
  `*_reverted` stock columns to compare in a pre-split-equivalent view.

- `pre_norm_device_share_sibling_sum` far from `100`:
  indicates share-family inconsistency before normalization.

- `mapping_relationship_type != one_to_one`:
  check cardinality logic before treating diffs as pure data quality issues.
