from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import argparse

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class ExplorationConfig:
    economy: str = "20_USA"
    scenarios: tuple[str, ...] = ("Reference", "Target")
    base_year: int = 2022
    final_year: int = 2060
    output_dir: Path = REPO_ROOT / "results/diagnostics/stock_projection_exploration"


def _resolve_transport_checkpoint(economy: str, scenario: str, base_year: int, final_year: int) -> Path:
    return REPO_ROOT / f"intermediate_data/transport_data_{economy}_{scenario}_{base_year}_{final_year}.pkl"


def _clean_transport_type(value: object) -> str:
    text = str(value).strip().lower()
    if text.startswith("combined "):
        text = text.replace("combined ", "", 1)
    return text


def _prepare_road_detail(df: pd.DataFrame, *, scenario: str, base_year: int, final_year: int) -> pd.DataFrame:
    work = df.copy()
    work["Scenario"] = work["Scenario"].astype(str).str.strip()
    work = work[work["Scenario"] == scenario].copy()
    work["Date"] = pd.to_numeric(work["Date"], errors="coerce")
    work = work[(work["Date"] >= base_year) & (work["Date"] <= final_year)].copy()
    work = work[work["Medium"].astype(str).str.strip().str.lower() == "road"].copy()

    work["transport_type_norm"] = work["Transport Type"].map(_clean_transport_type)
    work["is_combined_transport_type"] = (
        work["Transport Type"].astype(str).str.strip().str.lower().str.startswith("combined ")
    )
    work = work[work["transport_type_norm"].isin({"passenger", "freight"})].copy()
    # Prefer direct passenger/freight rows and drop "Combined ..." rows to avoid
    # double counting the same road stocks/sales in aggregated proxies.
    work = work[~work["is_combined_transport_type"]].copy()

    for col in ["Stocks", "Sales", "Turnover_rate", "Stock_turnover"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    detail_cols = [
        "Date",
        "transport_type_norm",
        "Vehicle Type",
        "Drive",
        "Fuel",
        "Stocks",
        "Sales",
        "Turnover_rate",
        "Stock_turnover",
    ]
    return work[detail_cols].copy()


def _project_stocks_from_sales(detail_df: pd.DataFrame, *, base_year: int) -> pd.DataFrame:
    key_cols = ["transport_type_norm", "Vehicle Type", "Drive", "Fuel"]
    out_rows: list[dict] = []

    grouped = detail_df.groupby(key_cols, dropna=False)
    for key, grp in grouped:
        g = grp.sort_values("Date").copy()
        if g.empty:
            continue

        # Per-series default retirement rate fallback.
        series_rate_median = pd.to_numeric(g["Turnover_rate"], errors="coerce").dropna().median()
        if pd.isna(series_rate_median):
            series_rate_median = 0.03

        years = sorted(g["Date"].dropna().astype(int).unique().tolist())
        if base_year not in years:
            continue

        g_by_year = g.groupby("Date", as_index=False)[["Stocks", "Sales", "Turnover_rate", "Stock_turnover"]].sum(min_count=1)
        g_by_year["Date"] = g_by_year["Date"].astype(int)
        g_by_year = g_by_year.set_index("Date").sort_index()

        prev_stock = pd.to_numeric(g_by_year.loc[base_year, "Stocks"], errors="coerce")
        if pd.isna(prev_stock):
            prev_stock = 0.0

        out_rows.append({
            "Date": int(base_year),
            "transport_type_norm": key[0],
            "Vehicle Type": key[1],
            "Drive": key[2],
            "Fuel": key[3],
            "projected_stock": float(prev_stock),
            "retirement_rate_used": pd.NA,
        })

        for year in [y for y in years if y > base_year]:
            sales_t = pd.to_numeric(g_by_year.loc[year, "Sales"], errors="coerce")
            sales_t = 0.0 if pd.isna(sales_t) else float(sales_t)

            rate_t = pd.to_numeric(g_by_year.loc[year, "Turnover_rate"], errors="coerce")
            if pd.isna(rate_t):
                stock_turnover_t = pd.to_numeric(g_by_year.loc[year, "Stock_turnover"], errors="coerce")
                if pd.notna(stock_turnover_t) and prev_stock > 1e-12:
                    rate_t = float(stock_turnover_t) / float(prev_stock)
                else:
                    rate_t = float(series_rate_median)

            rate_t = max(0.0, min(float(rate_t), 1.0))
            projected = prev_stock * (1.0 - rate_t) + sales_t
            projected = max(float(projected), 0.0)

            out_rows.append({
                "Date": int(year),
                "transport_type_norm": key[0],
                "Vehicle Type": key[1],
                "Drive": key[2],
                "Fuel": key[3],
                "projected_stock": projected,
                "retirement_rate_used": rate_t,
            })
            prev_stock = projected

    return pd.DataFrame(out_rows)


def _major_transport_type_from_norm(transport_type_norm: str) -> str:
    return "Passenger road" if transport_type_norm == "passenger" else "Freight road"


def _aggregate_for_chart_compare(detail_df: pd.DataFrame, projected_df: pd.DataFrame, *, scenario: str) -> pd.DataFrame:
    direct = detail_df.groupby(["Date", "transport_type_norm", "Fuel"], dropna=False)["Stocks"].sum(min_count=1).reset_index()
    direct = direct.rename(columns={"Stocks": "checkpoint_direct_stock", "Fuel": "fuel_label"})

    projected = projected_df.groupby(["Date", "transport_type_norm", "Fuel"], dropna=False)["projected_stock"].sum(min_count=1).reset_index()
    projected = projected.rename(columns={"Fuel": "fuel_label"})

    merged = direct.merge(projected, on=["Date", "transport_type_norm", "fuel_label"], how="outer")
    merged["scenario"] = scenario
    merged["major_transport_type"] = merged["transport_type_norm"].map(_major_transport_type_from_norm)
    direct_totals = merged.groupby(["Date", "transport_type_norm"], dropna=False)["checkpoint_direct_stock"].transform("sum")
    projected_totals = merged.groupby(["Date", "transport_type_norm"], dropna=False)["projected_stock"].transform("sum")
    merged["checkpoint_direct_stock_share"] = merged["checkpoint_direct_stock"] / direct_totals.replace(0, pd.NA)
    merged["projected_stock_share"] = merged["projected_stock"] / projected_totals.replace(0, pd.NA)
    merged["delta_projected_share_minus_direct_share"] = (
        merged["projected_stock_share"] - merged["checkpoint_direct_stock_share"]
    )
    merged["delta_projected_minus_direct"] = merged["projected_stock"] - merged["checkpoint_direct_stock"]
    denom = merged["checkpoint_direct_stock"].replace(0, pd.NA)
    merged["pct_delta_projected_minus_direct"] = merged["delta_projected_minus_direct"] / denom
    return merged


def _build_vehicle_type_share_compare(detail_df: pd.DataFrame, projected_df: pd.DataFrame, *, scenario: str) -> pd.DataFrame:
    keys = ["Date", "transport_type_norm", "Vehicle Type"]
    direct = (
        detail_df.groupby(keys, dropna=False)["Stocks"]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"Stocks": "checkpoint_direct_stock"})
    )
    projected = (
        projected_df.groupby(keys, dropna=False)["projected_stock"]
        .sum(min_count=1)
        .reset_index()
    )
    merged = direct.merge(projected, on=keys, how="outer")
    merged["scenario"] = scenario
    merged["major_transport_type"] = merged["transport_type_norm"].map(_major_transport_type_from_norm)

    direct_totals = merged.groupby(["Date", "transport_type_norm"], dropna=False)["checkpoint_direct_stock"].transform("sum")
    projected_totals = merged.groupby(["Date", "transport_type_norm"], dropna=False)["projected_stock"].transform("sum")
    merged["checkpoint_direct_stock_share"] = merged["checkpoint_direct_stock"] / direct_totals.replace(0, pd.NA)
    merged["projected_stock_share"] = merged["projected_stock"] / projected_totals.replace(0, pd.NA)
    merged["delta_projected_minus_direct"] = merged["projected_stock"] - merged["checkpoint_direct_stock"]
    merged["delta_projected_share_minus_direct_share"] = (
        merged["projected_stock_share"] - merged["checkpoint_direct_stock_share"]
    )
    return merged


def _join_with_comparison_long(df: pd.DataFrame, *, economy: str) -> pd.DataFrame:
    cmp_path = REPO_ROOT / "results/diagnostics/transport_results_series_comparison/comparison_long.csv"
    if not cmp_path.exists():
        return df

    cmp_df = pd.read_csv(cmp_path, low_memory=False)
    cmp_df = cmp_df[
        (cmp_df["economy"].astype(str).str.strip() == economy)
        & (cmp_df["metric"].astype(str).str.strip().str.lower() == "stock")
    ].copy()

    keep_cols = [
        "scenario",
        "major_transport_type",
        "fuel_label",
        "year",
        "input_value",
        "pre_value",
        "reconciled_value",
        "input_plus_alt_value",
        "pre_plus_alt_value",
        "reconciled_plus_alt_value",
    ]
    cmp_df = cmp_df[keep_cols].rename(columns={"year": "Date"})

    return df.merge(
        cmp_df,
        on=["scenario", "major_transport_type", "fuel_label", "Date"],
        how="left",
    )


def run_exploration(cfg: ExplorationConfig) -> list[Path]:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for scenario in cfg.scenarios:
        ckpt_path = _resolve_transport_checkpoint(cfg.economy, scenario, cfg.base_year, cfg.final_year)
        if not ckpt_path.exists():
            raise FileNotFoundError(f"Missing checkpoint: {ckpt_path}")

        df = pd.read_pickle(ckpt_path)
        detail_df = _prepare_road_detail(df, scenario=scenario, base_year=cfg.base_year, final_year=cfg.final_year)
        projected_df = _project_stocks_from_sales(detail_df, base_year=cfg.base_year)

        detail_out = cfg.output_dir / f"{cfg.economy}_{scenario}_road_stock_detail_with_projection.csv"
        detail_merged = detail_df.merge(
            projected_df,
            on=["Date", "transport_type_norm", "Vehicle Type", "Drive", "Fuel"],
            how="left",
        )
        detail_merged.to_csv(detail_out, index=False)
        written.append(detail_out)

        agg = _aggregate_for_chart_compare(detail_df, projected_df, scenario=scenario)
        agg_joined = _join_with_comparison_long(agg, economy=cfg.economy)
        agg_out = cfg.output_dir / f"{cfg.economy}_{scenario}_road_stock_projection_vs_chart_series.csv"
        agg_joined.to_csv(agg_out, index=False)
        written.append(agg_out)

        share_df = _build_vehicle_type_share_compare(detail_df, projected_df, scenario=scenario)
        share_out = cfg.output_dir / f"{cfg.economy}_{scenario}_road_vehicle_type_stock_share_compare.csv"
        share_df.to_csv(share_out, index=False)
        written.append(share_out)

    return written


def _parse_args() -> ExplorationConfig:
    parser = argparse.ArgumentParser(description="Explore post-2022 stock/share estimates from checkpoint and sales-flow projection.")
    parser.add_argument("--economy", default="20_USA")
    parser.add_argument("--scenarios", nargs="+", default=["Reference", "Target"])
    parser.add_argument("--base-year", type=int, default=2022)
    parser.add_argument("--final-year", type=int, default=2060)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "results/diagnostics/stock_projection_exploration"))
    args = parser.parse_args()

    return ExplorationConfig(
        economy=str(args.economy),
        scenarios=tuple(str(s) for s in args.scenarios),
        base_year=int(args.base_year),
        final_year=int(args.final_year),
        output_dir=Path(args.output_dir),
    )


if __name__ == "__main__":
    cfg = _parse_args()
    outputs = run_exploration(cfg)
    print("[OK] Wrote stock projection exploration outputs:")
    for out in outputs:
        print(f"- {out}")
