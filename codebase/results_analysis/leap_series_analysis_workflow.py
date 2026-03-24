from __future__ import annotations

import os
from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if REPO_ROOT.exists() and str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if (REPO_ROOT / "codebase").exists() and str(REPO_ROOT / "codebase") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "codebase"))

from results_analysis.leap_series_comparison import (
    ComparisonArtifacts,
    TransportResultsComparisonConfig,
    run_transport_results_table_comparison,
)


SCENARIOS = ("Reference", "Target")
INPUT_DIR = REPO_ROOT / "results/checkpoint_audit"
OUTPUT_DIR = REPO_ROOT / "results/diagnostics/transport_results_series_comparison"
INCLUDE_ECONOMIES: tuple[str, ...] | None = None
METRICS = (
    "activity",
    "stock",
    "mileage",
    "efficiency",
    "intensity",
    "energy",
)
CHART_MODE = "line"
CHART_BACKEND = "static"  # "static" or "plotly"
BAR_YEAR_STEP = 5
BAR_INCLUDE_BASE_YEAR = True
INCLUDE_INTERNATIONAL = True
INTERNATIONAL_INPUT_DIR = "results/international"
INTERNATIONAL_MEDIUM_SUMMARY_PATH: str | Path | None = None
INCLUDE_STOCK_PROXIES = False
STOCK_PROXY_DIR = REPO_ROOT / "results/diagnostics/stock_projection_exploration"
INCLUDE_APEC_AGGREGATE = False
SERIES_CATEGORIES: tuple[str, ...] | None = None


def build_config(
    scenario: str | None = None,
    scenarios: tuple[str, ...] | None = SCENARIOS,
    input_dir: str | Path = INPUT_DIR,
    output_dir: str | Path = OUTPUT_DIR,
    include_economies: tuple[str, ...] | None = INCLUDE_ECONOMIES,
    metrics: tuple[str, ...] = METRICS,
    chart_mode: str = CHART_MODE,
    chart_backend: str = CHART_BACKEND,
    bar_year_step: int = BAR_YEAR_STEP,
    bar_include_base_year: bool = BAR_INCLUDE_BASE_YEAR,
    include_international: bool = INCLUDE_INTERNATIONAL,
    international_input_dir: str | Path = INTERNATIONAL_INPUT_DIR,
    international_medium_summary_path: str | Path | None = INTERNATIONAL_MEDIUM_SUMMARY_PATH,
    include_stock_proxies: bool = INCLUDE_STOCK_PROXIES,
    stock_proxy_dir: str | Path = STOCK_PROXY_DIR,
    include_apec_aggregate: bool = INCLUDE_APEC_AGGREGATE,
    series_categories: tuple[str, ...] | None = SERIES_CATEGORIES,
) -> TransportResultsComparisonConfig:
    print("[INFO] Building transport comparison config.")
    print(f"[INFO] scenarios={scenarios if scenarios else scenario}")
    print(f"[INFO] include_economies={include_economies if include_economies else 'all'}")
    print(f"[INFO] input_dir={input_dir}")
    print(f"[INFO] output_dir={output_dir}")
    print(f"[INFO] chart_mode={chart_mode} (bar_year_step={bar_year_step}, include_base_year={bar_include_base_year})")
    print(f"[INFO] chart_backend={chart_backend}")
    print(f"[INFO] include_international={include_international}")
    print(f"[INFO] international_input_dir={international_input_dir}")
    print(f"[INFO] include_stock_proxies={include_stock_proxies}")
    print(f"[INFO] stock_proxy_dir={stock_proxy_dir}")
    print(f"[INFO] include_apec_aggregate={include_apec_aggregate}")
    print(f"[INFO] series_categories={series_categories if series_categories else 'default'}")
    if international_medium_summary_path:
        print(f"[INFO] international_medium_summary_path={international_medium_summary_path}")
    return TransportResultsComparisonConfig(
        scenario=scenario,
        scenarios=scenarios,
        input_dir=input_dir,
        output_dir=output_dir,
        include_economies=include_economies,
        metrics=metrics,
        chart_mode=chart_mode,
        chart_backend=chart_backend,
        bar_year_step=bar_year_step,
        bar_include_base_year=bar_include_base_year,
        include_international=include_international,
        international_input_dir=international_input_dir,
        international_medium_summary_path=international_medium_summary_path,
        include_stock_proxies=include_stock_proxies,
        stock_proxy_dir=stock_proxy_dir,
        include_apec_aggregate=include_apec_aggregate,
        series_categories=series_categories,
    )


def _safe_filename_token(value: object) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        return "group"
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text)
    return safe.strip("_") or "group"


def _build_sheet_dashboards(
    output_dir: str | Path,
    comparison_long_csv: str | Path,
    charts_dir: str | Path,
) -> Path | None:
    comparison_path = Path(comparison_long_csv)
    chart_path = Path(charts_dir)
    if not comparison_path.exists() or not chart_path.exists():
        print("[INFO] comparison_long.csv or charts_dir missing; skipping dashboards.")
        return None

    df = pd.read_csv(comparison_path)
    required_cols = {"economy", "metric", "major_transport_type", "fuel_label"}
    if not required_cols.issubset(df.columns):
        print("[INFO] comparison_long.csv missing required grouping columns; skipping dashboards.")
        return None

    dashboards_dir = Path(output_dir) / "dashboards"
    dashboards_dir.mkdir(parents=True, exist_ok=True)

    unique_economies = (
        df[["economy"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["economy"])
    )
    if unique_economies.empty:
        print("[INFO] No grouping keys found for dashboards.")
        return None

    branch_files: list[tuple[str, Path, int]] = []
    metric_order = ["energy", "stock", "mileage", "efficiency", "activity", "intensity"]
    all_economies = [str(x).strip() for x in unique_economies["economy"].tolist()]
    for _, row in unique_economies.iterrows():
        economy = str(row["economy"]).strip()
        econ_df = df[df["economy"].astype(str) == economy].copy()
        if econ_df.empty:
            continue

        chart_groups: dict[str, dict[str, list[dict[str, str]]]] = {}
        metrics_found = econ_df["metric"].dropna().astype(str).unique().tolist()
        metrics = [m for m in metric_order if m in metrics_found] + sorted(
            [m for m in metrics_found if m not in metric_order]
        )
        value_cols = [
            col
            for col in [
                "input_value",
                "pre_value",
                "reconciled_value",
                "reconciled_plus_alt_value",
                "checkpoint_direct_proxy_value",
                "sales_flow_projected_proxy_value",
            ]
            if col in econ_df.columns
        ]

        def _transport_group_label(transport_type: str) -> str:
            ttype_norm = str(transport_type).strip().lower()
            is_international_air = "international air" in ttype_norm
            is_international_marine = (
                "international shipping" in ttype_norm
                or "international ship" in ttype_norm
                or "international marine" in ttype_norm
            )
            if is_international_air or is_international_marine:
                return "International transport"
            return str(transport_type).strip()

        def _metric_order_for_transport(group_label: str) -> list[str]:
            token = str(group_label).strip().lower()
            if "road" in token and "non-road" not in token and "non road" not in token:
                return ["energy", "stock", "efficiency", "mileage", "activity", "intensity"]
            return ["energy", "activity", "intensity", "stock", "efficiency", "mileage"]

        ranking_lookup: dict[tuple[str, str, str, str], float] = {}
        energy_lookup: dict[tuple[str, str, str], float] = {}
        if value_cols:
            rank_df = econ_df[
                ["metric", "major_transport_type", "fuel_label"] + value_cols
            ].copy()
            for col in value_cols:
                rank_df[col] = pd.to_numeric(rank_df[col], errors="coerce")
            grouped_rank = (
                rank_df.groupby(["metric", "major_transport_type", "fuel_label"], dropna=False)[value_cols]
                .apply(lambda d: float(pd.to_numeric(d.stack(), errors="coerce").abs().max(skipna=True) or 0.0))
                .reset_index(name="abs_max")
            )
            for _, rr in grouped_rank.iterrows():
                transport = str(rr["major_transport_type"]).strip()
                group_label = _transport_group_label(transport)
                ranking_lookup[
                    (
                        group_label,
                        str(rr["metric"]).strip(),
                        transport,
                        str(rr["fuel_label"]).strip(),
                    )
                ] = float(rr["abs_max"] or 0.0)

            energy_df = econ_df[econ_df["metric"].astype(str).str.lower().eq("energy")].copy()
            if not energy_df.empty:
                for col in value_cols:
                    energy_df[col] = pd.to_numeric(energy_df[col], errors="coerce")
                energy_df["_energy_sort"] = (
                    energy_df["reconciled_plus_alt_value"]
                    .combine_first(energy_df["reconciled_value"])
                    .combine_first(energy_df["pre_value"])
                    .combine_first(energy_df["input_value"])
                )
                by_fuel = (
                    energy_df.groupby(["major_transport_type", "fuel_label"], dropna=False)["_energy_sort"]
                    .sum(min_count=1)
                    .reset_index()
                )
                for _, er in by_fuel.iterrows():
                    transport = str(er["major_transport_type"]).strip()
                    fuel = str(er["fuel_label"]).strip()
                    group_label = _transport_group_label(transport)
                    energy_lookup[(group_label, transport, fuel)] = float(
                        pd.to_numeric(er["_energy_sort"], errors="coerce") or 0.0
                    )
        for metric in metrics:
            metric_df = econ_df[econ_df["metric"].astype(str) == metric].copy()
            if metric_df.empty:
                continue
            transport_types = sorted(metric_df["major_transport_type"].dropna().astype(str).unique().tolist())
            for ttype in transport_types:
                t_df = metric_df[metric_df["major_transport_type"].astype(str) == ttype].copy()
                fuels = sorted(t_df["fuel_label"].dropna().astype(str).unique().tolist())
                if "Total" in fuels:
                    fuels = ["Total"] + [f for f in fuels if f != "Total"]
                group_label = _transport_group_label(ttype)
                for fuel in fuels:
                    base_name = (
                        f"{_safe_filename_token(economy)}__"
                        f"{_safe_filename_token(metric)}__"
                        f"{_safe_filename_token(ttype)}__"
                        f"{_safe_filename_token(fuel)}"
                    )
                    png_path = chart_path / f"{base_name}.png"
                    svg_path = chart_path / f"{base_name}.svg"
                    html_path = chart_path / f"{base_name}.html"
                    candidates = [p for p in (png_path, svg_path, html_path) if p.exists()]
                    if not candidates:
                        continue
                    # Prefer the freshest artifact to avoid stale PNGs shadowing newer SVGs.
                    chart_path_use = max(candidates, key=lambda p: p.stat().st_mtime)
                    rel_chart = os.path.relpath(chart_path_use, start=dashboards_dir).replace("\\", "/")
                    sort_key = ranking_lookup.get(
                        (group_label, str(metric).strip(), str(ttype).strip(), str(fuel).strip()),
                        0.0,
                    )
                    energy_sort_key = energy_lookup.get((group_label, str(ttype).strip(), str(fuel).strip()), 0.0)
                    chart_groups.setdefault(group_label, {}).setdefault(str(metric).strip(), []).append(
                        {
                            "transport": ttype,
                            "fuel": fuel,
                            "metric": str(metric).strip(),
                            "sort_value": float(sort_key),
                            "energy_sort_value": float(energy_sort_key),
                            "src": rel_chart,
                            "type": chart_path_use.suffix.lower(),
                            "title": f"{economy} | {metric} | {ttype} | {fuel}",
                        }
                    )

        if not chart_groups:
            continue
        group_order_pref = [
            "Passenger road",
            "Freight road",
            "Passenger Air",
            "Passenger Rail",
            "Passenger Ship",
            "Passenger non-road",
            "Freight Air",
            "Freight Rail",
            "Freight Ship",
            "Freight non-road",
            "Pipelines",
            "International transport",
        ]
        group_order_index = {label.lower(): i for i, label in enumerate(group_order_pref)}

        def _group_sort_key(label: str) -> tuple[int, str]:
            idx = group_order_index.get(str(label).strip().lower(), 999)
            return (idx, str(label).strip().lower())

        group_items: list[dict] = []
        for group_label in sorted(chart_groups.keys(), key=_group_sort_key):
            metric_map = chart_groups[group_label]
            desired_metric_order = _metric_order_for_transport(group_label)
            metric_labels = [m for m in desired_metric_order if m in metric_map] + sorted(
                [m for m in metric_map.keys() if m not in desired_metric_order]
            )
            metric_items: list[dict] = []
            for metric_label in metric_labels:
                charts = metric_map.get(metric_label, [])
                charts_sorted = sorted(
                    charts,
                    key=lambda c: (-float(c.get("sort_value", 0.0)), str(c.get("transport", "")), str(c.get("fuel", ""))),
                )
                metric_items.append(
                    {
                        "metric": metric_label,
                        "charts": charts_sorted,
                    }
                )
            group_items.append({"label": group_label, "metrics": metric_items})
        chart_count = sum(
            len(metric_item.get("charts", []))
            for group_item in group_items
            for metric_item in group_item.get("metrics", [])
        )

        key_label = f"{economy}"
        slug = _safe_filename_token(economy)
        dashboard_file = dashboards_dir / f"{slug}.html"
        economy_options = []
        for econ_option in all_economies:
            econ_slug = _safe_filename_token(econ_option)
            selected = " selected" if econ_option == economy else ""
            economy_options.append(
                f'<option value="{econ_slug}.html"{selected}>{econ_option}</option>'
            )
        group_options = [
            f'<option value="{idx}">{item["label"]}</option>'
            for idx, item in enumerate(group_items)
        ]
        import json
        group_items_json = json.dumps(group_items)
        html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{key_label} dashboard</title>
  <style>
    body {{ margin: 0; font-family: Segoe UI, Arial, sans-serif; background: #f2f5f8; color: #111; font-size: 36px; }}
    header {{ position: sticky; top: 0; background: #123b52; color: #fff; padding: 16px 18px; z-index: 10; }}
    header h1 {{ margin: 0; font-size: 44px; }}
    .header-row {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }}
    .econ-picker {{ font-size: 30px; padding: 6px 10px; border-radius: 6px; border: none; background: #e8f2ff; color: #123b52; }}
    .top-nav {{ margin-top: 10px; }}
    .group-picker {{ font-size: 28px; padding: 8px 10px; border-radius: 6px; border: none; min-width: 560px; background: #e8f2ff; color: #123b52; }}
    .top-actions {{ display: flex; gap: 10px; margin-top: 10px; flex-wrap: wrap; }}
    .action-btn {{ font-size: 22px; padding: 8px 12px; border: none; border-radius: 6px; background: #d0e7ff; color: #123b52; cursor: pointer; }}
    main {{ padding: 14px; }}
    .note {{ background: #eef4fa; border-left: 6px solid #2a6e90; padding: 12px 16px; border-radius: 6px; font-size: 24px; line-height: 1.4; margin: 8px 0 16px 0; }}
    .group-viewer {{ margin-bottom: 16px; }}
    .group-title {{ margin: 0 0 10px 0; font-size: 42px; }}
    .fuel-table {{ display: grid; gap: 10px; }}
    .fuel-row {{ display: grid; gap: 10px; align-items: start; }}
    .fuel-row.header .cell {{ background: #dfeaf3; font-weight: 700; text-transform: capitalize; }}
    .cell {{ background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); padding: 10px; font-size: 24px; }}
    .fuel-name {{
      font-size: 20px;
      line-height: 1.05;
      writing-mode: vertical-rl;
      text-orientation: mixed;
      transform: rotate(180deg);
      white-space: nowrap;
      text-align: center;
      margin: 0 auto;
      min-height: 180px;
    }}
    .cell.empty {{ color: #7c8a98; background: #f8fbff; text-align: center; }}
    .cell img {{ width: 100%; height: auto; border: 1px solid #e0e6ec; }}
    .cell iframe {{ width: 100%; height: 420px; border: 1px solid #e0e6ec; background: #fff; }}
    @media (max-width: 1300px) {{
      .fuel-row {{ grid-template-columns: 1fr !important; }}
      .fuel-row.header {{ display: none; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-row">
      <h1>{key_label} ({chart_count} charts)</h1>
      <select class="econ-picker" onchange="if (this.value) window.location.href=this.value;">
        {''.join(economy_options)}
      </select>
    </div>
    <div class="top-nav">
      <select id="group-picker" class="group-picker">
        {''.join(group_options)}
      </select>
    </div>
    <div class="top-actions">
      <button type="button" class="action-btn" id="prev-group-btn">Previous Group</button>
      <button type="button" class="action-btn" id="next-group-btn">Next Group</button>
    </div>
  </header>
  <main>
    <section class="note">
      <strong>Chart note:</strong> Red line = Input, Blue line = Pre-reconciled. Green line = Reconciled only when reconciled values exist and no alternative-overlay series is being shown. Purple dashed line = Reconciled + alternatives (when available), and it replaces green in those charts. If reconciled values are missing for a chart, no green line is drawn. CP proxy = checkpoint-direct stock proxy series (derived from checkpoint-aligned stock path). SF proxy = sales-flow projected stock proxy series (derived from projected sales-flow stock path). Small labels are added at the right edge of line charts to help identify overlapping series. Alternatives are mapped substitute fuels grouped into their base fuel totals (e.g., biodiesel with gas/diesel oil, biogasoline with motor gasoline, bio jet kerosene with jet fuel, and biogas with natural gas). Metric headers (energy, stock, mileage, efficiency, activity, intensity) define the y-axis quantity.
    </section>
    <section class="group-viewer">
      <h3 class="group-title" id="group-title"></h3>
      <div class="grid" id="group-grid"></div>
    </section>
    <section class="note">
      <strong>Reminder:</strong> Compare lines within the same chart and fuel card. The legend labels describe data source state (Input/Pre-reconciled/Reconciled/Reconciled + alternatives), while metric, transport type, and fuel labels describe what is being measured.
    </section>
  </main>
  <script>
    const groupItems = {group_items_json};
    const groupPicker = document.getElementById('group-picker');
    const groupTitle = document.getElementById('group-title');
    const groupGrid = document.getElementById('group-grid');

    function renderGroup(index) {{
      const i = Math.max(0, Math.min(groupItems.length - 1, Number(index) || 0));
      groupPicker.value = String(i);
      const group = groupItems[i];
      groupTitle.textContent = group.label;
      const metrics = (group.metrics || []).map((m) => String(m.metric || '').trim()).filter(Boolean);
      const flatCharts = [];
      (group.metrics || []).forEach((metricItem) => {{
        const metricLabel = String(metricItem.metric || '').trim();
        (metricItem.charts || []).forEach((chart) => {{
          flatCharts.push({{ ...chart, metric: metricLabel }});
        }});
      }});

      const fuelCount = {{}};
      flatCharts.forEach((c) => {{
        const k = String(c.fuel || '').trim();
        if (!k) return;
        fuelCount[k] = (fuelCount[k] || 0) + 1;
      }});
      const hasDuplicateFuel = Object.values(fuelCount).some((v) => v > 1);

      const rowsByKey = new Map();
            flatCharts.forEach((chart) => {{
                const fuel = String(chart.fuel || '').trim();
                const transport = String(chart.transport || '').trim();
                const metric = String(chart.metric || '').trim();
                const rowLabel = hasDuplicateFuel && transport ? `${{transport}} | ${{fuel}}` : fuel;
                const rowKey = hasDuplicateFuel ? `${{transport}}|||${{fuel}}` : fuel;
                if (!rowsByKey.has(rowKey)) {{
                  rowsByKey.set(rowKey, {{
                    label: rowLabel,
                    maxSort: Number(chart.sort_value || 0) || 0,
                    energySort: Number(chart.energy_sort_value || 0) || 0,
                    byMetric: {{}},
                  }});
                }}
                const row = rowsByKey.get(rowKey);
                row.maxSort = Math.max(row.maxSort, Number(chart.sort_value || 0) || 0);
                row.energySort = Math.max(row.energySort, Number(chart.energy_sort_value || 0) || 0);
                row.byMetric[metric] = chart;
              }});

      const rowItems = Array.from(rowsByKey.values()).sort(
        (a, b) => b.energySort - a.energySort || b.maxSort - a.maxSort || a.label.localeCompare(b.label)
      );
      const colCount = metrics.length + 1;
      const rowTemplate = `minmax(84px, 110px) repeat(${{metrics.length}}, minmax(320px, 1fr))`;

      const headerCells = [`<div class="cell">Fuel</div>`].concat(metrics.map((m) => `<div class="cell">${{m}}</div>`));
      const rowsHtml = [
        `<div class="fuel-row header" style="grid-template-columns:${{rowTemplate}};">${{headerCells.join('')}}</div>`
      ];
      rowItems.forEach((row) => {{
        const cells = [`<div class="cell fuel-name">${{row.label}}</div>`];
        metrics.forEach((metric) => {{
          const chart = row.byMetric[metric];
          if (!chart) {{
            cells.push('<div class="cell empty">-</div>');
            return;
          }}
          if (chart.type === '.html') {{
            cells.push(`<div class="cell"><iframe src="${{chart.src}}" title="${{chart.title || row.label}}" loading="lazy"></iframe></div>`);
          }} else {{
            cells.push(`<div class="cell"><img src="${{chart.src}}" alt="${{chart.title || row.label}}" loading="lazy" /></div>`);
          }}
        }});
        rowsHtml.push(`<div class="fuel-row" style="grid-template-columns:${{rowTemplate}};">${{cells.join('')}}</div>`);
      }});
      groupGrid.innerHTML = `<section class="fuel-table" data-cols="${{colCount}}">${{rowsHtml.join('')}}</section>`;
    }}

    groupPicker.addEventListener('change', () => renderGroup(groupPicker.value));
    document.getElementById('prev-group-btn').addEventListener('click', () => {{
      const i = Math.max(0, (Number(groupPicker.value) || 0) - 1);
      renderGroup(i);
    }});
    document.getElementById('next-group-btn').addEventListener('click', () => {{
      const i = Math.min(groupItems.length - 1, (Number(groupPicker.value) || 0) + 1);
      renderGroup(i);
    }});
    renderGroup(0);
  </script>
</body>
</html>
"""
        dashboard_file.write_text(html_doc, encoding="utf-8")
        branch_files.append((key_label, dashboard_file, chart_count))

    if not branch_files:
        print("[INFO] No dashboard pages generated.")
        return None

    links = [
        f'<li><a href="{file_path.name}">{label}</a> ({count} charts)</li>'
        for label, file_path, count in branch_files
    ]
    index_file = dashboards_dir / "index.html"
    index_file.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Transport Dashboards</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 24px; background: #f4f6f8; color: #111; font-size: 20px; }}
    h1 {{ font-size: 34px; }}
    p {{ font-size: 22px; }}
    a {{ color: #123b52; text-decoration: none; font-size: 20px; }}
    a:hover {{ text-decoration: underline; }}
    ul {{ line-height: 1.9; }}
  </style>
</head>
<body>
  <h1>Transport Comparison Dashboards</h1>
  <p>{len(branch_files)} dashboards generated.</p>
  <ul>{''.join(links)}</ul>
</body>
</html>
""",
        encoding="utf-8",
    )
    print(f"[INFO] Generated dashboards: {index_file}")
    return index_file


def run_with_config(config: TransportResultsComparisonConfig | None = None) -> ComparisonArtifacts:
    cfg = config or build_config()
    print("[INFO] Running comparison workflow with prepared config.")
    artifacts = run_transport_results_table_comparison(cfg)
    print("[OK] LEAP series analysis finished.")
    print(f"- comparison_long_csv: {artifacts.comparison_long_csv}")
    print(f"- comparison_summary_csv: {artifacts.comparison_summary_csv}")
    print(f"- charts_dir: {artifacts.charts_dir}")
    dashboard_index = _build_sheet_dashboards(
        output_dir=artifacts.comparison_long_csv.parent,
        comparison_long_csv=artifacts.comparison_long_csv,
        charts_dir=artifacts.charts_dir,
    )
    if dashboard_index:
        print(f"- dashboard_index: {dashboard_index}")
    return artifacts


if __name__ == "__main__":
    run_with_config()
