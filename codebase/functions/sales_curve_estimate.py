#%% imports
import os
from pathlib import Path
import numpy as np
import pandas as pd

from functions.merged_energy_io import load_transport_energy_dataset
from functions.path_utils import resolve_str
from functions.lifecycle_profile_editor import (
    build_vintage_from_survival_excel,
    load_lifecycle_profile_excel,
)

__all__ = [
    "logistic_envelope_from_base",
    "estimate_recent_energy_growth",
    "estimate_k_from_energy_trend",
    "load_survival_curve",
    "load_vintage_profile",
    "load_survival_and_vintage_profiles",
    "extract_energy_use_from_esto",
    "aggregate_base_stocks",
    "compute_base_capacity_index",
    "envelope_to_target_stocks",
    "initialise_cohorts",
    "compute_sales_from_stock_targets",
    "build_passenger_sales_for_economy",
    "estimate_passenger_sales_from_dataframe",
    "estimate_passenger_sales_from_files",
    "build_freight_sales_for_economy",
    "estimate_freight_sales_from_dataframe",
    "estimate_freight_sales_from_files",
    "plot_passenger_sales_result",
    "plot_freight_sales_result",
    "plot_transport_sales_dashboard",
]

# Optional manual saturation overrides keyed by (transport_type, economy, scenario)
# transport_type should be "passenger" or "freight" (case-insensitive).
# Example: {("passenger", "20_USA", "Target"): 1.3}
M_SAT_OVERRIDES: dict[tuple[str, str, str], float] = {}


# Default parameter bundles for quick access/config at top of file
DEFAULT_PASSENGER_M_SAT_CAP = 0.9
DEFAULT_PASSENGER_M_SAT_MULTIPLIER = 2.0
DEFAULT_PASSENGER_WEIGHTS = {"LPV": 1.0, "MC": 0.3, "Bus": 20.0}
DEFAULT_PASSENGER_VEHICLE_TYPE_MAP = {
    "car": "LPV",
    "suv": "LPV",
    "lt": "LPV",
    "lpv": "LPV",
    "2w": "MC",
    "mc": "MC",
    "bus": "Bus",
}

DEFAULT_FREIGHT_M_SAT_MULTIPLIER = 10.0
DEFAULT_FREIGHT_WEIGHTS = {"Trucks": 1.0, "LCVs": 0.5}
DEFAULT_FREIGHT_VEHICLE_TYPE_MAP = {
    "ht": "Trucks",
    "mt": "Trucks",
    "truck": "Trucks",
    "trucks": "Trucks",
    "lcv": "LCVs",
    "van": "LCVs",
    "lt": "LCVs",
}

USE_9TH_VEHICLE_TYPE_SALES_SHARES = False  # if True, build vehicle_shares from Vehicle_sales_share column
DEFAULT_PLOTTING_OUTPUT_DIR = (Path(__file__).resolve().parents[2] / "plotting_output").resolve()


def _raise_plot_failure(context: str, exc: Exception) -> None:
    """Raise a contextual plotting error instead of failing silently."""
    raise RuntimeError(
        f"{context} failed ({exc.__class__.__name__}): {exc}"
    ) from exc

#%% helper to fetch manual M_sat overrides

def _get_manual_M_sat(
    transport_type: str,
    economy: str | None,
    scenario: str | None,
) -> float | None:
    if economy is None or scenario is None:
        return None
    key = (str(transport_type).lower(), str(economy), str(scenario))
    return M_SAT_OVERRIDES.get(key)
#%% helpers: S-curve and k from energy trend

def logistic_envelope_from_base(
    years: pd.Index,
    M_base: float,
    M_sat: float,
    k: float,
    base_year: int,
) -> pd.Series:
    """
    Generate an S-curve (logistic) time series for the capacity-weighted
    motorisation envelope M(y), anchored on the base-year value M_base.

    M(y) = M_sat / (1 + exp(-k * (y - y0)))

    y0 is chosen so that M(base_year) == M_base.

    If k <= 0 or M_base >= M_sat, returns a flat series at the base level
    (capped at M_sat).
    """
    years = pd.Index(years)

    if k <= 0.0 or M_base >= 0.999 * M_sat:
        return pd.Series(min(M_base, M_sat), index=years, dtype=float)

    # y0 such that M(base_year) = M_base
    y0 = base_year + (1.0 / k) * np.log(M_sat / M_base - 1.0)

    y_array = years.to_numpy(dtype=float)
    M_array = M_sat / (1.0 + np.exp(-k * (y_array - y0)))

    return pd.Series(M_array, index=years, dtype=float)


def estimate_recent_energy_growth(
    energy_use: pd.Series,
    base_year: int,
    window_years: int = 10,
) -> float:
    """
    Estimate the recent average *annual* growth rate of energy use from the
    last `window_years` ending at base_year.

    Uses mean log-differences to get a geometric-average growth rate.

    Returns a decimal rate, e.g. 0.03 for 3%/year.
    """
    hist = energy_use.loc[energy_use.index <= base_year].sort_index()

    if len(hist) < 2:
        return 0.0  # fallback: assume flat if insufficient history

    n_points = min(len(hist), window_years + 1)
    hist_window = hist.iloc[-n_points:]

    vals = np.maximum(hist_window.values, 1e-9)

    growth_logs = np.log(vals[1:] / vals[:-1])
    g = float(np.mean(growth_logs))  # average log growth

    return g  # still in log units; exp(g) - 1 ~ g for small g


def estimate_k_from_energy_trend(
    M_base: float,
    M_sat: float,
    energy_use: pd.Series,
    base_year: int,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
) -> float:
    """
    Choose logistic steepness k so that the *initial* relative growth
    of M(y) is consistent with recent energy growth.

    Approximate relationship:
        (1/M * dM/dt at base) ~ k * (1 - M_base/M_sat) ~ g_E

    So:
        k ~ g_E / (1 - M_base/M_sat)

    where g_E is the recent geometric-average energy growth rate.
    """
    g_E = estimate_recent_energy_growth(energy_use, base_year, window_years)

    if g_E <= 0.0 or M_base >= 0.999 * M_sat:
        return 0.0

    gap = max(1e-6, 1.0 - M_base / M_sat)
    k_raw = g_E / gap

    k = max(k_min, min(k_raw, k_max))
    return float(k)

#%% data loaders: lifecycle profiles, energy, and base stocks


def _read_profile_excel(path: str | os.PathLike[str], value_scale: float = 1.0) -> pd.Series:
    """
    Load a lifecycle profile from Excel.

    Expects a sheet 'Lifecycle Profiles' with columns [Year, Value] after a
    short header block. Returns a Series indexed by year.
    """
    df = pd.read_excel(
        path,
        sheet_name="Lifecycle Profiles",
        skiprows=3,
        names=["Year", "Value"],
    )
    df = df.dropna(subset=["Year"])
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce") * value_scale
    df = df.dropna(subset=["Year", "Value"])
    df = df[df["Year"].astype(int) == df["Year"]]
    df["Year"] = df["Year"].astype(int)
    return pd.Series(df["Value"].values, index=pd.Index(df["Year"].values, dtype=int))


def load_survival_curve(path: str | os.PathLike[str]) -> pd.Series:
    """
    Load a survival curve and return probabilities (0-1) by age.
    Values stored as percentages are converted to probabilities.
    """
    series = _read_profile_excel(path)
    if series.max() > 1.0:
        series = series / 100.0
    return series.astype(float)


def load_vintage_profile(path: str | os.PathLike[str]) -> pd.Series:
    """
    Load a vintage profile and normalise so it sums to 1.0.
    """
    series = _read_profile_excel(path)
    if not series.empty:
        # Enforce zero stock in the first vintage year before normalising
        first_year = series.index.min()
        series.loc[first_year] = 0.0
    total = series.sum()
    if total > 0:
        series = series / total
    return series.astype(float)


def load_survival_and_vintage_profiles(
    survival_path: str | os.PathLike[str],
    vintage_path: str | os.PathLike[str],
    vehicle_keys: tuple[str, ...] = ("LPV", "MC", "Bus"),
    survival_is_cumulative: bool = True,
) -> tuple[dict, dict]:
    """
    Convenience to load the same survival/vintage profile for each vehicle key.

    If survival_is_cumulative is True, converts the loaded survival curve
    (% remaining by age) into annual survival probabilities.
    """
    survival_curve = load_survival_curve(survival_path)
    if survival_is_cumulative:
        survival_curve = _convert_cumulative_survival_to_annual(survival_curve)
    vintage_profile = load_vintage_profile(vintage_path)
    survival_curves = {k: survival_curve for k in vehicle_keys}
    vintage_profiles = {k: vintage_profile for k in vehicle_keys}
    return survival_curves, vintage_profiles


def extract_energy_use_from_esto(
    esto_path: str | os.PathLike[str],
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    sector: str = "15_transport_sector",
    sub1sector: str | None = None,
    sheet_name: str = "all econs",
) -> pd.Series:
    """
    Pull transport energy use series from the ESTO balances file.

    Filters for the given economy, scenario, and sector, then sums across rows
    for each year column.
    """
    esto_path = resolve_str(esto_path)
    if esto_path is None:
        raise ValueError("esto_path cannot be None.")

    df = load_transport_energy_dataset(
        esto_path,
        economy=economy,
        sector=sector,
        sheet_name=sheet_name,
    )
    mask = (
        (df["economy"] == economy)
        & (df["scenarios"] == scenario.lower())
        & ((df["sub1sectors"] == sub1sector) if sub1sector is not None else True)
    ) 
    df = df.loc[mask]
    #drop any ciols with Unnamed in them
    column_labels = pd.Index(df.columns.map(str))
    df = df.loc[:, ~column_labels.str.contains(r"^Unnamed", na=False)]
    #also, we need to split it by <=base year and >base year then drop subtotals depending onwhat years we have:
    #first make the year cols numeric
    year_cols = sorted([col for col in df.columns if isinstance(col, int) and base_year <= col <= final_year])
    if not year_cols:
        raise ValueError(f"No year columns found between {base_year} and {final_year}.")
    others = [col for col in df.columns if col not in year_cols]
    merge_keys = [col for col in others if col not in {"subtotal_layout", "subtotal_results"}]
    historical_cols = [col for col in year_cols if col <= base_year]
    future_cols = [col for col in year_cols if col > base_year]

    frames = []
    if historical_cols:
        if "subtotal_layout" in df.columns:
            historical = df.loc[df["subtotal_layout"] != True, merge_keys + historical_cols]
        else:
            historical = df[merge_keys + historical_cols]
        frames.append(historical)
    if future_cols:
        if "subtotal_results" in df.columns:
            future = df.loc[df["subtotal_results"] != True, merge_keys + future_cols]
        else:
            future = df[merge_keys + future_cols]
        frames.append(future)
    if len(frames) == 2:
        df = pd.merge(
            frames[0],
            frames[1],
            on=merge_keys,
            how="outer",
            suffixes=("", ""),
        ).fillna(0.0)
    elif len(frames) == 1:
        df = frames[0]
    else:
        df = df[merge_keys + year_cols]

    energy = (
        df[year_cols]
        .sum(axis=0, min_count=1)
        .reindex(year_cols, fill_value=0.0)
        .astype(float)
    )
    energy.index = year_cols
    return energy


def aggregate_base_stocks(
    df: pd.DataFrame,
    base_year: int,
    vehicle_type_map: dict | None = None,
    passenger_only: bool = True,
) -> dict:
    """
    Aggregate base-year stocks into LPV/MC/Bus buckets from a transport-style df.
    """
    if vehicle_type_map is None:
        vehicle_type_map = DEFAULT_PASSENGER_VEHICLE_TYPE_MAP

    df_base = df[df["Date"] == base_year].copy()
    if passenger_only:
        df_base = df_base[
            (df_base["Transport Type"].astype(str).str.lower() == "passenger")
            & (df_base["Medium"].astype(str).str.lower() == "road")
        ]

    if df_base.empty:
        raise ValueError(f"No base-year rows found for year {base_year}.")

    df_base["Stocks"] = pd.to_numeric(df_base["Stocks"], errors="coerce")
    df_base = df_base.dropna(subset=["Stocks"])

    if df_base.empty:
        raise ValueError(f"No base-year stock values found for year {base_year}.")

    stocks: dict[str, float] = {}
    for _, row in df_base.iterrows():
        bucket = vehicle_type_map.get(str(row["Vehicle Type"]).lower())
        if bucket is None:
            continue
        stocks[bucket] = stocks.get(bucket, 0.0) + float(row["Stocks"])

    if not stocks:
        raise ValueError("No base-year stocks mapped into LPV/MC/Bus buckets.")
    return stocks

#%% helpers: capacity index and envelope -> stocks


def compute_base_vehicle_shares(base_stocks: dict) -> dict:
    """
    Compute vehicle-count shares from base-year stocks.
    """
    total = sum(base_stocks.values())
    if total <= 0:
        raise ValueError("Base stocks must sum to a positive number.")
    return {v: float(stock) / float(total) for v, stock in base_stocks.items()}


def compute_base_capacity_index(
    base_stocks: dict,
    base_population: float,
    weights: dict,
) -> tuple[float, dict]:
    """
    Compute base-year capacity-weighted motorisation index M_base and
    capacity shares alpha_v for each vehicle type.

    M_base = (sum_v w_v * Stock_v) / Population
    """
    base_population = float(base_population)
    if not np.isfinite(base_population) or base_population <= 0:
        raise ValueError("base_population must be a finite positive number.")

    cap_by_type = {v: float(weights[v]) * float(stock) for v, stock in base_stocks.items()}
    total_capacity = float(sum(cap_by_type.values()))
    if not np.isfinite(total_capacity) or total_capacity <= 0:
        raise ValueError("Weighted base stock capacity must sum to a finite positive number.")
    M_base = total_capacity / base_population

    alpha = {v: cap / total_capacity for v, cap in cap_by_type.items()}

    return M_base, alpha


def _prepare_vehicle_share_dataframe(
    shares: dict,
    years: pd.Index,
    weights: dict,
) -> pd.DataFrame:
    """
    Build a DataFrame of vehicle-count shares by year, normalised to 1.0.
    """
    if not shares:
        raise ValueError("Vehicle shares cannot be empty.")

    share_df = pd.DataFrame(index=years)
    weight_keys = set(weights.keys())

    for v, share_val in shares.items():
        if v not in weight_keys:
            raise KeyError(f"Vehicle key '{v}' missing from weights.")

        if isinstance(share_val, pd.Series):
            s = (
                share_val.reindex(years)
                .interpolate()
                .ffill()
                .bfill()
                .astype(float)
            )
        else:
            s = pd.Series(float(share_val), index=years, dtype=float)
        share_df[v] = s

    share_df = share_df.ffill().bfill().fillna(0.0)
    share_df = share_df.reindex(columns=list(weights.keys()), fill_value=0.0)

    row_sums = share_df.sum(axis=1)
    if (row_sums <= 0).any():
        raise ValueError("Vehicle shares must sum to >0 each year.")

    share_df = share_df.div(row_sums, axis=0)
    return share_df


def envelope_to_target_stocks(
    M: pd.Series,
    population: pd.Series,
    shares: dict,
    weights: dict,
) -> tuple[dict, pd.DataFrame]:
    """
    Convert a car-equivalent envelope M(y) and vehicle-count shares into
    target stocks S_v(y) for each vehicle type v.

    Shares are interpreted as *vehicle* shares (not car-equivalents). The
    total vehicle count implied by the envelope is:
        N(y) = M(y) * Pop(y) / sum_v share_v * w_v
    and stock by type is share_v * N(y).
    """
    years = M.index
    share_df = _prepare_vehicle_share_dataframe(shares, years, weights)

    weights_series = pd.Series(weights, dtype=float)
    capacity = M * population

    denom = (share_df * weights_series).sum(axis=1)
    if (denom <= 0).any():
        raise ValueError("Weighted vehicle shares must sum to >0 each year.")

    target_stocks = {}
    for v in share_df.columns:
        share_v = share_df[v]
        if share_v.eq(0).all():
            target_stocks[v] = pd.Series(0.0, index=years, dtype=float)
            continue
        S_v = share_v * capacity / denom
        target_stocks[v] = S_v.astype(float)

    return target_stocks, share_df

#%% vehicle share builder from sales shares


def _vehicle_shares_from_sales(
    df: pd.DataFrame,
    years: pd.Index,
    vehicle_type_map: dict,
    transport_type: str,
) -> dict:
    """
    Build vehicle-count shares by year using Sales in df.
    Aggregates over technology/drive and maps to vehicle buckets via vehicle_type_map.
    """
    if "Sales" not in df.columns:
        return {}
    df_use = df.copy()
    df_use["Sales"] = pd.to_numeric(df_use["Sales"], errors="coerce")
    df_use = df_use.dropna(subset=["Sales"])
    df_use = df_use[
        (df_use["Transport Type"].astype(str).str.lower() == transport_type)
        & (df_use["Medium"].astype(str).str.lower() == "road")
    ]
    if df_use.empty:
        return {}

    df_use["vehicle_bucket"] = (
        df_use["Vehicle Type"]
        .astype(str)
        .str.lower()
        .map({k.lower(): v for k, v in vehicle_type_map.items()})
    )
    df_use = df_use.dropna(subset=["vehicle_bucket"])
    cols_to_keep = ["Date", "vehicle_bucket", "Sales"]
    df_use = df_use[cols_to_keep].groupby(["Date", "vehicle_bucket"])["Sales"].sum().reset_index()

    pivot = df_use.pivot(index="Date", columns="vehicle_bucket", values="Sales")
    pivot = pivot.reindex(years).interpolate().ffill().bfill().fillna(0.0)

    row_sums = pivot.sum(axis=1)
    pivot = pivot.div(row_sums.replace(0, np.nan), axis=0).ffill().bfill().fillna(0.0)
    # breakpoint()
    return {col: pivot[col].astype(float) for col in pivot.columns}

#%% turnover functions: vintage + survival -> sales


def initialise_cohorts(
    target_stock: pd.Series,
    vintage_profile: pd.Series,
) -> pd.DataFrame:
    years = target_stock.index
    vintage_profile = pd.Series(vintage_profile, dtype=float).sort_index()
    max_age = len(vintage_profile)

    cohorts = pd.DataFrame(
        data=0.0,
        index=years,
        columns=range(max_age),
        dtype=float,
    )

    base_year = years[0]
    base_stock = target_stock.loc[base_year]

    total_vintage = float(vintage_profile.sum())
    if total_vintage <= 0:
        raise ValueError("vintage_profile must sum to a positive value.")
    vp = vintage_profile / total_vintage
    cohorts.loc[base_year, :] = base_stock * vp.values

    return cohorts


def _validate_and_align_age_profiles(
    survival_curve: pd.Series,
    vintage_profile: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """
    Ensure survival and vintage profiles use the same contiguous integer age grid.
    """
    surv = pd.Series(survival_curve, dtype=float).sort_index()
    vint = pd.Series(vintage_profile, dtype=float).sort_index()

    if surv.empty or vint.empty:
        raise ValueError("survival_curve and vintage_profile must both be non-empty.")
    if not surv.index.is_unique or not vint.index.is_unique:
        raise ValueError("Age indices must not contain duplicates.")

    surv_ages = pd.to_numeric(pd.Index(surv.index), errors="coerce")
    vint_ages = pd.to_numeric(pd.Index(vint.index), errors="coerce")
    if surv_ages.isna().any() or vint_ages.isna().any():
        raise ValueError("Age indices must be numeric.")
    if not np.allclose(surv_ages, np.round(surv_ages)) or not np.allclose(vint_ages, np.round(vint_ages)):
        raise ValueError("Age indices must be integer years.")

    surv.index = pd.Index(np.round(surv_ages).astype(int), dtype=int)
    vint.index = pd.Index(np.round(vint_ages).astype(int), dtype=int)
    surv = surv.sort_index()
    vint = vint.sort_index()

    if not surv.index.equals(vint.index):
        raise ValueError("survival_curve and vintage_profile must use the same age index.")

    ages = surv.index.to_numpy(dtype=int)
    if len(ages) > 1 and not np.array_equal(np.diff(ages), np.ones(len(ages) - 1, dtype=int)):
        raise ValueError("Age indices must be contiguous with step 1.")

    return surv, vint


def compute_sales_from_stock_targets(
    target_stock: pd.Series,
    survival_curve: pd.Series,
    vintage_profile: pd.Series,
    *,
    turnover_policy: dict | None = None,
    return_retirements: bool = False,
) -> tuple:
    """
    Turn a target stock series into sales (and optionally retirements) given
    survival and vintage profiles.
    """
    if turnover_policy:
        # Keep policy mechanics centralized in the policy module while preserving
        # this module's API for plotting/counterfactual paths.
        from sales_workflow import (
            compute_sales_from_stock_targets as _policy_compute_sales_from_stock_targets,
        )

        return _policy_compute_sales_from_stock_targets(
            target_stock=target_stock,
            survival_curve=survival_curve,
            vintage_profile=vintage_profile,
            turnover_policy=turnover_policy,
            return_retirements=return_retirements,
        )

    survival_curve, vintage_profile = _validate_and_align_age_profiles(
        survival_curve,
        vintage_profile,
    )
    max_age = len(vintage_profile)

    years = target_stock.index
    cohorts = initialise_cohorts(target_stock, vintage_profile)
    sales = pd.Series(0.0, index=years, dtype=float)
    retirements = pd.Series(0.0, index=years, dtype=float) if return_retirements else None

    survival_probs = survival_curve.values

    for i in range(1, len(years)):
        year_prev = years[i - 1]
        year = years[i]

        prev_cohorts = cohorts.loc[year_prev].values

        new_cohorts = np.zeros_like(prev_cohorts)
        for age in range(1, max_age):
            survive_prob = survival_probs[age - 1]
            new_cohorts[age] = prev_cohorts[age - 1] * survive_prob

        survivors_total = new_cohorts.sum()
        target_total = target_stock.loc[year]
        
        # if i == 1:  # first dynamic year after base
        #     breakpoint()
        #     base_total = prev_cohorts.sum()
        #     growth = target_total - base_total
        #     print(
        #         f"[DEBUG] {year}: base_total={base_total:,.0f}, "
        #         f"survivors_total={survivors_total:,.0f}, "
        #         f"target_total={target_total:,.0f}, "
        #         f"growth={growth:,.0f}, "
        #         f"replacement≈{base_total - survivors_total:,.0f}, "
        #         f"required_sales={target_total - survivors_total:,.0f}"
        #     )
        if survivors_total <= target_total:
            required_sales = target_total - survivors_total
            new_cohorts[0] = required_sales
        else:
            scale = target_total / survivors_total if survivors_total > 0 else 0.0
            new_cohorts *= scale
            required_sales = 0.0

        # Natural retirements from survival (pre any scaling)
        if retirements is not None:
            retirements.loc[year] = max(0.0, prev_cohorts.sum() - survivors_total)

        cohorts.loc[year, :] = new_cohorts
        sales.loc[year] = required_sales

    if return_retirements:
        return sales, cohorts, retirements
    return sales, cohorts

#%% wrapper: passenger-only workflow using energy trend to set k


def build_passenger_sales_for_economy(
    years: pd.Index,
    population: pd.Series,
    energy_use_passenger: pd.Series,
    base_stocks: dict,
    survival_curves: dict,
    vintage_profiles: dict,
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    saturated: bool = False,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
    use_9th_vehicle_type_sales_shares: bool = True,
) -> dict:
    """
    Build LPV/MC/Bus sales curves for one economy's passenger road fleet.

    The S-curve steepness k is chosen so that the initial growth rate of the
    motorisation envelope is consistent with the recent growth rate of
    passenger energy use.

    M_envelope is a capacity-weighted motorisation index (car-equivalent
    vehicles per person), not a percent ownership level. The result also
    returns an adjusted percent version (car-equivalents per 100 people).

    vehicle_shares are interpreted as vehicle-count shares (not car-equivalents)
    and can be constant or time-varying (Series). If None, base-year vehicle
    shares are used.
    If saturated is True, the ownership envelope stays flat at M_base (or the
    provided M_sat if higher), matching the documented treatment of already
    saturated economies.

    If plot is True (default) and matplotlib is available, returns matplotlib
    figures in result["figures"].
    """
    if weights is None:
        weights = DEFAULT_PASSENGER_WEIGHTS

    years = pd.Index(years)
    population = (
        pd.Series(population).reindex(years).interpolate().ffill().bfill().astype(float)
    )
    energy_use_passenger = (
        pd.Series(energy_use_passenger)
        .reindex(years)
        .interpolate()
        .ffill()
        .bfill()
        .astype(float)
    )

    base_year = years[0]
    pop_base = population.loc[base_year]

    # 1) Base capacity index and shares
    M_base, alpha = compute_base_capacity_index(base_stocks, pop_base, weights)

    # Vehicle-count shares (constant or time-varying)
    if vehicle_shares is None:
        vehicle_shares = compute_base_vehicle_shares(base_stocks)

    # 2) Saturation handling
    if saturated:
        if M_sat is None:
            M_sat = M_base
    else:
        if M_sat is None:
            # Fallback if no explicit saturation is supplied
            M_sat = min(
                DEFAULT_PASSENGER_M_SAT_MULTIPLIER * M_base,
                DEFAULT_PASSENGER_M_SAT_CAP,
            )

    saturated_now = saturated or (M_base >= 0.999 * M_sat)
    if saturated_now:
        M_level = max(M_base, M_sat)
        M_series = pd.Series(M_level, index=years, dtype=float)
        k = 0.0
    else:
        # 3) Estimate k from recent passenger energy trend
        k = estimate_k_from_energy_trend(
            M_base=M_base,
            M_sat=M_sat,
            energy_use=energy_use_passenger,
            base_year=base_year,
            window_years=window_years,
            k_min=k_min,
            k_max=k_max,
        )

        # 4) Build envelope M(y)
        M_series = logistic_envelope_from_base(years, M_base, M_sat, k, base_year)
    adjusted_vehicle_ownership = M_series * 1000.0  # car-equivalents per 1000 people
    
    # 5) Convert envelope to target stocks by type
    target_stocks, vehicle_shares_df = envelope_to_target_stocks(
        M_series, population, vehicle_shares, weights
    )
    
    # 6) Turn each stock series into sales via turnover
    sales_by_type = {}
    retirements_by_type = {}
    for v, S_v in target_stocks.items():
        surv = survival_curves[v]
        vint = vintage_profiles[v]
        sales_v, _cohorts_v, retirements_v = compute_sales_from_stock_targets(
            S_v, surv, vint, return_retirements=True
        )
        sales_by_type[v] = sales_v
        retirements_by_type[v] = retirements_v
    # 7) Aggregate passenger total and shares (for LEAP parent/children)
    passenger_types = list(target_stocks.keys())
    passenger_total_sales = sum(sales_by_type[v] for v in passenger_types)
    passenger_total_retirements = sum(retirements_by_type[v] for v in passenger_types)

    passenger_shares = {}
    denom = passenger_total_sales.replace(0, np.nan)
    for v in passenger_types:
        share_v = sales_by_type[v] / denom
        passenger_shares[v] = share_v.fillna(0.0)
    result = {
        "M_envelope": M_series,
        "adjusted_vehicle_ownership": adjusted_vehicle_ownership,
        "target_stocks": target_stocks,
        "sales": sales_by_type,
        "retirements": retirements_by_type,
        "passenger_total_sales": passenger_total_sales,
        "passenger_total_retirements": passenger_total_retirements,
        "passenger_shares": passenger_shares,
        "k_used": k,
        "M_sat": M_sat,
        "M_base": M_base,
        "alpha_capacity_shares": alpha,
        "vehicle_shares": vehicle_shares_df,
        "saturated": saturated_now,
        "survival_curves": survival_curves,
        "vintage_profiles": vintage_profiles,
        "figures": None,
    }
    # breakpoint()
    if plot:
        try:
            result["figures"] = plot_passenger_sales_result(result, show=True)
        except ImportError as e:
            result["figures"] = {"error": e}
            _raise_plot_failure("Passenger dashboard plotting", e)
        except Exception as e:
            result["figures"] = {"error": e}
            _raise_plot_failure("Passenger dashboard plotting", e)
    
    return result

#%% freight workflow (simplified saturation handling)


def build_freight_sales_for_economy(
    years: pd.Index,
    population: pd.Series,
    energy_use_freight: pd.Series,
    base_stocks: dict,
    survival_curves: dict,
    vintage_profiles: dict,
    *,
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    economy: str | None = None,
    scenario: str | None = None,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
) -> dict:
    """
    Build freight (road) sales curves for an economy using the same
    survival/vintage machinery as passengers but without saturation logic.

    - M_sat is taken from M_SAT_OVERRIDES if provided for ("freight", economy, scenario);
      otherwise defaults to a high value (10 * M_base) to avoid early saturation.
    - Growth steepness k is inferred from freight energy trend.
    """
    if weights is None:
        weights = DEFAULT_FREIGHT_WEIGHTS

    years = pd.Index(years)
    population = (
        pd.Series(population).reindex(years).interpolate().ffill().bfill().astype(float)
    )
    energy_use_freight = (
        pd.Series(energy_use_freight)
        .reindex(years)
        .interpolate()
        .ffill()
        .bfill()
        .astype(float)
    )

    base_year = years[0]
    pop_base = population.loc[base_year]

    # Base capacity index and shares
    M_base, alpha = compute_base_capacity_index(base_stocks, pop_base, weights)

    # Vehicle-count shares (constant or time-varying)
    if vehicle_shares is None:
        vehicle_shares = compute_base_vehicle_shares(base_stocks)

    # M_sat handling: prefer manual overrides, else generous default
    if M_sat is None:
        manual = _get_manual_M_sat("freight", economy, scenario)
        if manual is not None:
            M_sat = manual
        else:
            M_sat = DEFAULT_FREIGHT_M_SAT_MULTIPLIER * M_base

    # Estimate k from freight energy trend
    k = estimate_k_from_energy_trend(
        M_base=M_base,
        M_sat=M_sat,
        energy_use=energy_use_freight,
        base_year=base_year,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
    )

    # Build envelope M(y)
    M_series = logistic_envelope_from_base(years, M_base, M_sat, k, base_year)
    adjusted_vehicle_ownership = M_series * 1000.0  # per 1000 people (for consistency)

    # Convert envelope to target stocks by type
    target_stocks, vehicle_shares_df = envelope_to_target_stocks(
        M_series, population, vehicle_shares, weights
    )

    # Turn each stock series into sales via turnover
    sales_by_type = {}
    retirements_by_type = {}
    for v, S_v in target_stocks.items():
        surv = survival_curves[v]
        vint = vintage_profiles[v]
        sales_v, _cohorts_v, retirements_v = compute_sales_from_stock_targets(
            S_v, surv, vint, return_retirements=True
        )
        sales_by_type[v] = sales_v
        retirements_by_type[v] = retirements_v

    freight_types = list(target_stocks.keys())
    freight_total_sales = sum(sales_by_type[v] for v in freight_types)
    freight_total_retirements = sum(retirements_by_type[v] for v in freight_types)

    freight_shares = {}
    denom = freight_total_sales.replace(0, np.nan)
    for v in freight_types:
        share_v = sales_by_type[v] / denom
        freight_shares[v] = share_v.fillna(0.0)

    result = {
        "M_envelope": M_series,
        "adjusted_vehicle_ownership": adjusted_vehicle_ownership,
        "target_stocks": target_stocks,
        "sales": sales_by_type,
        "retirements": retirements_by_type,
        "freight_total_sales": freight_total_sales,
        "freight_total_retirements": freight_total_retirements,
        "freight_shares": freight_shares,
        "k_used": k,
        "M_sat": M_sat,
        "M_base": M_base,
        "alpha_capacity_shares": alpha,
        "vehicle_shares": vehicle_shares_df,
        "transport_type": "freight",
        "survival_curves": survival_curves,
        "vintage_profiles": vintage_profiles,
        "figures": None,
    }

    if plot:
        try:
            result["figures"] = plot_freight_sales_result(result, show=True)
        except ImportError as e:
            result["figures"] = {"error": e}
            _raise_plot_failure("Freight dashboard plotting", e)
        except Exception as e:
            result["figures"] = {"error": e}
            _raise_plot_failure("Freight dashboard plotting", e)

    return result
#%% dataframe convenience wrapper for MAIN integration


def estimate_passenger_sales_from_dataframe(
    df: pd.DataFrame,
    survival_curves: dict,
    vintage_profiles: dict,
    *,
    economy: str | None = None,
    scenario: str | None = None,
    base_year: int | None = None,
    final_year: int | None = None,
    vehicle_type_map: dict | None = None,
    population_col: str = "Population",
    energy_col: str = "Energy",
    stocks_col: str = "Stocks",
    base_stocks_df: pd.DataFrame | None = None,
    esto_energy_path: str | os.PathLike | None = None,
    esto_sector: str = "15_transport_sector",
    esto_sheet: str = "all econs",
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    saturated: bool = False,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
    use_9th_vehicle_type_sales_shares: bool = True,
) -> dict:
    """
    Run the passenger S-curve workflow directly from the transport source
    dataframe used by MAIN_transport_leap_import.py.

    Only passenger road rows are used to infer:
      - population series (mean across rows per year)
      - passenger energy use (sum per year)
      - base-year stocks aggregated into LPV/MC/Bus buckets

    Base-year stocks can be taken from `base_stocks_df` (e.g. an output data
    sheet) if provided; otherwise the filtered `df` is used.

    Energy can be sourced from the ESTO balances if `esto_energy_path` is set,
    using sector == 15_transport_sector by default.

    vehicle_shares can be passed as constants or Series (by vehicle count) to
    reflect scenario shifts; if omitted, base-year vehicle shares are used.
    Set saturated=True to keep ownership flat at the base level (for already
    saturated economies).

    Returns the same dict as build_passenger_sales_for_economy, plus:
      - 'years': pd.Index of the modelling horizon
      - 'sales_table': pd.DataFrame with columns [Date, LPV, MC, Bus]
      - 'base_stocks': aggregated base-year stocks used as inputs
      - 'energy_use_passenger': passenger energy series used
    """
    df_use = df.copy()

    if economy is not None and "Economy" in df_use.columns:
        df_use = df_use[df_use["Economy"] == economy]
    if scenario is not None and "Scenario" in df_use.columns:
        df_use = df_use[df_use["Scenario"] == scenario]

    if vehicle_type_map is None:
        vehicle_type_map = {
            "car": "LPV",
            "suv": "LPV",
            "lt": "LPV",
            "lpv": "LPV",
            "2w": "MC",
            "mc": "MC",
            "bus": "Bus",
        }
    
    if "Date" not in df_use.columns:
        raise KeyError("Expected a 'Date' column in the transport dataframe.")

    if base_year is None:
        base_year = int(df_use["Date"].min())
    if final_year is None:
        final_year = int(df_use["Date"].max())

    years = pd.Index(range(base_year, final_year + 1))
    if len(years) == 0:
        raise ValueError("No years available after applying base_year/final_year.")

    if population_col not in df_use.columns:
        raise KeyError(f"Missing '{population_col}' column for population series.")
    #mean of population is necessary and most simple option since there are multiple rows per year with the same population
    population = (
        df_use.groupby("Date")[population_col]
        .mean(numeric_only=True)
        .reindex(years)
        .interpolate()
        .ffill()
        .bfill()
        .astype(float)
    )

    passenger_mask = (
        df_use["Transport Type"].astype(str).str.lower() == "passenger"
    ) & (df_use["Medium"].astype(str).str.lower() == "road")
    if esto_energy_path:
        if economy is None or scenario is None:
            raise ValueError("economy and scenario are required when using ESTO energy.")
        energy_use_passenger = extract_energy_use_from_esto(
            esto_path=esto_energy_path,
            economy=economy,
            scenario=scenario,
            base_year=base_year,
            final_year=final_year,
            sector=esto_sector,
            sheet_name=esto_sheet,
        )
    else:
        raise NotImplementedError(
            "Direct extraction of passenger energy from another dataframe is not implemented since source dataframe doesnt have historical energy data.")
    stocks_source_df = base_stocks_df.copy() if base_stocks_df is not None else df_use
    if stocks_col not in stocks_source_df.columns:
        raise KeyError(f"Missing '{stocks_col}' column for stock levels.")
    
    base_stocks = aggregate_base_stocks(
        df=stocks_source_df.rename(columns={stocks_col: "Stocks"}),
        base_year=base_year,
        vehicle_type_map=vehicle_type_map,
        passenger_only=True,
    )

    # Optional: derive vehicle shares from Vehicle_sales_share if requested and not provided
    if vehicle_shares is None:
        use_sales_shares = (
            USE_9TH_VEHICLE_TYPE_SALES_SHARES
            if use_9th_vehicle_type_sales_shares is None
            else use_9th_vehicle_type_sales_shares
        )
        if use_sales_shares:
            derived_shares = _vehicle_shares_from_sales(
                df_use, years, vehicle_type_map, transport_type="passenger"
            )
            if derived_shares:
                vehicle_shares = derived_shares

    result = build_passenger_sales_for_economy(
        years=years,
        population=population,
        energy_use_passenger=energy_use_passenger,
        base_stocks=base_stocks,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        weights=weights,
        M_sat=M_sat,
        vehicle_shares=vehicle_shares,
        saturated=saturated,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=plot,
    )

    sales_table = pd.DataFrame(result["sales"])
    sales_table.index.name = "Date"
    result["sales_table"] = sales_table.reset_index()
    result["years"] = years
    result["base_stocks"] = base_stocks
    result["energy_use_passenger"] = energy_use_passenger

    result = convert_result_to_dataframe(result)
    
    return result


def estimate_freight_sales_from_dataframe(
    df: pd.DataFrame,
    survival_curves: dict,
    vintage_profiles: dict,
    *,
    economy: str | None = None,
    scenario: str | None = None,
    base_year: int | None = None,
    final_year: int | None = None,
    vehicle_type_map: dict | None = None,
    population_col: str = "Population",
    energy_col: str = "Energy",
    stocks_col: str = "Stocks",
    base_stocks_df: pd.DataFrame | None = None,
    esto_energy_path: str | os.PathLike | None = None,
    esto_sector: str = "15_transport_sector",
    esto_sheet: str = "all econs",
    weights: dict | None = None,
    vehicle_shares: dict | None = None,
    M_sat: float | None = None,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = False,
    use_9th_vehicle_type_sales_shares: bool | None = None,
) -> dict:
    """
    Freight road workflow (separate from passenger) using energy trend only.
    """
    df_use = df.copy()

    if economy is not None and "Economy" in df_use.columns:
        df_use = df_use[df_use["Economy"] == economy]
    if scenario is not None and "Scenario" in df_use.columns:
        df_use = df_use[df_use["Scenario"] == scenario]

    if vehicle_type_map is None:
        vehicle_type_map = DEFAULT_FREIGHT_VEHICLE_TYPE_MAP

    if "Date" not in df_use.columns:
        raise KeyError("Expected a 'Date' column in the transport dataframe.")

    if base_year is None:
        base_year = int(df_use["Date"].min())
    if final_year is None:
        final_year = int(df_use["Date"].max())

    years = pd.Index(range(base_year, final_year + 1))
    if len(years) == 0:
        raise ValueError("No years available after applying base_year/final_year.")

    if population_col not in df_use.columns:
        raise KeyError(f"Missing '{population_col}' column for population series.")

    population = (
        df_use.groupby("Date")[population_col]
        .mean(numeric_only=True)
        .reindex(years)
        .interpolate()
        .ffill()
        .bfill()
        .astype(float)
    )

    freight_mask = (
        df_use["Transport Type"].astype(str).str.lower() == "freight"
    ) & (df_use["Medium"].astype(str).str.lower() == "road")

    if esto_energy_path:
        if economy is None or scenario is None:
            raise ValueError("economy and scenario are required when using ESTO energy.")
        energy_use_freight = extract_energy_use_from_esto(
            esto_path=esto_energy_path,
            economy=economy,
            scenario=scenario,
            base_year=base_year,
            final_year=final_year,
            sector=esto_sector,
            sheet_name=esto_sheet,
        )
    else:
        raise NotImplementedError(
            "Direct extraction of freight energy from another dataframe is not implemented since source dataframe doesnt have historical energy data.")

    stocks_source_df = base_stocks_df.copy() if base_stocks_df is not None else df_use
    stocks_source_df = stocks_source_df[
        (stocks_source_df["Transport Type"].astype(str).str.lower() == "freight")
        & (stocks_source_df["Medium"].astype(str).str.lower() == "road")
    ]
    if stocks_col not in stocks_source_df.columns:
        raise KeyError(f"Missing '{stocks_col}' column for stock levels.")

    base_stocks = aggregate_base_stocks(
        df=stocks_source_df.rename(columns={stocks_col: "Stocks"}),
        base_year=base_year,
        vehicle_type_map=vehicle_type_map,
        passenger_only=False,
    )

    # Optional: derive vehicle shares from Vehicle_sales_share if requested and not provided
    if vehicle_shares is None:
        use_sales_shares = (
            USE_9TH_VEHICLE_TYPE_SALES_SHARES
            if use_9th_vehicle_type_sales_shares is None
            else use_9th_vehicle_type_sales_shares
        )
        if use_sales_shares:
            derived_shares = _vehicle_shares_from_sales(
                df_use, years, vehicle_type_map, transport_type="freight"
            )
            if derived_shares:
                vehicle_shares = derived_shares

    result = build_freight_sales_for_economy(
        years=years,
        population=population,
        energy_use_freight=energy_use_freight,
        base_stocks=base_stocks,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        weights=weights,
        vehicle_shares=vehicle_shares,
        M_sat=M_sat,
        economy=economy,
        scenario=scenario,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=plot,
    )

    sales_table = pd.DataFrame(result["sales"])
    sales_table.index.name = "Date"
    result["sales_table"] = sales_table.reset_index()
    result["years"] = years
    result["base_stocks"] = base_stocks
    result["energy_use_freight"] = energy_use_freight

    return result

def convert_result_to_dataframe(result: dict) -> dict:
    """
    Convert the passenger sales result into a *tall* DataFrame with a
    vehicle_type column for easier export/analysis.

    - Per-vehicle series (stocks, sales, retirements, shares) are stacked long.
    - Economy-wide series (envelope, adjusted ownership, total sales/retirements)
      are merged on Date.
    - The final DataFrame is stored in result["result_dataframe"].
    """

    def _series_from(obj, name: str) -> pd.Series | None:
        if obj is None:
            return None
        s = pd.Series(obj)
        s.name = name or getattr(obj, "name", "value")
        return s

    # Economy-wide series (repeat across vehicle rows after merge)
    general_series = []
    for key, col_name in (
        ("M_envelope", "M_envelope"),
        ("adjusted_vehicle_ownership", "adjusted_vehicle_ownership"),
        ("passenger_total_sales", "passenger_total_sales"),
        ("passenger_total_retirements", "passenger_total_retirements"),
    ):
        s = _series_from(result.get(key), col_name)
        if s is not None:
            general_series.append(s)

    general_df = (
        pd.concat(general_series, axis=1) if general_series else pd.DataFrame()
    )
    if not general_df.empty:
        general_df.index.name = "Date"
        general_df = general_df.reset_index()

    # Vehicle-level series -> stack into long format
    vehicle_frames = {}
    for key, prefix in (
        ("target_stocks", "stock"),
        ("sales", "sales"),
        ("retirements", "retirements"),
        ("passenger_shares", "share"),
        ("vehicle_shares", "vehicle_share"),
    ):
        data = result.get(key)
        if isinstance(data, dict) and data:
            df_part = pd.DataFrame({k: pd.Series(v) for k, v in data.items()})
            vehicle_frames[prefix] = df_part
        elif isinstance(data, pd.DataFrame) and not data.empty:
            vehicle_frames[prefix] = data.copy()

    if vehicle_frames:
        vehicle_df = pd.concat(vehicle_frames, axis=1, keys=vehicle_frames.keys())
        vehicle_df.index.name = "Date"
        vehicle_long = (
            vehicle_df.stack(level=1)
            .reset_index()
            .rename(columns={"level_1": "vehicle_type"})
        )
    else:
        vehicle_long = pd.DataFrame(columns=["Date", "vehicle_type"])

    # Merge economy-wide series onto the tall vehicle table
    if not general_df.empty:
        result_df = vehicle_long.merge(general_df, on="Date", how="left")
    else:
        result_df = vehicle_long

    # Consistent column ordering (keep any extras at the end)
    preferred_order = [
        "Date",
        "vehicle_type",
        "stock",
        "sales",
        "retirements",
        "share",
        "vehicle_share",
        "M_envelope",
        "adjusted_vehicle_ownership",
        "passenger_total_sales",
        "passenger_total_retirements",
    ]
    ordered = [c for c in preferred_order if c in result_df.columns]
    remainder = [c for c in result_df.columns if c not in ordered]
    result_df = result_df[ordered + remainder]

    result["result_dataframe"] = result_df
    return result


def estimate_passenger_sales_from_files(
    *,
    source_path: str | os.PathLike,
    survival_path: str | os.PathLike,
    vintage_path: str | os.PathLike,
    esto_path: str | os.PathLike,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    base_stocks_path: str | os.PathLike | None = None,
    source_reader_kwargs: dict | None = None,
    vehicle_shares: dict | None = None,
    saturated: bool = False,
    weights: dict | None = None,
    M_sat: float | None = None,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
    survival_is_cumulative: bool = True,
    use_9th_vehicle_type_sales_shares: bool | None = None,
) -> dict:
    """
    File-based convenience wrapper:
      - Loads source dataframe (population, default base stocks)
      - Loads survival/vintage profiles from lifecycle profile xlsx
      - Pulls energy from ESTO balances (sector == 15_transport_sector)
      - Optionally overrides base stocks using a separate data sheet
      - Accepts vehicle_shares (by count) and saturation flag to align with
        documented workflow
    """
    source_reader_kwargs = source_reader_kwargs or {}
    df_src = pd.read_excel(source_path, **source_reader_kwargs)
    base_stocks_df = None
    if base_stocks_path:
        if str(base_stocks_path).lower().endswith(".csv"):
            base_stocks_df = pd.read_csv(base_stocks_path)
        else:
            base_stocks_df = pd.read_excel(base_stocks_path)

    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path,
        vintage_path,
        survival_is_cumulative=survival_is_cumulative,
    )

    result = estimate_passenger_sales_from_dataframe(
        df=df_src,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        base_stocks_df=base_stocks_df,
        esto_energy_path=esto_path,
        esto_sector="15_transport_sector",
        esto_sheet="all econs",
        vehicle_shares=vehicle_shares,
        saturated=saturated,
        weights=weights,
        M_sat=M_sat,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=plot,
        use_9th_vehicle_type_sales_shares=use_9th_vehicle_type_sales_shares,
    )
    
    return result


def estimate_freight_sales_from_files(
    *,
    source_path: str | os.PathLike,
    survival_path: str | os.PathLike,
    vintage_path: str | os.PathLike,
    esto_path: str | os.PathLike,
    economy: str,
    scenario: str,
    base_year: int,
    final_year: int,
    base_stocks_path: str | os.PathLike | None = None,
    source_reader_kwargs: dict | None = None,
    vehicle_shares: dict | None = None,
    weights: dict | None = None,
    M_sat: float | None = None,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = False,
    survival_is_cumulative: bool = True,
    use_9th_vehicle_type_sales_shares: bool | None = None,
) -> dict:
    """
    File-based convenience wrapper for freight road workflow.
    """
    source_reader_kwargs = source_reader_kwargs or {}
    df_src = pd.read_excel(source_path, **source_reader_kwargs)
    base_stocks_df = None
    if base_stocks_path:
        if str(base_stocks_path).lower().endswith(".csv"):
            base_stocks_df = pd.read_csv(base_stocks_path)
        else:
            base_stocks_df = pd.read_excel(base_stocks_path)

    survival_curves, vintage_profiles = load_survival_and_vintage_profiles(
        survival_path,
        vintage_path,
        survival_is_cumulative=survival_is_cumulative,
        vehicle_keys=("Trucks", "LCVs"),
    )

    result = estimate_freight_sales_from_dataframe(
        df=df_src,
        survival_curves=survival_curves,
        vintage_profiles=vintage_profiles,
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        base_stocks_df=base_stocks_df,
        esto_energy_path=esto_path,
        esto_sector="15_transport_sector",
        esto_sheet="all econs",
        vehicle_shares=vehicle_shares,
        weights=weights,
        M_sat=M_sat,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=plot,
        use_9th_vehicle_type_sales_shares=use_9th_vehicle_type_sales_shares,
    )
    return result


#%% plotting helper

def _sanitize_plot_filename_token(value: str) -> str:
    token = "".join(ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in str(value))
    token = "_".join(part for part in token.split("_") if part)
    return token or "plot"


def _save_dashboard_figures(
    figs: dict | None,
    *,
    dashboard_name: str,
    output_dir: str | os.PathLike | None = None,
) -> list[str]:
    """
    Save dashboard figure objects to disk and return saved file paths.
    """
    if not isinstance(figs, dict) or not figs:
        return []

    out_dir = Path(output_dir) if output_dir is not None else DEFAULT_PLOTTING_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    dashboard_token = _sanitize_plot_filename_token(dashboard_name)

    # Debug hook requested for investigating figure state before writes.
    breakpoint()

    saved_paths: list[str] = []
    saved_ids: set[int] = set()
    for name, fig in figs.items():
        if not hasattr(fig, "savefig"):
            continue
        fig_id = id(fig)
        if fig_id in saved_ids:
            continue
        panel_token = _sanitize_plot_filename_token(str(name))
        save_path = (out_dir / f"{timestamp}_{dashboard_token}_{panel_token}.png").resolve()
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
        saved_paths.append(str(save_path))
        saved_ids.add(fig_id)

    return saved_paths


def _annual_to_cumulative_for_plot(annual: pd.Series) -> pd.Series:
    """
    Build a cumulative survival S(age) from an annual survival curve p(age).

    annual[age] is interpreted as the probability to survive from age -> age+1
    (0–1 or 0–100). We return S(age) in 0–1 with S(0) = 1.0.
    """
    surv = annual.astype(float)
    # allow 0–100 input
    if surv.max() > 1.0:
        surv = surv / 100.0
    surv = surv.clip(lower=0.0, upper=1.0)

    ages = surv.index
    S_vals = []
    S = 1.0
    for p in surv:
        S_vals.append(S)
        S *= p
    return pd.Series(S_vals, index=ages, dtype=float)


def _aggregate_series_dict(series_map: dict | None, years: pd.Index) -> pd.Series:
    """
    Sum a dict of year-indexed series onto a common year index.
    """
    if not isinstance(series_map, dict) or not series_map:
        return pd.Series(0.0, index=years, dtype=float)
    total = pd.Series(0.0, index=years, dtype=float)
    for series in series_map.values():
        total = total.add(pd.Series(series).reindex(years).fillna(0.0).astype(float), fill_value=0.0)
    return total.astype(float)


def _first_profile(curves: dict | None) -> tuple[str | None, pd.Series | None]:
    """
    Return the first profile from a dict as a sorted float Series.
    """
    if not isinstance(curves, dict) or not curves:
        return None, None
    first_key = next(iter(curves.keys()))
    series = pd.Series(curves[first_key]).sort_index().astype(float)
    return str(first_key), series


def _profiles_are_equal(a: pd.Series | None, b: pd.Series | None, atol: float = 1e-12) -> bool:
    """
    Check if two profiles share the same index and values within tolerance.
    """
    if a is None or b is None:
        return False
    if not a.index.equals(b.index):
        return False
    return bool(np.allclose(a.values, b.values, atol=atol, equal_nan=True))


DRIVE_FAMILY_ORDER: tuple[str, ...] = ("ICE", "HEV", "PHEV", "EV", "Other")


def _classify_drive_family(drive_key: str) -> str:
    """Map detailed drive labels into high-level families."""
    key = str(drive_key).strip().lower()
    if not key:
        return "Other"

    if "phev" in key:
        return "PHEV"
    if key.startswith("bev") or key.startswith("fcev") or key.startswith("erev") or key == "ev":
        return "EV"
    if "hev" in key or "hybrid" in key:
        return "HEV"
    if "ice" in key:
        return "ICE"
    return "Other"


def _aggregate_drive_frame_by_family(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-drive series into ICE/HEV/PHEV/EV/Other families."""
    if df.empty:
        return pd.DataFrame(index=df.index, dtype=float)

    grouped: dict[str, pd.Series] = {}
    for family in DRIVE_FAMILY_ORDER:
        cols = [col for col in df.columns if _classify_drive_family(str(col)) == family]
        if not cols:
            continue
        family_series = df[cols].sum(axis=1)
        if float(family_series.abs().max()) <= 1e-12:
            continue
        grouped[family] = family_series

    if not grouped:
        return pd.DataFrame(index=df.index, dtype=float)
    return pd.DataFrame(grouped, index=df.index).astype(float)


def plot_passenger_sales_result(
    result: dict,
    economy: str | None = None,
    show: bool = False,
    skip_base_year: bool = True,
    *,
    share_key: str = "passenger_shares",
    transport_label: str = "Passenger road",
    n_cols: int = 3,
    show_guide: bool = False,
) -> dict:
    """
    Build simple matplotlib charts from a transport sales result dict.

    Returns a dict of figures: envelope, stocks, sales, sales shares, and
    retirements (if available).

    If skip_base_year is True (default), year-based panels omit the first
    model year. This avoids showing the forced/initial base-year point
    (commonly zero for sales), which can be visually misleading.

    share_key controls which share dictionary to plot (e.g. passenger or freight).
    """
    
    try:
        import matplotlib.pyplot as plt
    except Exception as e:  # pragma: no cover - optional dependency
        raise ImportError(
            "matplotlib is required for plotting; install with `pip install matplotlib`."
        ) from e

    years_full = pd.Index(result.get("M_envelope").index)
    years = years_full[1:] if (skip_base_year and len(years_full) > 1) else years_full
    omitted_base_year = bool(skip_base_year and len(years_full) > 1)
    economy_label = f"{economy} " if economy else ""

    adjusted_ownership = result.get("adjusted_vehicle_ownership")
    if adjusted_ownership is None and "M_envelope" in result:
        adjusted_ownership = result["M_envelope"] * 1000.0

    figs: dict[str, object] = {}

    panels: list[tuple[str, callable]] = []

    def _legend(ax) -> None:
        """Standard legend with higher transparency."""
        leg = ax.legend(framealpha=0.45)
        if leg:
            leg.set_alpha(0.55)
    def _legend(ax) -> None:
        leg = ax.legend(framealpha=0.45)
        if leg:
            leg.set_alpha(0.55)
    
    def plot_envelope(ax):
        envelope_vals = pd.Series(adjusted_ownership).reindex(years).astype(float)
        ax.plot(years, envelope_vals.values, label="Ownership trajectory (per 1000)")
        ax.set_title(f"{economy_label}Ownership Trajectory")
        ax.set_xlabel("Year")
        ax.set_ylabel("car-equivalent vehicles per 1000 people")
        ax.grid(True)
        _legend(ax)

    panels.append(("envelope", plot_envelope))

    stocks_df = pd.DataFrame(result["target_stocks"]).reindex(years)

    def plot_stocks(ax):
        for col in stocks_df.columns:
            ax.plot(years, stocks_df[col], label=col)
        ax.set_title(f"{economy_label}{transport_label}: Target Stocks")
        ax.set_xlabel("Year")
        ax.set_ylabel("Stocks (vehicles)")
        ax.grid(True)
        _legend(ax)

    panels.append(("stocks", plot_stocks))

    sales_df = pd.DataFrame(result["sales"]).reindex(years)

    def plot_sales(ax):
        for col in sales_df.columns:
            ax.plot(years, sales_df[col], label=col)
        ax.set_title(f"{economy_label}{transport_label}: Sales by Vehicle")
        ax.set_xlabel("Year")
        ax.set_ylabel("Annual sales (vehicles)")
        ax.grid(True)
        _legend(ax)

    panels.append(("sales", plot_sales))

    shares_data = result.get(share_key)
    if isinstance(shares_data, dict) and shares_data:
        shares_df = pd.DataFrame(shares_data).reindex(years)

        def plot_shares(ax):
            for col in shares_df.columns:
                ax.plot(years, shares_df[col], label=col)
            ax.set_title(f"{economy_label}{transport_label}: Sales Shares")
            ax.set_xlabel("Year")
            ax.set_ylabel("Share of sales")
            ax.set_ylim(0, 1)
            ax.grid(True)
            _legend(ax)

        panels.append(("sales_shares", plot_shares))

    retire_df = pd.DataFrame(index=years, dtype=float)
    if "retirements" in result:
        retire_df = pd.DataFrame(result["retirements"]).reindex(years)

        def plot_retires(ax):
            for col in retire_df.columns:
                ax.plot(years, retire_df[col], label=col)
            ax.set_title(f"{economy_label}{transport_label}: Retirements")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual retirements (vehicles)")
            ax.grid(True)
            _legend(ax)

        panels.append(("retirements", plot_retires))

    survival_curves = result.get("survival_curves")
    vintage_profiles = result.get("vintage_profiles")
    if survival_curves and vintage_profiles:
        first_key = next(iter(survival_curves.keys()))
        surv = survival_curves[first_key]
        vint = vintage_profiles[first_key]
        def plot_surv(ax):
            # annual survival p(age) from result
            annual = surv.astype(float)
            if annual.max() > 1.0:
                annual = annual / 100.0
            annual = annual.clip(lower=0.0, upper=1.0)

            # cumulative S(age) reconstructed from annual p(age)
            cum = _annual_to_cumulative_for_plot(annual)

            ax.plot(
                annual.index,
                annual.values * 100.0,
                marker="o",
                label="Annual survival p(age)",
            )
            ax.plot(
                cum.index,
                cum.values * 100.0,
                linestyle="--",
                label="Cumulative survival S(age)",
            )
            ax.set_title(f"{economy_label}Vehicle Survival by Age ({first_key})")
            ax.set_xlabel("Age (years)")
            ax.set_ylabel("Survival (%)")
            ax.grid(True)
            _legend(ax)

        def plot_vint(ax):
            ax.plot(vint.index, vint.values * 100.0, marker="o", color="tab:orange")
            ax.set_title(f"{economy_label}Vintage Profile ({first_key})")
            ax.set_xlabel("Age (years)")
            ax.set_ylabel("Vintage share (%)")
            ax.grid(True)
            _legend(ax)

        panels.append(("survival_curve", plot_surv))
        panels.append(("vintage_profile", plot_vint))

    # Optional drive-policy panels: only show when drive-specific policy inputs are active.
    drive_policy_diag = result.get("drive_policy_diagnostics")
    drive_rates_df = pd.DataFrame()
    drive_stock_share_df = pd.DataFrame()
    drive_sales_df = pd.DataFrame()
    drive_sales_grouped_df = pd.DataFrame()
    drive_sales_counterfactual_affected_avg = pd.Series(dtype=float)
    drive_sales_counterfactual_non_affected_avg = pd.Series(dtype=float)
    drive_sales_affected_avg = pd.Series(dtype=float)
    drive_retirements_df = pd.DataFrame()
    drive_retirements_grouped_df = pd.DataFrame()
    drive_sales_non_affected_avg = pd.Series(dtype=float)
    drive_retirements_affected_avg = pd.Series(dtype=float)
    drive_retirements_non_affected_avg = pd.Series(dtype=float)
    drive_retirements_counterfactual_affected_avg = pd.Series(dtype=float)
    drive_retirements_counterfactual_non_affected_avg = pd.Series(dtype=float)
    drive_retirement_rate_affected_avg = pd.Series(dtype=float)
    drive_retirement_rate_non_affected_avg = pd.Series(dtype=float)
    drive_retirement_rate_counterfactual_affected_avg = pd.Series(dtype=float)
    drive_retirement_rate_counterfactual_non_affected_avg = pd.Series(dtype=float)
    total_sales_policy_series = pd.Series(dtype=float)
    total_sales_counterfactual_series = pd.Series(dtype=float)
    drive_display_drives: list[str] = []
    non_affected_drives: list[str] = []
    changed_drives: list[str] = []
    contrib_df = pd.DataFrame()
    all_drive_shares_df = pd.DataFrame()
    contrib_cols_needed = {"Date", "drive", "vehicle_bucket", "drive_stock", "bucket_total_stock"}
    drive_panel_note: str | None = None
    drive_counterfactual_parenthetical = "no policy"
    if isinstance(drive_policy_diag, dict):
        raw_drive_rates = drive_policy_diag.get("drive_rates")
        if raw_drive_rates is not None:
            drive_rates_df = pd.DataFrame(raw_drive_rates).copy()
            if not drive_rates_df.empty:
                drive_rates_df.index = pd.to_numeric(pd.Index(drive_rates_df.index), errors="coerce")
                drive_rates_df = drive_rates_df[drive_rates_df.index.notna()].copy()
                drive_rates_df.index = pd.Index(drive_rates_df.index.astype(int), dtype=int)
                drive_rates_df = (
                    drive_rates_df.groupby(level=0)
                    .mean()
                    .reindex(years)
                    .ffill()
                    .fillna(0.0)
                    .astype(float)
                )

        raw_contrib = drive_policy_diag.get("contributions_long")
        if raw_contrib is not None:
            contrib_df = pd.DataFrame(raw_contrib).copy()
            if not contrib_df.empty and contrib_cols_needed.issubset(contrib_df.columns):
                contrib_df["Date"] = pd.to_numeric(contrib_df["Date"], errors="coerce")
                contrib_df = contrib_df[contrib_df["Date"].notna()].copy()
                contrib_df["Date"] = contrib_df["Date"].astype(int)
                contrib_df["drive"] = contrib_df["drive"].astype(str)
                contrib_df = contrib_df[contrib_df["Date"].isin(set(years))].copy()
                if "rate_contribution" in contrib_df.columns:
                    contrib_df["rate_contribution"] = pd.to_numeric(
                        contrib_df["rate_contribution"],
                        errors="coerce",
                    ).fillna(0.0)
                    drives_with_effect = (
                        contrib_df.groupby("drive")["rate_contribution"]
                        .apply(lambda x: float(np.max(np.abs(x.to_numpy(dtype=float)))))
                        .sort_index()
                    )
                    changed_drives = [
                        drive for drive, max_effect in drives_with_effect.items()
                        if max_effect > 1e-12
                    ]

        raw_all_drive_shares = drive_policy_diag.get("all_drive_stock_shares_long")
        all_drive_cols_needed = {"Date", "vehicle_bucket", "drive", "drive_stock_share"}
        if raw_all_drive_shares is not None:
            all_drive_shares_df = pd.DataFrame(raw_all_drive_shares).copy()
            if not all_drive_shares_df.empty and all_drive_cols_needed.issubset(all_drive_shares_df.columns):
                all_drive_shares_df["Date"] = pd.to_numeric(all_drive_shares_df["Date"], errors="coerce")
                all_drive_shares_df = all_drive_shares_df[all_drive_shares_df["Date"].notna()].copy()
                all_drive_shares_df["Date"] = all_drive_shares_df["Date"].astype(int)
                all_drive_shares_df["drive"] = all_drive_shares_df["drive"].astype(str)
                all_drive_shares_df["drive_stock_share"] = pd.to_numeric(
                    all_drive_shares_df["drive_stock_share"],
                    errors="coerce",
                ).fillna(0.0)
                all_drive_shares_df = all_drive_shares_df[
                    all_drive_shares_df["Date"].isin(set(years))
                ].copy()
            else:
                all_drive_shares_df = pd.DataFrame()

        if not changed_drives and not drive_rates_df.empty:
            changed_drives = [
                str(col) for col in drive_rates_df.columns
                if drive_rates_df[col].abs().max() > 1e-12
            ]

        if changed_drives and not drive_rates_df.empty:
            drive_rate_columns = [drive for drive in changed_drives if drive in drive_rates_df.columns]
            if drive_rate_columns:
                changed_drives = drive_rate_columns
                drive_rates_df = drive_rates_df.reindex(columns=changed_drives)
            else:
                changed_drives = []
                drive_rates_df = pd.DataFrame(index=years, dtype=float)
        else:
            drive_rates_df = pd.DataFrame(index=years, dtype=float)

        baseline_sales_df = pd.DataFrame(index=years, dtype=float)
        baseline_retire_df = pd.DataFrame(index=years, dtype=float)
        counterfactual_policy_map_raw = result.get("drive_policy_counterfactual_turnover_policies")
        counterfactual_policy_map = (
            {str(k): dict(v) for k, v in counterfactual_policy_map_raw.items()}
            if isinstance(counterfactual_policy_map_raw, dict)
            else {}
        )
        if counterfactual_policy_map:
            drive_counterfactual_parenthetical = "no drive policy"
        target_stocks_map = result.get("target_stocks", {})
        survival_map = result.get("survival_curves", {})
        vintage_map = result.get("vintage_profiles", {})
        if (
            isinstance(target_stocks_map, dict)
            and isinstance(survival_map, dict)
            and isinstance(vintage_map, dict)
        ):
            for vehicle_key, target_series in target_stocks_map.items():
                if vehicle_key not in survival_map or vehicle_key not in vintage_map:
                    continue
                baseline_sales, _, baseline_ret = compute_sales_from_stock_targets(
                    target_stock=pd.Series(target_series),
                    survival_curve=pd.Series(survival_map[vehicle_key]),
                    vintage_profile=pd.Series(vintage_map[vehicle_key]),
                    turnover_policy=counterfactual_policy_map.get(str(vehicle_key)),
                    return_retirements=True,
                )
                key_str = str(vehicle_key)
                baseline_sales_df[key_str] = pd.Series(baseline_sales).reindex(years).fillna(0.0)
                baseline_retire_df[key_str] = pd.Series(baseline_ret).reindex(years).fillna(0.0)

        if not baseline_sales_df.empty:
            common_sales_cols = sorted(set(sales_df.columns).union(set(baseline_sales_df.columns)))
            sales_policy_compare_df = sales_df.reindex(columns=common_sales_cols, fill_value=0.0)
            baseline_sales_df = baseline_sales_df.reindex(columns=common_sales_cols, fill_value=0.0)
            total_sales_policy_series = sales_policy_compare_df.sum(axis=1).reindex(years).fillna(0.0)
            total_sales_counterfactual_series = baseline_sales_df.sum(axis=1).reindex(years).fillna(0.0)

        share_source_df = all_drive_shares_df if not all_drive_shares_df.empty else contrib_df
        share_cols_needed = {"Date", "vehicle_bucket", "drive", "drive_stock_share"}
        if not share_source_df.empty and share_cols_needed.issubset(share_source_df.columns):
            share_source_df = share_source_df.copy()
            share_source_df["Date"] = pd.to_numeric(share_source_df["Date"], errors="coerce")
            share_source_df = share_source_df[share_source_df["Date"].notna()].copy()
            share_source_df["Date"] = share_source_df["Date"].astype(int)
            share_source_df["drive"] = share_source_df["drive"].astype(str)
            share_source_df["vehicle_bucket"] = share_source_df["vehicle_bucket"].astype(str)
            share_source_df["drive_stock_share"] = pd.to_numeric(
                share_source_df["drive_stock_share"],
                errors="coerce",
            ).fillna(0.0)
            share_source_df = share_source_df[share_source_df["Date"].isin(set(years))].copy()
            drive_stock_all_df = pd.DataFrame()
            if "drive_stock" in share_source_df.columns:
                share_source_df["drive_stock"] = pd.to_numeric(
                    share_source_df["drive_stock"],
                    errors="coerce",
                ).fillna(0.0)
                drive_stock_all_df = (
                    share_source_df.groupby(["Date", "drive"])["drive_stock"]
                    .sum()
                    .unstack(fill_value=0.0)
                    .reindex(years)
                    .fillna(0.0)
                )

            changed_share_df = share_source_df[share_source_df["drive"].isin(changed_drives)].copy()
            if not changed_share_df.empty and "drive_stock" in changed_share_df.columns:
                total_col = None
                if "vehicle_type_total_stock" in changed_share_df.columns:
                    total_col = "vehicle_type_total_stock"
                elif "bucket_total_stock" in changed_share_df.columns:
                    total_col = "bucket_total_stock"

                if total_col is not None:
                    changed_share_df[total_col] = pd.to_numeric(
                        changed_share_df[total_col],
                        errors="coerce",
                    ).fillna(0.0)
                    changed_share_df["drive_stock"] = pd.to_numeric(
                        changed_share_df["drive_stock"],
                        errors="coerce",
                    ).fillna(0.0)
                    vehicle_type_totals = (
                        changed_share_df[["Date", "vehicle_bucket", total_col]]
                        .drop_duplicates()
                        .groupby("Date")[total_col]
                        .sum()
                    )
                    drive_stock = (
                        changed_share_df.groupby(["Date", "drive"])["drive_stock"]
                        .sum()
                        .unstack(fill_value=0.0)
                    )
                    drive_stock_share_df = (
                        drive_stock.div(vehicle_type_totals.replace(0.0, np.nan), axis=0)
                        .fillna(0.0)
                        .reindex(years)
                        .fillna(0.0)
                    ).reindex(columns=changed_drives, fill_value=0.0)

            # Allocate vehicle-type sales/retirements to drives using drive stock shares.
            share_alloc = share_source_df[["Date", "vehicle_bucket", "drive", "drive_stock_share"]].copy()
            share_alloc["vehicle_type_key"] = (
                share_alloc["vehicle_bucket"].astype(str).str.lower().str.strip()
            )

            sales_long = (
                sales_df.copy()
                .rename_axis("Date")
                .reset_index()
                .melt(id_vars="Date", var_name="vehicle_type", value_name="vehicle_total")
            )
            sales_long["Date"] = pd.to_numeric(sales_long["Date"], errors="coerce")
            sales_long = sales_long[sales_long["Date"].notna()].copy()
            sales_long["Date"] = sales_long["Date"].astype(int)
            sales_long["vehicle_type_key"] = (
                sales_long["vehicle_type"].astype(str).str.lower().str.strip()
            )
            sales_long["vehicle_total"] = pd.to_numeric(
                sales_long["vehicle_total"],
                errors="coerce",
            ).fillna(0.0)

            merged_sales = share_alloc.merge(
                sales_long[["Date", "vehicle_type_key", "vehicle_total"]],
                on=["Date", "vehicle_type_key"],
                how="left",
            )
            merged_sales["attributed"] = (
                merged_sales["drive_stock_share"] * merged_sales["vehicle_total"].fillna(0.0)
            )
            drive_sales_all_df = (
                merged_sales.groupby(["Date", "drive"])["attributed"]
                .sum()
                .unstack(fill_value=0.0)
                .reindex(years)
                .fillna(0.0)
            )
            if not drive_sales_all_df.empty:
                drives_with_sales = [
                    str(col) for col in drive_sales_all_df.columns
                    if drive_sales_all_df[col].abs().max() > 1e-12
                ]
                drive_display_drives = sorted(drives_with_sales)
                if changed_drives:
                    changed_set = set(changed_drives)
                    drive_display_drives = changed_drives + [
                        d for d in drive_display_drives if d not in changed_set
                    ]
                drive_sales_df = drive_sales_all_df.reindex(
                    columns=drive_display_drives,
                    fill_value=0.0,
                )
                non_affected_drives = [
                    drive for drive in drives_with_sales
                    if drive not in set(changed_drives)
                ]
                if non_affected_drives:
                    drive_sales_non_affected_avg = drive_sales_all_df[non_affected_drives].mean(axis=1)
                affected_sales_cols = [
                    drive for drive in changed_drives
                    if drive in drive_sales_all_df.columns
                ]
                if affected_sales_cols:
                    drive_sales_affected_avg = drive_sales_all_df[affected_sales_cols].mean(axis=1)
            if not baseline_sales_df.empty:
                baseline_sales_long = (
                    baseline_sales_df.copy()
                    .rename_axis("Date")
                    .reset_index()
                    .melt(id_vars="Date", var_name="vehicle_type", value_name="vehicle_total")
                )
                baseline_sales_long["Date"] = pd.to_numeric(baseline_sales_long["Date"], errors="coerce")
                baseline_sales_long = baseline_sales_long[baseline_sales_long["Date"].notna()].copy()
                baseline_sales_long["Date"] = baseline_sales_long["Date"].astype(int)
                baseline_sales_long["vehicle_type_key"] = (
                    baseline_sales_long["vehicle_type"].astype(str).str.lower().str.strip()
                )
                baseline_sales_long["vehicle_total"] = pd.to_numeric(
                    baseline_sales_long["vehicle_total"],
                    errors="coerce",
                ).fillna(0.0)
                merged_baseline_sales = share_alloc.merge(
                    baseline_sales_long[["Date", "vehicle_type_key", "vehicle_total"]],
                    on=["Date", "vehicle_type_key"],
                    how="left",
                )
                merged_baseline_sales["attributed"] = (
                    merged_baseline_sales["drive_stock_share"] * merged_baseline_sales["vehicle_total"].fillna(0.0)
                )
                drive_baseline_sales_df = (
                    merged_baseline_sales.groupby(["Date", "drive"])["attributed"]
                    .sum()
                    .unstack(fill_value=0.0)
                    .reindex(years)
                    .fillna(0.0)
                )
                baseline_affected_sales_cols = [
                    drive for drive in changed_drives
                    if drive in drive_baseline_sales_df.columns
                ]
                if baseline_affected_sales_cols:
                    drive_sales_counterfactual_affected_avg = drive_baseline_sales_df[
                        baseline_affected_sales_cols
                    ].mean(axis=1)
                baseline_non_affected_sales_cols = [
                    drive for drive in non_affected_drives
                    if drive in drive_baseline_sales_df.columns
                ]
                if baseline_non_affected_sales_cols:
                    drive_sales_counterfactual_non_affected_avg = drive_baseline_sales_df[
                        baseline_non_affected_sales_cols
                    ].mean(axis=1)

            if not retire_df.empty:
                retire_policy_df = retire_df.copy().reindex(years).fillna(0.0)

                common_vehicle_cols = sorted(set(retire_policy_df.columns).union(set(baseline_retire_df.columns)))
                retire_policy_df = retire_policy_df.reindex(columns=common_vehicle_cols, fill_value=0.0)
                baseline_retire_df = baseline_retire_df.reindex(columns=common_vehicle_cols, fill_value=0.0)
                extra_retire_df = (retire_policy_df - baseline_retire_df).clip(lower=0.0)

                # Allocate baseline retirements to all drives via stock shares.
                baseline_retire_long = (
                    baseline_retire_df.rename_axis("Date")
                    .reset_index()
                    .melt(id_vars="Date", var_name="vehicle_type", value_name="vehicle_total")
                )
                baseline_retire_long["Date"] = pd.to_numeric(baseline_retire_long["Date"], errors="coerce")
                baseline_retire_long = baseline_retire_long[baseline_retire_long["Date"].notna()].copy()
                baseline_retire_long["Date"] = baseline_retire_long["Date"].astype(int)
                baseline_retire_long["vehicle_type_key"] = (
                    baseline_retire_long["vehicle_type"].astype(str).str.lower().str.strip()
                )
                baseline_retire_long["vehicle_total"] = pd.to_numeric(
                    baseline_retire_long["vehicle_total"],
                    errors="coerce",
                ).fillna(0.0)
                merged_baseline_retire = share_alloc.merge(
                    baseline_retire_long[["Date", "vehicle_type_key", "vehicle_total"]],
                    on=["Date", "vehicle_type_key"],
                    how="left",
                )
                merged_baseline_retire["attributed"] = (
                    merged_baseline_retire["drive_stock_share"] * merged_baseline_retire["vehicle_total"].fillna(0.0)
                )
                drive_baseline_retire_df = (
                    merged_baseline_retire.groupby(["Date", "drive"])["attributed"]
                    .sum()
                    .unstack(fill_value=0.0)
                    .reindex(years)
                    .fillna(0.0)
                )
                baseline_affected_cols = [
                    drive for drive in changed_drives
                    if drive in drive_baseline_retire_df.columns
                ]
                if baseline_affected_cols:
                    drive_retirements_counterfactual_affected_avg = drive_baseline_retire_df[
                        baseline_affected_cols
                    ].mean(axis=1)

                # Allocate policy-induced extra retirements to affected drives via rate contributions.
                drive_extra_retire_df = pd.DataFrame(index=years, dtype=float)
                if not contrib_df.empty and {"rate_contribution", "effective_bucket_rate"}.issubset(contrib_df.columns):
                    extra_retire_long = (
                        extra_retire_df.rename_axis("Date")
                        .reset_index()
                        .melt(id_vars="Date", var_name="vehicle_type", value_name="vehicle_total")
                    )
                    extra_retire_long["Date"] = pd.to_numeric(extra_retire_long["Date"], errors="coerce")
                    extra_retire_long = extra_retire_long[extra_retire_long["Date"].notna()].copy()
                    extra_retire_long["Date"] = extra_retire_long["Date"].astype(int)
                    extra_retire_long["vehicle_type_key"] = (
                        extra_retire_long["vehicle_type"].astype(str).str.lower().str.strip()
                    )
                    extra_retire_long["vehicle_total"] = pd.to_numeric(
                        extra_retire_long["vehicle_total"],
                        errors="coerce",
                    ).fillna(0.0)

                    contrib_alloc = contrib_df.copy()
                    contrib_alloc["vehicle_type_key"] = (
                        contrib_alloc["vehicle_bucket"].astype(str).str.lower().str.strip()
                    )
                    contrib_alloc["rate_contribution"] = pd.to_numeric(
                        contrib_alloc["rate_contribution"],
                        errors="coerce",
                    ).fillna(0.0)
                    contrib_alloc["effective_bucket_rate"] = pd.to_numeric(
                        contrib_alloc["effective_bucket_rate"],
                        errors="coerce",
                    ).fillna(0.0)
                    contrib_alloc["allocation_share"] = np.where(
                        contrib_alloc["effective_bucket_rate"] > 1e-12,
                        contrib_alloc["rate_contribution"] / contrib_alloc["effective_bucket_rate"],
                        0.0,
                    )
                    contrib_alloc["allocation_share"] = contrib_alloc["allocation_share"].clip(lower=0.0, upper=1.0)

                    merged_extra_retire = contrib_alloc.merge(
                        extra_retire_long[["Date", "vehicle_type_key", "vehicle_total"]],
                        on=["Date", "vehicle_type_key"],
                        how="left",
                    )
                    merged_extra_retire["attributed"] = (
                        merged_extra_retire["allocation_share"] * merged_extra_retire["vehicle_total"].fillna(0.0)
                    )
                    drive_extra_retire_df = (
                        merged_extra_retire.groupby(["Date", "drive"])["attributed"]
                        .sum()
                        .unstack(fill_value=0.0)
                        .reindex(years)
                        .fillna(0.0)
                    )

                drive_retirements_all_df = (
                    drive_baseline_retire_df.add(drive_extra_retire_df, fill_value=0.0)
                    .reindex(years)
                    .fillna(0.0)
                )
                if not drive_retirements_all_df.empty:
                    drives_with_retirements = [
                        str(col) for col in drive_retirements_all_df.columns
                        if drive_retirements_all_df[col].abs().max() > 1e-12
                    ]
                    drive_display_drives = sorted(
                        set(drive_display_drives).union(drives_with_retirements)
                    )
                    if changed_drives:
                        changed_set = set(changed_drives)
                        drive_display_drives = changed_drives + [
                            d for d in drive_display_drives if d not in changed_set
                        ]
                    drive_retirements_df = drive_retirements_all_df.reindex(
                        columns=drive_display_drives,
                        fill_value=0.0,
                    )
                    if not drive_sales_df.empty:
                        drive_sales_df = drive_sales_df.reindex(
                            columns=drive_display_drives,
                            fill_value=0.0,
                        )
                    non_affected_for_retire = [
                        drive for drive in drives_with_retirements
                        if drive not in set(changed_drives)
                    ]
                    if non_affected_for_retire:
                        drive_retirements_non_affected_avg = drive_retirements_all_df[
                            non_affected_for_retire
                        ].mean(axis=1)
                        affected_retire_cols = [
                            drive for drive in changed_drives
                            if drive in drive_retirements_all_df.columns
                        ]
                        if affected_retire_cols:
                            drive_retirements_affected_avg = drive_retirements_all_df[
                                affected_retire_cols
                            ].mean(axis=1)
                        baseline_non_affected_retire_cols = [
                            drive for drive in non_affected_for_retire
                            if drive in drive_baseline_retire_df.columns
                        ]
                        if baseline_non_affected_retire_cols:
                            drive_retirements_counterfactual_non_affected_avg = drive_baseline_retire_df[
                                baseline_non_affected_retire_cols
                            ].mean(axis=1)
                        non_affected_drives = sorted(set(non_affected_drives).union(non_affected_for_retire))

                    if not drive_stock_all_df.empty:
                        stock_for_rate = drive_stock_all_df.reindex(
                            index=years,
                            columns=drive_retirements_all_df.columns,
                            fill_value=0.0,
                        )
                        drive_retirement_rate_all_df = (
                            drive_retirements_all_df.div(stock_for_rate.replace(0.0, np.nan))
                            .replace([np.inf, -np.inf], np.nan)
                            .fillna(0.0)
                            .clip(lower=0.0)
                        )
                        affected_rate_cols = [
                            drive for drive in changed_drives
                            if drive in drive_retirement_rate_all_df.columns
                        ]
                        if affected_rate_cols:
                            drive_retirement_rate_affected_avg = drive_retirement_rate_all_df[
                                affected_rate_cols
                            ].mean(axis=1)
                            cf_stock_for_rate = stock_for_rate.reindex(
                                index=years,
                                columns=affected_rate_cols,
                                fill_value=0.0,
                            )
                            cf_retirements = drive_baseline_retire_df.reindex(
                                index=years,
                                columns=affected_rate_cols,
                                fill_value=0.0,
                            )
                            drive_retirement_rate_counterfactual_affected_avg = (
                                cf_retirements.div(cf_stock_for_rate.replace(0.0, np.nan))
                                .replace([np.inf, -np.inf], np.nan)
                                .fillna(0.0)
                                .clip(lower=0.0)
                                .mean(axis=1)
                            )
                        non_affected_rate_cols = [
                            drive for drive in drive_retirement_rate_all_df.columns
                            if drive not in set(changed_drives)
                            and float(stock_for_rate[drive].max()) > 1e-12
                        ]
                        if non_affected_rate_cols:
                            drive_retirement_rate_non_affected_avg = drive_retirement_rate_all_df[
                                non_affected_rate_cols
                            ].mean(axis=1)
                            cf_stock_for_non_affected = stock_for_rate.reindex(
                                index=years,
                                columns=non_affected_rate_cols,
                                fill_value=0.0,
                            )
                            cf_retirements_non_affected = drive_baseline_retire_df.reindex(
                                index=years,
                                columns=non_affected_rate_cols,
                                fill_value=0.0,
                            )
                            drive_retirement_rate_counterfactual_non_affected_avg = (
                                cf_retirements_non_affected.div(
                                    cf_stock_for_non_affected.replace(0.0, np.nan)
                                )
                                .replace([np.inf, -np.inf], np.nan)
                                .fillna(0.0)
                                .clip(lower=0.0)
                                .mean(axis=1)
                            )
                            non_affected_drives = sorted(set(non_affected_drives).union(non_affected_rate_cols))
        if (
            not changed_drives
            and drive_rates_df.empty
            and drive_stock_share_df.empty
            and drive_sales_df.empty
            and drive_retirements_df.empty
        ):
            unused_drives = drive_policy_diag.get("unused_policy_drives", [])
            if isinstance(unused_drives, (list, tuple)) and unused_drives:
                drive_panel_note = (
                    "Drive-specific panels hidden: policy drives were not present in filtered stock data "
                    f"({', '.join(str(d) for d in unused_drives)})."
                )
            else:
                drive_panel_note = (
                    "Drive-specific panels hidden: no non-zero drive-level turnover effects were detected."
                )
    else:
        if result.get("drive_turnover_policy_input") is None and result.get("turnover_policies"):
            drive_panel_note = (
                "Drive-specific panels hidden: this run applied vehicle-level turnover policies only."
            )
        else:
            drive_panel_note = (
                "Drive-specific panels hidden: no drive_policy_diagnostics were included in the result."
            )

    if not drive_sales_df.empty:
        drive_sales_grouped_df = _aggregate_drive_frame_by_family(drive_sales_df)
    if not drive_retirements_df.empty:
        drive_retirements_grouped_df = _aggregate_drive_frame_by_family(drive_retirements_df)

    if not drive_rates_df.empty:
        def plot_drive_policy_rates(ax):
            for col in drive_rates_df.columns:
                ax.plot(years, drive_rates_df[col] * 100.0, label=col)
            ax.set_title(f"{economy_label}{transport_label}: Policy-Adjusted Drive Turnover")
            ax.set_xlabel("Year")
            ax.set_ylabel("Additional retirement rate (%)")
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_policy_rates", plot_drive_policy_rates))

    if not drive_stock_share_df.empty:
        def plot_affected_drive_shares(ax):
            for col in drive_stock_share_df.columns:
                ax.plot(years, drive_stock_share_df[col], label=col)
            ax.set_title(f"{economy_label}{transport_label}: Affected Drive Stock Shares")
            ax.set_xlabel("Year")
            ax.set_ylabel("Share of mapped vehicle-type stocks")
            ax.set_ylim(0, 1)
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_policy_stock_shares", plot_affected_drive_shares))

    if not drive_sales_df.empty:
        def plot_drive_sales(ax):
            for col in drive_sales_df.columns:
                ax.plot(years, drive_sales_df[col], label=col)
            if not drive_sales_counterfactual_affected_avg.empty:
                ax.plot(
                    years,
                    drive_sales_counterfactual_affected_avg.reindex(years).fillna(0.0),
                    color="tab:gray",
                    linestyle=":",
                    linewidth=1.6,
                    label=f"Affected drives counterfactual ({drive_counterfactual_parenthetical})",
                )
            if not drive_sales_counterfactual_non_affected_avg.empty:
                ax.plot(
                    years,
                    drive_sales_counterfactual_non_affected_avg.reindex(years).fillna(0.0),
                    color="black",
                    linestyle=":",
                    linewidth=1.4,
                    label=f"Non-affected drives counterfactual ({drive_counterfactual_parenthetical})",
                )
            if not drive_sales_non_affected_avg.empty:
                if not drive_sales_affected_avg.empty:
                    ax.plot(
                        years,
                        drive_sales_affected_avg.reindex(years).fillna(0.0),
                        color="tab:blue",
                        linestyle=":",
                        linewidth=1.6,
                        label="Average (affected drives)",
                    )
                ax.plot(
                    years,
                    drive_sales_non_affected_avg.reindex(years).fillna(0.0),
                    color="black",
                    linestyle="--",
                    linewidth=1.5,
                    label="Average (non-affected drives)",
                )
            elif len(drive_sales_df.columns) > 1:
                avg_label = "Average (affected drives)" if changed_drives else "Average (all drives)"
                ax.plot(
                    years,
                    drive_sales_df.mean(axis=1),
                    color="black",
                    linestyle="--",
                    linewidth=1.5,
                    label=avg_label,
                )
            ax.set_title(f"{economy_label}{transport_label}: Drive-Attributed Sales")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual sales (vehicles)")
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_attributed_sales", plot_drive_sales))

    if not drive_sales_grouped_df.empty:
        def plot_drive_sales_grouped(ax):
            for col in drive_sales_grouped_df.columns:
                ax.plot(years, drive_sales_grouped_df[col], label=col)
            ax.set_title(f"{economy_label}{transport_label}: Drive-Grouped Sales")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual sales (vehicles)")
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_grouped_sales", plot_drive_sales_grouped))

    if not drive_retirements_df.empty:
        def plot_drive_retirements(ax):
            for col in drive_retirements_df.columns:
                ax.plot(years, drive_retirements_df[col], label=col)
            if not drive_retirements_counterfactual_affected_avg.empty:
                ax.plot(
                    years,
                    drive_retirements_counterfactual_affected_avg.reindex(years).fillna(0.0),
                    color="tab:gray",
                    linestyle=":",
                    linewidth=1.6,
                    label=f"Affected drives counterfactual ({drive_counterfactual_parenthetical})",
                )
            if not drive_retirements_counterfactual_non_affected_avg.empty:
                ax.plot(
                    years,
                    drive_retirements_counterfactual_non_affected_avg.reindex(years).fillna(0.0),
                    color="black",
                    linestyle=":",
                    linewidth=1.4,
                    label=f"Non-affected drives counterfactual ({drive_counterfactual_parenthetical})",
                )
            if not drive_retirements_non_affected_avg.empty:
                if not drive_retirements_affected_avg.empty:
                    ax.plot(
                        years,
                        drive_retirements_affected_avg.reindex(years).fillna(0.0),
                        color="tab:blue",
                        linestyle=":",
                        linewidth=1.6,
                        label="Average (affected drives)",
                    )
                ax.plot(
                    years,
                    drive_retirements_non_affected_avg.reindex(years).fillna(0.0),
                    color="black",
                    linestyle="--",
                    linewidth=1.5,
                    label="Average (non-affected drives)",
                )
            elif len(drive_retirements_df.columns) > 1:
                avg_label = "Average (affected drives)" if changed_drives else "Average (all drives)"
                ax.plot(
                    years,
                    drive_retirements_df.mean(axis=1),
                    color="black",
                    linestyle="--",
                    linewidth=1.5,
                    label=avg_label,
                )
            ax.set_title(f"{economy_label}{transport_label}: Drive-Attributed Retirements")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual retirements (vehicles)")
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_attributed_retirements", plot_drive_retirements))

    if not drive_retirements_grouped_df.empty:
        def plot_drive_retirements_grouped(ax):
            for col in drive_retirements_grouped_df.columns:
                ax.plot(years, drive_retirements_grouped_df[col], label=col)
            ax.set_title(f"{economy_label}{transport_label}: Drive-Grouped Retirements")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual retirements (vehicles)")
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_grouped_retirements", plot_drive_retirements_grouped))

    if not total_sales_counterfactual_series.empty:
        def plot_total_sales_counterfactual(ax):
            policy_total = (
                total_sales_policy_series.reindex(years).fillna(0.0)
                if not total_sales_policy_series.empty
                else sales_df.sum(axis=1).reindex(years).fillna(0.0)
            )
            ax.plot(years, policy_total, label="Total sales (policy)")
            ax.plot(
                years,
                total_sales_counterfactual_series.reindex(years).fillna(0.0),
                color="tab:gray",
                linestyle=":",
                linewidth=1.6,
                label=f"Total sales counterfactual ({drive_counterfactual_parenthetical})",
            )
            ax.set_title(f"{economy_label}{transport_label}: Total Sales (Policy vs Counterfactual)")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual sales (vehicles)")
            ax.grid(True)
            _legend(ax)

        panels.append(("total_sales_counterfactual", plot_total_sales_counterfactual))

    if (
        not drive_retirement_rate_affected_avg.empty
        or not drive_retirement_rate_non_affected_avg.empty
        or not drive_retirement_rate_counterfactual_affected_avg.empty
        or not drive_retirement_rate_counterfactual_non_affected_avg.empty
    ):
        def plot_drive_retirement_rate_comparison(ax):
            if not drive_retirement_rate_affected_avg.empty:
                ax.plot(
                    years,
                    drive_retirement_rate_affected_avg.reindex(years).fillna(0.0) * 100.0,
                    label="Affected drives (average)",
                )
            if not drive_retirement_rate_counterfactual_affected_avg.empty:
                ax.plot(
                    years,
                    drive_retirement_rate_counterfactual_affected_avg.reindex(years).fillna(0.0) * 100.0,
                    color="tab:gray",
                    linestyle=":",
                    linewidth=1.6,
                    label=f"Affected drives counterfactual ({drive_counterfactual_parenthetical})",
                )
            if not drive_retirement_rate_non_affected_avg.empty:
                ax.plot(
                    years,
                    drive_retirement_rate_non_affected_avg.reindex(years).fillna(0.0) * 100.0,
                    color="black",
                    linestyle="--",
                    linewidth=1.5,
                    label="Non-affected drives (average)",
                )
            if not drive_retirement_rate_counterfactual_non_affected_avg.empty:
                ax.plot(
                    years,
                    drive_retirement_rate_counterfactual_non_affected_avg.reindex(years).fillna(0.0) * 100.0,
                    color="black",
                    linestyle=":",
                    linewidth=1.4,
                    label=f"Non-affected drives counterfactual ({drive_counterfactual_parenthetical})",
                )
            ax.set_title(f"{economy_label}{transport_label}: Average Drive Retirement Rate")
            ax.set_xlabel("Year")
            ax.set_ylabel("Vehicles retired each year (%)")
            ax.grid(True)
            _legend(ax)

        panels.append(("drive_retirement_rate_comparison", plot_drive_retirement_rate_comparison))

    n_panels = len(panels)
    n_cols = max(1, int(n_cols))
    n_chart_rows = (n_panels + n_cols - 1) // n_cols
    guide_rows = 1 if show_guide else 0
    total_rows = n_chart_rows + guide_rows
    fig_height = 4.0 * n_chart_rows + (2.6 if show_guide else 0.8)
    fig_width = 6.0 * n_cols
    fig_grid = plt.figure(figsize=(fig_width, fig_height))
    grid_spec = fig_grid.add_gridspec(
        total_rows,
        n_cols,
        height_ratios=[1.0] * n_chart_rows + ([1.0] if show_guide else []),
    )

    axes_flat = [
        fig_grid.add_subplot(grid_spec[idx // n_cols, idx % n_cols])
        for idx in range(n_chart_rows * n_cols)
    ]
    guide_ax = None
    if show_guide:
        guide_ax = fig_grid.add_subplot(grid_spec[n_chart_rows, :])
        guide_ax.axis("off")

    for ax, (name, fn) in zip(axes_flat, panels):
        fn(ax)
        figs[name] = fig_grid
    for ax in axes_flat[n_panels:]:
        ax.axis("off")

    figs["grid"] = fig_grid
    if show_guide and guide_ax is not None:
        title_size = plt.rcParams.get("axes.titlesize", "medium")
        guide_ax.set_title("Dashboard Guide", fontsize=title_size)
        guide_lines = [
            "- Ownership trajectory: car-equivalent vehicles per 1,000 people (M_envelope x 1000).",
            "- Target stocks by vehicle type: required fleet sizes after splitting ownership path by type.",
            "- Sales by vehicle type: additions needed after carrying survivors forward to hit targets.",
            "- Sales shares: each type's share of total new sales.",
            "- Retirements: prior-year fleet minus survivors.",
            "- Survival and vintage profiles: lifecycle assumptions controlling turnover timing.",
            "- Survival chart: solid line is annual survival p(age); dashed line is cumulative survival S(age).",
        ]
        has_drive_panels = (
            not drive_rates_df.empty
            or not drive_stock_share_df.empty
            or not drive_sales_df.empty
            or not drive_retirements_df.empty
            or not drive_sales_grouped_df.empty
            or not drive_retirements_grouped_df.empty
        )
        if has_drive_panels:
            if changed_drives:
                guide_lines.append(
                    f"- Drive-specific panels include all observed drives; policy-affected drives: {', '.join(changed_drives)}."
                )
            else:
                guide_lines.append(
                    "- Drive-specific panels include all observed drives from the mapped vehicle buckets."
                )

            if not drive_sales_df.empty or not drive_retirements_df.empty:
                guide_lines.append(
                    "- Drive-attributed sales/retirements allocate each vehicle-type total using drive stock shares."
                )
            if not drive_sales_grouped_df.empty or not drive_retirements_grouped_df.empty:
                guide_lines.append(
                    "- Grouped drive panels aggregate detailed drives into ICE/HEV/PHEV/EV/Other families."
                )
            if (
                not drive_sales_counterfactual_affected_avg.empty
                or not drive_retirements_counterfactual_affected_avg.empty
            ):
                guide_lines.append(
                    f"- Dotted gray line in drive-attributed panels is affected-drive counterfactual ({drive_counterfactual_parenthetical})."
                )
            if (
                not drive_sales_counterfactual_non_affected_avg.empty
                or not drive_retirements_counterfactual_non_affected_avg.empty
            ):
                guide_lines.append(
                    f"- Dotted black line in drive-attributed panels is non-affected-drive counterfactual ({drive_counterfactual_parenthetical})."
                )
            if not drive_sales_non_affected_avg.empty or not drive_retirements_non_affected_avg.empty:
                guide_lines.append(
                    "- Dashed black line in drive-attributed panels is the average across non-affected drives."
                )
                if not drive_sales_affected_avg.empty or not drive_retirements_affected_avg.empty:
                    guide_lines.append(
                        "- Dotted blue line in drive-attributed panels is the average across affected drives."
                    )
                if non_affected_drives:
                    guide_lines.append(
                        f"- Non-affected comparison drives: {', '.join(non_affected_drives)}."
                    )
            elif len(changed_drives) > 1:
                guide_lines.append(
                    "- Dashed black line in drive-attributed panels is the average across affected drives."
                )
            if (
                not drive_retirement_rate_affected_avg.empty
                or not drive_retirement_rate_non_affected_avg.empty
                or not drive_retirement_rate_counterfactual_affected_avg.empty
                or not drive_retirement_rate_counterfactual_non_affected_avg.empty
            ):
                guide_lines.append(
                    "- Average drive retirement rate = drive-attributed retirements divided by drive stock each year."
                )
                if not drive_retirement_rate_counterfactual_affected_avg.empty:
                    guide_lines.append(
                        f"- Dotted gray line is affected-drive counterfactual ({drive_counterfactual_parenthetical}) in the retirement-rate panel."
                    )
                if not drive_retirement_rate_counterfactual_non_affected_avg.empty:
                    guide_lines.append(
                        f"- Dotted black line is non-affected-drive counterfactual ({drive_counterfactual_parenthetical}) in the retirement-rate panel."
                    )
            if not total_sales_counterfactual_series.empty:
                guide_lines.append(
                    f"- Total sales panel compares policy run against a counterfactual ({drive_counterfactual_parenthetical}) dotted-gray line."
                )
        elif drive_panel_note:
            guide_lines.append(f"- {drive_panel_note}")
        if omitted_base_year:
            guide_lines.append(f"- Base year ({years_full[0]}) is omitted from year-based panels for readability.")
        guide_ax.text(
            0.02,
            0.98,
            "\n".join(guide_lines),
            transform=guide_ax.transAxes,
            ha="left",
            va="top",
            fontsize=title_size,
            wrap=True,
        )

    if show:
        fig_grid.tight_layout(rect=(0.0, 0.0, 1.0, 1.0))
        plt.show()

    saved_plot_paths = _save_dashboard_figures(
        figs,
        dashboard_name=f"{transport_label}_dashboard",
    )
    if saved_plot_paths:
        figs["saved_plot_paths"] = saved_plot_paths

    return figs


def plot_freight_sales_result(
    result: dict,
    economy: str | None = None,
    show: bool = False,
    skip_base_year: bool = True,
    *,
    n_cols: int = 3,
    show_guide: bool = False,
) -> dict:
    """
    Freight-specific dashboard with freight share labels.
    """
    return plot_passenger_sales_result(
        result=result,
        economy=economy,
        show=show,
        skip_base_year=skip_base_year,
        share_key="freight_shares",
        transport_label="Freight road",
        n_cols=n_cols,
        show_guide=show_guide,
    )


def plot_transport_sales_dashboard(
    passenger_result: dict,
    freight_result: dict,
    economy: str | None = None,
    show: bool = False,
    skip_base_year: bool = True,
    *,
    n_cols: int = 3,
    show_guide: bool = False,
) -> dict:
    """
    Build a combined passenger+freight dashboard.

    Ownership trajectories are overlaid by mode (passenger/freight), while
    stock-flow panels are combined by vehicle type (LPV, MC, Bus, Trucks, LCVs).
    Profile panels collapse to a single line when assumptions are identical.

    n_cols, page_size, and max_height_inches mirror plot_passenger_sales_result
    and make it easier to export an A4-friendly image without excessive height.
    """
    try:
        import matplotlib.pyplot as plt
    except Exception as e:  # pragma: no cover - optional dependency
        raise ImportError(
            "matplotlib is required for plotting; install with `pip install matplotlib`."
        ) from e

    passenger_envelope = passenger_result.get("M_envelope")
    freight_envelope = freight_result.get("M_envelope")
    passenger_years = pd.Index(passenger_envelope.index) if passenger_envelope is not None else pd.Index([], dtype=int)
    freight_years = pd.Index(freight_envelope.index) if freight_envelope is not None else pd.Index([], dtype=int)
    years_full = passenger_years.union(freight_years).sort_values()
    if len(years_full) == 0:
        raise ValueError("Cannot build combined dashboard: missing M_envelope years for both results.")
    years = years_full[1:] if (skip_base_year and len(years_full) > 1) else years_full
    omitted_base_year = bool(skip_base_year and len(years_full) > 1)
    economy_label = f"{economy} " if economy else ""
    preferred_vehicle_order = ["LPV", "MC", "Bus", "Trucks", "LCVs"]

    def _series_or_default(result_obj: dict, key: str, default: pd.Series | None = None) -> pd.Series:
        if key in result_obj and result_obj.get(key) is not None:
            return pd.Series(result_obj[key]).reindex(years).astype(float)
        if default is not None:
            return pd.Series(default).reindex(years).astype(float)
        return pd.Series(np.nan, index=years, dtype=float)

    def _legend(ax, loc: str = "best", max_per_col: int = 5) -> None:
        handles, labels = ax.get_legend_handles_labels()
        if not labels:
            return
        ncol = max(1, math.ceil(len(labels) / max_per_col))
        for h in handles:
            if hasattr(h, "set_alpha"):
                try:
                    h.set_alpha(0.8)
                except Exception:
                    pass
        leg = ax.legend(handles, labels, ncol=ncol, loc=loc, framealpha=0.78)
        if leg:
            leg.set_alpha(0.85)

    def _plot_overlay(ax, a: pd.Series, b: pd.Series, title: str, ylabel: str):
        if a.notna().any():
            ax.plot(years, a.values, label="Passenger")
        if b.notna().any():
            ax.plot(years, b.values, label="Freight")
        ax.set_title(f"{economy_label}{title}")
        ax.set_xlabel("Year")
        ax.set_ylabel(ylabel)
        ax.grid(True)
        _legend(ax)

    def _combined_vehicle_frame(metric_key: str) -> pd.DataFrame:
        frames = []
        passenger_map = passenger_result.get(metric_key)
        freight_map = freight_result.get(metric_key)
        if isinstance(passenger_map, dict) and passenger_map:
            frames.append(pd.DataFrame(passenger_map).reindex(years).astype(float))
        if isinstance(freight_map, dict) and freight_map:
            frames.append(pd.DataFrame(freight_map).reindex(years).astype(float))
        if not frames:
            return pd.DataFrame(index=years, dtype=float)

        combined = pd.concat(frames, axis=1)
        if combined.columns.duplicated().any():
            # Safe-guard for duplicated type labels across mode inputs.
            combined = combined.T.groupby(level=0).sum().T

        ordered = [c for c in preferred_vehicle_order if c in combined.columns]
        ordered += [c for c in combined.columns if c not in ordered]
        return combined.reindex(columns=ordered).fillna(0.0)

    def _plot_vehicle_lines(
        ax,
        df: pd.DataFrame,
        title: str,
        ylabel: str,
        ylim: tuple[float, float] | None = None,
    ):
        for col in df.columns:
            ax.plot(years, df[col], label=col)
        ax.set_title(f"{economy_label}{title}")
        ax.set_xlabel("Year")
        ax.set_ylabel(ylabel)
        if ylim is not None:
            ax.set_ylim(*ylim)
        ax.grid(True)
        _legend(ax)

    # Ownership overlay
    passenger_ownership_default = pd.Series(passenger_result.get("M_envelope", pd.Series(dtype=float))) * 1000.0
    freight_ownership_default = pd.Series(freight_result.get("M_envelope", pd.Series(dtype=float))) * 1000.0
    passenger_ownership = _series_or_default(passenger_result, "adjusted_vehicle_ownership", default=passenger_ownership_default)
    freight_ownership = _series_or_default(freight_result, "adjusted_vehicle_ownership", default=freight_ownership_default)

    # Vehicle-type combined panels
    stocks_df = _combined_vehicle_frame("target_stocks")
    sales_df = _combined_vehicle_frame("sales")
    retirements_df = _combined_vehicle_frame("retirements")

    if not sales_df.empty:
        total_sales = sales_df.sum(axis=1).replace(0.0, np.nan)
        sales_shares_df = sales_df.div(total_sales, axis=0).fillna(0.0)
    else:
        sales_shares_df = pd.DataFrame(index=years, dtype=float)

    if not stocks_df.empty:
        total_stocks = stocks_df.sum(axis=1).replace(0.0, np.nan)
        stock_mix_df = stocks_df.div(total_stocks, axis=0).fillna(0.0)
    else:
        stock_mix_df = pd.DataFrame(index=years, dtype=float)

    panels: list[tuple[str, callable]] = []

    def panel_adjusted(ax):
        _plot_overlay(
            ax,
            passenger_ownership,
            freight_ownership,
            title="Ownership Trajectory (Passenger vs Freight)",
            ylabel="car-equivalent vehicles per 1000 people",
        )

    panels.append(("adjusted_ownership_overlay", panel_adjusted))

    if not stocks_df.empty:
        def panel_stocks(ax):
            _plot_vehicle_lines(
                ax,
                stocks_df,
                title="Target Stocks by Vehicle Type",
                ylabel="Stocks (vehicles)",
            )

        panels.append(("stocks_by_vehicle", panel_stocks))

    if not sales_df.empty:
        def panel_sales(ax):
            _plot_vehicle_lines(
                ax,
                sales_df,
                title="Sales by Vehicle Type",
                ylabel="Annual sales (vehicles)",
            )

        panels.append(("sales_by_vehicle", panel_sales))

    if not retirements_df.empty:
        def panel_retirements(ax):
            _plot_vehicle_lines(
                ax,
                retirements_df,
                title="Retirements by Vehicle Type",
                ylabel="Annual retirements (vehicles)",
            )

        panels.append(("retirements_by_vehicle", panel_retirements))

    if not sales_shares_df.empty:
        def panel_sales_shares(ax):
            _plot_vehicle_lines(
                ax,
                sales_shares_df,
                title="Sales Shares by Vehicle Type",
                ylabel="Share of sales",
                ylim=(0.0, 1.0),
            )

        panels.append(("sales_shares_by_vehicle", panel_sales_shares))

    if not stock_mix_df.empty:
        def panel_stock_mix(ax):
            _plot_vehicle_lines(
                ax,
                stock_mix_df,
                title="Fleet Mix by Vehicle Type",
                ylabel="Share of stocks",
                ylim=(0.0, 1.0),
            )

        panels.append(("stock_mix_by_vehicle", panel_stock_mix))

    # Survival comparison (collapse to one line if identical)
    p_surv_key, p_surv = _first_profile(passenger_result.get("survival_curves"))
    f_surv_key, f_surv = _first_profile(freight_result.get("survival_curves"))
    if p_surv is not None:
        if p_surv.max() > 1.0:
            p_surv = p_surv / 100.0
        p_surv = p_surv.clip(lower=0.0, upper=1.0)
    if f_surv is not None:
        if f_surv.max() > 1.0:
            f_surv = f_surv / 100.0
        f_surv = f_surv.clip(lower=0.0, upper=1.0)

    if p_surv is not None or f_surv is not None:
        def panel_survival(ax):
            if _profiles_are_equal(p_surv, f_surv):
                if p_surv is not None:
                    ax.plot(p_surv.index, p_surv.values * 100.0, marker="o", label="Passenger + Freight (identical)")
                ax.set_title(f"{economy_label}Vehicle Survival by Age (identical)")
            else:
                if p_surv is not None:
                    ax.plot(p_surv.index, p_surv.values * 100.0, marker="o", label=f"Passenger ({p_surv_key or 'profile'})")
                if f_surv is not None:
                    ax.plot(f_surv.index, f_surv.values * 100.0, marker="x", label=f"Freight ({f_surv_key or 'profile'})")
                ax.set_title(f"{economy_label}Vehicle Survival by Age")
            ax.set_xlabel("Age (years)")
            ax.set_ylabel("Survival (%)")
            ax.grid(True)
            ax.legend()

        panels.append(("survival_overlay", panel_survival))

    # Vintage comparison (collapse to one line if identical)
    p_vint_key, p_vint = _first_profile(passenger_result.get("vintage_profiles"))
    f_vint_key, f_vint = _first_profile(freight_result.get("vintage_profiles"))
    if p_vint is not None:
        p_vint = p_vint * (100.0 if p_vint.max() <= 1.0 else 1.0)
    if f_vint is not None:
        f_vint = f_vint * (100.0 if f_vint.max() <= 1.0 else 1.0)

    if p_vint is not None or f_vint is not None:
        def panel_vintage(ax):
            if _profiles_are_equal(p_vint, f_vint):
                if p_vint is not None:
                    ax.plot(p_vint.index, p_vint.values, marker="o", label="Passenger + Freight (identical)")
                ax.set_title(f"{economy_label}Vintage Profile (identical)")
            else:
                if p_vint is not None:
                    ax.plot(p_vint.index, p_vint.values, marker="o", label=f"Passenger ({p_vint_key or 'profile'})")
                if f_vint is not None:
                    ax.plot(f_vint.index, f_vint.values, marker="x", label=f"Freight ({f_vint_key or 'profile'})")
                ax.set_title(f"{economy_label}Vintage Profiles")
            ax.set_xlabel("Age (years)")
            ax.set_ylabel("Vintage share (%)")
            ax.grid(True)
            ax.legend()

        panels.append(("vintage_overlay", panel_vintage))

    n_panels = len(panels)
    n_cols = max(1, int(n_cols))
    n_chart_rows = (n_panels + n_cols - 1) // n_cols
    guide_rows = 1 if show_guide else 0
    total_rows = n_chart_rows + guide_rows
    fig_height = 3.6 * n_chart_rows + (2.4 if show_guide else 0.8)
    fig_width = 6.0 * n_cols
    fig_grid = plt.figure(figsize=(fig_width, fig_height))
    grid_spec = fig_grid.add_gridspec(
        total_rows,
        n_cols,
        height_ratios=[1.0] * n_chart_rows + ([0.95] if show_guide else []),
    )
    axes_flat = [
        fig_grid.add_subplot(grid_spec[idx // n_cols, idx % n_cols])
        for idx in range(n_chart_rows * n_cols)
    ]
    guide_ax = None
    if show_guide:
        guide_ax = fig_grid.add_subplot(grid_spec[n_chart_rows, :])
        guide_ax.axis("off")

    figs: dict[str, object] = {}
    for ax, (name, fn) in zip(axes_flat, panels):
        fn(ax)
        figs[name] = fig_grid

    for ax in axes_flat[n_panels:]:
        ax.axis("off")

    figs["grid"] = fig_grid
    title_size = plt.rcParams.get("axes.titlesize", "medium")

    if isinstance(title_size, (int, float)):
        suptitle_size = title_size + 2
    else:
        suptitle_size = "x-large"
    dashboard_title = f"{economy_label}Transport Sales Dashboard (Passenger + Freight)".strip()
    fig_grid.suptitle(dashboard_title, fontsize=suptitle_size, y=0.995)

    if show_guide and guide_ax is not None:
        guide_lines = [
            "- Ownership trajectory compares passenger and freight at the mode level.",
            "- Vehicle-type panels combine LPV, MC, Bus, Trucks, and LCVs in each chart.",
            "- Target stocks are required fleet size; sales are additions needed after survivors are carried forward.",
            "- Retirements are exits from the fleet; share panels show mix over time.",
            "- Survival and vintage profiles are lifecycle assumptions that shape turnover dynamics.",
        ]
        if omitted_base_year:
            guide_lines.append(f"- Base year ({years_full[0]}) is omitted from year-based panels for readability.")
        guide_lines.append("- Profile panels collapse to one line when passenger and freight assumptions are identical.")

        guide_ax.set_title("Dashboard Guide", fontsize=title_size, pad=8)
        guide_ax.text(
            0.01,
            0.98,
            "\n".join(guide_lines),
            transform=guide_ax.transAxes,
            ha="left",
            va="top",
            fontsize=title_size,
            wrap=True,
        )

    if show:
        fig_grid.tight_layout(rect=(0.0, 0.03, 1.0, 0.965))
        plt.show()

    saved_plot_paths = _save_dashboard_figures(
        figs,
        dashboard_name="transport_sales_dashboard",
    )
    if saved_plot_paths:
        figs["saved_plot_paths"] = saved_plot_paths

    return figs


#%% lifecycle profile loader using lifecycle_profile_editor outputs


def _profile_dict_to_series(profile: dict) -> pd.Series:
    ages = sorted(profile.keys())
    return pd.Series([profile[a] for a in ages], index=pd.Index(ages, dtype=int), dtype=float)

def _convert_cumulative_survival_to_annual(survival: pd.Series) -> pd.Series:
    """
    Convert a cumulative survival curve S(age) (% remaining) to annual survival
    probabilities p(age), where p(age) is the probability to survive from
    age -> age+1. The last age has p=0.
    """
    surv = survival.astype(float)
    scale = 100.0 if surv.max() > 1.0 else 1.0
    surv = (surv / scale).clip(lower=1e-9, upper=1.0)

    ages = surv.index
    annual = surv.shift(-1) / surv
    annual.iloc[-1] = 0.0  # no survival beyond max age

    return annual.clip(lower=0.0, upper=1.0)

#%% dummy data builders for the example workflow

def run_example_with_real_data(
    *,
    source_path: str | os.PathLike = "data/transport_data_9th/model_output_detailed_2/20_USA_NON_ROAD_DETAILED_model_output20250225.csv",
    survival_path: str | os.PathLike = "data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_path: str | os.PathLike = "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    esto_path: str | os.PathLike | None = "data/merged_file_energy_ALL_20250814_pretrump.csv",
    economy: str = "20_USA",
    scenario: str = "Target",
    base_year: int = 2022,
    final_year: int = 2060,
    base_stocks_path: str | os.PathLike | None = None,
    source_reader_kwargs: dict | None = None,
    vehicle_shares: dict | None = None,
    freight_vehicle_shares: dict | None = None,
    saturated: bool = False,
    weights: dict | None = None,
    freight_weights: dict | None = None,
    M_sat: float | None = None,
    freight_M_sat: float | None = None,
    window_years: int = 10,
    k_min: float = 0.0,
    k_max: float = 0.15,
    plot: bool = True,
    survival_is_cumulative: bool = True,
) -> dict:
    """
    Convenience wrapper for running passenger + freight sales workflows from files.

    Combines the functionality of estimate_passenger_sales_from_files and
    estimate_freight_sales_from_files with
    sensible defaults so it is easy to run an end-to-end example. Raises if
    inputs are missing or invalid.
    """
    source_reader_kwargs = source_reader_kwargs or {}
    source_path = resolve_str(source_path)
    survival_path = resolve_str(survival_path)
    vintage_path = resolve_str(vintage_path)
    if esto_path is not None:
        esto_path = resolve_str(esto_path)
    # Validate file existence early for clearer errors
    for p, label in [
        (source_path, "source_path"),
        (survival_path, "survival_path"),
        (vintage_path, "vintage_path"),
        (esto_path, "esto_path"),
    ]:
        if p is None:
            continue
        if not Path(p).expanduser().resolve().exists():
            raise FileNotFoundError(f"{label} not found: {p}")

    passenger_result = estimate_passenger_sales_from_files(
        source_path=source_path,
        survival_path=survival_path,
        vintage_path=vintage_path,
        esto_path=esto_path if esto_path is not None else "",
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        base_stocks_path=base_stocks_path,
        source_reader_kwargs=source_reader_kwargs,
        vehicle_shares=vehicle_shares,
        saturated=saturated,
        weights=weights,
        M_sat=M_sat,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=False,
        survival_is_cumulative=survival_is_cumulative,
    )
    freight_result = estimate_freight_sales_from_files(
        source_path=source_path,
        survival_path=survival_path,
        vintage_path=vintage_path,
        esto_path=esto_path if esto_path is not None else "",
        economy=economy,
        scenario=scenario,
        base_year=base_year,
        final_year=final_year,
        base_stocks_path=base_stocks_path,
        source_reader_kwargs=source_reader_kwargs,
        vehicle_shares=freight_vehicle_shares,
        weights=freight_weights,
        M_sat=freight_M_sat,
        window_years=window_years,
        k_min=k_min,
        k_max=k_max,
        plot=False,
        survival_is_cumulative=survival_is_cumulative,
    )

    combined_figures = None
    if plot:
        try:
            combined_figures = plot_transport_sales_dashboard(
                passenger_result,
                freight_result,
                economy=economy,
                show=True,
                skip_base_year=True,
            )
        except ImportError as e:
            combined_figures = {"error": e}
            _raise_plot_failure("Combined transport dashboard plotting", e)
        except Exception as e:
            combined_figures = {"error": e}
            _raise_plot_failure("Combined transport dashboard plotting", e)

    result = dict(passenger_result)
    result["passenger_result"] = passenger_result
    result["freight_result"] = freight_result
    result["combined_figures"] = combined_figures
    print(f"[INFO] Ran passenger + freight example with real data from {Path(source_path).name}")
    return result


#%% example workflow (prefers real files; falls back to dummy)
if __name__ == "__main__":

    result = run_example_with_real_data()

    M_envelope = result["M_envelope"]
    target_stocks = result["target_stocks"]
    sales_by_type = result["sales"]
    passenger_total_sales = result["passenger_total_sales"]
    passenger_shares = result["passenger_shares"]
    k_used = result["k_used"]

#%%
