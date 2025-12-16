#%% imports
import os
from pathlib import Path
import numpy as np
import pandas as pd
from lifecycle_profile_editor import (
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
    df = pd.read_excel(esto_path, sheet_name=sheet_name)
    mask = (
        (df["economy"] == economy)
        & (df["scenarios"] == scenario.lower())
        & (df["sectors"] == sector)
        & ((df["sub1sectors"] == sub1sector) if sub1sector is not None else True)
    ) 
    df = df.loc[mask]
    #drop any ciols with Unnamed in them
    column_labels = pd.Index(df.columns.map(str))
    df = df.loc[:, ~column_labels.str.contains(r"^Unnamed", na=False)]
    #also, we need to split it by <=base year and >base year then drop subtotals depending onwhat years we have:
    #first make the year cols numeric
    year_cols = [col for col in df.columns if isinstance(col, (int, float)) or (isinstance(col, str) and col.isdigit())]
    others = [col for col in df.columns if col not in year_cols]
    historical = df[others + [col for col in year_cols if col <= base_year]].loc[df['subtotal_layout']!=True]
    future = df[others + [col for col in year_cols if col > base_year]].loc[df['subtotal_results']!=True]
    df = pd.merge(
        historical,
        future,
        on=others,
        how="outer",
        suffixes=("", ""),
    ).fillna(0.0)
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
    cap_by_type = {v: weights[v] * stock for v, stock in base_stocks.items()}
    total_capacity = sum(cap_by_type.values())
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

    share_df = share_df.fillna(method="ffill").fillna(method="bfill").fillna(0.0)
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
    max_age = len(vintage_profile)

    cohorts = pd.DataFrame(
        data=0.0,
        index=years,
        columns=range(max_age),
        dtype=float,
    )

    base_year = years[0]
    base_stock = target_stock.loc[base_year]

    vp = vintage_profile / vintage_profile.sum()
    cohorts.loc[base_year, :] = base_stock * vp.values

    return cohorts


def compute_sales_from_stock_targets(
    target_stock: pd.Series,
    survival_curve: pd.Series,
    vintage_profile: pd.Series,
    *,
    return_retirements: bool = False,
) -> tuple:
    """
    Turn a target stock series into sales (and optionally retirements) given
    survival and vintage profiles.
    """
    max_age = len(vintage_profile)
    assert len(survival_curve) == max_age, (
        "survival_curve and vintage_profile must have same length."
    )

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
        except ImportError:
            result["figures"] = None
        except Exception as e:
            result["figures"] = {"error": e}
    
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
            # Reuse plotting by providing the expected key
            temp_result = dict(result)
            temp_result["passenger_shares"] = freight_shares
            result["figures"] = plot_passenger_sales_result(temp_result, show=True)
        except ImportError:
            result["figures"] = None
        except Exception as e:
            result["figures"] = {"error": e}

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
        breakpoint()
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


def plot_passenger_sales_result(
    result: dict,
    economy: str | None = None,
    show: bool = False,
) -> dict:
    """
    Build simple matplotlib charts from a passenger sales result dict.

    Returns a dict of figures: envelope, stocks, sales, sales shares, and
    retirements (if available).
    """
    
    try:
        import matplotlib.pyplot as plt
    except Exception as e:  # pragma: no cover - optional dependency
        raise ImportError(
            "matplotlib is required for plotting; install with `pip install matplotlib`."
        ) from e

    years = result.get("M_envelope").index
    economy_label = f"{economy} " if economy else ""

    adjusted_ownership = result.get("adjusted_vehicle_ownership")
    if adjusted_ownership is None and "M_envelope" in result:
        adjusted_ownership = result["M_envelope"] * 1000.0

    figs: dict[str, object] = {}

    panels: list[tuple[str, callable]] = []

    def plot_envelope(ax):
        ax.plot(years, adjusted_ownership.values, label="Adjusted ownership (per 1000)")
        ax.set_title(f"{economy_label}Adjusted Vehicle Ownership")
        ax.set_xlabel("Year")
        ax.set_ylabel("car-equivalent vehicles per 1000 people")
        ax.grid(True)
        ax.legend()

    panels.append(("envelope", plot_envelope))

    stocks_df = pd.DataFrame(result["target_stocks"])

    def plot_stocks(ax):
        for col in stocks_df.columns:
            ax.plot(years, stocks_df[col], label=col)
        ax.set_title(f"{economy_label}Target Stocks by Vehicle")
        ax.set_xlabel("Year")
        ax.set_ylabel("Stocks (vehicles)")
        ax.grid(True)
        ax.legend()

    panels.append(("stocks", plot_stocks))

    sales_df = pd.DataFrame(result["sales"])

    def plot_sales(ax):
        for col in sales_df.columns:
            ax.plot(years, sales_df[col], label=col)
        ax.set_title(f"{economy_label}Sales by Vehicle")
        ax.set_xlabel("Year")
        ax.set_ylabel("Annual sales (vehicles)")
        ax.grid(True)
        ax.legend()

    panels.append(("sales", plot_sales))

    shares_df = pd.DataFrame(result["passenger_shares"])

    def plot_shares(ax):
        for col in shares_df.columns:
            ax.plot(years, shares_df[col], label=col)
        ax.set_title(f"{economy_label}Sales Shares")
        ax.set_xlabel("Year")
        ax.set_ylabel("Share of sales")
        ax.set_ylim(0, 1)
        ax.grid(True)
        ax.legend()

    panels.append(("sales_shares", plot_shares))

    if "retirements" in result:
        retire_df = pd.DataFrame(result["retirements"])

        def plot_retires(ax):
            for col in retire_df.columns:
                ax.plot(years, retire_df[col], label=col)
            ax.set_title(f"{economy_label}Retirements by Vehicle")
            ax.set_xlabel("Year")
            ax.set_ylabel("Annual retirements (vehicles)")
            ax.grid(True)
            ax.legend()

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
            ax.set_title(f"{economy_label}Survival Curves ({first_key})")
            ax.set_xlabel("Age (years)")
            ax.set_ylabel("Survival (%)")
            ax.grid(True)
            ax.legend()

        def plot_vint(ax):
            ax.plot(vint.index, vint.values * 100.0, marker="o", color="tab:orange")
            ax.set_title(f"{economy_label}Vintage Profile ({first_key})")
            ax.set_xlabel("Age (years)")
            ax.set_ylabel("Vintage share (%)")
            ax.grid(True)

        panels.append(("survival_curve", plot_surv))
        panels.append(("vintage_profile", plot_vint))

    n_panels = len(panels)
    n_cols = 3
    n_rows = (n_panels + n_cols - 1) // n_cols
    fig_grid, axes = plt.subplots(n_rows, n_cols, figsize=(5 * n_cols, 4 * n_rows))
    axes_flat = axes.flatten() if hasattr(axes, "flatten") else [axes]

    for ax in axes_flat[n_panels:]:
        ax.axis("off")

    for ax, (name, fn) in zip(axes_flat, panels):
        fn(ax)
        figs[name] = fig_grid

    figs["grid"] = fig_grid
    # Explanatory note for survival curves
    note = (
        "Survival curves: solid line = annual survival p(age), the probability a "
        "vehicle survives from age to age+1; dashed line = cumulative survival "
        "S(age), the % of the original cohort still in the fleet at each age."
    )
    fig_grid.text(
        0.5,
        0.02,
        note,
        ha="center",
        va="bottom",
        fontsize=8,
        wrap=False,
    )
    if show:
        fig_grid.tight_layout(rect=(0.0, 0.06, 1.0, 1.0))
        plt.show()

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
    source_path: str | os.PathLike = "../data/USA transport file.xlsx",
    survival_path: str | os.PathLike = "../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_path: str | os.PathLike = "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    esto_path: str | os.PathLike | None = "../data/all transport balances data.xlsx",
    economy: str = "20_USA",
    scenario: str = "Target",
    base_year: int = 2022,
    final_year: int = 2060,
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
) -> dict:
    """
    Convenience wrapper for running the passenger sales workflow from files.

    Combines the functionality of estimate_passenger_sales_from_files with
    sensible defaults so it is easy to run an end-to-end example. Raises if
    inputs are missing or invalid.
    """
    source_reader_kwargs = source_reader_kwargs or {}
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

    result = estimate_passenger_sales_from_files(
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
        plot=plot,
        survival_is_cumulative=survival_is_cumulative,
    )
    print(f"[INFO] Ran example with real data from {Path(source_path).name}")
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
