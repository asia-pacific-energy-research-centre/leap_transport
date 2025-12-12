
#%% IMPORTS
import numpy as np
import os
import sys
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd


#%% CORE IO FUNCTIONS (load/save LEAP-style lifecycle profile)

def load_lifecycle_profile_excel(path, sheet_name="Lifecycle Profiles"):
    """
    Load a LEAP lifecycle profile from an Excel file in the format:

    Row 0: 'Area:'    | <area_name>
    Row 1: 'Profile:' | <profile_name>
    Row 2: (optional blank row)
    Next: 'Year'      | 'Value'
    Then: year        | value ...

    Returns:
        area_name (str)
        profile_name (str)
        profile_dict (dict[int, float])  # {year/age: value}
    """
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)

    area_name = df.iloc[0, 1]
    profile_name = df.iloc[1, 1]

    # Find header row ("Year")
    year_rows = df[0] == "Year"
    if not year_rows.any():
        raise ValueError("Could not find 'Year' header row in first column.")

    header_idx = year_rows[year_rows].index[0]

    data = df.iloc[header_idx + 1 :].dropna()
    years = data.iloc[:, 0].astype(int).tolist()
    values = data.iloc[:, 1].astype(float).tolist()

    profile = dict(zip(years, values))
    return area_name, profile_name, profile


def save_lifecycle_profile_excel(new_path, area_name, profile_name, profile, sheet_name="Lifecycle Profiles"):
    """
    Save a lifecycle profile dict {year: value} to Excel in the same
    LEAP export format (Area/Profile/Year/Value).

    If a file already exists at `new_path`, it is moved into an `archive/`
    subfolder beside it. If a file with the same name already exists in
    `archive/`, an incrementing suffix (_1, _2, ...) is applied **in the
    archive only**. The new file is always written to `new_path` itself.
    """
    target_path = Path(new_path)
    years = sorted(profile.keys())
    values = [profile[y] for y in years]

    rows = []
    rows.append(["Area:", area_name])
    rows.append(["Profile:", profile_name])
    rows.append([None, None])
    rows.append(["Year", "Value"])
    for y, v in zip(years, values):
        rows.append([y, v])

    out_df = pd.DataFrame(rows)

    # Move any existing file into archive with de-duplication there.
    if target_path.exists():
        archive_dir = target_path.parent / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        base_str = target_path.stem
        ext = target_path.suffix
        archived_candidate = archive_dir / target_path.name
        counter = 1
        while archived_candidate.exists():
            archived_candidate = archive_dir / f"{base_str}_{counter}{ext}"
            counter += 1
        try:
            target_path.rename(archived_candidate)
            print(f"[INFO] Archived existing file to {archived_candidate}")
        except PermissionError as e:
            raise PermissionError(
                f"Could not archive existing file (is it open?): {target_path}"
            ) from e

    # Write new file to the desired path (no numbering in the main folder).
    with pd.ExcelWriter(target_path, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
    return target_path


def open_file_cross_platform(path: Path):
    """
    Open a file using the default application on the current OS.
    """
    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(path)

    try:
        if os.name == "nt":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.run(["open", str(path)], check=True)
        else:
            subprocess.run(["xdg-open", str(path)], check=True)
        print(f"Opened {path}")
    except Exception as e:
        print(f"[WARN] Failed to open file automatically: {e}")


#%% PROFILE MANIPULATION + UTILITIES

def scale_age_band(profile: dict, age_min: int, age_max: int, factor: float):
    """
    Scale values in profile between age_min and age_max by 'factor'.
    Returns a NEW dict.
    """
    new_profile = deepcopy(profile)
    for age in range(age_min, age_max + 1):
        if age in new_profile:
            new_profile[age] *= factor
    return new_profile


def smooth_profile(profile: dict, smoothing_dict: Optional[dict[int, int]] = None):
    """
    Simple moving-average smoother over ages.

    smoothing_dict: {pass_number: window_size} controls the window per pass; runs in sorted pass order.
    If smoothing_dict is None, defaults to a single pass with window=1.

    window = number of ages on each side (total span = 2*window+1).
    Returns a NEW dict.
    """

    def _single_pass(data: dict, win: int):
        if win < 1:
            return deepcopy(data)

        ages_local = sorted(data.keys())
        vals_local = [data[a] for a in ages_local]
        n_local = len(vals_local)
        smoothed_local = []

        for i in range(n_local):
            i0 = max(0, i - win)
            i1 = min(n_local - 1, i + win)
            local = vals_local[i0 : i1 + 1]
            smoothed_local.append(sum(local) / len(local))

        return {a: v for a, v in zip(ages_local, smoothed_local)}

    smoothed_profile = deepcopy(profile)
    smoothing_plan = {1: 1} if smoothing_dict is None else smoothing_dict

    if not smoothing_plan:
        return smoothed_profile

    for _, win in sorted(smoothing_plan.items()):
        smoothed_profile = _single_pass(smoothed_profile, win)

    return smoothed_profile


def plot_profile(profile: dict, title: str, ylabel: str):
    """
    Quick line plot for any {age: value} profile.
    """
    ages = sorted(profile.keys())
    vals = [profile[a] for a in ages]
    plt.figure()
    plt.plot(ages, vals)
    plt.xlabel("Age (years)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.show()


def check_sum_100(profile: dict):
    """
    For vintage profiles in percent: check if they sum to 100.
    Returns (sum_value, is_ok).
    """
    total = sum(profile.values())
    return total, abs(total - 100.0) < 1e-6


def renormalize_to_100(profile: dict):
    """
    Renormalize a profile so values sum to exactly 100.
    Useful for vintage distributions.
    """
    total = sum(profile.values())
    if total == 0:
        raise ValueError("Cannot renormalize a profile that sums to zero.")
    return {age: val * 100.0 / total for age, val in profile.items()}


def convert_cumulative_survival_to_annual(profile: dict) -> dict:
    """
    Convert a cumulative survival curve (% of original stock remaining at age)
    into annual survival probabilities (% surviving each year).
    """
    if not profile:
        return {}
    ages = sorted(profile.keys())
    vals = [float(profile[a]) for a in ages]
    scale = 100.0 if max(vals) > 1.0 else 1.0
    surv = [v / scale for v in vals]

    annual = []
    for i, v in enumerate(surv):
        if i == 0:
            annual.append(1.0)
        else:
            prev = max(surv[i - 1], 1e-9)
            annual.append(min(max(v / prev, 0.0), 1.0))
    return {age: a * 100.0 for age, a in zip(ages, annual)}


def apply_lifecycle_type_rules(profile: dict, lifecycle_type: str, base_year: Optional[int] = None):
    """
    Apply lifecycle-type specific adjustments.

    Currently enforced rules:
    - For lifecycle_type == "vintage", force the base_year value to 0.
      If base_year is not provided, the earliest year/age in the profile is used.
    - For lifecycle_type == "vehicle_survival", force the first year/age to 100 (start at full survival) and
      enforce a non-increasing curve across ages (each value <= any earlier value); no automatic normalization is
      applied (profiles need not sum to 100).
    - For lifecycle_type in {"aging", "aging_profile", "vehicle_aging"}, force the first year/age to 100
      (values are relative to a new device).
    """
    adjusted = deepcopy(profile)
    lifecycle_type_norm = (lifecycle_type or "").lower().strip().replace(" ", "_").replace("-", "_")
    aging_types = {"aging", "aging_profile", "vehicle_aging"}
    survival_types = {"vehicle_survival", "survival", "survival_profile"}

    if lifecycle_type_norm == "vintage":
        inferred_base_year = base_year if base_year is not None else (min(adjusted.keys()) if adjusted else None)
        if inferred_base_year is None:
            raise ValueError("Cannot enforce base-year rule: profile has no entries.")
        if inferred_base_year not in adjusted:
            raise ValueError(f"Base year {inferred_base_year} not found in profile; cannot enforce vintage rule.")
        adjusted[inferred_base_year] = 0.0
    elif lifecycle_type_norm in aging_types:
        first_year = min(adjusted.keys()) if adjusted else None
        if first_year is None:
            raise ValueError("Cannot enforce aging rule: profile has no entries.")
        adjusted[first_year] = 100.0
    elif lifecycle_type_norm in survival_types or "survival" in lifecycle_type_norm:
        ages_sorted = sorted(adjusted.keys())
        if not ages_sorted:
            raise ValueError("Cannot enforce vehicle_survival rule: profile has no entries.")

        # Start at 100 for the youngest age
        first_year = ages_sorted[0]
        adjusted[first_year] = 100.0

        # Running minimum so every later age is <= ALL earlier ages
        running_min = adjusted[first_year]
        for age in ages_sorted[1:]:
            running_min = min(running_min, adjusted[age])
            adjusted[age] = running_min

    return adjusted


#%% WORKFLOW EXAMPLE: VINTAGE / MILEAGE / SURVIVAL FROM SPREADSHEETS
def main(
    lifecycle_type="vintage",
    base_year=None,
    original_path="../data/lifecycle_profiles/vintage_original.xlsx",
    new_path="../data/lifecycle_profiles/vintage_modified.xlsx",
    scale_age_band_age_min=5,
    scale_age_band_age_max=12,
    scale_age_band_factor=0.95,
    smoothing_dict: Optional[dict[int, int]] = None,
    auto_open=False,
):
    new_path = Path(new_path).resolve()
    area, profile_name, profile_original = load_lifecycle_profile_excel(original_path)
    lifecycle_type_norm = (lifecycle_type or "").lower().strip().replace(" ", "_").replace("-", "_")
    requires_sum_100 = lifecycle_type_norm == "vintage"

    print(f"Loaded profile from area='{area}', name='{profile_name}', lifecycle_type='{lifecycle_type}'")
    v_sum, v_ok = check_sum_100(profile_original)
    if requires_sum_100:
        print(f"Original profile sum: {v_sum:.6f}, OK={v_ok}")
    else:
        print(f"Original profile sum (no 100%% requirement): {v_sum:.6f}")

    plot_profile(profile_original, "Original Profile", "Share (%)")
    if scale_age_band_factor != 1.0:
        profile_mod = scale_age_band(profile_original, age_min=scale_age_band_age_min, age_max=scale_age_band_age_max, factor=scale_age_band_factor)
    else:
        print("Skipping scaling step (factor=1.0)")
        profile_mod = deepcopy(profile_original)
    if smoothing_dict:
        profile_mod = smooth_profile(profile_mod, smoothing_dict=smoothing_dict)
    profile_mod = apply_lifecycle_type_rules(profile_mod, lifecycle_type=lifecycle_type, base_year=base_year)
    if requires_sum_100:
        profile_mod = renormalize_to_100(profile_mod)

    v_sum2, v_ok2 = check_sum_100(profile_mod)
    if requires_sum_100:
        print(f"Modified profile sum: {v_sum2:.6f}, OK={v_ok2}")
    else:
        print(f"Modified profile sum (no 100%% requirement): {v_sum2:.6f}")

    plot_profile(profile_mod, "Modified Profile", "Share (%)")

    saved_path = save_lifecycle_profile_excel(
        new_path=new_path,
        area_name=area,
        profile_name=profile_name + " (modified)",
        profile=profile_mod,
    )
    print(f"Saved modified profile to {saved_path}")

    if auto_open:
        open_file_cross_platform(saved_path)
        

#%% STEADY-STATE VINTAGE FROM SURVIVAL


def survival_profile_to_vintage_profile(
    survival_profile: dict[int, float],
    total_stock: float = 1.0,
    output_in_percent: bool = True,
) -> tuple[dict[int, float], float]:
    """
    Given a survival profile {age: value} (e.g. 'vehicle_survival' in percent),
    construct the steady-state vintage profile under:

    - stationary survival curve (age-based);
    - constant total stock;
    - 100% replacement of turnover.

    The logic is:
        survival_fraction[age] = survival_profile[age] / survival_profile[min_age]
        stock_at_age ∝ survival_fraction
        vintage_share[age] = stock_at_age / sum(stock_at_age)

    Parameters
    ----------
    survival_profile : dict[int, float]
        Keys are ages (years), values are survival in any units (typically %).
        Only the relative shape matters; it will be normalised internally.
    total_stock : float
        Arbitrary total stock level used to compute implied constant sales.
    output_in_percent : bool
        If True, returns vintage profile so that values sum to ~100.

    Returns
    -------
    vintage_profile : dict[int, float]
        Steady-state vintage distribution by age.
    constant_sales : float
        Annual sales needed to keep `total_stock` constant under this survival.
    """
    if not survival_profile:
        raise ValueError("Empty survival_profile provided.")

    ages = sorted(survival_profile.keys())
    vals = np.array([survival_profile[a] for a in ages], dtype=float)

    # Normalise survival to 1.0 at the youngest age
    s0 = vals[0]
    if s0 <= 0:
        raise ValueError("First survival value must be positive to normalise.")
    surv_frac = vals / s0

    surv_sum = surv_frac.sum()
    if surv_sum <= 0:
        raise ValueError("Sum of normalised survival must be positive.")

    # Steady-state:
    #   total_stock = constant_sales * sum_a S(a)
    constant_sales = total_stock / surv_sum

    stock_at_age = constant_sales * surv_frac
    stock_share = stock_at_age / stock_at_age.sum()

    if output_in_percent:
        stock_share = stock_share * 100.0

    vintage_profile = {age: float(v) for age, v in zip(ages, stock_share)}
    # breakpoint()
    
    # profile_df = (
    #     pd.DataFrame(
    #         {
    #             "age": ages,
    #             "survival_input": vals,
    #             "survival_fraction": surv_frac,
    #             "vintage_share": [vintage_profile[a] for a in ages],
    #         }
    #     )
    #     .set_index("age")
    # )
    # survival_profile_to_vintage_profile.last_profile_df = profile_df
    return vintage_profile, float(constant_sales)

def build_vintage_from_survival_excel(
    survival_excel_path: str,
    vintage_excel_path: str,
    sheet_name: str = "Lifecycle Profiles",
    total_stock: float = 1000,
    profile_name_suffix: str = " (steady-state vintage from survival)",
    auto_open: bool = False,
    annual_survival_output_path: str | None = None,
    run_simulation: bool = True,
    simulation_years: int = 60,
    turnover_rate_bounds: tuple[float, float] | None = None,
):
    """
    Read a survival lifecycle profile (vehicle_survival_*), compute the
    steady-state vintage profile, renormalise to 100%, and save as a new
    LEAP-style lifecycle profile Excel.

    Uses existing helpers:
      - load_lifecycle_profile_excel
      - save_lifecycle_profile_excel
      - renormalize_to_100
      - open_file_cross_platform
      - plot_survival_and_vintage

    turnover_rate_bounds : (min_rate, max_rate), optional
        If provided, checks whether the implied average turnover rate
        (constant_sales / total_stock) lies within the bounds and prints
        a warning if outside.
    """
    area, surv_profile_name, survival_profile = load_lifecycle_profile_excel(
        survival_excel_path, sheet_name=sheet_name
    )

    print(f"Loaded survival profile from area='{area}', name='{surv_profile_name}'")

    if annual_survival_output_path:
        annual_profile = convert_cumulative_survival_to_annual(survival_profile)
        save_lifecycle_profile_excel(
            new_path=annual_survival_output_path,
            area_name=area,
            profile_name=surv_profile_name + " (annual survival)",
            profile=annual_profile,
            sheet_name=sheet_name,
        )
        print(f"Saved annual survival profile to {annual_survival_output_path}")

    # Convert survival profile (likely in %) to steady-state vintage profile (%)
    vintage_profile, constant_sales = survival_profile_to_vintage_profile_dynamic(
        survival_profile=survival_profile,
        total_stock=total_stock,
        n_years=400,          # or 200, 500, whatever you like
        output_in_percent=True,
    )#dynammic version is designed to match the cohort logic used in sales estimation (i.e. how vehicles age each year, survival applied annually, etc.)
    turnover_rate = constant_sales / total_stock if total_stock > 0 else float("nan")
    if turnover_rate_bounds is not None:
        lo, hi = turnover_rate_bounds
        if not (lo <= turnover_rate <= hi):
            print(
                f"[WARN] Implied turnover rate {turnover_rate:.4f} "
                f"outside target range [{lo:.4f}, {hi:.4f}]."
            )
        else:
            print(
                f"Implied turnover rate {turnover_rate:.4f} within target range "
                f"[{lo:.4f}, {hi:.4f}]."
            )

    # Ensure exact sum of 100
    vintage_profile = renormalize_to_100(vintage_profile)
    v_sum, v_ok = check_sum_100(vintage_profile)
    print(f"Steady-state vintage profile sum: {v_sum:.6f}, OK={v_ok}")

    # === NEW: PLOTS ===
    # Optional steady-state turnover simulation to verify behaviour
    if run_simulation:
        #this should show that sales stabilize to constant_sales from the first year onwards. hekps to make sure logic is correct.
        sim_result = simulate_steady_state_turnover(
            survival_profile=survival_profile,
            vintage_profile_percent=vintage_profile,
            total_stock=total_stock,
            n_years=simulation_years,
        )
        
        plot_steady_state_simulation(sim_result)
        
    # 1) Survival vs derived vintage on same axes
    plot_survival_and_vintage(
        survival_profile,
        vintage_profile,
        title="Survival profile vs steady-state vintage (from survival)",
    )
    
    # ===================

    # Compose new profile name
    vintage_profile_name = surv_profile_name + profile_name_suffix

    saved_path = save_lifecycle_profile_excel(
        new_path=vintage_excel_path,
        area_name=area,
        profile_name=vintage_profile_name,
        profile=vintage_profile,
        sheet_name=sheet_name,
    )

    print(f"Saved steady-state vintage profile to {saved_path}")
    print(f"Implied constant annual sales (for total_stock={total_stock}): {constant_sales:,.3f}")

    if auto_open:
        open_file_cross_platform(saved_path)

    return saved_path, constant_sales




#%% DYNAMIC STEADY-STATE VINTAGE FROM SURVIVAL (aligned with cohort logic)

def _cumulative_to_annual_survival_profile(survival_profile: dict[int, float]) -> dict[int, float]:
    """
    Convert a cumulative survival curve S(age) (% of original cohort remaining)
    into annual survival probabilities p(age), where p(age) is the probability
    to survive from age -> age+1.

    Convention:
      - survival_profile[age] is S(age) in % or 0-1.
      - annual[age] = S(age+1) / S(age) for all ages except the last.
      - annual[max_age] = 0.0  (no survival beyond max modelled age).
    """
    if not survival_profile:
        raise ValueError("Empty survival_profile.")

    ages = sorted(survival_profile.keys())
    vals = np.array([float(survival_profile[a]) for a in ages], dtype=float)

    # Normalise cumulative to [0, 1].
    scale = 100.0 if vals.max() > 1.0 else 1.0
    S = vals / scale
    S = np.clip(S, 1e-9, 1.0)

    annual = np.zeros_like(S)
    for i, age in enumerate(ages):
        if i == len(ages) - 1:
            annual[i] = 0.0
        else:
            if S[i] <= 0:
                annual[i] = 0.0
            else:
                annual[i] = S[i + 1] / S[i]
        annual[i] = min(max(annual[i], 0.0), 1.0)

    return {age: float(p) for age, p in zip(ages, annual)}


def survival_profile_to_vintage_profile_dynamic(
    survival_profile: dict[int, float],
    total_stock: float = 1_000.0,
    n_years: int = 400,
    output_in_percent: bool = True,
) -> tuple[dict[int, float], float]:
    """
    Compute the steady-state vintage profile *dynamically*, using the same
    cohort logic as the sales estimator:

      - ages increase by 1 each year,
      - annual survival p(age) is applied,
      - retirements are replaced by new age-0 sales,
      - total_stock is kept constant.

    Starting from an arbitrary initial distribution, run the cohort model
    for n_years and use the final age distribution as the steady-state vintage.

    Returns:
        vintage_profile: {age: share}  (sums to 1.0 or 100.0)
        constant_sales:  annual steady-state sales (≈ retirements per year)
    """
    if not survival_profile:
        raise ValueError("Empty survival_profile provided.")

    ages = sorted(survival_profile.keys())
    max_age = ages[-1]

    # Annual survival p(age) consistent with our cohort logic.
    annual_surv = _cumulative_to_annual_survival_profile(survival_profile)
    surv_probs = np.array([annual_surv[a] for a in ages], dtype=float)

    # Initial stock: put everything at age 0 (it will converge anyway).
    stock = np.zeros(len(ages), dtype=float)
    stock[0] = total_stock

    retirements_history: list[float] = []

    for _ in range(n_years):
        new_stock = np.zeros_like(stock)
        retirements = 0.0

        for i, age in enumerate(ages):
            current = stock[i]
            if age == max_age:
                # Everything at max_age retires
                retirements += current
            else:
                p_survive = surv_probs[i]
                survivors = current * p_survive
                retirements += current - survivors
                new_stock[i + 1] += survivors

        # Replace retirements with new age-0 sales to keep total_stock ~ constant
        new_stock[0] += retirements

        stock = new_stock
        retirements_history.append(retirements)

    final_total = stock.sum() or 1.0
    vintage_profile = {age: float(stock[i] / final_total) for i, age in enumerate(ages)}

    if output_in_percent:
        vintage_profile = {age: v * 100.0 for age, v in vintage_profile.items()}

    constant_sales = float(retirements_history[-1])  # last-year retirements ≈ steady-state sales

    return vintage_profile, constant_sales












#%% EXTRA PLOTTING HELPERS

def plot_survival_and_vintage(
    survival_profile: dict,
    vintage_profile: dict,
    title: str = "Survival vs steady-state vintage profile",
):
    """
    Plot the input survival profile and the derived steady-state vintage
    profile on the same axes for visual comparison.
    """
    # Restrict to ages common to both profiles
    ages = sorted(set(survival_profile.keys()) & set(vintage_profile.keys()))
    surv_vals = [survival_profile[a] for a in ages]
    vint_vals = [vintage_profile[a] for a in ages]

    fig_surv, ax_surv = plt.subplots()
    ax_surv.plot(ages, surv_vals, label="Survival profile (input)")
    ax_surv.set_xlabel("Age (years)")
    ax_surv.set_ylabel("Survival (%)")
    ax_surv.set_title(f"{title} - Survival")
    ax_surv.grid(True)

    fig_vint, ax_vint = plt.subplots()
    ax_vint.plot(ages, vint_vals, label="Vintage profile (steady-state)", linestyle="--")
    ax_vint.set_xlabel("Age (years)")
    ax_vint.set_ylabel("Vintage share (%)")
    ax_vint.set_title(f"{title} - Vintage")
    ax_vint.grid(True)

    plt.show()

def simulate_steady_state_turnover(
    survival_profile: dict[int, float],
    vintage_profile_percent: dict[int, float],
    total_stock: float,
    n_years: int = 60,
) -> dict:
    """
    Simulate stock-by-age, retirements, and sales over time starting from
    the steady-state vintage profile implied by the survival curve.

    Each year:
      - Apply annual survival by age.
      - Age surviving vehicles by 1 year.
      - Treat all vehicles beyond the maximum modelled age as retired.
      - Set new sales = total retirements.
      - Add new sales into the age-0 bin.

    Returns a dict with time series for:
      - "years"
      - "sales"
      - "total_stock"
      - "vintage_start" (initial % by age)
      - "vintage_end"   (final % by age after n_years)
    """
    if not survival_profile or not vintage_profile_percent:
        raise ValueError("Both survival_profile and vintage_profile_percent must be non-empty.")

    # Ages and annual survival probabilities
    ages = sorted(survival_profile.keys())
    max_age = max(ages)

    annual_surv_pct = convert_cumulative_survival_to_annual(survival_profile)
    annual_surv_frac = {age: annual_surv_pct[age] / 100.0 for age in ages}

    # Initial absolute stock by age (starting from steady-state vintage %)
    stock_by_age = {
        age: (vintage_profile_percent.get(age, 0.0) / 100.0) * total_stock
        for age in ages
    }

    years = list(range(n_years))
    sales_history: list[float] = []
    total_stock_history: list[float] = []

    for _ in years:
        # Compute survivors and retirements
        new_stock = {age: 0.0 for age in ages}
        retirements = 0.0

        for age in ages:
            current = stock_by_age.get(age, 0.0)
            surv_prob = annual_surv_frac.get(age, 0.0)

            if age == max_age:
                # Everything at max_age is considered retired in this simple scheme
                retirements += current
            else:
                survivors = current * surv_prob
                retirements += current - survivors
                next_age = age + 1
                new_stock[next_age] += survivors

        # New sales = retirements, all go into age 0
        sales_t = retirements
        new_stock[ages[0]] += sales_t

        stock_by_age = new_stock
        total_stock_t = sum(stock_by_age.values())

        sales_history.append(sales_t)
        total_stock_history.append(total_stock_t)

    # Vintage distribution at the end of the simulation (as % of total stock)
    final_total = sum(stock_by_age.values()) or 1.0
    vintage_end_percent = {
        age: (stock_by_age[age] / final_total) * 100.0 for age in ages
    }

    return {
        "years": years,
        "sales": sales_history,
        "total_stock": total_stock_history,
        "vintage_start": vintage_profile_percent,
        "vintage_end": vintage_end_percent,
    }
    
def plot_steady_state_simulation(sim_result: dict):
    """
    Plot results of the steady-state turnover simulation:
      - sales over time
      - total stock over time
      - initial vs final vintage (% by age)
    """
    years = sim_result["years"]
    sales = sim_result["sales"]
    total_stock = sim_result["total_stock"]
    vintage_start = sim_result["vintage_start"]
    vintage_end = sim_result["vintage_end"]

    # 1) Sales over time
    plt.figure()
    plt.plot(years, sales)
    plt.xlabel("Year (relative)")
    plt.ylabel("Annual sales (vehicles)")
    plt.title("Steady-state simulation: annual sales over time")
    plt.tight_layout()
    plt.show()

    # 2) Total stock over time
    plt.figure()
    plt.plot(years, total_stock)
    plt.xlabel("Year (relative)")
    plt.ylabel("Total stock (vehicles)")
    plt.title("Steady-state simulation: total stock over time")
    plt.tight_layout()
    plt.show()

    # 3) Initial vs final vintage distribution
    ages = sorted(vintage_start.keys())
    start_vals = [vintage_start[a] for a in ages]
    end_vals = [vintage_end[a] for a in ages]

    plt.figure()
    plt.plot(ages, start_vals, label="Initial vintage (%)")
    plt.plot(ages, end_vals, linestyle="--", label="Final vintage (%) after simulation")
    plt.xlabel("Age (years)")
    plt.ylabel("Vintage share (%)")
    plt.title("Steady-state simulation: initial vs final vintage distribution")
    plt.legend()
    plt.tight_layout()
    plt.show()

#%%

if __name__ == "__main__":
    # main(
    #     lifecycle_type="vintage",
    #     base_year=None,
    #     original_path="../data/lifecycle_profiles/vintage_original.xlsx",
    #     new_path="../data/lifecycle_profiles/vintage_modified.xlsx",
    #     scale_age_band_age_min=4,
    #     scale_age_band_age_max=15,
    #     scale_age_band_factor=0.5,
    #     smoothing_dict={i: 36 for i in range(1, 101)},
    #     auto_open=True,
    #     )
    main(
        lifecycle_type="vehicle_survival",
        base_year=None,
        original_path="../data/lifecycle_profiles/vehicle_survival_original.xlsx",
        new_path="../data/lifecycle_profiles/vehicle_survival_modified.xlsx",
        scale_age_band_age_min=4,
        scale_age_band_age_max=15,
        scale_age_band_factor=1,
        smoothing_dict={1:2},#i: 36 for i in range(1, 101)},
        auto_open=False,
    )
            
    ###########################
    #FIND VINTAGE CURVE GIVEN SURVIVAL CURVE
    ###########################
    #WORKFLOW: build vintage from modified survival profile so that sales are constant in steady state (i.e. total stock constant, no growth in activity, sales=retirements)

    # This expects you’ve already produced e.g. vehicle_survival_modified.xlsx
    # with your existing main() call.

    survival_excel = "../data/lifecycle_profiles/vehicle_survival_modified.xlsx"
    vintage_excel  = "../data/lifecycle_profiles/vintage_modelled_from_survival.xlsx"

    saved_path, const_sales = build_vintage_from_survival_excel(
        survival_excel_path=survival_excel,
        vintage_excel_path=vintage_excel,
        turnover_rate_bounds=(0.03, 0.07),
    )

    print(f"File written: {saved_path}")
    print(f"Constant annual sales in steady state: {const_sales:,.0f} vehicles/year")

#%%
