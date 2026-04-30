
#%% IMPORTS
import numpy as np
import os
import sys
import subprocess
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError:  # optional dependency for plotting utilities only
    plt = None
import pandas as pd
from functions.path_utils import resolve_str


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
    subfolder beside it with a timestamp suffix.
    If a timestamped archive name already exists, an incrementing suffix
    (_01, _02, ...) is applied in the archive only.
    The new file is always written to `new_path` itself.
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
        date_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base_str = target_path.stem
        ext = target_path.suffix
        archived_candidate = archive_dir / f"{base_str}_{date_id}{ext}"
        counter = 1
        while archived_candidate.exists():
            archived_candidate = archive_dir / f"{base_str}_{date_id}_{counter:02d}{ext}"
            counter += 1
        try:
            target_path.rename(archived_candidate)
            print(f"[INFO] Archived existing file to {archived_candidate}")
        except PermissionError as e:
            raise PermissionError(
                f"Could not archive existing file (is it open?): {target_path}"
            ) from e

    # Write new file to the desired path (no numbering in the main folder).
    try:
        with pd.ExcelWriter(target_path, engine="xlsxwriter") as writer:
            out_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
    except ModuleNotFoundError as exc:
        if "xlsxwriter" not in str(exc).lower():
            raise
        with pd.ExcelWriter(target_path, engine="openpyxl") as writer:
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


def summarize_profile_changes(original: dict[int, float], modified: dict[int, float]) -> dict[str, float | int]:
    """
    Return a compact summary of how much a profile changed.
    """
    ages = sorted(set(original.keys()) | set(modified.keys()))
    if not ages:
        return {
            "n_ages": 0,
            "n_changed": 0,
            "max_abs_change": 0.0,
            "mean_abs_change": 0.0,
        }

    diffs = [abs(float(modified.get(a, 0.0)) - float(original.get(a, 0.0))) for a in ages]
    n_changed = int(sum(d > 1e-12 for d in diffs))
    return {
        "n_ages": int(len(ages)),
        "n_changed": n_changed,
        "max_abs_change": float(max(diffs)),
        "mean_abs_change": float(np.mean(diffs)),
    }


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


def plot_profile(profile: dict, title: str, ylabel: str, note: Optional[str] = None):
    """
    Quick line plot for any {age: value} profile.
    """
    if plt is None:
        raise ModuleNotFoundError(
            "matplotlib is required for plotting. Install it or avoid calling plot_profile()."
        )
    ages = sorted(profile.keys())
    vals = [profile[a] for a in ages]
    fig, ax = plt.subplots()
    ax.plot(ages, vals)
    ax.set_xlabel("Age (years)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if note:
        fig.text(0.5, 0.01, note, ha="center", va="bottom", fontsize=8, wrap=True)
        fig.tight_layout(rect=(0.0, 0.06, 1.0, 1.0))
    else:
        fig.tight_layout()
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
    Convert cumulative survival S(age) into annual survival p(age) in percent.

    Convention:
    - p(age) = S(age+1) / S(age) for all but the last age.
    - p(max_age) = 0 (no survival beyond the modelled maximum age).
    """
    if not profile:
        return {}
    annual_fraction = _cumulative_to_annual_survival_profile(profile)
    return {age: p * 100.0 for age, p in annual_fraction.items()}


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
    original_path="data/lifecycle_profiles/vintage_original.xlsx",
    new_path="data/lifecycle_profiles/vintage_modified.xlsx",
    scale_age_band_age_min=5,
    scale_age_band_age_max=12,
    scale_age_band_factor=0.95,
    smoothing_dict: Optional[dict[int, int]] = None,
    auto_open=False,
    verbose_explanations: bool = True,
    plot_profiles: bool = True,
):
    original_path = resolve_str(original_path)
    new_path = resolve_str(new_path)
    area, profile_name, profile_original = load_lifecycle_profile_excel(original_path)
    lifecycle_type_norm = (lifecycle_type or "").lower().strip().replace(" ", "_").replace("-", "_")
    requires_sum_100 = lifecycle_type_norm == "vintage"

    print(f"Loaded profile from area='{area}', name='{profile_name}', lifecycle_type='{lifecycle_type}'")
    if verbose_explanations:
        print(
            "Why this workflow exists: keep the original profile as the baseline, then create a modified profile "
            "for scenario calibration/testing. The modified file is the one to use in the model run."
        )
    v_sum, v_ok = check_sum_100(profile_original)
    if requires_sum_100:
        print(f"Original profile sum: {v_sum:.6f}, OK={v_ok}")
    else:
        print(f"Original profile sum (no 100%% requirement): {v_sum:.6f}")

    if plot_profiles:
        if verbose_explanations:
            print("Displaying the original profile so you can compare against the adjusted output.")
        plot_profile(profile_original, "Original Profile", "Share (%)")
    if scale_age_band_factor != 1.0:
        if verbose_explanations:
            print(
                f"Applying scaling: ages {scale_age_band_age_min}-{scale_age_band_age_max} multiplied by {scale_age_band_factor}."
            )
        profile_mod = scale_age_band(profile_original, age_min=scale_age_band_age_min, age_max=scale_age_band_age_max, factor=scale_age_band_factor)
    else:
        print("Skipping scaling step (factor=1.0)")
        profile_mod = deepcopy(profile_original)
    if smoothing_dict:
        if verbose_explanations:
            print(f"Applying smoothing passes: {smoothing_dict}")
        profile_mod = smooth_profile(profile_mod, smoothing_dict=smoothing_dict)
    if verbose_explanations:
        print(
            f"Applying lifecycle rules for '{lifecycle_type_norm}' "
            "(e.g., vintage base-age=0, survival monotonicity, or aging first-age=100)."
        )
    profile_mod = apply_lifecycle_type_rules(profile_mod, lifecycle_type=lifecycle_type, base_year=base_year)
    if requires_sum_100:
        if verbose_explanations:
            print("Renormalizing modified profile to sum to 100 for vintage consistency.")
        profile_mod = renormalize_to_100(profile_mod)

    v_sum2, v_ok2 = check_sum_100(profile_mod)
    if requires_sum_100:
        print(f"Modified profile sum: {v_sum2:.6f}, OK={v_ok2}")
    else:
        print(f"Modified profile sum (no 100%% requirement): {v_sum2:.6f}")

    change_summary = summarize_profile_changes(profile_original, profile_mod)
    if verbose_explanations:
        print(
            "Modification summary: "
            f"{change_summary['n_changed']}/{change_summary['n_ages']} ages changed, "
            f"max abs change={change_summary['max_abs_change']:.4f}, "
            f"mean abs change={change_summary['mean_abs_change']:.4f}."
        )
    modified_profile_note = (
        "Displaying the modified profile. This curve is intended to replace the original in the run inputs "
        "because it includes your chosen calibration adjustments."
    )
    if plot_profiles:
        if verbose_explanations:
            print(modified_profile_note)
        plot_profile(
            profile_mod,
            "Modified Profile",
            "Share (%)",
            note=modified_profile_note if verbose_explanations else None,
        )

    saved_path = save_lifecycle_profile_excel(
        new_path=new_path,
        area_name=area,
        profile_name=profile_name + " (modified)",
        profile=profile_mod,
    )
    print(f"Saved modified profile to {saved_path}")
    if verbose_explanations:
        print(
            "Interpretation: keep the original file as reference; point the model workflow to this modified file "
            "when you want the adjusted behavior."
        )

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
    verbose_explanations: bool = True,
    plot_profiles: bool = True,
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
    vintage_derivation_note = (
        "Why derive a vintage profile from survival: the stock-turnover model needs an initial age mix. "
        "We compute the age mix that is internally consistent with the survival curve under steady-state assumptions."
    )
    if verbose_explanations:
        print(vintage_derivation_note)

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

    # Enforce zero stock in the first vintage year, then renormalise to 100
    if not vintage_profile:
        raise ValueError("Derived vintage profile is empty; cannot save vintage lifecycle profile.")
    vintage_profile = apply_lifecycle_type_rules(
        vintage_profile,
        lifecycle_type="vintage",
        base_year=min(vintage_profile.keys()),
    )
    vintage_profile = renormalize_to_100(vintage_profile)
    v_sum, v_ok = check_sum_100(vintage_profile)
    print(f"Steady-state vintage profile sum: {v_sum:.6f}, OK={v_ok}")

    # === NEW: PLOTS ===
    # Optional steady-state turnover simulation to verify behaviour
    if run_simulation:
        if verbose_explanations:
            print(
                "Running steady-state turnover simulation: "
                "the initial vintage is the derived steady-state curve at year 0; "
                "the final vintage is after repeated turnover updates. "
                "If the method is internally consistent, initial and final should be very similar."
            )
        sim_result = simulate_steady_state_turnover(
            survival_profile=survival_profile,
            vintage_profile_percent=vintage_profile,
            total_stock=total_stock,
            n_years=simulation_years,
        )
        
        if plot_profiles:
            plot_steady_state_simulation(sim_result, verbose_explanations=verbose_explanations)
        
    # 1) Survival vs derived vintage on same axes
    if plot_profiles:
        if verbose_explanations:
            print(
                "Plotting survival vs steady-state vintage: "
                "survival shows how each cohort decays with age; "
                "vintage shows the fleet age composition that those survival rates imply in long-run equilibrium."
            )
        plot_survival_and_vintage(
            survival_profile,
            vintage_profile,
            title="Survival profile vs steady-state vintage (from survival)",
            context_note=vintage_derivation_note if verbose_explanations else None,
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


def _sanitize_filename_token(value: str) -> str:
    token = "".join(ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in str(value))
    token = "_".join(part for part in token.split("_") if part)
    return token or "token"


def _append_token_to_filename(path: str | Path, token: str) -> str:
    path_obj = Path(path)
    safe = _sanitize_filename_token(token)
    return str(path_obj.with_name(f"{path_obj.stem}_{safe}{path_obj.suffix}"))


def _resolve_economy_list(selection: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(selection, str):
        if selection.strip().lower() == "all":
            try:
                from configurations.transport_economy_config import TRANSPORT_ECONOMY_CONFIGS
            except Exception as exc:
                raise RuntimeError(
                    "Could not resolve 'all' economies from configurations.transport_economy_config."
                ) from exc
            return sorted(TRANSPORT_ECONOMY_CONFIGS.keys())
        token = selection.strip()
        return [token] if token else []

    economies: list[str] = []
    seen: set[str] = set()
    for item in selection:
        token = str(item).strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        economies.append(token)
    return economies


def generate_economy_specific_lifecycle_profiles(
    *,
    economy_selection: str | list[str] | tuple[str, ...] = "all",
    lifecycle_type: str = "vehicle_survival",
    base_year: int | None = None,
    original_survival_path: str = "data/lifecycle_profiles/vehicle_survival_original.xlsx",
    survival_output_template: str = "data/lifecycle_profiles/vehicle_survival_modified.xlsx",
    vintage_output_template: str = "data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
    scale_age_band_age_min: int = 4,
    scale_age_band_age_max: int = 15,
    scale_age_band_factor: float = 1.0,
    smoothing_dict: Optional[dict[int, int]] = None,
    auto_open: bool = False,
    run_simulation: bool = False,
    simulation_years: int = 60,
    turnover_rate_bounds: tuple[float, float] | None = (0.03, 0.07),
    verbose_explanations: bool = True,
    plot_profiles: bool = False,
) -> list[dict[str, object]]:
    """
    Create economy-specific lifecycle profile outputs.

    For each economy, writes:
    - vehicle_survival_modified_<economy>.xlsx
    - vintage_modelled_from_survival_<economy>.xlsx
    """
    economies = _resolve_economy_list(economy_selection)
    if not economies:
        raise ValueError("No economies resolved for economy-specific lifecycle profile generation.")

    results: list[dict[str, object]] = []
    for economy in economies:
        economy_token = str(economy).strip()
        survival_output = _append_token_to_filename(survival_output_template, economy_token)
        vintage_output = _append_token_to_filename(vintage_output_template, economy_token)

        print(f"\n=== Generating lifecycle profiles for {economy_token} ===")
        main(
            lifecycle_type=lifecycle_type,
            base_year=base_year,
            original_path=original_survival_path,
            new_path=survival_output,
            scale_age_band_age_min=scale_age_band_age_min,
            scale_age_band_age_max=scale_age_band_age_max,
            scale_age_band_factor=scale_age_band_factor,
            smoothing_dict=smoothing_dict,
            auto_open=False,
            verbose_explanations=verbose_explanations,
            plot_profiles=plot_profiles,
        )

        saved_path, const_sales = build_vintage_from_survival_excel(
            survival_excel_path=resolve_str(survival_output),
            vintage_excel_path=resolve_str(vintage_output),
            turnover_rate_bounds=turnover_rate_bounds,
            run_simulation=run_simulation,
            simulation_years=simulation_years,
            verbose_explanations=verbose_explanations,
            plot_profiles=plot_profiles,
        )

        results.append(
            {
                "economy": economy_token,
                "survival_output": resolve_str(survival_output),
                "vintage_output": str(saved_path),
                "constant_sales": float(const_sales),
            }
        )

    if auto_open and results:
        try:
            open_file_cross_platform(Path(results[-1]["vintage_output"]))
        except Exception as exc:
            print(f"[WARN] Could not auto-open last vintage file: {exc}")

    return results




#%% DYNAMIC STEADY-STATE VINTAGE FROM SURVIVAL (aligned with cohort logic)

def _validate_contiguous_integer_ages(profile: dict[int, float], profile_name: str) -> list[int]:
    """
    Validate profile keys are unique contiguous integer ages and return sorted ages.
    """
    if not profile:
        raise ValueError(f"Empty {profile_name}.")

    raw_ages = np.array(list(profile.keys()), dtype=float)
    if not np.isfinite(raw_ages).all():
        raise ValueError(f"{profile_name} age keys must be finite numbers.")
    if not np.allclose(raw_ages, np.round(raw_ages)):
        raise ValueError(f"{profile_name} age keys must be integer years.")

    ages = np.sort(np.round(raw_ages).astype(int))
    if len(np.unique(ages)) != len(ages):
        raise ValueError(f"{profile_name} has duplicate ages.")
    if len(ages) > 1 and not np.array_equal(np.diff(ages), np.ones(len(ages) - 1, dtype=int)):
        raise ValueError(f"{profile_name} ages must be contiguous with step 1.")

    return ages.tolist()


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
    ages = _validate_contiguous_integer_ages(survival_profile, "survival_profile")
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
    ages = _validate_contiguous_integer_ages(survival_profile, "survival_profile")
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
    context_note: Optional[str] = None,
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
    ax_surv.legend()
    ax_surv.text(
        0.02,
        0.02,
        "Why this plot: this is the input cohort-retention shape used to age vehicles and compute retirements.",
        transform=ax_surv.transAxes,
        fontsize=8,
        va="bottom",
        ha="left",
    )

    fig_vint, ax_vint = plt.subplots()
    ax_vint.plot(ages, vint_vals, label="Vintage profile (steady-state)", linestyle="--")
    ax_vint.set_xlabel("Age (years)")
    ax_vint.set_ylabel("Vintage share (%)")
    ax_vint.set_title(f"{title} - Vintage")
    ax_vint.grid(True)
    ax_vint.legend()
    ax_vint.text(
        0.02,
        0.02,
        "Why this plot: this is the derived age mix that keeps stock turnover balanced in steady state.",
        transform=ax_vint.transAxes,
        fontsize=8,
        va="bottom",
        ha="left",
    )

    if context_note:
        fig_surv.text(0.5, 0.01, context_note, ha="center", va="bottom", fontsize=8, wrap=True)
        fig_vint.text(0.5, 0.01, context_note, ha="center", va="bottom", fontsize=8, wrap=True)
        fig_surv.tight_layout(rect=(0.0, 0.08, 1.0, 1.0))
        fig_vint.tight_layout(rect=(0.0, 0.08, 1.0, 1.0))
    else:
        fig_surv.tight_layout()
        fig_vint.tight_layout()
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

    ages = _validate_contiguous_integer_ages(survival_profile, "survival_profile")
    vintage_ages = _validate_contiguous_integer_ages(vintage_profile_percent, "vintage_profile_percent")
    if ages != vintage_ages:
        raise ValueError("survival_profile and vintage_profile_percent must use the same age index.")

    # Ages and annual survival probabilities
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
    
def plot_steady_state_simulation(
    sim_result: dict,
    verbose_explanations: bool = True,
    skip_initial_year: bool = True,
):
    """
    Plot results of the steady-state turnover simulation:
      - sales over time
      - total stock over time
      - initial vs final vintage (% by age)
    """
    years = list(sim_result["years"])
    sales = list(sim_result["sales"])
    total_stock = list(sim_result["total_stock"])
    vintage_start = sim_result["vintage_start"]
    vintage_end = sim_result["vintage_end"]
    ages = sorted(vintage_start.keys())
    start_vals = [vintage_start[a] for a in ages]
    end_vals = [vintage_end[a] for a in ages]
    max_abs_vintage_diff = max(abs(e - s) for s, e in zip(start_vals, end_vals)) if ages else 0.0

    if sales:
        sales_arr = np.array(sales, dtype=float)
        sales_last = float(sales_arr[-1])
        sales_cv = float(sales_arr.std() / sales_arr.mean()) if sales_arr.mean() > 0 else float("nan")
    else:
        sales_last = float("nan")
        sales_cv = float("nan")

    if verbose_explanations:
        print(
            "Steady-state simulation interpretation: "
            "Initial vintage = derived starting age mix; Final vintage = age mix after repeated turnover updates."
        )
        print(
            f"Simulation diagnostics: max |final-initial vintage|={max_abs_vintage_diff:.4f} percentage points, "
            f"final annual sales={sales_last:,.3f}, sales CV={sales_cv:.6f}."
        )
        print(
            "If these differences are small, the derived vintage is behaving as a steady-state solution under the "
            "same survival logic used in turnover calculations."
        )
        if skip_initial_year and len(years) > 1:
            print("For readability, plots omit simulation year 0 so trend lines do not start at the initial point.")

    plot_years = years[1:] if (skip_initial_year and len(years) > 1) else years
    plot_sales = sales[1:] if (skip_initial_year and len(sales) > 1) else sales
    plot_total_stock = total_stock[1:] if (skip_initial_year and len(total_stock) > 1) else total_stock

    # 1) Sales over time
    plt.figure()
    plt.plot(plot_years, plot_sales)
    plt.xlabel("Year (relative)")
    plt.ylabel("Annual sales (vehicles)")
    plt.title("Steady-state simulation: annual sales over time")
    plt.figtext(
        0.5,
        0.01,
        "Purpose: check that annual sales settle near a stable level in steady state.",
        ha="center",
        fontsize=8,
    )
    plt.tight_layout()
    plt.show()

    # 2) Total stock over time
    plt.figure()
    plt.plot(plot_years, plot_total_stock)
    plt.xlabel("Year (relative)")
    plt.ylabel("Total stock (vehicles)")
    plt.title("Steady-state simulation: total stock over time")
    plt.figtext(
        0.5,
        0.01,
        "Purpose: verify that retirements and new sales keep total stock approximately constant.",
        ha="center",
        fontsize=8,
    )
    plt.tight_layout()
    plt.show()

    # 3) Initial vs final vintage distribution
    plt.figure()
    plt.plot(ages, start_vals, label="Initial vintage (%)")
    plt.plot(ages, end_vals, linestyle="--", label="Final vintage (%) after simulation")
    plt.xlabel("Age (years)")
    plt.ylabel("Vintage share (%)")
    plt.title("Steady-state simulation: initial vs final vintage distribution")
    plt.legend()
    plt.figtext(
        0.5,
        0.01,
        "Why these differ: vehicles are aged/retired each year in discrete steps. "
        "Small gaps are expected; large gaps indicate the starting vintage is not fully steady-state.",
        ha="center",
        fontsize=8,
    )
    plt.tight_layout()
    plt.show()

#%%

if __name__ == "__main__":
    # main(
    #     lifecycle_type="vintage",
    #     base_year=None,
    #     original_path="./data/lifecycle_profiles/vintage_original.xlsx",
    #     new_path="./data/lifecycle_profiles/vintage_modified.xlsx",
    #     scale_age_band_age_min=4,
    #     scale_age_band_age_max=15,
    #     scale_age_band_factor=0.5,
    #     smoothing_dict={i: 36 for i in range(1, 101)},
    #     auto_open=True,
    #     )
    ECONOMY_SELECTION = "all"  # e.g. "20_USA", ["20_USA", "12_NZ"], or "all"
    CREATE_LEGACY_UNSCOPED_FILES = True
    PLOT_PROFILES = False

    if CREATE_LEGACY_UNSCOPED_FILES:
        main(
            lifecycle_type="vehicle_survival",
            base_year=None,
            original_path="data/lifecycle_profiles/vehicle_survival_original.xlsx",
            new_path="data/lifecycle_profiles/vehicle_survival_modified.xlsx",
            scale_age_band_age_min=4,
            scale_age_band_age_max=15,
            scale_age_band_factor=1,
            smoothing_dict={1: 2},
            auto_open=False,
            plot_profiles=PLOT_PROFILES,
        )
        saved_path, const_sales = build_vintage_from_survival_excel(
            survival_excel_path=resolve_str("data/lifecycle_profiles/vehicle_survival_modified.xlsx"),
            vintage_excel_path=resolve_str("data/lifecycle_profiles/vintage_modelled_from_survival.xlsx"),
            turnover_rate_bounds=(0.03, 0.07),
            plot_profiles=PLOT_PROFILES,
            run_simulation=False,
        )
        print(f"Legacy file written: {saved_path}")
        print(f"Legacy constant annual sales in steady state: {const_sales:,.0f} vehicles/year")

    economy_results = generate_economy_specific_lifecycle_profiles(
        economy_selection=ECONOMY_SELECTION,
        lifecycle_type="vehicle_survival",
        base_year=None,
        original_survival_path="data/lifecycle_profiles/vehicle_survival_original.xlsx",
        survival_output_template="data/lifecycle_profiles/vehicle_survival_modified.xlsx",
        vintage_output_template="data/lifecycle_profiles/vintage_modelled_from_survival.xlsx",
        scale_age_band_age_min=4,
        scale_age_band_age_max=15,
        scale_age_band_factor=1,
        smoothing_dict={1: 2},
        auto_open=False,
        run_simulation=False,
        turnover_rate_bounds=(0.03, 0.07),
        verbose_explanations=True,
        plot_profiles=PLOT_PROFILES,
    )

    print("\n=== Economy-specific lifecycle outputs ===")
    for row in economy_results:
        print(
            f"{row['economy']}: survival={row['survival_output']} | "
            f"vintage={row['vintage_output']} | constant_sales={row['constant_sales']:,.0f}"
        )

#%%
