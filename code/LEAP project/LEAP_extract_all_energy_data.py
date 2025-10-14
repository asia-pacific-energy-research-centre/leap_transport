import pandas as pd
from win32com.client import Dispatch, GetActiveObject

def connect_to_leap():
    try:
        return GetActiveObject('LEAP.LEAPApplication')
    except:
        return Dispatch('LEAP.LEAPApplication')

def dump_all_energy_results(years, unit="PJ"):
    """
    Dump ALL energy-related results from LEAP:
    Demand, Transformation, Resources (supply).
    Includes branch, variable, fuel (if applicable), year, value.
    """
    L = connect_to_leap()
    L.ActiveView = "Results"
    results = []
    scen = L.ActiveScenario
    region = L.ActiveRegion.Name

    for br in L.Branches:
        for var in br.Variables:
            if not var.Results:
                continue

            # Totals (no fuel filter)
            for yr in years:
                try:
                    val = var.Value(yr, unit, "")
                    results.append([scen, region, br.FullPath, var.Name, None, yr, val])
                except:
                    pass

            # Try fuel breakdowns
            for fuel in L.Fuels:
                for yr in years:
                    try:
                        val = var.Value(yr, unit, f"fuel={fuel.Name}")
                        results.append([scen, region, br.FullPath, var.Name, fuel.Name, yr, val])
                    except:
                        pass

    df = pd.DataFrame(results, columns=["Scenario","Region","Branch","Variable","Fuel","Year","Value"])
    return df

# Example usage
years = list(range(2000, 2051))  # adjust to your model horizon
df = dump_all_energy_results(years, unit="PJ")
df.to_csv("leap_full_energy_results.csv", index=False)
print("✅ Exported LEAP full energy results to leap_full_energy_results.csv")

