# Take in data/all_sectors_fuels_9th_outlook.xlsx and remove all duplicate rows acorss all columns. the save with same name
#%%
import pandas as pd
import numpy as np
#%%
def prepare_sector_fuel_combinations(SEPARATE_HISTORICAL_PROJECTED=True):
    """
    Prepare all sector-fuel combinations for LEAP import.
        """
    if not SEPARATE_HISTORICAL_PROJECTED:
        # ----------------------------
        # File paths
        # ----------------------------
        INPUT_XLSX = r"data/all_sectors_fuels_9th_outlook.xlsx"
        OUTPUT_XLSX = r"data/all_sectors_fuels_9th_outlook.xlsx"
        # ----------------------------
        # Read the Excel file
        df = pd.read_excel(INPUT_XLSX)
        # ----------------------------
        # Remove duplicate rows
        df_cleaned = df.drop_duplicates()
        # ----------------------------
    elif SEPARATE_HISTORICAL_PROJECTED:
        #WE WILL load in two files:

        INPUT_XLSX_PROJECTED = r"data/all_projected_sectors_fuels_9th_outlook.xlsx"
        INPUT_XLSX_HISTORICAL = r"data/all_historical_sectors_fuels_9th_outlook.xlsx"
        OUTPUT_XLSX_HISTORICAL = r"data/all_historical_sectors_fuels_9th_outlook.xlsx"
        OUTPUT_XLSX_PROJECTED = r"data/all_projected_sectors_fuels_9th_outlook.xlsx"
        
        # ----------------------------
        # Read the Excel file
        df_historical = pd.read_excel(INPUT_XLSX_HISTORICAL)
        df_projected = pd.read_excel(INPUT_XLSX_PROJECTED)
        # ----------------------------
        # Remove duplicate rows in historical
        df_cleaned_historical = df_historical.drop_duplicates()
        #for future we first need to remove all rows full of 0s fr projected years, then seaprate the cols we need, then remove duplicates
        #cols currently:
        # scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels	subtotal_layout	subtotal_results	2023	2024	2025	2026	2027	2028	2029	2030	2031	2032	2033	2034	2035	2036	2037	2038	2039	2040	2041	2042	2043	2044	2045	2046	2047	2048	2049	2050	2051	2052	2053	2054	2055	2056	2057	2058	2059	2060	2061	2062	2063	2064	2065	2066	2067	2068	2069	2070

        projected_years = [col for col in df_projected.columns if isinstance(col, int) and col > 2022]
        # Remove rows where all projected years are 0
        df_projected_nonzero = df_projected.loc[~(df_projected[projected_years] == 0).all(axis=1)]
        # Select columns to keep
        cols_to_keep = [
            "sectors", "sub1sectors", "sub2sectors", "sub3sectors", "sub4sectors", "fuels", "subfuels"
        ]
        df_projected_selected = df_projected_nonzero[cols_to_keep]
        # Remove duplicate rows
        df_cleaned_projected = df_projected_selected.drop_duplicates()
        
        # ----------------------------
        # Save the cleaned DataFrames to separate Excel files
        df_cleaned_historical.to_excel(OUTPUT_XLSX_HISTORICAL, index=False)
        df_cleaned_projected.to_excel(OUTPUT_XLSX_PROJECTED, index=False)
        print(f"✅ Exported cleaned historical sector-fuel combinations to {OUTPUT_XLSX_HISTORICAL}")
        print(f"✅ Exported cleaned projected sector-fuel combinations to {OUTPUT_XLSX_PROJECTED}")
        return df_cleaned_historical, df_cleaned_projected
        
#%%
df_cleaned_historical, df_cleaned_projected = prepare_sector_fuel_combinations(SEPARATE_HISTORICAL_PROJECTED=True)
# %%
