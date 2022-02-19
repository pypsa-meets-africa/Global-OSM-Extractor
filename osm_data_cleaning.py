#%%
import logging
import os
import sys

import geopandas as gpd
import numpy as np
import pandas as pd
from pygeos.measurement import length

#%%
cc_code = "SL"
raw_outputfile_partial = os.path.join(os.getcwd(), "data", "raw", cc_code + "_raw")
df = pd.read_csv(raw_outputfile_partial + "_lines" + ".csv")
display(df)

#%%
df["tags.voltage"] = (df["tags.voltage"].apply(
        lambda x: pd.to_numeric(x, errors="coerce")).astype(float))
display(df)

#  drop nans
# if length > 100 000
# if voltage > 33000

#%%
df["ref_length"] = df["refs"].apply(lambda x : len(x))
df["ref_ratio"] = df["ref_length"]/df["Length"]
r_df = df[["Length", "ref_length", "ref_ratio", "tags.voltage"]]
display(r_df)

def split_cells(df, lst_col="voltage"):
    """
    Split semicolon separated cells i.e. [66000;220000] and create new identical rows

        Parameters
    ----------
    df : dataframe
        Dataframe under analysis
    lst_col : str
        Target column over which to perform the analysis
    """
    x = df.assign(**{lst_col: df[lst_col].str.split(";")})
    x = pd.DataFrame({
        col: np.repeat(x[col].values, x[lst_col].str.len())
        for col in x.columns.difference([lst_col])
    }).assign(
        **{lst_col: np.concatenate(x[lst_col].values)})[x.columns.tolist()]
    return x


def filter_voltage(df, threshold_voltage=35000):

    # Drop any row with N/A voltage
    df = df.dropna(subset=["voltage"])

    # Split semicolon separated cells i.e. [66000;220000] and create new identical rows
    df = split_cells(df)

    # Convert voltage to float, if impossible, discard row
    df["voltage"] = (df["voltage"].apply(
        lambda x: pd.to_numeric(x, errors="coerce")).astype(float))
    df = df.dropna(subset=["voltage"])  # Drop any row with Voltage = N/A

    # convert voltage to int
    df.loc[:, "voltage"] = df["voltage"].astype(int)

    # keep only lines with a voltage no lower than than threshold_voltage
    df = df[df.voltage >= threshold_voltage]

    return df


def clean_data(
    ext_country_shapes=None,
    names_by_shapes=True,
    tag_substation="transmission",
    threshold_voltage=35000,
):

    # Output file directory
    outputfile_partial = os.path.join(os.getcwd(), "data", "clean",
                                      "africa_all")
    # Output file directory
    raw_outputfile_partial = os.path.join(os.getcwd(), "data", "raw",
                                          "africa_all" + "_raw")

    # ----------- LINES AND CABLES -----------

    # Load raw data lines
    df_lines = gpd.read_file(raw_outputfile_partial + "_lines" +
                             ".geojson").set_crs(epsg=4326, inplace=True)



    # # filter lines by voltage
    # df_all_lines = filter_voltage(df_all_lines, threshold_voltage)


    df_all_lines = gpd.GeoDataFrame(df_all_lines,
                                    geometry="geometry",
                                    crs="EPSG:4326")


    df_all_lines.to_file(outputfile_partial + "_lines" + ".geojson",
                         driver="GeoJSON")

    return None


# if __name__ == "__main__":
#     print("main")
# %%
