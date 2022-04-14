# SPDX-FileCopyrightText: : 2021 PyPSA-Africa Authors
#
# SPDX-License-Identifier: GPL-3.0-or-later
# This script does the following
# 1. Downloads OSM files for specified countries from Geofabrik
# 2. Filters files for substations, lines and generators
# 3. Process and clean data
# 4. Exports to CSV
# 5. Exports to GeoJson

""" OSM extraction script."""

import json
import logging
import multiprocessing as mp
import os
import pickle


import geopandas as gpd
import pandas as pd

from esy.osmfilter import (Node, Relation, Way)  # https://gitlab.com/dlr-ve-esy/esy-osmfilter/-/tree/master/
from esy.osmfilter import osm_info, osm_pickle, run_filter
from geofabrik_data import get_region_dict, get_short_id, view_regions

from geofabrik_download import geofabrik_downloader
from osm_data_args import get_args
from osm_data_config import feature_category, feature_columns
from shapely.geometry import LineString, Point, Polygon


import logging

# logging.basicConfig()
_logger = logging.getLogger("osm_data_extractor")
_logger.setLevel(logging.INFO)
# logger.setLevel(logging.WARNING)

os.chdir(
    os.path.dirname(os.path.abspath(__file__))
)  # move up to root directory

# Downloads PBF File for given Country Code

feature_list = ["line", "tower"]
# feature_list = ["substation", "line", "generator", "cable"]
# feature_list = ["substation", "line"]
# feature_list = ["line"]
# feature_list = ["substation"]


pre_filtered = []
def download_and_filter(feature, region_id, update=False, verify=False):
    """
    Download OpenStreetMap raw file for selected tag.

    Apply pbf download and filter with esy.osmfilter selected OpenStreetMap
    tags or data. Examples of possible tags are listed at `OpenStreetMap wiki
    <https://wiki.openstreetmap.org/wiki/Key:power>`_.
    More information on esy.osmfilter `here <https://gitlab.com/dlr-ve-esy/esy-osmfilter>`_.

    Parameters
    ----------
    country_code : str
        Three letter country codes of the downloaded files
    update : bool
        Name of the network component
        Update = true, forces re-download of files
        Update = false, uses existing or previously downloaded files to safe time

    Returns
    -------
    substation_data : Data, Elements
    line_data : Data, Elements
    generator_data : Data, Elements
        Nested dictionary with all OpenStreetMap keys of specific component.
        Example of lines. See https://wiki.openstreetmap.org/wiki/Tag:power%3Dline
    """
    # PBF_inputfile = download_pbf(country_code, update, verify)
    geofabrik_pbf_url= get_region_dict(region_id)['urls']['pbf']
    PBF_inputfile = geofabrik_downloader(geofabrik_pbf_url)
    country_code = get_short_id(region_id)

    filter_file_exists = False
    # json file for the Data dictionary
    JSON_outputfile = os.path.join(
        os.getcwd(), "data", "osm", country_code + "_power.json"
    )  # json file for the Elements dictionary is automatically written to "data/osm/Elements"+filename)

    if os.path.exists(JSON_outputfile):
        filter_file_exists = True
        Data = osm_info.ReadJason(JSON_outputfile, verbose="no")
        DataDict = {"Data": Data}
        osm_pickle.picklesave(
            DataDict,
            os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(JSON_outputfile))
            ),
        )

    if not os.path.exists(
        os.path.join(
            os.getcwd(), "data", "osm", "Elements", country_code + f"_{feature}s.json"
        )
    ):
        _logger.warning("Element file not found so pre-filtering")
        filter_file_exists = False

    elementname = f"{country_code}_{feature}s"

    # Load Previously Pre-Filtered Files
    if update is False and verify is False and filter_file_exists is True:
        create_elements = True  # Do not create elements again
        # TODO: There is a bug somewhere, the line above create_elements should be set to False and the elements loaded from the pickle

        # ElementsDict = {elementname:{}}
        # Elements = osm_pickle.pickleload(ElementsDict,os.path.join(os.getcwd(),os.path.dirname(JSON_outputfile), 'Elements'))

        new_prefilter_data = False  # Do not pre-filter data again
        # HACKY: esy.osmfilter code to re-create Data.pickle
        # with open(JSON_outputfile,encoding="utf-8") as f:
        #     Data = json.load(f)


        _logger.info(f"Loading {feature} Pickle for {country_code}")
        # feature_data = Data, Elements
        # return feature_data

    else:
        create_elements = True
        if country_code not in pre_filtered:  # Ensures pre-filter is not run everytime
            new_prefilter_data = True
            _logger.info(f"Pre-filtering {country_code} ")
            pre_filtered.append(country_code)
        else:
            new_prefilter_data = False
        _logger.info(f"Creating  New {feature} Elements for {country_code}")

    prefilter = {
        Node: {"power": feature_list},
        Way: {"power": feature_list},
        Relation: {"power": feature_list},
    }  # see https://dlr-ve-esy.gitlab.io/esy-osmfilter/filter.html for filter structures

    blackfilter = [
        ("", ""),
    ]

    whitefilter = [
        [
            ("power", feature),
        ],
    ]

    Data, Elements = run_filter(
        elementname,
        PBF_inputfile,
        JSON_outputfile,
        prefilter,
        whitefilter,
        blackfilter,
        NewPreFilterData=new_prefilter_data,
        CreateElements=create_elements,
        LoadElements=True,
        verbose=False,
        multiprocess=True,
    )

    logging.disable(
        logging.NOTSET
    )  # Re-enable logging as run_filter disables logging.INFO
    _logger.info(
        f"Pre: {new_prefilter_data}, Elem: {create_elements}, for {feature} in {country_code}"
    )

    feature_data = Data, Elements

    return feature_data


def convert_filtered_data_to_dfs(country_code, feature_data, feature):
    """Convert Filtered Data, Elements to Pandas Dataframes"""
    Data, Elements = feature_data
    elementname = f"{country_code}_{feature}s"
    df_way = pd.json_normalize(Elements[elementname]["Way"].values())
    df_node = pd.json_normalize(Elements[elementname]["Node"].values())
    return (df_node, df_way, Data)


def lonlat_lookup(df_way, Data):
    """Lookup refs and convert to list of longlats"""
    if "refs" not in df_way.columns:
        _logger.warning("refs column not found")
        print(df_way.columns)
        # df_way[col] = pd.Series([], dtype=pd.StringDtype()).astype(float)  # create empty "refs" if not in dataframe

    def look(ref):
        lonlat_row = list(map(lambda r: tuple(Data["Node"][str(r)]["lonlat"]), ref))
        return lonlat_row

    lonlat_list = df_way["refs"].apply(look)

    return lonlat_list


def convert_ways_points(df_way, Data):
    """Convert Ways to Point Coordinates"""
    lonlat_list = lonlat_lookup(df_way, Data)
    way_polygon = list(
        map(
            lambda lonlat: Polygon(lonlat) if len(lonlat) >= 3 else Point(lonlat[0]),
            lonlat_list,
        )
    )
    area_column = list(
        map(
            int,
            round(
                gpd.GeoSeries(way_polygon)
                .set_crs("EPSG:4326")
                .to_crs("EPSG:3857")
                .area,
                -1,
            ),
        )
    )  # TODO: Rounding should be done in cleaning scripts

    def find_center_point(p):
        if p.geom_type == "Polygon":
            center_point = p.centroid
        else:
            center_point = p
        return list((center_point.x, center_point.y))

    lonlat_column = list(map(find_center_point, way_polygon))

    # df_way.drop("refs", axis=1, inplace=True, errors="ignore")
    df_way.insert(0, "Area", area_column)
    df_way.insert(0, "lonlat", lonlat_column)


def convert_ways_lines(df_way, Data):
    """Convert Ways to Line Coordinates"""
    lonlat_list = lonlat_lookup(df_way, Data)
    lonlat_column = lonlat_list
    df_way.insert(0, "lonlat", lonlat_column)

    way_linestring = map(lambda lonlats: LineString(lonlats), lonlat_list)
    length_column = (
        gpd.GeoSeries(way_linestring).set_crs("EPSG:4326").to_crs("EPSG:3857").length
    )

    df_way.insert(0, "Length", length_column)


def convert_pd_to_gdf_nodes(df_way):
    """Convert Points Pandas Dataframe to GeoPandas Dataframe"""
    gdf = gpd.GeoDataFrame(
        df_way, geometry=[Point(x, y) for x, y in df_way.lonlat], crs="EPSG:4326"
    )
    gdf.drop(columns=["lonlat"], inplace=True)
    return gdf


def convert_pd_to_gdf_lines(df_way, simplified=False):
    """Convert Lines Pandas Dataframe to GeoPandas Dataframe"""
    # df_way["geometry"] = df_way["lonlat"].apply(lambda x: LineString(x))
    if simplified is True:
        df_way["geometry"] = df_way["geometry"].apply(
            lambda x: x.simplify(0.005, preserve_topology=False)
        )
    gdf = gpd.GeoDataFrame(
        df_way, geometry=[LineString(x) for x in df_way.lonlat], crs="EPSG:4326"
    )
    gdf.drop(columns=["lonlat"], inplace=True)
    return gdf

def output_csv_geojson(df_all_feature, columns_feature, feature, cc_list):

    def filenamer(cc_list):
        if len(cc_list) == 1:
            return str(cc_list[0])
        # if len(set([continent for cc in cc_list for continent, country in get_continent_country(cc)]))==1:
        #     if set(cc_list) == world_cc[get_continent_country(cc_list[0])].keys():
        #         return f"{get_continent_country(cc_list[0])}_all"
        
    fn_name = filenamer(cc_list)
    print(fn_name)
    outputfile_partial = os.path.join(
        os.getcwd(), "data", "raw", fn_name + "_raw"
    )  # Output file directory

    if not os.path.exists(outputfile_partial):
        os.makedirs(
            os.path.dirname(outputfile_partial), exist_ok=True
        )  # create raw directory

    df_all_feature = df_all_feature[
        df_all_feature.columns.intersection(set(columns_feature))
    ]
    df_all_feature.reset_index(drop=True, inplace=True)

    # Generate Files

    if df_all_feature.empty:
        _logger.warning(f"All feature data frame empty for {feature}")
        return None

    df_all_feature.to_csv(
        outputfile_partial + f"_{feature}s" + ".csv"
    )  # Generate CSV

    if feature_category[feature] == "way":
        gdf_feature = convert_pd_to_gdf_lines(df_all_feature)
    else:
        gdf_feature = convert_pd_to_gdf_nodes(df_all_feature)

    try:
        gdf_feature.drop(columns=["refs"], inplace=True)
    except:
        pass

    _logger.info("Writing GeoJSON file")
    gdf_feature.to_file(
        outputfile_partial + f"_{feature}s" + ".geojson", driver="GeoJSON"
    )  # Generate GeoJson


def process_country(feature, region_id, update, verify):
    feature_data = download_and_filter(feature, region_id, update, verify)

    country_code = get_short_id(region_id)
    df_node, df_way, Data = convert_filtered_data_to_dfs(
        country_code, feature_data, feature
    )

    if feature_category[feature] == "way":
        convert_ways_lines(
            df_way, Data
        ) if not df_way.empty else _logger.warning(
            f"Empty Way Dataframe for {feature} in {country_code}"
        )
        if not df_node.empty:
            _logger.warning(
                f"Node dataframe not empty for {feature} in {country_code}"
            )

    if feature_category[feature] == "node":
        convert_ways_points(df_way, Data) if not df_way.empty else None

    # Add Type Column
    df_node["Type"] = "Node"
    df_way["Type"] = "Way"

    # Concatinate Nodes and Ways
    df_feature = pd.concat([df_node, df_way], axis=0)

    # Add Country Column
    df_feature["Country"] = country_code

    return df_feature

def process_per_country(cc_list, update, verify):
    """Process Data individually for countries specified in cc_list for features in feature_list"""
    for feature in feature_list:
        for country_code in cc_list:
            df_all_feature = pd.DataFrame()
            df_feature = process_country(feature, country_code, update, verify)
            df_all_feature = pd.concat([df_all_feature, df_feature])
            output_csv_geojson(df_all_feature, feature_columns[feature], feature, [get_short_id(country_code)])

        # output_csv_geojson(df_all_feature, feature_columns[feature], feature, cc_list)


if __name__ == "__main__":
    # Set update # Verify = True checks local md5s and pre-filters data again
    # args = get_args()  # TODO: implement parser get arguments from command line
    
    # How to use:
    # 1. use view_regions() to view a list of supported region ids
    # view_regions() # Prints exhaustive list of regions supported by this tool.
    # 2. set feature_list = ["line", "tower"] (supported features given by feature_category.keys())
    # 2. copy the names into a list passed into process_per_country

    process_per_country(["us/california"], update=False, verify=False)