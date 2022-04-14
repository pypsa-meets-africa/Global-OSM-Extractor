import multiprocessing as mp
import os

import logging

import tqdm

# from geofabrik_data import get_region_dict

# logging.basicConfig()
_logger = logging.getLogger("osm_data_extractor")
_logger.setLevel(logging.INFO)
# logger.setLevel(logging.WARNING)


import requests
import urllib3
import shutil
import hashlib

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def geofabrik_downloader(url):
    geofabrik_filename = os.path.basename(url)
    geofabrik_filepath = os.path.join(
        os.getcwd(), "data", "osm", "pbf", geofabrik_filename
    )  # Input filepath

    if not os.path.exists(geofabrik_filepath):
        _logger.info(f"{geofabrik_filename} downloading to {geofabrik_filepath}")
        #  create data/osm directory
        os.makedirs(os.path.dirname(geofabrik_filepath), exist_ok=True)
        with requests.get(url, stream=True, verify=False) as r:
            if r.status_code == 200:
                # url properly found, thus execute as expected
                with open(geofabrik_filepath, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
            else:
                # error status code: file not found
                _logger.error(
                    f"Error code: {r.status_code}. File {geofabrik_filename} not downloaded from {url}"
                )
    return geofabrik_filepath

def download_sitemap(geom, update):
    geofabrik_geo= f"https://download.geofabrik.de/index-v1.json"
    geofabrik_nogeo= f"https://download.geofabrik.de/index-v1-nogeom.json"
    if geom:
        geofabrik_sitemap_url = geofabrik_geo
    else:
        geofabrik_sitemap_url = geofabrik_nogeo

    sitemap_file = geofabrik_downloader(geofabrik_sitemap_url)

    return sitemap_file


# def download_pbf(country_code, update, verify):
#     """
#     Download pbf file from geofabrik for a given country code

#     Parameters
#     ----------
#     country_code : str
#         Three letter country codes of the downloaded files
#     update : bool
#         Name of the network component
#         Update = true, forces re-download of files

#     Returns
#     -------
#     Pbf file per country

#     """

#     geofabrik_pbf_url= get_region_dict(country_code)['url']['pbf']
#     PBF_inputfile = geofabrik_downloader(geofabrik_pbf_url)


#     # continent, country_name = get_continent_country(country_code)
#     # # Filename for geofabrik
#     # geofabrik_filename = f"{country_name}-latest.osm.pbf"

#     # # Specify the url depending on the requested element, whether it is a continent or a region
#     # if continent == country_name:
#     #     # Example continent-specific data: https://download.geofabrik.de/africa/nigeria-latest.osm.pbf
#     #     geofabrik_url = f"https://download.geofabrik.de/{geofabrik_filename}"
#     # else:
#     #     # Example country- or sub-region-specific data: https://download.geofabrik.de/africa-latest.osm.pbf
#     #     geofabrik_url = (
#     #         f"https://download.geofabrik.de/{continent}/{geofabrik_filename}")

#     # PBF_inputfile = os.path.join(
#     #     os.getcwd(), "data", "osm", "pbf", geofabrik_filename
#     # )  # Input filepath

#     # if not os.path.exists(PBF_inputfile):
#     #     _logger.info(f"{geofabrik_filename} downloading to {PBF_inputfile}")
#     #     #  create data/osm directory
#     #     os.makedirs(os.path.dirname(PBF_inputfile), exist_ok=True)
#     #     with requests.get(geofabrik_url, stream=True, verify=False) as r:
#     #         if r.status_code == 200:
#     #             # url properly found, thus execute as expected
#     #             with open(PBF_inputfile, "wb") as f:
#     #                 shutil.copyfileobj(r.raw, f)
#     #         else:
#     #             # error status code: file not found
#     #             _logger.error(
#     #                 f"Error code: {r.status_code}. File {geofabrik_filename} not downloaded from {geofabrik_url}"
#     #             )
    
#     # TODO: move verify to a separate function
#     # if verify is True:
#     #     if verify_pbf(PBF_inputfile, geofabrik_url, update) is False:
#     #         _logger.warning(f"md5 mismatch, deleting {geofabrik_filename}")
#     #         if os.path.exists(PBF_inputfile):
#     #             os.remove(PBF_inputfile)

#     #         download_pbf(country_code, update=False, verify=False)  # Only try downloading once

#     return PBF_inputfile


verified_pbf = []


def verify_pbf(PBF_inputfile, geofabrik_url, update):
    if PBF_inputfile in verified_pbf:
        return True

    geofabrik_md5_url = geofabrik_url + ".md5"
    PBF_md5file = PBF_inputfile + ".md5"

    def calculate_md5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    if update is True or not os.path.exists(PBF_md5file):
        with requests.get(geofabrik_md5_url, stream=True, verify=False) as r:
            with open(PBF_md5file, "wb") as f:
                shutil.copyfileobj(r.raw, f)

    local_md5 = calculate_md5(PBF_inputfile)

    with open(PBF_md5file) as f:
        contents = f.read()
        remote_md5 = contents.split()[0]

    if local_md5 == remote_md5:
        verified_pbf.append(PBF_inputfile)
        return True
    else:
        return False



# # Auxiliary function to initialize the parallel data download
# def _init_process_pop(update_, verify_):
#     global update, verify
#     update, verify = update_, verify_


# # Auxiliary function to download the data
# def _process_func_pop(c_code):
#     download_pbf(c_code, update, verify)


# def parallel_download_pbf(country_list,
#                           nprocesses,
#                           update=False,
#                           verify=False):
#     """
#     Function to download pbf data in parallel

#     Parameters
#     ----------
#     country_list : str
#         List of geofabrik country codes to download
#     nprocesses : int
#         Number of parallel processes
#     update : bool
#         If true, existing pbf files are updated. Default: False
#     verify : bool
#         If true, checks the md5 of the file. Default: False
#     """

#     # argument for the parallel processing
#     kwargs = {
#         "initializer": _init_process_pop,
#         "initargs": (update, verify),
#         "processes": nprocesses,
#     }

#     # execute the parallel download with tqdm progressbar
#     with mp.get_context("spawn").Pool(**kwargs) as pool:
#         for _ in tqdm(
#                 pool.imap(_process_func_pop, country_list),
#                 ascii=False,
#                 unit=" countries",
#                 total=len(country_list),
#                 desc="Download pbf ",
#         ):
#             pass

# def download_pbf_list(country_list, nprocesses):
#     # parallel download of data if parallel download is enabled
#     if nprocesses > 1:
#         _logger.info(
#             f"Parallel raw osm data (pbf files) download with {nprocesses} threads"
#         )
#         parallel_download_pbf(country_list, nprocesses, update, verify)