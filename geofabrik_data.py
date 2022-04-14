import logging
_logger = logging.getLogger("osm_data_extractor")
_logger.setLevel(logging.INFO)
import geopandas as gpd
import pandas as pd
from geofabrik_download import download_sitemap
import json

# def get_df():
geom = False
sitemap = download_sitemap(geom, update = False)
if geom is True: gpd.read_file(sitemap)
if geom is False:
    with open(sitemap) as f:
        d = json.load(f)

    row_list =[]
    for feature in d['features']:
        row_list.append(feature['properties'])
    df = pd.DataFrame(row_list)
    # Add short code column
    col1 = df['iso3166-1:alpha2'].apply(lambda x: '-'.join(x) if type(x) == list else x)
    col2 = df['iso3166-2'].apply(lambda x: '-'.join(x) if type(x) == list else x)
    df['short_code'] = col1.combine_first(col2)

def view_regions():
    # get_df() if df is None else None
    by_parent = df.groupby("parent", as_index=False)["id"]
    parent_dict = by_parent.groups
    root = list(df.loc[df['parent'].isna(), 'id'])
    world_dict = {}
    local_dict = {}
    for key in parent_dict:
        if key in root:
            world_dict[key] = list(by_parent.get_group(key))
        else:
            local_dict[key] = list(by_parent.get_group(key))

    all_dict = {**world_dict, **local_dict}
    print(json.dumps(all_dict, sort_keys=True, indent=4))

# mask = df['iso3166-1:alpha2'].str.len() >2
# df.loc[mask]
# na_alpha = df[df['iso3166-1:alpha2'].isna() & df['iso3166-2'].isna()]
# na_alpha
# df[df['short_code'].isna()]
# df[df['short_code'].str.len() > 2]

def get_region_dict(id):
    # df = get_df() if df is None else None
    try:
        c_dict = df.loc[df['id']== id].drop('iso3166-1:alpha2', 1).drop('iso3166-2', 1).to_dict('records')[0]
    except:
        _logger.error(f'{id} not found')

    return c_dict

def get_id_by_code(code):
    # get_df() if df is None else None
    try:
        id = df.loc[df['short_code']== code, 'id'].item()
    except:
        _logger.error(f'{id} not found')

    return id


def get_short_id(id):
    c_dict = get_region_dict(id)
    short_code = str(c_dict["short_code"])
    real_id = c_dict['id']
    if short_code == 'nan':
        _logger.warning(f'{real_id} short_code not found')
        return c_dict['id']
    else:
        return short_code

if __name__ == "__main__":
    print(get_region_dict("us/california"))
    print(get_short_id('africa'))
    print(get_id_by_code('DZ'))
    # view_regions()