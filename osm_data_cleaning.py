#%%
import os
import geopandas as gpd
import numpy as np
import pandas as pd

#%%
cc_code = "BR"
raw_outputfile_partial = os.path.join(os.getcwd(), "data", "raw", cc_code + "_raw")
df_l = pd.read_csv(raw_outputfile_partial + "_lines" + ".csv")
df_t = pd.read_csv(raw_outputfile_partial + "_towers" + ".csv")
#%%
def join_csv():
        
#%%
cc_list = ['AU', 'BD', 'BR', 'CD', 'GH', 'MW', 'SL', 'TD', 'US-CA', 'US-TX']
line_csv = [os.path.join(os.getcwd(), "data", "raw", cc_code + "_raw" + "_lines" + ".csv") for cc_code in cc_list]
tower_csv = [os.path.join(os.getcwd(), "data", "raw", cc_code + "_raw" + "_towers" + ".csv") for cc_code in cc_list]
df_l = pd.concat(map(pd.read_csv, line_csv))
df_t = pd.concat(map(pd.read_csv, tower_csv))

#%%
df_l = df_l.dropna(axis=1, how='all', thresh=int(0.05*df_l.shape[0]))
df_t = df_t.dropna(axis=1, how='all', thresh=int(0.002*df_t.shape[0]))
#%%
df_l.drop(df_l.filter(regex="Unnamed"),axis=1, inplace=True)
df_t.drop(df_t.filter(regex="Unnamed"),axis=1, inplace=True)
#%%
df_l = df_l.set_index('id', verify_integrity= True).reset_index()
df_t = df_t.set_index('id', verify_integrity= True).reset_index()
#%%
from ast import literal_eval
df_l['refs'] = df_l['refs'].apply(literal_eval)
#%%
dfl = df_l[['id', 'refs', "tags.voltage", 'Length', 'Country']]
dfl
#%%
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def max_v(v_string):
    if isinstance(v_string, str):
        if 'kV' in v_string:
            kv_string = v_string.split()[0]
            return int(float(kv_string) * 1000)
        elif ';' in v_string:
            semi_split = v_string.split(';')
            v_split = [int(x) for x in semi_split if isfloat(x)]
            return max(v_split)
        elif '/' in v_string:
            semi_split = v_string.split('/')
            v_split = [int(x) for x in semi_split if isfloat(x)]
            return max(v_split)
        elif isfloat(v_string):
            return int(v_string)
        else:
            return v_string
    elif isfloat(v_string):
        return int(v_string)
    else:
        print(v_string) 
        return v_string
#%%
dfl['tags.voltage'] = dfl['tags.voltage'].apply(lambda v: max_v(v) if pd.notnull(v) else v)
# e = dfl["tags.voltage"].value_counts()
#%%
dfl['high_voltage'] = dfl[dfl['tags.voltage'].apply(lambda x: isinstance(x, int))]['tags.voltage'] > 100000

#%%
dfl.groupby('high_voltage').agg(['mean','median', 'min', 'max'])
#%%
dfl.loc[dfl['high_voltage']== True, 'Length'].plot.hist(bins = 100)
#%%
dfl.loc[dfl['high_voltage']== False, 'Length'].plot.hist(bins = 100)
#%%
dflr = dfl.explode('refs')

#%%
dflr = dflr.sort_values(by=['high_voltage'], ascending=False).drop_duplicates(subset=['refs'])

#%%
dft = df_t[['id', 'tags.structure', 'tags.design', 'tags.material', 'Country']]
#%%
dft['id'] = dft.id.astype('int')

#%%
dft.count()
#%%

#%%
dflt = dft.merge(right = dflr, how='left', left_on = 'id', right_on = 'refs')

# Drop Columns
# rename id_x to id
# rename id_y to line_id
# 
#%%
dflt.groupby('high_voltage').size()
#%%
dft['id'].unique().size
#%%
dflt['high_voltage'].value_counts()
#%%
a = df_l['tags.voltage'].value_counts()



#%%
df_t['tags.material'].value_counts(dropna=False)
# steel is tagged as lattice, ususally transmission
# wooden or concrete is tagged as solid
# metal is ?

#%%
df_t['tags.structure'].value_counts(dropna=False)
# lattice structure is mostly used in transmission towers
# tubular seems to be transmission
# solid is ?
#%%
df_t['tags.design'].value_counts(dropna=False)
# h-frame can be anything
# three level is most likely transmission tower



# want transmission lines

#  drop nans
# if length > 100 000
# if voltage > 33000

#%%
# df["ref_length"] = df["refs"].apply(lambda x : len(x))
# df["ref_ratio"] = df["ref_length"]/df["Length"]
# r_df = df[["Length", "ref_length", "ref_ratio", "tags.voltage"]]
# display(r_df)

