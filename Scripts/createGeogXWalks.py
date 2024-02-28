import geopandas as gpd
import pandas as pd
import sys
import os
import numpy as np

parameters_file = sys.argv[1]
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']
USER_DIR = os.path.join(os.getcwd(),'Data', 'USER')

maz = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'MAZ_DIR']['Value'].item().strip(),'MZ_nashville_clean.shp'))[['ID','geometry']]
taz = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'TAZ_DIR']['Value'].item().strip(),'TAZ_nashville_split_project.shp'))[['ID','geometry']]
tract = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'CT_DIR']['Value'].item().strip(),'nashville_tract.shp'))[['GEOID','geometry']]
bg = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'BG_DIR']['Value'].item().strip(),'nashville_bg.shp'))[['GEOID','geometry']]
puma = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'PUMA_DIR']['Value'].item().strip(),'nashville_puma.shp'))[['GEOID10','geometry']]
county = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'CNTY_DIR']['Value'].item().strip(),'nashville_county.shp'))[['GEOID','geometry']]
gq_pumas = pd.read_csv(os.path.join(USER_DIR, 'gq_pumas.csv'))

maz = maz.to_crs(taz.crs)
tract = tract.to_crs(taz.crs)
bg = bg.to_crs(taz.crs)
puma = puma.to_crs(taz.crs)
county = county.to_crs(taz.crs)

# maz['point'] = maz.geometry.map(lambda n: n.representative_point())
# maz['geometry'] = maz['point']

# x_walk_0 = gpd.sjoin(maz, taz, how = 'left', op='within')[['ID_left','ID_right']]
# x_walk_0.columns = ['MAZ','TAZ']

maz_taz = gpd.overlay(maz, taz, how='intersection').rename(columns={'ID_1':'MAZ', 'ID_2':'TAZ'})
maz_taz['area'] = maz_taz.area
ids_largest_area = maz_taz.groupby('MAZ', as_index=True)['area'].idxmax()
x_walk_0 = maz_taz.loc[ids_largest_area.values, ['MAZ', 'TAZ']].sort_values('MAZ')

# taz['point'] = taz.geometry.map(lambda n: n.representative_point())
# taz['geometry'] = taz['point']

# x_walk_1 = gpd.sjoin(taz,bg, how = 'left', op = 'within')[['ID','GEOID']]
# x_walk_1.columns = ['TAZ','BLOCKGRP']

taz_bg = gpd.overlay(taz, bg, how='intersection').rename(columns={'ID':'TAZ', 'GEOID':'BLOCKGRP'})
taz_bg['area'] = taz_bg.area
ids_largest_area = taz_bg.groupby('TAZ', as_index=True)['area'].idxmax()
x_walk_1 = taz_bg.loc[ids_largest_area.values, ['TAZ', 'BLOCKGRP']].sort_values('TAZ')

# bg['point'] = bg.geometry.map(lambda n:n.representative_point())
# bg['geometry'] = bg['point']

# x_walk_2 = gpd.sjoin(bg,tract, how = 'left', op = 'within',lsuffix = 'bg', rsuffix = 'tract')[['GEOID_bg','GEOID_tract']]
# x_walk_2.columns = ['BLOCKGRP','TRACT']

bg_tract = gpd.overlay(bg, tract, how='intersection').rename(columns={'GEOID_1':'BLOCKGRP', 'GEOID_2':'TRACT'})
bg_tract['area'] = bg_tract.area
ids_largest_area = bg_tract.groupby('BLOCKGRP', as_index=True)['area'].idxmax()
x_walk_2 = bg_tract.loc[ids_largest_area.values, ['BLOCKGRP', 'TRACT']].sort_values('BLOCKGRP')

# tract['point'] = tract.geometry.map(lambda n: n.representative_point())
# tract.geometry = tract['point']

# x_walk_3 = gpd.sjoin(tract,puma,how = 'left', op = 'within')[['GEOID','GEOID10']]
# x_walk_3.columns = ['TRACT','PUMA']

tract_puma = gpd.overlay(tract, puma, how='intersection').rename(columns={'GEOID':'TRACT', 'GEOID10':'PUMA'})
tract_puma['area'] = tract_puma.area
ids_largest_area = tract_puma.groupby('TRACT', as_index=True)['area'].idxmax()
x_walk_3 = tract_puma.loc[ids_largest_area.values, ['TRACT', 'PUMA']].sort_values('TRACT')

# puma['point'] = puma.geometry.map(lambda n: n.representative_point())
# puma.geometry = puma['point']

# x_walk_4 = gpd.sjoin(puma,county, how = 'left', op = 'within', lsuffix = 'puma', rsuffix = 'county')[['GEOID10','GEOID']]
# x_walk_4.columns = ['PUMA', 'COUNTY']
# x_walk_4.loc[x_walk_4.PUMA == '4702700', 'COUNTY'] = '47119'
# x_walk_4.loc[x_walk_4.PUMA == '4700400', 'COUNTY'] = '47147'

puma_county = gpd.overlay(puma, county, how='intersection').rename(columns={'GEOID10':'PUMA', 'GEOID':'COUNTY'})
puma_county['area'] = puma_county.area
ids_largest_area = puma_county.groupby('PUMA', as_index=True)['area'].idxmax()
x_walk_4 = puma_county.loc[ids_largest_area.values, ['PUMA', 'COUNTY']].sort_values('PUMA')


x_walk_5 = x_walk_0.merge(x_walk_1, how = 'left', on = 'TAZ').merge(x_walk_2, how = 'left', on = 'BLOCKGRP').merge(x_walk_3, how = 'left', on = 'TRACT' ).merge(x_walk_4, how = 'left', on = 'PUMA')
x_walk_5['REGION'] = 3 # https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf
x_walk_5.to_csv(os.path.join(parameters.loc[parameters.Key == 'XWALK_DIR']['Value'].item().strip(),'geo_crosswalks.csv'), index = None)

#generate crosswalks for GQs. Only PUMA=47037 has GQs, so retain only this PUMA in the crosswalk
print(gq_pumas.head())
print(gq_pumas.dtypes)
print(x_walk_5.head())
print(x_walk_5.dtypes)
x_walk_6 = x_walk_5[x_walk_5.PUMA.astype(int).isin(gq_pumas.PUMA)]
print(x_walk_6.head())
x_walk_6.to_csv(os.path.join(parameters.loc[parameters.Key == 'XWALK_DIR']['Value'].item().strip(),'geo_crosswalksGQ.csv'), index = None)

