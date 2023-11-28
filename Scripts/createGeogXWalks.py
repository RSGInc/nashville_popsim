import geopandas as gpd
import pandas as pd
import sys
import os
import numpy as np

parameters_file = sys.argv[1]
# 
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']

maz = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'MAZ_DIR']['Value'].item().strip(),'MZ_nashville_clean.shp'))[['ID','geometry']]
taz = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'TAZ_DIR']['Value'].item().strip(),'TAZ_nashville_split_project.shp'))[['ID','geometry']]
tract = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'CT_DIR']['Value'].item().strip(),'nashville_tract.shp'))[['GEOID','geometry']]
bg = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'BG_DIR']['Value'].item().strip(),'nashville_bg.shp'))[['GEOID','geometry']]
puma = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'PUMA_DIR']['Value'].item().strip(),'nashville_puma.shp'))[['GEOID20','geometry']]
county = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'CNTY_DIR']['Value'].item().strip(),'nashville_county.shp'))[['GEOID','geometry']]

maz = maz.to_crs(taz.crs)
tract = tract.to_crs(taz.crs)
bg = bg.to_crs(taz.crs)
puma = puma.to_crs(taz.crs)
county = county.to_crs(taz.crs)

maz['point'] = maz.geometry.map(lambda n: n.representative_point())
maz['geometry'] = maz['point']

x_walk_0 = gpd.sjoin(maz, taz, how = 'left', op='within')[['ID_left','ID_right']]
x_walk_0.columns = ['MAZ','TAZ']

taz['point'] = taz.geometry.map(lambda n: n.representative_point())
taz['geometry'] = taz['point']

x_walk_1 = gpd.sjoin(taz,bg, how = 'left', op = 'within')[['ID','GEOID']]
x_walk_1.columns = ['TAZ','BLOCKGRP']

bg['point'] = bg.geometry.map(lambda n:n.representative_point())
bg.geometry = bg['point']

x_walk_2 = gpd.sjoin(bg,tract, how = 'left', op = 'within',lsuffix = 'bg', rsuffix = 'tract')[['GEOID_bg','GEOID_tract']]
x_walk_2.columns = ['BLOCKGRP','TRACT']

tract['point'] = tract.geometry.map(lambda n: n.representative_point())
tract.geometry = tract['point']

x_walk_3 = gpd.sjoin(tract,puma,how = 'left', op = 'within')[['GEOID','GEOID20']]
x_walk_3.columns = ['TRACT','PUMA']

puma['point'] = puma.geometry.map(lambda n: n.representative_point())
puma.geometry = puma['point']

x_walk_4 = gpd.sjoin(puma,county, how = 'left', op = 'within', lsuffix = 'puma', rsuffix = 'county')[['GEOID20','GEOID']]
x_walk_4.columns = ['PUMA', 'COUNTY']


x_walk_5 = x_walk_0.merge(x_walk_1, how = 'left', on = 'TAZ').merge(x_walk_2, how = 'left', on = 'BLOCKGRP').merge(x_walk_3, how = 'left', on = 'TRACT' ).merge(x_walk_4, how = 'left', on = 'PUMA')
x_walk_5['REGION'] = 3 # https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf
x_walk_5.to_csv(os.path.join(parameters.loc[parameters.Key == 'XWALK_DIR']['Value'].item().strip(),'geo_crosswalks.csv'), index = None)

#generate crosswalks for GQs. Only PUMA=47037 has GQs, so retain only this PUMA in the crosswalk
# x_walk_6 = x_walk_5[x_walk_5.PUMA=='4702505']
# x_walk_6.to_csv(os.path.join(parameters.loc[parameters.Key == 'XWALK_DIR']['Value'].item().strip(),'geo_crosswalksGQ.csv'), index = None)

