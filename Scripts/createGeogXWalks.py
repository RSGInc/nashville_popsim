import geopandas as gpd
import pandas as pd
import sys
import os
import numpy as np

parameters_file = sys.argv[1]

parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']

maz = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'MAZ_DIR']['Value'].item().strip(),'MZ_nashville_clean.shp'))[['ID','TAZID_NEW','geometry']]
taz = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'TAZ_DIR']['Value'].item().strip(),'TAZ_nashville_split_project.shp'))[['ID_NEW_NEW','geometry']]
tract = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'CT_DIR']['Value'].item().strip(),'nashville_tract.shp'))[['GEOID','geometry']]
bg = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'BG_DIR']['Value'].item().strip(),'nashville_bg.shp'))[['GEOID','geometry']]
puma = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'PUMA_DIR']['Value'].item().strip(),'nashville_puma.shp'))[['GEOID10','geometry']]
county = gpd.read_file(os.path.join(parameters.loc[parameters.Key == 'CNTY_DIR']['Value'].item().strip(),'nashville_county.shp'))[['GEOID','geometry']]

maz = maz.to_crs(taz.crs)
tract = tract.to_crs(taz.crs)
bg = bg.to_crs(taz.crs)
puma = puma.to_crs(taz.crs)
county = county.to_crs(taz.crs)

maz['point'] = maz.geometry.map(lambda n: n.representative_point())
maz['geometry'] = maz['point']

x_walk_0 = gpd.sjoin(maz, taz, how = 'left', op='within')[['ID','ID_NEW_NEW']]
x_walk_0.columns = ['MAZ','TAZ']

taz['point'] = taz.geometry.map(lambda n: n.representative_point())
taz['geometry'] = taz['point']

x_walk_1 = gpd.sjoin(taz,bg, how = 'left', op = 'within')[['ID_NEW_NEW','GEOID']]
x_walk_1.columns = ['TAZ','BLOCKGRP']

bg['point'] = bg.geometry.map(lambda n:n.representative_point())
bg.geometry = bg['point']

x_walk_2 = gpd.sjoin(bg,tract, how = 'left', op = 'within',lsuffix = 'bg', rsuffix = 'tract')[['GEOID_bg','GEOID_tract']]
x_walk_2.columns = ['BLOCKGRP','TRACT']

tract['point'] = tract.geometry.map(lambda n: n.representative_point())
tract.geometry = tract['point']

x_walk_3 = gpd.sjoin(tract,puma,how = 'left', op = 'within')[['GEOID','GEOID10']]
x_walk_3.columns = ['TRACT','PUMA']

puma['point'] = puma.geometry.map(lambda n: n.representative_point())
puma.geometry = puma['point']

x_walk_4 = gpd.sjoin(puma,county, how = 'left', op = 'within', lsuffix = 'puma', rsuffix = 'county')[['GEOID10','GEOID']]
x_walk_4.columns = ['PUMA', 'COUNTY']
# print x_walk_4.PUMA.unique()
x_walk_4.loc[x_walk_4.PUMA == '4702700', 'COUNTY'] = '47119'
x_walk_4.loc[x_walk_4.PUMA == '4700400', 'COUNTY'] = '47147'
# x_walk_4['COUNTY'] = np.where(x_walk_4['PUMA'] == 4702700, 47119, x_walk_4['COUNTY'])
# x_walk_4['COUNTY'] = np.where(x_walk_4['PUMA'] == 4700400, 47147, x_walk_4['COUNTY'])

x_walk_5 = x_walk_0.merge(x_walk_1, how = 'left', on = 'TAZ').merge(x_walk_2, how = 'left', on = 'BLOCKGRP').merge(x_walk_3, how = 'left', on = 'TRACT' ).merge(x_walk_4, how = 'left', on = 'PUMA')
# x_walk_5 = x_walk_5[['ID','TAZ_New','GEOID_tract','GEOID_bg','GEOID10backlog','GEOID']]
# x_walk_5.columns = ['MAZ','TAZ','TRACT','BLOCKGRP','PUMA','COUNTY']
# x_walk_0.to_csv('crosswalks0.csv', index = None)
# x_walk_1.to_csv('crosswalks1.csv', index = None)
# x_walk_2.to_csv('crosswalks2.csv', index = None)
# x_walk_3.to_csv('crosswalks3.csv', index = None)
# x_walk_4.to_csv('crosswalks4.csv', index = None)
# x_walk_5.to_csv('crosswalks5.csv', index = None)
# x_walk_6.to_csv('crosswalks6.csv', index = None)
# x_walk_7.to_csv('crosswalks7.csv', index = None)
#x_walk_5['PUMA'] = x_walk_5['PUMA'].map(lambda n: int(n) - 4700000)
x_walk_5['REGION'] = 3
x_walk_5.to_csv(os.path.join(parameters.loc[parameters.Key == 'XWALK_DIR']['Value'].item().strip(),'geo_crosswalks.csv'), index = None)

#generate crosswalks for GQs. Only PUMA=47037 has GQs, so retain only this PUMA in the crosswalk
x_walk_6 = x_walk_5[x_walk_5.PUMA=='4702505']
x_walk_6.to_csv(os.path.join(parameters.loc[parameters.Key == 'XWALK_DIR']['Value'].item().strip(),'geo_crosswalksGQ.csv'), index = None)

#print x_walk_5.columns