from Scripts.buildControls import USER_DIR
import geopandas as gpd
import numpy as np
import os
directory = r'.\Data'
taz = gpd.read_file(r'.\Data\TAZ\TAZ_nashville_split_project.shp')
taz_union = taz.unary_union
USER_DIR = os.path.join(os.getcwd(),'Data', 'USER')

def within_taz(df, PUMAFIELD='PUMACE10'):
	df_filtered = df.loc[df.intersects(taz_union)]
	df_filtered['AREA'] = df_filtered.to_crs(crs=3857).area
	df_filtered = df_filtered.overlay(taz[['geometry']].dissolve(),how='intersection', keep_geom_type=True)
	df_filtered['INTAREA'] = df_filtered.to_crs(crs=3857).area
	df_filtered['PROP'] = df_filtered['INTAREA']/df_filtered['AREA']
	df_filtered = df_filtered.loc[df_filtered.PROP>.05]
	# df['point'] = df.geometry.map(lambda n: n.representative_point())
	# df['in'] = df['point'].map(lambda l: l.within(taz_union))
	df['in'] = df[PUMAFIELD].isin(df_filtered[PUMAFIELD])
	return df
	
bg = gpd.read_file(os.path.join(directory,'BlockGroup','tl_rd22_47_bg.shp'))
bg = bg.to_crs(taz.crs)
bg = within_taz(bg)
# bg[(bg['in']) | (bg.index.isin([51,1066,2523,2917,4020]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'BlockGroup','nashville_bg.shp'), driver = 'ESRI Shapefile')
bg[bg['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'BlockGroup','nashville_bg.shp'), driver = 'ESRI Shapefile')


tract = gpd.read_file(os.path.join(directory,'CensusTract','tl_2022_47_tract.shp'))
tract = tract.to_crs(taz.crs)
tract = within_taz(tract)
# tract[(tract['in']) | (tract.index.isin([795,1380]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'CensusTract','nashville_tract.shp'), driver = 'ESRI Shapefile')
tract[tract['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'CensusTract','nashville_tract.shp'), driver = 'ESRI Shapefile')

county = gpd.read_file(os.path.join(directory,'County','tl_2022_us_county.shp'))
county = county.to_crs(taz.crs)
county = within_taz(county)
#county[(county['in']) | (county.index == 2405)].drop('in',axis = 1).to_file(filename = os.path.join(directory,'County','nashville_county.shp'), driver = 'ESRI Shapefile')
county[county['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'County','nashville_county.shp'), driver = 'ESRI Shapefile')

puma20 = gpd.read_file(os.path.join(directory,'PUMS','Geography', 'tl_2022','tl_2022_47_puma20.shp'))
puma10 = gpd.read_file(os.path.join(directory,'PUMS','Geography','tl_2021_47_puma10.shp'))

puma20 = puma20.to_crs(taz.crs)
puma10 = puma10.to_crs(taz.crs)
puma20 = within_taz(puma20, PUMAFIELD='PUMACE20')
puma10 = within_taz(puma10, PUMAFIELD='PUMACE10')
puma20 = puma20[puma20['in']==1].drop('in',axis = 1)
puma10 = puma10[puma10['in']==1].drop('in',axis = 1)
puma20['POLYAREA20'] = puma20.to_crs(crs=3857).area
puma10['POLYAREA10'] = puma10.to_crs(crs=3857).area

puma = puma10.overlay(puma20, how='union', keep_geom_type=True)
puma = puma.explode().reset_index(drop=True)
puma['PUMA'] = puma['PUMACE20'].astype(str) + puma['PUMACE10'].astype('str')
puma['PUMA'] = puma['PUMA'] + puma.groupby('PUMA', as_index=False)['PUMA'].transform('cumcount').astype('str')
puma['INTSECTIONAREA'] = puma.to_crs(crs=3857).area
puma['PROP10'] = puma['INTSECTIONAREA']/puma['POLYAREA10']
puma['PROP20'] = puma['INTSECTIONAREA']/puma['POLYAREA20']
puma.groupby(['PUMACE10','POLYAREA10'],as_index=False)['INTSECTIONAREA'].sum().astype(np.int64)
puma.groupby(['PUMACE20','POLYAREA20'],as_index=False)['INTSECTIONAREA'].sum().astype(np.int64)
puma.loc[puma.PUMACE10.isna(),'PROP20'] = 0.0
puma.loc[puma.PUMACE20.isna(),'PROP10'] = 0.0
puma.groupby(['PUMACE10','POLYAREA10'],as_index=False)['PROP10'].sum()
puma.groupby(['PUMACE20','POLYAREA20'],as_index=False)['PROP20'].sum()
puma = puma.loc[(~puma.PUMACE10.isna()) & (~puma.PUMACE20.isna())]
puma['NEWPROP10'] = puma.groupby('PUMACE10')['PROP10'].transform(lambda x: x/sum(x))
puma['NEWPROP20'] = puma.groupby('PUMACE20')['PROP20'].transform(lambda x: x/sum(x))
puma = puma.loc[(puma.NEWPROP10 > 1E-2) | (puma.NEWPROP20 > 1E-2)]
puma['NEWPROP10'] = puma.groupby('PUMACE10')['PROP10'].transform(lambda x: x/sum(x))
puma['NEWPROP20'] = puma.groupby('PUMACE20')['PROP20'].transform(lambda x: x/sum(x))
puma['PROP10'] = puma['NEWPROP10']
puma['PROP20'] = puma['NEWPROP20']
puma = puma.drop(['NEWPROP20', 'NEWPROP10'], axis=1)

#puma[(puma['in']) | (puma.index.isin([9,25]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'PUMS','Geography','nashville_puma.shp'), driver = 'ESRI Shapefile')
# puma[puma['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'PUMS','Geography','nashville_puma.shp'), driver = 'ESRI Shapefile')
puma[['PUMACE10','PUMACE20','PUMA','PROP10','PROP20','POLYAREA10','POLYAREA20','INTSECTIONAREA','geometry']].\
	to_file(filename = os.path.join(directory,'PUMS','Geography','nashville_newpuma.shp'), driver = 'ESRI Shapefile')

puma[['PUMACE10','PUMACE20','PUMA','PROP10','PROP20','POLYAREA10','POLYAREA20','INTSECTIONAREA']].to_csv(os.path.join(USER_DIR, 'puma20_puma10_newpuma_xwalk.csv'))
