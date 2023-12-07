import geopandas as gpd
import os
directory = r'E:\Projects\Clients\NashvilleMPO\ModelUpdate2023\Tasks\Task4_Enhancements\Update_PopulationSim_Software\GitHub_William\Data'
taz = gpd.read_file(r'E:\Projects\Clients\NashvilleMPO\ModelUpdate2023\Tasks\Task4_Enhancements\Update_PopulationSim_Software\GitHub_William\Data\TAZ\TAZ_nashville_split_project.shp')
taz_union = taz.unary_union

def within_taz(df):
	df['point'] = df.geometry.map(lambda n: n.representative_point())
	df['in'] = df['point'].map(lambda l: l.within(taz_union))
	return df.drop('point', axis = 1)
	
bg = gpd.read_file(os.path.join(directory,'BlockGroup','tl_2021_47_bg.shp'))
bg = bg.to_crs(taz.crs)
bg = within_taz(bg)
# bg[(bg['in']) | (bg.index.isin([51,1066,2523,2917,4020]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'BlockGroup','nashville_bg.shp'), driver = 'ESRI Shapefile')
bg[bg['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'BlockGroup','nashville_bg.shp'), driver = 'ESRI Shapefile')


tract = gpd.read_file(os.path.join(directory,'CensusTract','tl_2021_47_tract.shp'))
tract = tract.to_crs(taz.crs)
tract = within_taz(tract)
# tract[(tract['in']) | (tract.index.isin([795,1380]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'CensusTract','nashville_tract.shp'), driver = 'ESRI Shapefile')
tract[tract['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'CensusTract','nashville_tract.shp'), driver = 'ESRI Shapefile')

county = gpd.read_file(os.path.join(directory,'County','tl_2021_us_county.shp'))
county = county.to_crs(taz.crs)
county = within_taz(county)
#county[(county['in']) | (county.index == 2405)].drop('in',axis = 1).to_file(filename = os.path.join(directory,'County','nashville_county.shp'), driver = 'ESRI Shapefile')
county[county['in']==1].drop('in',axis = 1).to_file(filename = os.path.join(directory,'County','nashville_county.shp'), driver = 'ESRI Shapefile')

puma = gpd.read_file(os.path.join(directory,'PUMS','Geography','tl_2021_47_puma10.shp'))
puma = puma.to_crs(taz.crs)
puma = within_taz(puma)
#puma[(puma['in']) | (puma.index.isin([9,25]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'PUMS','Geography','nashville_puma.shp'), driver = 'ESRI Shapefile')
puma[(puma['in']==1) | (puma.index.isin([9,25]))].drop('in',axis = 1).to_file(filename = os.path.join(directory,'PUMS','Geography','nashville_puma.shp'), driver = 'ESRI Shapefile')