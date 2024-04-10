from re import U
import requests
import pandas as pd
import os
import sys
import json
import numpy as np

parameters_file = os.path.join(os.getcwd(),'Data', 'parameters.csv')

parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']
WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
outputDir = os.path.join(os.getcwd(),'Setup', 'data')
censusDownloadDir = os.path.join(WORKING_DIR, 'Data','Census','Downloads')

api_key = pd.read_csv(os.path.join(WORKING_DIR,"Data","Census","API","census_api_key.csv")).iloc[0].key
STATE_FIPS = "47"

COUNTY_FIPS_STRING = ["149","165","187","147","189","037","119"]
CENSUS_YEAR = '2022'


def getCensusData(name, vintage, key, vars, region, regionin):
  
	url ="https://api.census.gov/data/{}/{}?key={}&get=".format(vintage, name,key)

	url = url+ vars +"&for="+ region+ "&in="+ regionin+ "&key="+ key
	#cat(url)

	response = requests.get(url)
	formattedResponse = json.loads(response.text)
	response_again = pd.DataFrame(data = formattedResponse)
	response_again = response_again.rename(columns = response_again.iloc[0]).drop(response_again.index[0])
	if 'block group' in response_again.columns:
		response_again[['state','county','tract','block group']] = response_again[['state','county','tract','block group']].astype(str)
		response_again['county2'] = response_again['county'].map(lambda n: '{:^03}'.format(n))
		response_again['BKGPIDFP10'] = response_again['state'] + response_again['county2'] + response_again['tract']+response_again['block group']
	else:
		response_again[['state','county','tract']] = response_again[['state','county','tract']].astype(str)
		response_again['county2'] = response_again['county'].map(lambda n: '{:^03}'.format(n))
		response_again['CTIDFP10'] = response_again['state'] + response_again['county2']  + response_again['tract']
	
	return(response_again.drop('county2', axis = 1))
    
if not os.path.exists(censusDownloadDir):
        os.mkdir(censusDownloadDir)
	
if not os.path.isfile(os.path.join(censusDownloadDir, 'hhsize_BG_' + CENSUS_YEAR + '_acs5.csv')):	
	hhsize_BG = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhsize_BG = getCensusData(name = "acs/acs5", 	# some may not available eg. 2022 acs1 in some cases
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B11016_001E,B11016_002E,B11016_003E,B11016_004E,B11016_005E,B11016_006E,B11016_007E,B11016_008E,B11016_009E,B11016_010E,B11016_011E,B11016_012E,B11016_013E,B11016_014E,B11016_015E,B11016_016E",
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhsize_BG = pd.concat([hhsize_BG, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B11016_001E,B11016_002E,B11016_003E,B11016_004E,B11016_005E,B11016_006E,B11016_007E,B11016_008E,B11016_009E,B11016_010E,B11016_011E,B11016_012E,B11016_013E,B11016_014E,B11016_015E,B11016_016E",
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])
	hhsize_BG.to_csv(os.path.join(censusDownloadDir, "hhsize_BG_" + CENSUS_YEAR + "_acs5.csv"), index = False)

if not os.path.isfile(os.path.join(censusDownloadDir, 'hhtype_BG_' + CENSUS_YEAR + '_acs5.csv')):	
	hhtype_BG = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhtype_BG = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B11001_003E,B11001_005E,B11001_006E,B11001_008E,B11001_009E",
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhtype_BG = pd.concat([hhtype_BG,getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B11001_003E,B11001_005E,B11001_006E,B11001_008E,B11001_009E",
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	hhtype_BG.to_csv(os.path.join(censusDownloadDir, 'hhtype_BG_' + CENSUS_YEAR + '_acs5.csv'), index = False)

#B26103_001E,B26103_006E,B26103_007E - total gqs, total non-inst gqs, college/univ gqs
if not os.path.isfile(os.path.join(censusDownloadDir, 'gqtot_CT_' + CENSUS_YEAR + '_acs5.csv')):
	gqs_BG = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			gqs_BG = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B26001_001E", 
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			gqs_BG = pd.concat([gqs_BG, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B26001_001E", 
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	gqs_BG.to_csv(os.path.join(censusDownloadDir, 'gqtot_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)


	
if not os.path.isfile(os.path.join(censusDownloadDir, 'hhtenure_BG_' + CENSUS_YEAR + '_acs5.csv')):
	hhtenure_BG = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhtenure_BG = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B25007_001E,B25007_002E,B25007_003E,B25007_004E,B25007_005E,B25007_006E,B25007_007E,B25007_008E,B25007_009E,B25007_010E,B25007_011E,B25007_012E,B25007_013E,B25007_014E,B25007_015E,B25007_016E,B25007_017E,B25007_018E,B25007_019E,B25007_020E,B25007_021E", 
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhtenure_BG = pd.concat([hhtenure_BG ,getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B25007_001E,B25007_002E,B25007_003E,B25007_004E,B25007_005E,B25007_006E,B25007_007E,B25007_008E,B25007_009E,B25007_010E,B25007_011E,B25007_012E,B25007_013E,B25007_014E,B25007_015E,B25007_016E,B25007_017E,B25007_018E,B25007_019E,B25007_020E,B25007_021E", 
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	hhtenure_BG.to_csv(os.path.join(censusDownloadDir, 'hhtenure_BG_' + CENSUS_YEAR + '_acs5.csv'), index = False)
	
	
if not os.path.isfile(os.path.join(censusDownloadDir, 'hhworker_CT_' + CENSUS_YEAR + '_acs5.csv')):
	hhworker_CT = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhworker_CT = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B08202_001E,B08202_002E,B08202_003E,B08202_004E,B08202_005E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhworker_CT = pd.concat([hhworker_CT, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B08202_001E,B08202_002E,B08202_003E,B08202_004E,B08202_005E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	hhworker_CT.to_csv(os.path.join(censusDownloadDir, 'hhworker_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)

if not os.path.isfile(os.path.join(censusDownloadDir, 'hhincome_CT_' + CENSUS_YEAR + '_acs5.csv')):
	hhincome_CT = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhincome_CT = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B19001_001E,B19001_002E,B19001_003E,B19001_004E,B19001_005E,B19001_006E,B19001_007E,B19001_008E,B19001_009E,B19001_010E,B19001_011E,B19001_012E,B19001_013E,B19001_014E,B19001_015E,B19001_016E,B19001_017E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhincome_CT = pd.concat([hhincome_CT, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B19001_001E,B19001_002E,B19001_003E,B19001_004E,B19001_005E,B19001_006E,B19001_007E,B19001_008E,B19001_009E,B19001_010E,B19001_011E,B19001_012E,B19001_013E,B19001_014E,B19001_015E,B19001_016E,B19001_017E",	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	hhincome_CT.to_csv(os.path.join(censusDownloadDir, 'hhincome_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)

if not os.path.isfile(os.path.join(censusDownloadDir, 'populationbyageandsex_CT_' + CENSUS_YEAR + '_acs5.csv')):
	popage_CT = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			popage_CT = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B01001_001E,B01001_002E,B01001_003E,B01001_004E,B01001_005E,B01001_006E,B01001_007E,B01001_008E,B01001_009E,B01001_010E,B01001_011E,B01001_012E,B01001_013E,B01001_014E,B01001_015E,B01001_016E,B01001_017E,B01001_018E,B01001_019E,B01001_020E,B01001_021E,B01001_022E,B01001_023E,B01001_024E,B01001_025E,B01001_026E,B01001_027E,B01001_028E,B01001_029E,B01001_030E,B01001_031E,B01001_032E,B01001_033E,B01001_034E,B01001_035E,B01001_036E,B01001_037E,B01001_038E,B01001_039E,B01001_040E,B01001_041E,B01001_042E,B01001_043E,B01001_044E,B01001_045E,B01001_046E,B01001_047E,B01001_048E,B01001_049E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			popage_CT = pd.concat([popage_CT, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B01001_001E,B01001_002E,B01001_003E,B01001_004E,B01001_005E,B01001_006E,B01001_007E,B01001_008E,B01001_009E,B01001_010E,B01001_011E,B01001_012E,B01001_013E,B01001_014E,B01001_015E,B01001_016E,B01001_017E,B01001_018E,B01001_019E,B01001_020E,B01001_021E,B01001_022E,B01001_023E,B01001_024E,B01001_025E,B01001_026E,B01001_027E,B01001_028E,B01001_029E,B01001_030E,B01001_031E,B01001_032E,B01001_033E,B01001_034E,B01001_035E,B01001_036E,B01001_037E,B01001_038E,B01001_039E,B01001_040E,B01001_041E,B01001_042E,B01001_043E,B01001_044E,B01001_045E,B01001_046E,B01001_047E,B01001_048E,B01001_049E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	popage_CT.to_csv(os.path.join(censusDownloadDir, 'populationbyageandsex_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)

if not os.path.isfile(os.path.join(censusDownloadDir, 'hh_kids_nokids_CT_' + CENSUS_YEAR + '_acs5.csv')):
	hhkids_CT = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhkids_CT = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B25012_001E,B25012_002E,B25012_003E,B25012_004E,B25012_005E,B25012_006E,B25012_007E,B25012_008E,B25012_009E,B25012_010E,B25012_011E,B25012_012E,B25012_013E,B25012_014E,B25012_015E,B25012_016E,B25012_017E",
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhkids_CT = pd.concat([hhkids_CT, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B25012_001E,B25012_002E,B25012_003E,B25012_004E,B25012_005E,B25012_006E,B25012_007E,B25012_008E,B25012_009E,B25012_010E,B25012_011E,B25012_012E,B25012_013E,B25012_014E,B25012_015E,B25012_016E,B25012_017E",
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	hhkids_CT.to_csv(os.path.join(censusDownloadDir, 'hh_kids_nokids_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)

if not os.path.isfile(os.path.join(censusDownloadDir, 'hhunit_CT_' + CENSUS_YEAR + '_acs5.csv')):
        #list of variables: https://api.census.gov/data/2022/acs/acs1/variables.html
	hhunit_CT = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			hhunit_CT = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B11011_004E,B11011_005E,B11011_006E,B11011_009E,B11011_010E,B11011_011E,B11011_013E,B11011_014E,B11011_015E,B11011_017E,B11011_018E,B11011_019E",
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			hhunit_CT = pd.concat([hhunit_CT, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B11011_004E,B11011_005E,B11011_006E,B11011_009E,B11011_010E,B11011_011E,B11011_013E,B11011_014E,B11011_015E,B11011_017E,B11011_018E,B11011_019E",
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	hhunit_CT.to_csv(os.path.join(censusDownloadDir, 'hhunit_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)

# Total: S0101_C01_001E
# Male: S0101_C03_001E
# Female: S0101_C05_001E
if not os.path.isfile(os.path.join(censusDownloadDir, 'populationbysex_CT_' + CENSUS_YEAR + '_acs5.csv')):
	popsex_CT = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			popsex_CT = getCensusData(name = "acs/acs5/subject", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="S0101_C01_001E,S0101_C03_001E,S0101_C05_001E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			popsex_CT = pd.concat([popsex_CT, getCensusData(name = "acs/acs5/subject", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="S0101_C01_001E,S0101_C03_001E,S0101_C05_001E", 	
									 region = "tract:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	popsex_CT.to_csv(os.path.join(censusDownloadDir, 'populationbysex_CT_' + CENSUS_YEAR + '_acs5.csv'), index = False)


# Total Population: B03002_001E
# Total Non-Hispanic: B03002_002E
# race_white_nohisp: B03002_003E
# race_black_nohisp: B03002_004E
# race_native_american_nohisp: B03002_005E
# race_asian_pi_nohisp: B03002_006E
# ethnicity_hisp: B03002_012E

if not os.path.isfile(os.path.join(censusDownloadDir, 'populationbyrace_BG_' + CENSUS_YEAR + '_acs5.csv')):
	poprace_BG = pd.DataFrame()
	for i,cen in enumerate(COUNTY_FIPS_STRING):	
		if i == 0:
			poprace_BG = getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B03002_001E,B03002_002E,B03002_003E,B03002_004E,B03002_005E,B03002_006E,B03002_012E", 	
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))
		else:
			poprace_BG = pd.concat([poprace_BG, getCensusData(name = "acs/acs5", 	
									 vintage = CENSUS_YEAR, 	
									 key = api_key, 	
									 vars="B03002_001E,B03002_002E,B03002_003E,B03002_004E,B03002_005E,B03002_006E,B03002_012E", 	
									 region = "block%20group:*",	
									 regionin = "state:{}%20county:{}".format(STATE_FIPS, cen))])

	poprace_BG.to_csv(os.path.join(censusDownloadDir, 'populationbyrace_BG_' + CENSUS_YEAR + '_acs5.csv'), index = False)
