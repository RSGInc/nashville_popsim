#######################

# Controls
# maz - total hhs
# taz - hhs by size (1, 2, 3, 4+)
# taz - hhs by type (family married, family no wife, family no husband, non-family living alone, and non-family not living alone)
# taz - hhs by workers (0, 1, 2, 3+)
# taz - hhs by income (0-25, 25-50, 50-75, 75-100, 100+)
# taz - hhs by presence of kids (no kids, one or more kids)
# taz - persons by age (0-17, 18-24, 25-34, 35-49, 50-64, 65-79, 80+(Done))
# region - total population
# tazControl['HHAGE15_44'] change to (15-34,35-49,50-64,65-79,80+)
# tazControl['HHAGE45_64']
# tazControl['HHAGE65M'] 
# TODO:
# add processing for separate crosswalk for GQs - done
# add processing of seed households and person for GQs - done
# 
######################

from __future__ import division
from tokenize import group
import pandas as pd
import os
import sys
import numpy as np
import shutil
print("Python version:", sys.version)
#read properties from parameters file

# Utility function to round values while maintaining the sum
def roundsum(values):
	return(np.diff(np.round(np.cumsum(values)), prepend=0).astype(int))

def matchprop(values, target):
	value_sum = sum(values)
	adj_factor = target/value_sum
	return(np.array(values) * adj_factor)

# Borrowed from ActivitySim
def reindex(series1, series2):
    """
    This reindexes the first series by the second series.  This is an extremely
    common operation that does not appear to  be in Pandas at this time.
    If anyone knows of an easier way to do this in Pandas, please inform the
    UrbanSim developers.

    The canonical example would be a parcel series which has an index which is
    parcel_ids and a value which you want to fetch, let's say it's land_area.
    Another dataset, let's say of buildings has a series which indicate the
    parcel_ids that the buildings are located on, but which does not have
    land_area.  If you pass parcels.land_area as the first series and
    buildings.parcel_id as the second series, this function returns a series
    which is indexed by buildings and has land_area as values and can be
    added to the buildings dataset.

    In short, this is a join on to a different table using a foreign key
    stored in the current table, but with only one attribute rather than
    for a full dataset.

    This is very similar to the pandas "loc" function or "reindex" function,
    but neither of those functions return the series indexed on the current
    table.  In both of those cases, the series would be indexed on the foreign
    table and would require a second step to change the index.

    Parameters
    ----------
    series1, series2 : pandas.Series

    Returns
    -------
    reindexed : pandas.Series

    """

    result = series1.reindex(series2)
    try:
        result.index = series2.index
    except AttributeError:
        pass
    return result

def target_round(weights, target_sum=None):
    """
    Round weights while ensuring (as far as possible that result sums to target_sum)

    Parameters
    ----------
    weights : numpy.ndarray(float)
    target_sum : int

    Returns
    -------
    rounded_weights : numpy.ndarray array of ints
    """

    if target_sum is None:
        target_sum = round(weights.sum())

    assert target_sum == int(target_sum)

    target_sum = int(target_sum)

    # integer part of numbers to round (astype both copies and coerces)
    rounded_weights = weights.astype(int)
    resid_weights = weights % 1.0

    # find number of residuals that we need to round up
    int_shortfall = target_sum - rounded_weights.sum()

    # clip to feasible, in case target was not achievable by rounding
    int_shortfall = np.clip(int_shortfall, 0, len(resid_weights))

    # Order the residual weights and round at the tipping point where target_sum is achieved
    if int_shortfall > 0:
        # indices of the int_shortfall highest resid_weights
        i = np.argsort(resid_weights)[-int_shortfall:]

        # add 1 to the integer weights that we want to round upwards
        rounded_weights[i] += 1

    assert rounded_weights.sum() == target_sum

    return rounded_weights


# parameters_file = sys.argv[1]
parameters_file=os.path.join(os.getcwd(),'Data', 'parameters.csv')
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']
WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'Data')
censusDownloadDir = os.path.join(WORKING_DIR, 'Data','Census','Downloads')

# USER_DIR = parameters[parameters.Key == 'USER_DIR']['Value'].item().strip(' ')
USER_DIR = os.path.join(os.getcwd(),'Data', 'USER')

# nashville_pop = 1894704  
# William: where is this from. placeholder B01001 20121 5-yr, use data from GNRC later.
# Aditya 12142023 - Updated to a value of 1875620 from county forecasts

#set region specific settings
STATE_FIPS = 47 # 47 for Tennessee
COUNTY_FIPS_STRING = ["149","165","187","147","189","037","119"]
COUNTY_NAMES = ["Rutherford","Sumner","Williamson","Robertson","Wilson","Davidson","Maury"]
COUNTY_DF = pd.DataFrame({'COUNTY':COUNTY_NAMES}, index=[int('47'+x) for x in COUNTY_FIPS_STRING])

###############
# PROCESS DATA
###############

# Read in the user inputs
MODEL_YEAR = '2023'
county_control_forecasts = pd.read_csv(os.path.join(USER_DIR, 'control_forecasts.csv'))
# Ensure that all the zone names match the hard-coded COUNTY_NAMES list
assert all(county_name in county_control_forecasts.County.unique() for county_name in COUNTY_NAMES), 'Mismatch in county names'

# read Census data from Census download directory
hhsize_BG   = pd.read_csv(os.path.join(censusDownloadDir, "hhsize_BG_2021_acs5.csv"))
hhtype_BG   = pd.read_csv(os.path.join(censusDownloadDir, "hhtype_BG_2021_acs5.csv"))
hhtenure_BG = pd.read_csv(os.path.join(censusDownloadDir, "hhtenure_BG_2021_acs5.csv"))
hhunit_CT   = pd.read_csv(os.path.join(censusDownloadDir, "hhunit_CT_2021_acs5.csv"))
hhworker_CT = pd.read_csv(os.path.join(censusDownloadDir, "hhworker_CT_2021_acs5.csv"))
hhincome_CT = pd.read_csv(os.path.join(censusDownloadDir, "hhincome_CT_2021_acs5.csv"))
hhkids_CT  	= pd.read_csv(os.path.join(censusDownloadDir, "hh_kids_nokids_CT_2021_acs5.csv"))
pop_CT 		= pd.read_csv(os.path.join(censusDownloadDir,"populationbyageandsex_CT_2021_acs5.csv"))
gqtot_acs_CT	= pd.read_csv(os.path.join(censusDownloadDir, "gqtot_CT_2021_acs5.csv"))
#gqage_sf1_CT	= pd.read_csv(os.path.join(censusDownloadDir, "gqage_CT_2010_sf1.csv"))

gqtot_acs_CT['gq_total'] = gqtot_acs_CT['B26001_001E']
print("GWTOT:", gqtot_acs_CT.gq_total.sum(), sep=' ')

# Total households from County Forecasts
totHHs = county_control_forecasts.loc[county_control_forecasts.Target=='num_hh',MODEL_YEAR].sum()
totHHs_county = county_control_forecasts.loc[county_control_forecasts.Target=='num_hh',['County',MODEL_YEAR]].rename(columns={MODEL_YEAR:'HH'}).set_index('County')

totPOP_county = county_control_forecasts.loc[county_control_forecasts.Target=='tot_pop',['County',MODEL_YEAR]].rename(columns={MODEL_YEAR:'POP'}).set_index('County')

# read maz hh data
mazControl = pd.read_csv(os.path.join(USER_DIR,"control_totals_maz.csv"))
print(mazControl.columns, sep=' ')

# Read XWalk
popsyn_xwalk = pd.read_csv(os.path.join(WORKING_DIR, "Setup","data","geo_crosswalks.csv"))
popsyn_xwalk['PUMA10'] = popsyn_xwalk['PUMA'].astype(str)
popsyn_xwalk['PUMA10'] = popsyn_xwalk['PUMA10'].map(lambda n: int(n[-5:]))

# Update the mazControl to match county forecasts
# # Generate county factors
maz_county_control = pd.merge(mazControl, popsyn_xwalk[['MAZ','COUNTY']],on='MAZ')
maz_county_control['COUNTYNAME'] = reindex(COUNTY_DF.COUNTY, maz_county_control.COUNTY)
maz_county_control['KEEP1HH'] = maz_county_control.hh==1
totHHs_county['MAZHH'] = maz_county_control[~maz_county_control.KEEP1HH].groupby('COUNTYNAME', group_keys=True)['hh'].sum()
totHHs_county['REMOVEMAZHH'] = maz_county_control[maz_county_control.KEEP1HH].groupby('COUNTYNAME', group_keys=True)['hh'].sum()
totHHs_county['ADJFACTORHH'] = (totHHs_county['HH']-totHHs_county['REMOVEMAZHH'])/totHHs_county['MAZHH']
# # Join the adjustment factor to maz controls
# # Make sure that mazs with 1 househods don't get dropped
maz_county_control['ADJFACTORHH'] = reindex(totHHs_county.ADJFACTORHH, maz_county_control.COUNTYNAME)
maz_county_control.loc[maz_county_control.KEEP1HH,'ADJFACTORHH'] = 1
maz_county_control['HHADJ'] = maz_county_control['hh'] * maz_county_control['ADJFACTORHH']
for key, _df in maz_county_control[~maz_county_control.KEEP1HH].groupby('COUNTYNAME'):
	# _df = _df.sort_values('HHADJ', ascending=False)
	add_hhadj = reindex(pd.Series(roundsum(_df.HHADJ.values), index=_df.MAZ), mazControl[mazControl.MAZ.isin(_df.MAZ)].MAZ)
	print(_df[(_df.HHADJ > 0 ) & (roundsum(_df.HHADJ)<1)])
	mazControl.loc[add_hhadj.index, 'HHADJ'] = add_hhadj

mazControl.loc[mazControl.hh==1,'HHADJ'] = 1
# mazControl['HHADJ'] = reindex(maz_county_control.groupby('COUNTYNAME', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'HH':roundsum(_df.hh * _df.ADJFACTORHH)}, index=_df.MAZ)).droplevel(0),
# 	mazControl.MAZ)

assert mazControl.HHADJ.sum()==totHHs, 'The MAZ controls are not the same as county forecasts'

mazControl.to_csv('Setup/logs/maz_control_county_adjustments.csv', index=False)
mazControl['hh'] = mazControl['HHADJ']
mazControl.drop(columns=['HHADJ'], inplace=True)

# Read MAZ-BLK_GRP crosswalk
MAZ_BG15 = popsyn_xwalk[["MAZ","BLOCKGRP","TAZ","PUMA","COUNTY"]]

# Read TAZ-CT XWalk
TAZ_CT10 = popsyn_xwalk[["TAZ","TRACT"]]

# **************************
# Create TAZ control file
# **************************
#Load Census Data
hhsize_BG = hhsize_BG.rename(columns = {'BKGPIDFP10':'BLOCKGRP'}).drop(columns=['state','county','tract'], errors='ignore')
hhtype_BG = hhtype_BG.rename(columns = {'BKGPIDFP10':'BLOCKGRP'}).drop(columns=['state','county','tract'], errors='ignore')
hhtenure_BG = hhtenure_BG.rename(columns = {'BKGPIDFP10':'BLOCKGRP'}).drop(columns=['state','county','tract'], errors='ignore')
hhunit_CT = hhunit_CT.rename(columns = {'CTIDFP10':'TRACT'}).drop(columns=['state','county','tract'], errors='ignore')
hhworker_CT = hhworker_CT.rename(columns = {'CTIDFP10':'TRACT'}).drop(columns=['state','county','tract'], errors='ignore')
hhincome_CT = hhincome_CT.rename(columns = {'CTIDFP10':'TRACT'}).drop(columns=['state','county','tract'], errors='ignore')
hhkids_CT = hhkids_CT.rename(columns = {'CTIDFP10':'TRACT'}).drop(columns=['state','county','tract'], errors='ignore')
pop_CT = pop_CT.rename(columns = {'CTIDFP10':'TRACT'}).drop(columns=['state','county','tract'], errors='ignore')
gqtot_acs_CT = gqtot_acs_CT.rename(columns = {'CTIDFP10':'TRACT'}).drop(columns=['state','county','tract'], errors='ignore')

TAZ_xwalk = pd.read_csv(os.path.join(WORKING_DIR,"Setup","data","geo_crosswalks.csv"))

# Check if block group and tract match
check_bg_hh = pd.merge(hhsize_BG[['BLOCKGRP', 'B11016_001E']], TAZ_xwalk[['BLOCKGRP', 'TRACT']].drop_duplicates(), on='BLOCKGRP', how='left').\
	groupby('TRACT', as_index=False)['B11016_001E'].sum()
check_bg_hh['TRACT_hh'] = reindex(hhworker_CT[['B08202_001E', 'TRACT']].set_index('TRACT'), check_bg_hh.TRACT)

assert check_bg_hh[check_bg_hh.TRACT_hh!=check_bg_hh.B11016_001E].shape[0] == 0, "There's a mismatch in blockgroup and census data"


#Sum HH per TAZ, BG, CT
mazControl = mazControl[['MAZ','hh','GQ']].merge(TAZ_xwalk, how='left', on='MAZ')
mazControl['COUNTYNAME'] = reindex(COUNTY_DF.COUNTY, mazControl.COUNTY)

# Make blockgroup level adjustments to MAZ
bg_hh = mazControl.groupby(['BLOCKGRP', 'TRACT', 'COUNTY'], as_index = False)['hh'].sum()
bg_hh = pd.merge(bg_hh, hhsize_BG[['B11016_001E', 'BLOCKGRP']], on ='BLOCKGRP', how='left')
zero_bg = bg_hh[(bg_hh.hh>0 ) & (bg_hh.B11016_001E==0)].BLOCKGRP
add_hh_ct = bg_hh[(bg_hh.hh>0 ) & (bg_hh.B11016_001E==0)].groupby('TRACT')['hh'].sum()
mazControl['hh_old'] = mazControl['hh']
mazControl.loc[mazControl.BLOCKGRP.isin(zero_bg),'hh'] = 0
mazControl['addprop'] = mazControl['hh']/mazControl.groupby('TRACT')['hh'].transform(sum)
mazControl = pd.merge(mazControl, add_hh_ct, on='TRACT', how='left', suffixes=['','_add']).fillna(0)
mazControl['hh_adj'] = mazControl['hh']+(mazControl['addprop']*mazControl['hh_add'])
mazControl['hh_adj'] = mazControl.groupby('TRACT', group_keys=True).apply(lambda _df: pd.DataFrame({'hh_adj':roundsum(_df.hh_adj)}, index=_df.index)).droplevel(0)
mazControl = mazControl.drop(columns=['addprop', 'hh_add'])
# Check if the adjustments matches the original
mazControl['hh'] = mazControl['hh_old']
print('Original Total: ', mazControl.hh.sum(), " New Total: ", mazControl.hh_adj.sum())
# Add missing totals
bg_hh = mazControl.groupby(['TRACT', 'COUNTY'], as_index = False)[['hh', 'hh_adj']].sum()
zero_ct = bg_hh[(bg_hh.hh != bg_hh.hh_adj)].TRACT
add_hh_cty = bg_hh[(bg_hh.hh != bg_hh.hh_adj)].groupby('COUNTY')['hh'].sum().to_frame()
mazControl['hh'] = mazControl['hh_adj']
mazControl.loc[mazControl.TRACT.isin(zero_ct),'hh'] = 0
mazControl['addprop'] = mazControl['hh']/mazControl.groupby('COUNTY')['hh'].transform(sum)
mazControl = pd.merge(mazControl, add_hh_cty, on='COUNTY', how='left', suffixes=['','_add']).fillna(0)
mazControl['hh_adj'] = mazControl['hh']+(mazControl['addprop']*mazControl['hh_add'])
mazControl['hh_adj'] = mazControl.groupby('COUNTY', group_keys=True).apply(lambda _df: pd.DataFrame({'hh_adj':roundsum(_df.hh_adj)}, index=_df.index)).droplevel(0)
# Check if the adjustments matches the original
mazControl['hh'] = mazControl['hh_old']
print('Original Total: ', mazControl.hh.sum(), " New Total: ", mazControl.hh_adj.sum())

# Clean MAZ Controls
mazControl[(mazControl.hh_old!=mazControl.hh_adj)].drop(columns=['hh_add', 'addprop']).to_csv('Setup/logs/maz_control_shift_households.csv', index=False)
mazControl['hh'] = mazControl['hh_adj'].astype(int)
mazControl = mazControl[['MAZ','hh','GQ']]
mazControl.to_csv(os.path.join(outputDir, "control_totals_maz.csv"), index = False)
mazControl = mazControl[['MAZ','hh','GQ']].merge(TAZ_xwalk, how='left', on='MAZ')

# Create TAZ Controls
tazControl = mazControl.groupby('TAZ', as_index = False).agg({'hh':'sum','GQ':'sum','BLOCKGRP':'first','TRACT':'first','PUMA':'first','COUNTY':'first'})
print("CHECK MAZ/TAZ: ", mazControl.hh.sum(),tazControl.hh.sum(), sep=' ')
bg_hh = mazControl.groupby('BLOCKGRP', as_index = False)['hh'].sum()
ct_hh = mazControl.groupby('TRACT', as_index = False)['hh'].sum()

# Calculate tract census adjusment factors
ct_hh = pd.merge(ct_hh, hhworker_CT[['B08202_001E', 'TRACT']], on ='TRACT', how='left')
ct_hh['CENSUSADJFACTOR'] = np.where(ct_hh['B08202_001E']>0, ct_hh['hh']/ct_hh['B08202_001E'], 0)
ct_hh = ct_hh.drop(columns=['B08202_001E'])

# Calculate block group census adjustment factors
bg_hh = pd.merge(bg_hh, hhsize_BG[['B11016_001E', 'BLOCKGRP']], on ='BLOCKGRP', how='left')
bg_hh['CENSUSADJFACTOR'] = np.where(bg_hh['B11016_001E']>0, bg_hh['hh']/bg_hh['B11016_001E'], 0)
bg_hh = bg_hh.drop(columns=['B11016_001E'])



#get factors to scale by BG, CT
tazControl = tazControl.\
	merge(bg_hh, how = 'left', on = 'BLOCKGRP', suffixes = ['','_BG']).\
	merge(ct_hh, how = 'left', on = 'TRACT', suffixes = ['','_CT'])
mazControl = mazControl.\
	merge(bg_hh, how = 'left', on = 'BLOCKGRP', suffixes = ['','_BG']).\
	merge(ct_hh, how = 'left', on = 'TRACT', suffixes = ['','_CT'])
mazControl['mz_bg_pct'] = np.where(mazControl['hh_BG'] > 0, mazControl['hh']/mazControl['hh_BG'], 0)
tazControl['bg_pct'] = np.where(tazControl['hh_BG'] > 0, tazControl['hh']/tazControl['hh_BG'], 0)
tazControl['ct_pct'] = np.where(tazControl['hh_CT'] > 0, tazControl['hh']/tazControl['hh_CT'], 0)
mazControl['mz_bg_pct'] = mazControl['mz_bg_pct'] * mazControl['CENSUSADJFACTOR']
tazControl['bg_pct'] = tazControl['bg_pct'] * tazControl['CENSUSADJFACTOR'] 
tazControl['ct_pct'] = tazControl['ct_pct'] * tazControl['CENSUSADJFACTOR_CT']
# tazControl['ct_pct_adj'] = tazControl.groupby('BLOCKGRP', as_index=False)[['ct_pct', 'BGADJFACTOR']].\
# 	apply(lambda _df: pd.DataFrame({'ct_pct':matchprop(_df.ct_pct.values, _df.BGADJFACTOR.unique())}, index=_df.index)).\
# 		droplevel(0)

# print("CHECK Block group for hhsize" , hhsize_BG[['BLOCKGRP','B11016_010E']].head(), tazControl[['BLOCKGRP']].head(), sep=' ')

#Merge Census data to TAZ file
tazControl = tazControl.merge(hhsize_BG, how = 'left', on = 'BLOCKGRP').\
	merge(hhtenure_BG, how = 'left', on = 'BLOCKGRP').\
	merge(hhtype_BG, how = 'left', on = 'BLOCKGRP')

tazControl['HHSIZE1'] = tazControl['bg_pct']*tazControl['B11016_010E']
tazControl['HHSIZE2'] = tazControl['bg_pct']*(tazControl['B11016_003E'] + tazControl['B11016_011E'])
tazControl['HHSIZE3'] = tazControl['bg_pct']*(tazControl['B11016_004E'] + tazControl['B11016_012E'])
tazControl['HHSIZE4M'] = tazControl['bg_pct']*(tazControl['B11016_005E'] + tazControl['B11016_006E'] + tazControl['B11016_007E'] + tazControl['B11016_008E'] + tazControl['B11016_013E'] + tazControl['B11016_014E'] + tazControl['B11016_015E'] + tazControl['B11016_016E'])
tazControl['HHAGE15_44'] = tazControl['bg_pct']*(tazControl['B25007_003E'] + tazControl['B25007_004E'] + tazControl['B25007_005E'] + tazControl['B25007_013E'] + tazControl['B25007_014E'] + tazControl['B25007_015E']) 
tazControl['HHAGE45_64'] = tazControl['bg_pct']*(tazControl['B25007_006E'] + tazControl['B25007_007E'] + tazControl['B25007_008E'] + tazControl['B25007_016E'] + tazControl['B25007_017E'] + tazControl['B25007_018E']) 
tazControl['HHAGE65M'] = tazControl['bg_pct']*(tazControl['B25007_009E'] + tazControl['B25007_010E'] + tazControl['B25007_011E'] + tazControl['B25007_019E'] + tazControl['B25007_020E'] + tazControl['B25007_021E']) 
tazControl['HHFAMMAR'] = tazControl['bg_pct']*tazControl['B11001_003E']
tazControl['HHFAMNOWIFE'] = tazControl['bg_pct']*tazControl['B11001_005E']
tazControl['HHFAMNOHUSBAND'] = tazControl['bg_pct']*tazControl['B11001_006E']
tazControl['HHFAMNON'] = tazControl['bg_pct']*tazControl['B11001_008E'] #non-family living alone
tazControl['HHFAMNONALONE'] = tazControl['bg_pct']*tazControl['B11001_009E'] #non-family not living alone
# tazControl['hhsize_tot_bg'] = tazControl['HHSIZE1'] + tazControl['HHSIZE2'] + tazControl['HHSIZE3'] + tazControl['HHSIZE4M']
# tazControl['hhfam_tot_bg'] = tazControl['HHFAMMAR'] + tazControl['HHFAMNOWIFE'] + tazControl['HHFAMNOHUSBAND'] + tazControl['HHFAMNON'] + tazControl['HHFAMNONALONE']
# tazControl['bg_size_factor'] = np.where(tazControl['hhsize_tot_bg'] > 0, tazControl['hh']/tazControl['hhsize_tot_bg'], 0)
# tazControl['bg_fam_factor'] = np.where(tazControl['hhfam_tot_bg'] > 0, tazControl['hh']/tazControl['hhfam_tot_bg'], 0)
# tazControl['hhAGE_tot_bg'] = tazControl['HHAGE15_44'] + tazControl['HHAGE45_64'] + tazControl['HHAGE65M']
# tazControl['bg_AGE_factor'] = np.where(tazControl['hhAGE_tot_bg'] > 0, tazControl['hh']/tazControl['hhAGE_tot_bg'], 0)

# for col in ['HHSIZE{}'.format(n) for n in ['1','2','3','4M']]:
# 	tazControl[col] = tazControl[col] * tazControl['bg_size_factor']
# for col in ['HHFAM{}'.format(n) for n in ['MAR','NOWIFE','NOHUSBAND','NON','NONALONE']]:
# 	tazControl[col] = tazControl[col] * tazControl['bg_fam_factor']	
# for col in ['HHAGE{}'.format(n) for n in ['15_44','45_64','65M']]:
# 	tazControl[col] = tazControl[col] * tazControl['bg_AGE_factor']

# Check the following blockgrups as the MAZ data don't match the census
#               HHSIZE1  HHSIZE2  HHSIZE3  HHSIZE4M    hh  HHSIZE_ALL
# BLOCKGRP
# 470370130021      0.0      0.0      0.0       0.0   616         0.0
# 470370156134      0.0      0.0      0.0       0.0  1732         0.0
# 470370165002      0.0      0.0      0.0       0.0  3442         0.0

# Zero out the maz controls for these blockgrups and tracts
# mazControl.loc[mazControl.BLOCKGRP.isin([470370130021, 470370156134, 470370165002]), 'hh'] = 0
# tazControl.loc[tazControl.BLOCKGRP.isin([470370130021, 470370156134, 470370165002]), 'hh'] = 0

# Rounding to make sure row sums match
tazControl[['HHSIZE1', 'HHSIZE2','HHSIZE3','HHSIZE4M']] = \
	pd.DataFrame(tazControl[['HHSIZE1', 'HHSIZE2','HHSIZE3','HHSIZE4M']].\
		apply(roundsum, axis=1).tolist(), columns=['HHSIZE1', 'HHSIZE2','HHSIZE3','HHSIZE4M'], index=tazControl.index)
# Rounding to make sure row sums match
tazControl[['HHAGE15_44', 'HHAGE45_64','HHAGE65M']] = \
	pd.DataFrame(tazControl[['HHAGE15_44', 'HHAGE45_64','HHAGE65M']].\
		apply(roundsum, axis=1).tolist(), columns=['HHAGE15_44', 'HHAGE45_64','HHAGE65M'], index=tazControl.index)
# Rounding to make sure row sums match
tazControl[['HHFAMMAR','HHFAMNOWIFE','HHFAMNOHUSBAND', 'HHFAMNON', 'HHFAMNONALONE']] = \
	pd.DataFrame(tazControl[['HHFAMMAR','HHFAMNOWIFE','HHFAMNOHUSBAND', 'HHFAMNON', 'HHFAMNONALONE']].\
		apply(roundsum, axis=1).tolist(), columns=['HHFAMMAR','HHFAMNOWIFE','HHFAMNOHUSBAND', 'HHFAMNON', 'HHFAMNONALONE'], index=tazControl.index)

print("CHECK TOT HH by Size", tazControl['hh'].sum(), " = ", tazControl[['HHSIZE1', 'HHSIZE2','HHSIZE3','HHSIZE4M']].sum().sum(), sep=' ')
print("CHECK TOT HH by AGE", tazControl['hh'].sum(), " = ", tazControl[['HHAGE15_44', 'HHAGE45_64','HHAGE65M']].sum().sum(), sep=' ')
print("CHECK TOT HH by FAM", tazControl['hh'].sum(), " = ", tazControl[['HHFAMMAR','HHFAMNOWIFE','HHFAMNOHUSBAND', 'HHFAMNON', 'HHFAMNONALONE']].sum().sum(), sep=' ')

# worker , kid, and income data
# print(hhworker_CT.columns)

tazControl = tazControl.merge(hhworker_CT, how = 'left', on = "TRACT").\
	merge(hhincome_CT, how = 'left', on = "TRACT").\
		merge(hhkids_CT, how = 'left', on = 'TRACT').\
			merge(hhunit_CT, how='left', on='TRACT')
tazControl['HHWRKS0'] = tazControl['ct_pct']*tazControl['B08202_002E']                 # allocatE'] to E']ach MAZ from tazControl['Block group tazControl['BasE']d on proportion
tazControl['HHWRKS1'] = tazControl['ct_pct']*tazControl['B08202_003E']
tazControl['HHWRKS2'] = tazControl['ct_pct']*tazControl['B08202_004E'] 
tazControl['HHWRKS3M'] = tazControl['ct_pct']*tazControl['B08202_005E'] 
tazControl['HHINC0TO20K'] = tazControl['ct_pct']*(tazControl['B19001_002E'] + tazControl['B19001_003E'] + tazControl['B19001_004E'])
tazControl['HHINC20TO30K'] = tazControl['ct_pct']*(tazControl['B19001_005E'] + tazControl['B19001_006E'])
tazControl['HHINC30TO45K'] = tazControl['ct_pct']*(tazControl['B19001_007E'] + tazControl['B19001_008E'] + tazControl['B19001_009E'])
tazControl['HHINC45TO75K'] = tazControl['ct_pct']*(tazControl['B19001_010E'] + tazControl['B19001_011E']+tazControl['B19001_012E'])
tazControl['HHINC75TO100K'] = tazControl['ct_pct']*(tazControl['B19001_013E'])
tazControl['HHINC100KM'] = tazControl['ct_pct']*(tazControl['B19001_014E'] + tazControl['B19001_015E'] + tazControl['B19001_016E'] + tazControl['B19001_017E'])
tazControl['HH_WIKID']  = tazControl['ct_pct']*(tazControl['B25012_003E'] + tazControl['B25012_011E'])
tazControl['HH_WOKID']  = tazControl['ct_pct']*(tazControl['B25012_009E'] + tazControl['B25012_017E'])
tazControl['HHUNITSINGLE'] = tazControl['ct_pct']*(tazControl['B11011_004E']+tazControl['B11011_009E']+tazControl['B11011_013E']+tazControl['B11011_017E'])
tazControl['HHUNITMULTI'] = tazControl['ct_pct']*(tazControl['B11011_005E']+tazControl['B11011_010E']+tazControl['B11011_014E']+tazControl['B11011_018E'])
tazControl['HHUNITMOBILE'] = tazControl['ct_pct']*(tazControl['B11011_006E']+tazControl['B11011_011E']+tazControl['B11011_015E']+tazControl['B11011_019E'])

# #scale to MAZ totals
# tazControl['hhwrk_tot_ct'] = tazControl['HHWRKS0']+tazControl['HHWRKS1'] + tazControl['HHWRKS2'] + tazControl['HHWRKS3M']
# tazControl['ct_wrk_factor'] = np.where(tazControl['hhwrk_tot_ct'] > 0, tazControl['hh']/tazControl['hhwrk_tot_ct'], 0)
# tazControl['hhinc_tot_ct'] = tazControl['HHINC0TO25K'] + tazControl['HHINC25TO50K'] + tazControl['HHINC50TO75K'] + tazControl['HHINC75TO100K'] + tazControl['HHINC100KM']
# tazControl['ct_inc_factor'] = np.where(tazControl['hhinc_tot_ct'] > 0, tazControl['hh']/tazControl['hhinc_tot_ct'], 0)
# tazControl['hhkid_tot_ct'] = tazControl['HH_WIKID'] + tazControl['HH_WOKID']
# tazControl['ct_kid_factor'] = np.where(tazControl['hhkid_tot_ct'] > 0, tazControl['hh']/tazControl['hhkid_tot_ct'], 0)
# tazControl['hhunit_tot_ct'] = tazControl['HHUNITSINGLE'] + tazControl['HHUNITMULTI'] + tazControl['HHUNITMOBILE']
# tazControl['ct_unit_factor'] = np.where(tazControl['hhunit_tot_ct'] > 0, tazControl['hh']/tazControl['hhunit_tot_ct'], 0)

# for col in ['HHWRKS{}'.format(n) for n in ['0','1','2','3M']]:
# 	tazControl[col] = tazControl[col] * tazControl['ct_wrk_factor']
# for col in ['HHINC{}'.format(n) for n in ['0TO25K','25TO50K','50TO75K','75TO100K','100KM']]:
# 	tazControl[col]  = tazControl[col]*tazControl['ct_inc_factor']
# for col in ['HH_WIKID','HH_WOKID']:
# 	tazControl[col]  = tazControl[col]*tazControl['ct_kid_factor']
# for col in ['HHUNIT{}'.format(n) for n in ['SINGLE','MULTI','MOBILE']]:
# 	tazControl[col] = tazControl[col] * tazControl['ct_unit_factor']	


# Rounding to make sure row sums match
tazControl[['HHWRKS0','HHWRKS1', 'HHWRKS2','HHWRKS3M']] = \
	pd.DataFrame(tazControl[['HHWRKS0','HHWRKS1', 'HHWRKS2','HHWRKS3M']].\
		apply(roundsum, axis=1).tolist(), columns=['HHWRKS0','HHWRKS1', 'HHWRKS2','HHWRKS3M'], index=tazControl.index)

		
# Rounding to make sure row sums match
tazControl[['HHINC0TO20K', 'HHINC20TO30K', 'HHINC30TO45K', 'HHINC45TO75K', 'HHINC75TO100K', 'HHINC100KM']] = \
	pd.DataFrame(tazControl[['HHINC0TO20K', 'HHINC20TO30K', 'HHINC30TO45K', 'HHINC45TO75K', 'HHINC75TO100K', 'HHINC100KM']].\
		apply(roundsum, axis=1).tolist(), columns=['HHINC0TO20K', 'HHINC20TO30K', 'HHINC30TO45K', 'HHINC45TO75K', 'HHINC75TO100K', 'HHINC100KM'], index=tazControl.index)

		
# Rounding to make sure row sums match
tazControl[['HHUNITSINGLE','HHUNITMULTI','HHUNITMOBILE']] = \
	pd.DataFrame(tazControl[['HHUNITSINGLE','HHUNITMULTI','HHUNITMOBILE']].\
		apply(roundsum, axis=1).tolist(), columns=['HHUNITSINGLE','HHUNITMULTI','HHUNITMOBILE'], index=tazControl.index)


print("CHECK TOT HHWRK", tazControl['hh'].sum(), " = ", tazControl[['HHWRKS0','HHWRKS1', 'HHWRKS2','HHWRKS3M']].sum().sum())
print("CHECK TOT HHINC", tazControl['hh'].sum(), " = ", tazControl[['HHINC0TO20K', 'HHINC20TO30K', 'HHINC30TO45K', 'HHINC45TO75K', 'HHINC75TO100K', 'HHINC100KM']].sum().sum())
print("CHECK TOT HHUNIT", tazControl['hh'].sum(), " = ", tazControl[['HHUNITSINGLE','HHUNITMULTI','HHUNITMOBILE']].sum().sum())


hh_age_inc_work = ['HHSIZE{}'.format(l) for l in ['1','2','3','4M']] + \
	['HHAGE{}'.format(n) for n in ['15_44','45_64','65M']] + \
		['HHFAM{}'.format(n) for n in ['MAR','NOWIFE','NOHUSBAND','NON','NONALONE']] + \
			['HHWRKS{}'.format(n) for n in ['0','1','2','3M']] + \
				['HHINC{}'.format(n) for n in ['0TO20K', '20TO30K','30TO45K','45TO75K','75TO100K','100KM']]+\
					['HH_WIKID','HH_WOKID']+ \
						['HHUNIT{}'.format(n) for n in ['SINGLE','MULTI','MOBILE']]

#round values to integers	
tazControl = tazControl.fillna(0)	
tazControl[[col+'_S3' for col in hh_age_inc_work]] = tazControl[hh_age_inc_work].astype(int)
tazControl['diff_round_0'] = tazControl['hh']-(tazControl['HHSIZE1_S3'] + tazControl['HHSIZE2_S3'] + tazControl['HHSIZE3_S3'] + tazControl['HHSIZE4M_S3'] )
tazControl['diff_round_11'] = tazControl['hh']-(tazControl['HHFAMMAR_S3'] + tazControl['HHFAMNOWIFE_S3'] + tazControl['HHFAMNOHUSBAND_S3'] + tazControl['HHFAMNON_S3'] + tazControl['HHFAMNONALONE_S3'])
tazControl['diff_round_1'] =tazControl['hh'] - (tazControl['HHWRKS0_S3'] + tazControl['HHWRKS1_S3'] + tazControl['HHWRKS2_S3'] + tazControl['HHWRKS3M_S3'])
tazControl['diff_round_00'] =tazControl['hh'] - (tazControl['HHAGE15_44_S3'] + tazControl['HHAGE45_64_S3'] + tazControl['HHAGE65M_S3'])
tazControl['diff_round_10'] = tazControl['hh'] - (tazControl['HHINC0TO20K_S3'] + tazControl['HHINC20TO30K_S3'] + tazControl['HHINC30TO45K_S3'] + tazControl['HHINC45TO75K_S3'] +tazControl['HHINC75TO100K_S3'] + tazControl['HHINC100KM_S3'])
tazControl['diff_round_000'] = tazControl['hh'] - (tazControl['HH_WIKID_S3'] + tazControl['HH_WOKID_S3'] )
tazControl['diff_round_20'] = tazControl['hh'] - (tazControl['HHUNITSINGLE_S3'] + tazControl['HHUNITMULTI_S3'] + tazControl['HHUNITMOBILE_S3'])

hhsize_list = ['HHSIZE{}_S3'.format(l) for l in ['1','2','3','4M']]
hhage_list = ['HHAGE{}_S3'.format(n) for n in ['15_44','45_64','65M']]
hhfam_list = ['HHFAM{}_S3'.format(n) for n in ['MAR','NOWIFE','NOHUSBAND','NON','NONALONE']]
hhworks_list = ['HHWRKS{}_S3'.format(n) for n in ['0','1','2','3M']]
hhinc_list = ['HHINC{}_S3'.format(n) for n in ['0TO20K', '20TO30K','30TO45K','45TO75K','75TO100K','100KM']]
hhunit_list = ['HHUNIT{}_S3'.format(n) for n in ['SINGLE','MULTI','MOBILE']]

#add diff to max of each hh type for income and size
for idx1,listie in enumerate([hhinc_list, hhfam_list]):
	for idx,col in enumerate(listie):
		col_list = [c for c in listie]
		del col_list[idx]
		tazControl[col] = np.where((tazControl[col] >= tazControl[col_list[0]]) & (tazControl[col] >= tazControl[col_list[1]]) & (tazControl[col] >= tazControl[col_list[2]]) & (tazControl[col] >= tazControl[col_list[3]]), tazControl[col]+tazControl['diff_round_1{}'.format(idx1)], tazControl[col])


for idx1,listie in enumerate([hhsize_list, hhworks_list]):
	for idx,col in enumerate(listie):
		col_list = [c for c in listie]
		del col_list[idx]
		tazControl[col] = np.where((tazControl[col] >= tazControl[col_list[0]]) & (tazControl[col] >= tazControl[col_list[1]]) & (tazControl[col] >= tazControl[col_list[2]]), tazControl[col]+tazControl['diff_round_{}'.format(idx1)], tazControl[col])

for idx1,listie in enumerate([hhage_list]):
	for idx,col in enumerate(listie):
		col_list = [c for c in listie]
		del col_list[idx]
		tazControl[col] = np.where((tazControl[col] >= tazControl[col_list[0]]) & (tazControl[col] >= tazControl[col_list[1]]), tazControl[col]+tazControl['diff_round_0{}'.format(idx1)], tazControl[col])

for idx,col in enumerate(['HH_WIKID_S3','HH_WOKID_S3']):
	col_list = [c for c in ['HH_WIKID_S3','HH_WOKID_S3']]
	del col_list[idx]
	tazControl[col] = np.where((tazControl[col] >= tazControl[col_list[0]]), tazControl[col]+tazControl['diff_round_000'], tazControl[col])

for idx1,listie in enumerate([hhunit_list]):
	for idx,col in enumerate(listie):
		col_list = [c for c in listie]
		del col_list[idx]
		tazControl[col] = np.where((tazControl[col] >= tazControl[col_list[0]]) & (tazControl[col] >= tazControl[col_list[1]]), tazControl[col]+tazControl['diff_round_2{}'.format(idx1)], tazControl[col])

		
print(tazControl['diff_round_1'].describe(), sep=' ')
print("difference from rounding size", tazControl['hh'].sum() - tazControl[['HHSIZE{}_S3'.format(l) for l in ['1','2','3','4M']]].sum().sum(), sep=' ')
print("difference from rounding age", tazControl['hh'].sum() - tazControl[['HHAGE{}_S3'.format(n) for n in ['15_44','45_64','65M']]].sum().sum(), sep=' ')
print("difference from rounding wrks", tazControl['hh'].sum() - tazControl[['HHWRKS{}_S3'.format(l) for l in ['0','1','2','3M']]].sum().sum(), sep=' ')
print("difference from rounding INC", tazControl['hh'].sum() - tazControl[['HHINC{}_S3'.format(l) for l in ['0TO20K', '20TO30K','30TO45K','45TO75K','75TO100K','100KM']]].sum().sum(), sep=' ')
print("difference from rounding FAM", tazControl['hh'].sum() - tazControl[['HHFAM{}_S3'.format(l) for l in ['MAR','NOWIFE','NOHUSBAND','NON','NONALONE']]].sum().sum(), sep=' ')
print("difference from rounding KID", tazControl['hh'].sum() - tazControl[['HH_WIKID_S3','HH_WOKID_S3']].sum().sum(), sep=' ')
print("difference from rounding unit", tazControl['hh'].sum() - tazControl[['HHUNIT{}_S3'.format(n) for n in ['SINGLE','MULTI','MOBILE']]].sum().sum(), sep=' ')

tazControl = tazControl.rename(columns = {'hh':'HHS',\
	'HHAGE15_44_S3':'HHAGE1_S3','HHAGE45_64_S3':'HHAGE2_S3', 'HHAGE65M_S3':'HHAGE3_S3',\
		'HHINC0TO20K_S3':'HHINC1_S3', 'HHINC20TO30K_S3':'HHINC2_S3', 'HHINC30TO45K_S3':'HHINC3_S3','HHINC45TO75K_S3':'HHINC4_S3','HHINC75TO100K_S3':'HHINC5_S3','HHINC100KM_S3':'HHINC6_S3',\
			'HHWRKS0_S3':'HHWRK1_S3','HHWRKS1_S3':'HHWRK2_S3','HHWRKS2_S3':'HHWRK3_S3','HHWRKS3M_S3':'HHWRK4_S3'})
tazControl3 = tazControl[['ct_pct','TAZ', 'HHS', 'GQ','PUMA', 'TRACT','COUNTY', \
	'HHSIZE1_S3', 'HHSIZE2_S3', 'HHSIZE3_S3', 'HHSIZE4M_S3',\
		'HHAGE1_S3', 'HHAGE2_S3', 'HHAGE3_S3', \
			'HHWRK1_S3', 'HHWRK2_S3', 'HHWRK3_S3', 'HHWRK4_S3', \
				'HHINC1_S3', 'HHINC2_S3', 'HHINC3_S3', 'HHINC4_S3','HHINC5_S3', 'HHINC6_S3', \
					'HH_WIKID_S3', 'HH_WOKID_S3',\
						'HHFAMMAR_S3','HHFAMNOWIFE_S3','HHFAMNOHUSBAND_S3','HHFAMNON_S3','HHFAMNONALONE_S3', \
							'HHUNITSINGLE_S3','HHUNITMULTI_S3','HHUNITMOBILE_S3']]

#----------------
# Household seed File
#----------------

# read PUMS person and HH data
pums_per = pd.read_csv(os.path.join(WORKING_DIR, "Data","PUMS","Person","psam_p47.csv"), dtype={'SERIALNO': str})
pums_hh = pd.read_csv(os.path.join(WORKING_DIR, "Data","PUMS","Household","psam_h47.csv"), dtype={'SERIALNO': str})

pums_per = pums_per[pums_per['PUMA'].isin(popsyn_xwalk.PUMA10.unique())]
pums_hh = pums_hh[pums_hh['PUMA'].isin(popsyn_xwalk.PUMA10.unique())]
print("pums puma", pums_hh.PUMA.unique())
print("xwalkpuma",popsyn_xwalk.PUMA10.unique())
# remove vacant units and  group quarters households
print("len raw pums", len(pums_hh))

seed_house = pums_hh[(pums_hh.NP != 0) & (pums_hh.TYPEHUGQ.isin([1]))]	
# 'TYPE' is Type of unit in 2011-2015 data, changed to 'TYPEHUGQ' in 2017-2021 data,
print("len seed", len(seed_house))
seed_house = seed_house.fillna(0)
seed_person = pums_per[pums_per.SERIALNO.isin(seed_house.SERIALNO.unique())]
seed_person = seed_person.fillna(0)


# compute number of workers in the household	
seed_person['workers'] = np.where(seed_person.ESR.isin([1,2,4,5]), 1,0)
seed_person['workers'] = seed_person['workers'].astype(np.int64)
# seed_person['SERIALNO'] = seed_person['SERIALNO'].astype(str)
# seed_house['SERIALNO'] = seed_house['SERIALNO'].astype(str)

# Convert 'workers' column in seed_person to int (if needed)
seed_person['workers'] = seed_person['workers'].astype(int)

print("seed_person['workers'] unique values:", seed_person['workers'].unique())
print("seed_person['workers'] missing values:", seed_person['workers'].isnull().sum())
print("seed_person['SERIALNO'] NaN count:", seed_person['SERIALNO'].isnull().sum())

# Join seed_person with seed_house
seed_house = seed_house.merge(seed_person[['SERIALNO', 'workers']].groupby('SERIALNO', group_keys=True).sum(), how='left', on='SERIALNO')

# use ESR to set employment dummy	
seed_person['employed'] = np.where(seed_person.ESR.isin([1,2,4,5]), 1, 0)	

#dummy hh_id
seed_house['hh_id'] = np.arange(1,len(seed_house)+1)

new_HH_ID = seed_house[["SERIALNO","hh_id"]]

seed_person = seed_person.merge(new_HH_ID, how = 'left', on = "SERIALNO")
age_householder = seed_person[["SERIALNO","AGEP",'RELSHIPP']]
# 'RELP' is Relationship, changed to 'RELSHIPP', 20 is the head of household (reference person).
max_age = age_householder[age_householder['RELSHIPP'] == 20]#.groupby('SERIALNO', as_index = False).agg({'AGEP':'max'})
max_age = max_age.rename(columns = {'AGEP':'AGEHOH'})
seed_house = seed_house.merge(max_age, how = 'left', on = 'SERIALNO')

# put income in constant year dollars (SQL says reported income * rolling reference factor * inflation adjustment)	
  
#ADJINC Character 7
# Adjustment factor for income and earnings dollar amounts (6 implied
# decimal places)
# 1061971 .2013 factor (1.007549 * 1.05401460)
# 1045195 .2014 factor (1.008425 * 1.03646282)
# 1035988 .2015 factor (1.001264 * 1.03468042)
# 1029257 .2016 factor (1.007588 * 1.02150538)
# 1011189 .2017 factor (1.011189 * 1.00000000)
# ADJINC/1,000,000 is the final factor

seed_house['HHINCADJ'] = seed_house['ADJINC']/1000000*seed_house['HINCP']
seed_house['HHINCADJ'] = seed_house['HHINCADJ'].astype(int)

## This is remnant of R code, I think its specific to tulare
# seed_house <- seed_house  
  # mutate(PUMA_NEW=ifelse(PUMA10==10701,610701,ifelse(PUMA10==10702,610702,ifelse(PUMA10==10703,610703,PUMA10))))

# seed_house <- seed_house  
    # mutate(PUMA=PUMA_NEW)
  
# seed_person <- seed_person  
  # mutate(PUMA_NEW=ifelse(PUMA10==10701,610701,ifelse(PUMA10==10702,610702,ifelse(PUMA10==10703,610703,PUMA10))))

# seed_person <- seed_person  
  # mutate(PUMA=PUMA_NEW)
seed_person['PUMA'] = seed_person['PUMA'] +STATE_FIPS*100000
seed_house['PUMA'] = seed_house['PUMA'] +STATE_FIPS*100000
#kids
seed_house['KID'] =np.where(seed_house['HUPAC']==4,0,1)

# Confirm that all the household and person seeds are present based on SERIALNO match

print("seed_person['SERIALNO'] person missing from household:", seed_person[~seed_person.SERIALNO.isin(seed_house.SERIALNO)].shape[0])
print("seed_person['SERIALNO'] NaN count:", seed_person['SERIALNO'].isnull().sum())


#Get GQ Weights

seed_house_gq = pums_hh[(pums_hh.NP != 0) & (pums_hh.TYPEHUGQ.isin([3]))]	
seed_person_gq = pums_per[pums_per.SERIALNO.isin(seed_house_gq.SERIALNO.unique())]

gqpersons = seed_person_gq#.merge(seed_house_gq, how = 'left', on = 'SERIALNO')
#gqpersons = gqpersons[gqpersons['TYPEHUGQ'] == 3]
#gqpersons['SCHG'] = np.where(gqpersons['SCHG'].isna(), 0, gqpersons['SCHG'])
#gqpersons['MIL'] = np.where(gqpersons['MIL'].isna(), 0, gqpersons['MIL'])

gqpersons['SCHG'] = np.where(gqpersons['SCHG'].isnull(), 0, gqpersons['SCHG'])
gqpersons['MIL'] = np.where(gqpersons['MIL'].isnull(), 0, gqpersons['MIL'])

gqpersons['GQTYPE'] = np.where(gqpersons['SCHG'].isin([15,16]),1,np.where(gqpersons.MIL == 1, 2,3))
gqpersons = gqpersons.rename(columns = {'PWGTP':'GQWGTP'})
seed_house_gq = seed_house_gq.merge(gqpersons[['GQWGTP','SERIALNO','GQTYPE']], how = 'left', on = 'SERIALNO').fillna(0)


seed_house.to_csv(os.path.join(outputDir, "seed_households.csv"), index = False)
seed_person.to_csv(os.path.join(outputDir, "seed_persons.csv"), index = False )


# use this section of code on Step 01 PUMS to Database - before leaving out GQs
# Create distribution of GQ population by age for each Census Tract [run only once]
# Non-Institutional GQ population
pums_pop = pums_per.merge(pums_hh[["SERIALNO", "WGTP", "TYPEHUGQ"]],  on = "SERIALNO", how = 'left')
pums_pop = pums_pop[pums_pop.TYPEHUGQ>=2]#filter(TYPEHUGQ >= 2) %>%  # GQ population
pums_pop['age_group7'] = np.where(pums_pop.AGEP>=80, 1, 0)
pums_pop['age_group6'] = np.where((pums_pop.AGEP>=65) & (pums_pop.AGEP<=79), 1, 0)
pums_pop['age_group5'] = np.where((pums_pop.AGEP>=50) & (pums_pop.AGEP<=64), 1, 0) 
pums_pop['age_group4'] = np.where((pums_pop.AGEP>=35) & (pums_pop.AGEP<=49), 1, 0) 
pums_pop['age_group3'] = np.where((pums_pop.AGEP>=25) & (pums_pop.AGEP<=34), 1, 0) 
pums_pop['age_group2'] = np.where((pums_pop.AGEP>=18) & (pums_pop.AGEP<=24), 1, 0) 
pums_pop['age_group1'] = np.where((pums_pop.AGEP>=0 )& (pums_pop.AGEP<=17), 1, 0) 
for i in range(1,8):
	pums_pop['age_group{}'.format(i)] = pums_pop['age_group{}'.format(i)]*pums_pop['PWGTP']
pums_pop = pums_pop.groupby('PUMA', as_index = False).sum()	
for i in range(1,8):
	pums_pop['percent_age_group{}'.format(i)] = pums_pop['age_group{}'.format(i)]/(pums_pop['age_group1']+pums_pop['age_group2']+pums_pop['age_group3']+pums_pop['age_group4']+pums_pop['age_group5']+pums_pop['age_group6']+pums_pop['age_group7'])
print("pums_pop check:", pums_pop[['age_group1','percent_age_group1']].head(), sep=' ')
print("pre merge:", len(pums_pop), sep=' ')
#pums_pop = pums_pop.merge(popsyn_xwalk, how = 'left', on = 'PUMA')
#print "post merge:", len(pums_pop), len(popsyn_xwalk.TRACT.unique())

print('CHECK RECORDS')
#print pums_pop[['TRACT']+['percent_age_group{}'.format(i) for i in range(1,8)]].fillna(0).describe()#, gqtot_acs_CT.TRACT.head(),pums_pop.TRACT.head()#[['TRACT']+['percent_age_group{}'.format(i) for i in range(1,8)]].head()
pums_pop['PUMA'] = pums_pop['PUMA'].map(lambda n: n +STATE_FIPS*100000)

gqtot_acs_CT = gqtot_acs_CT.merge(popsyn_xwalk[['PUMA','TRACT']].groupby('TRACT',as_index = False).first(), how = 'left', on = 'TRACT').merge(pums_pop[['PUMA']+['percent_age_group{}'.format(i) for i in range(1,8)]].fillna(0), how = 'left', on = 'PUMA').fillna(0)
print(gqtot_acs_CT[['gq_total','percent_age_group1', 'percent_age_group2']].fillna(0).describe())
# gqtot_acs_CT = gqtot_acs_CT.merge(pums_pop[['PUMA']+['percent_age_group{}'.format(i) for i in range(1,8)]].fillna(0), how = 'left', on = 'PUMA').fillna(0)

#output GQ seed data
seed_house_GQ = seed_house_gq[seed_house_gq.GQWGTP>0]
seed_house_GQ['PUMA'] = seed_house_GQ['PUMA'] +STATE_FIPS*100000
seed_house_GQ['hh_id'] = np.arange(1,len(seed_house_GQ)+1)

new_GQ_ID = seed_house_GQ[["SERIALNO","hh_id"]]

seed_house_GQ.to_csv(os.path.join(outputDir, "seed_households_GQ.csv"), index = False)
seed_person_GQ = gqpersons.merge(new_GQ_ID, how = 'left', on = 'SERIALNO')
seed_person_GQ['PUMA'] = seed_person_GQ['PUMA'] +STATE_FIPS*100000
seed_person_GQ.to_csv(os.path.join(outputDir, "seed_persons_GQ.csv"), index = False )
#gqpersons.to_csv(os.path.join(outputDir, "seed_persons_GQ.csv"), index = False )

# tazControl4 = tazControl3.merge(ct_hh, how = 'left', left_on = "TRACT", right_on ='TRACT')
# ct_pop = tazControl3.groupby('TRACT', as_index = False)['hh'].sum()
tazControl4 = tazControl3.merge(pop_CT, how = 'left', on = 'TRACT').merge(gqtot_acs_CT, how = 'left', on = 'TRACT')
tazControl4 = tazControl4.fillna(0)
tazControl4['MALE'] = tazControl4['ct_pct']*tazControl4['B01001_002E']
tazControl4['FEMALE'] = tazControl4['ct_pct']*tazControl4['B01001_026E']
tazControl4['AGE0to17'] = (tazControl4['ct_pct']*(tazControl4['B01001_003E']+tazControl4['B01001_027E']+tazControl4['B01001_004E']+tazControl4['B01001_005E']+tazControl4['B01001_006E']+tazControl4['B01001_028E']+tazControl4['B01001_029E']+tazControl4['B01001_030E'] - (tazControl4['gq_total']*tazControl4['percent_age_group1']))).clip(lower=0)
tazControl4['AGE0to17_no_subtract'] = tazControl4['ct_pct']*(tazControl4['B01001_003E']+tazControl4['B01001_027E']+tazControl4['B01001_004E']+tazControl4['B01001_005E']+tazControl4['B01001_006E']+tazControl4['B01001_028E']+tazControl4['B01001_029E']+tazControl4['B01001_030E'])
print('AGE GROUP1',tazControl4.AGE0to17.sum(), tazControl4.AGE0to17_no_subtract.sum(), sep=' ')
print(tazControl4[['gq_total','percent_age_group1', 'percent_age_group2']].describe(), sep=' ')
tazControl4['AGE18to24'] = (tazControl4['ct_pct']*(tazControl4['B01001_007E']+tazControl4['B01001_008E']+tazControl4['B01001_009E']+tazControl4['B01001_010E']+tazControl4['B01001_031E']+tazControl4['B01001_032E']+tazControl4['B01001_033E']+tazControl4['B01001_034E']- (tazControl4['gq_total']*tazControl4['percent_age_group2']))).clip(lower=0)
tazControl4['AGE18to24_no_subtract'] = tazControl4['ct_pct']*(tazControl4['B01001_007E']+tazControl4['B01001_008E']+tazControl4['B01001_009E']+tazControl4['B01001_010E']+tazControl4['B01001_031E']+tazControl4['B01001_032E']+tazControl4['B01001_033E']+tazControl4['B01001_034E'])
tazControl4['AGE25to34'] = tazControl4['ct_pct']*(tazControl4['B01001_011E']+tazControl4['B01001_012E']+tazControl4['B01001_035E']+tazControl4['B01001_036E']- (tazControl4['gq_total']*tazControl4['percent_age_group3']))
tazControl4['AGE35to49'] = tazControl4['ct_pct']*(tazControl4['B01001_013E']+tazControl4['B01001_014E']+tazControl4['B01001_015E']+tazControl4['B01001_037E']+tazControl4['B01001_038E']+tazControl4['B01001_039E']- (tazControl4['gq_total']*tazControl4['percent_age_group4']))
tazControl4['AGE50to64'] = tazControl4['ct_pct']*(tazControl4['B01001_016E']+tazControl4['B01001_017E']+tazControl4['B01001_018E']+tazControl4['B01001_019E']+tazControl4['B01001_040E']+tazControl4['B01001_041E']+tazControl4['B01001_042E']+tazControl4['B01001_043E']- (tazControl4['gq_total']*tazControl4['percent_age_group5']))
tazControl4['AGE65to79'] = tazControl4['ct_pct']*(tazControl4['B01001_020E']+tazControl4['B01001_021E']+tazControl4['B01001_022E']+tazControl4['B01001_023E']+tazControl4['B01001_044E']+tazControl4['B01001_045E']+tazControl4['B01001_046E']+tazControl4['B01001_047E'] - (tazControl4['gq_total']*tazControl4['percent_age_group6']) )
tazControl4['AGE80M'] = tazControl4['ct_pct']*(tazControl4['B01001_024E']+tazControl4['B01001_025E']+tazControl4['B01001_048E']+tazControl4['B01001_049E']- (tazControl4['gq_total']*tazControl4['percent_age_group7']))
print('AGE GROUP 2', tazControl4.AGE18to24.sum(), tazControl4.AGE18to24_no_subtract.sum(), sep=' ')

##Total number of 4+ HHs
seed_house_HH4M = seed_house[seed_house.NP>=4]
sum_hh4m = seed_house_HH4M.WGTP.sum()
print('Total households with 4 and more persons', sum_hh4m, sep=' ')
 
##average number of person in 4+ HHs weighted
wgt_avg_per = (seed_house_HH4M.NP*seed_house_HH4M.WGTP).sum()/float(sum_hh4m)
print('Avg. number of persons in 4+ person household (PUMS)', wgt_avg_per, sep=' ')

# # total number of person in TAZ (using weighted average)
# nashville_pop_4m = nashville_pop - 1*tazControl4['HHSIZE1_S3'].sum()-2*tazControl4['HHSIZE2_S3'].sum()-3*tazControl4['HHSIZE3_S3'].sum()  #required population in 4 and more person households
# pop_hh_4m = wgt_avg_per*tazControl4['HHSIZE4M_S3'].sum() # existing population in 4 and more person households when used avg persons in 4 and more person households
# nashville_factor_4m = nashville_pop_4m/pop_hh_4m  #factor to have population as required
# wgt_avg_per = nashville_factor_4m*wgt_avg_per  #updated weight for households with 4 or more persons
# print('Avg. number of persons in 4+ person household (PUMS)', wgt_avg_per, sep=' ')


# nashville_pop_4m_county = tazControl4.groupby('COUNTY').apply(lambda _df: (_df.HHSIZE1_S3 + (2*_df.HHSIZE2_S3) + (3*_df.HHSIZE3_S3) + (4*_df.HHSIZE4M_S3)).sum())
# nashville_pop_4m_county = reindex(totPOP_county, COUNTY_DF.COUNTY).subtract(nashville_pop_4m_county,axis=0)
# nashville_pop_4m_county.index.name = 'COUNTY'
# pop_hh_4m_county = wgt_avg_per*tazControl4.groupby('COUNTY')['HHSIZE4M_S3'].sum() # existing population in 4 and more person households when used avg persons in 4 and more person households
# nashville_factor_4m_county = nashville_pop_4m_county.div(pop_hh_4m_county,axis=0)  #factor to have population as required
# wgt_avg_per_county = nashville_factor_4m_county*wgt_avg_per  #updated weight for households with 4 or more persons
# wgt_avg_per_county.rename(columns={'POP':'wgt_avg_per'}, inplace=True)

# Required population in County level
# Get existing population and then subtract from required population 
nashville_pop_4m_county = tazControl4.groupby('COUNTY', group_keys=True).apply(lambda _df: (_df.HHSIZE1_S3 + (2*_df.HHSIZE2_S3) + (3*_df.HHSIZE3_S3) + (4*_df.HHSIZE4M_S3)).sum())
nashville_pop_4m_county = reindex(totPOP_county, COUNTY_DF.COUNTY).subtract(nashville_pop_4m_county,axis=0)
# Weighted averge for 4m household
hh_4m_county = tazControl4.groupby('COUNTY', group_keys=True)['HHSIZE4M_S3'].sum()
wgt_avg_per_county = nashville_pop_4m_county.div(hh_4m_county,axis=0)+4  #factor to have population as required
wgt_avg_per_county.rename(columns={'POP':'wgt_avg_per'}, inplace=True)
wgt_avg_per_county.index.name='COUNTY'


# #total number of person in TAZ (using weighted average)
# total_popu_TAZ_unwgt = 1*(tazControl['HHSIZE1']/tazControl['bg_pct']).sum()+2*(tazControl['HHSIZE2']/tazControl['bg_pct']).sum()+3*(tazControl['HHSIZE3']/tazControl['bg_pct']).sum()+wgt_avg_per*(tazControl['HHSIZE4M']/tazControl['bg_pct']).sum()
# total_popu_TAZ  = 1*tazControl['HHSIZE1'].sum() +2*tazControl['HHSIZE2'].sum() +3*tazControl['HHSIZE3'].sum() +wgt_avg_per*tazControl['HHSIZE4M'].sum()

#total number of person in TAZ (using weighted average)
tazControl = tazControl.merge(wgt_avg_per_county, how='left', on='COUNTY')
total_popu_TAZ_unwgt_county = tazControl.groupby('COUNTY', group_keys=True).apply(lambda _df: ((_df.HHSIZE1 + (2*_df.HHSIZE2) + (3*_df.HHSIZE3) + (_df.wgt_avg_per * _df.HHSIZE4M))/_df.bg_pct).sum()).to_frame(name='pop')
total_popu_TAZ_county  = tazControl.groupby('COUNTY', group_keys=True).apply(lambda _df: ((_df.HHSIZE1 + (2*_df.HHSIZE2) + (3*_df.HHSIZE3) + (_df.wgt_avg_per * _df.HHSIZE4M))).sum()).to_frame(name='pop')

#total number of person in county
per_sc = tazControl4.groupby('COUNTY', as_index=False)['B01001_001E'].sum()
print('Total persons in ACS', per_sc, sep=' ')

tazControl4 = tazControl4.merge(wgt_avg_per_county, how='left', on='COUNTY')
tazControl4 = tazControl4.applymap(lambda x: max(0, x))

tazControl4['MALE'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'MALE':roundsum(_df.MALE)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)
tazControl4['FEMALE'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'FEMALE':roundsum(_df.FEMALE)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)
                                   
tazControl4['TOT_POP_CENSUS'] = tazControl4['MALE']+tazControl4['FEMALE']
tazControl4['TOTPOP_WGT'] = (tazControl4['HHSIZE1_S3']*1 + tazControl4['HHSIZE2_S3']*2 + tazControl4['HHSIZE3_S3']*3 + tazControl4['HHSIZE4M_S3']*tazControl4['wgt_avg_per'])

tazControl4['TOTPOP_WGT'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'TOTPOP_WGT':roundsum(_df.TOTPOP_WGT)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

# Gender
tazControl4['MALE_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['MALE']/tazControl4.TOT_POP_CENSUS,0)
tazControl4['FEMALE_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['FEMALE']/tazControl4.TOT_POP_CENSUS,0)

tazControl4[['MALE_S', 'FEMALE_S']] = pd.DataFrame(tazControl4[['MALE_S', 'FEMALE_S']].apply(roundsum, axis=1).tolist(),columns=['MALE_S','FEMALE_S'], index=tazControl4.index)


# Age
# tazControl4['AGE0to17'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE0to17':roundsum(_df.AGE0to17)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

# tazControl4['AGE18to24'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE18to24':roundsum(_df.AGE18to24)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

# tazControl4['AGE25to34'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE25to34':roundsum(_df.AGE25to34)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

# tazControl4['AGE35to49'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE35to49':roundsum(_df.AGE35to49)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

# tazControl4['AGE50to64'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE50to64':roundsum(_df.AGE50to64)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

# tazControl4['AGE65to79'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE65to79':roundsum(_df.AGE65to79)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

# tazControl4['AGE80M'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
# 	apply(lambda _df: pd.DataFrame({'AGE80M':roundsum(_df.AGE80M)}, index=_df.TAZ)).droplevel(0),
# 	tazControl4.TAZ)

tazControl4[['AGE0to17','AGE18to24','AGE25to34','AGE35to49','AGE50to64','AGE65to79','AGE80M']] = \
	pd.DataFrame(tazControl4[['AGE0to17','AGE18to24','AGE25to34','AGE35to49','AGE50to64','AGE65to79','AGE80M']].\
		apply(roundsum, axis=1).tolist(),columns=['AGE0to17','AGE18to24','AGE25to34','AGE35to49','AGE50to64','AGE65to79','AGE80M'], index=tazControl4.index)

tazControl4['AGETOTALPOP'] = tazControl4['AGE0to17']+tazControl4['AGE18to24']+tazControl4['AGE25to34']+tazControl4['AGE35to49']+tazControl4['AGE50to64']+tazControl4['AGE65to79']+tazControl4['AGE80M']
tazControl4['AGE0to17_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE0to17']/tazControl4.AGETOTALPOP,0)
tazControl4['AGE18to24_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE18to24']/tazControl4.AGETOTALPOP,0)
tazControl4['AGE25to34_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE25to34']/tazControl4.AGETOTALPOP,0)
tazControl4['AGE35to49_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE35to49']/tazControl4.AGETOTALPOP,0)
tazControl4['AGE50to64_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE50to64']/tazControl4.AGETOTALPOP,0)
tazControl4['AGE65to79_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE65to79']/tazControl4.AGETOTALPOP,0)
tazControl4['AGE80M_S']=np.where(tazControl4.TOT_POP_CENSUS > 0,tazControl4.TOTPOP_WGT*tazControl4['AGE80M']/tazControl4.AGETOTALPOP,0)

tazControl4[['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']] = \
	pd.DataFrame(tazControl4[['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']].\
		apply(roundsum, axis=1).tolist(),columns=['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S'], index=tazControl4.index)

#rounding for person gender variables
tazControl4['diff_round_sex']=tazControl4['TOTPOP_WGT'] - tazControl4['MALE_S'] - tazControl4['FEMALE_S']
# for idx,col in enumerate(['MALE_S','FEMALE_S']):
# 	col_list = [c for c in ['MALE_S','FEMALE_S']]
# 	del col_list[idx]
# 	tazControl4[col] = np.where((tazControl4[col] >= tazControl4[col_list[0]]), tazControl4[col]+tazControl4['diff_round_sex'], tazControl4[col])

tazControl4['MALE_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'MALE_S':roundsum(_df.MALE_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)
tazControl4['FEMALE_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'FEMALE_S':roundsum(_df.FEMALE_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)
print("TOTPOP_WGT", tazControl4.TOTPOP_WGT.sum(), sep=' ')
print("diff round sex", tazControl4['TOTPOP_WGT'].sum()-tazControl4.MALE_S.sum()-tazControl4.FEMALE_S.sum(), sep=' ')
print("diff round sex", (tazControl4['TOTPOP_WGT']-tazControl4[['MALE_S','FEMALE_S']].sum(axis=1)).sum(), sep=' ')

# rounding for person age variables
tazControl4['diff_round_age']=tazControl4['TOTPOP_WGT'] - tazControl4['AGE0to17_S'] - tazControl4['AGE18to24_S'] - tazControl4['AGE25to34_S'] - tazControl4['AGE35to49_S'] - tazControl4['AGE50to64_S'] - tazControl4['AGE65to79_S'] - tazControl4['AGE80M_S']
tazControl4=tazControl4.fillna(0)
print("total pop before adding diff", tazControl4[['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']].sum().sum(), sep=' ')
# for idx,col in enumerate(['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']):
# 	col_list = [c for c in ['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']]
# 	del col_list[idx]
# 	tazControl4[col] = np.where((tazControl4[col] >= tazControl4[col_list[0]]) & (tazControl4[col] >= tazControl4[col_list[1]]) & (tazControl4[col] >= tazControl4[col_list[2]]) & (tazControl4[col] >= tazControl4[col_list[3]])& (tazControl4[col] >= tazControl4[col_list[4]])& (tazControl4[col] >= tazControl4[col_list[5]]), tazControl4[col]+tazControl4['diff_round_age'], tazControl4[col])

tazControl4['AGE0to17_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE0to17_S':roundsum(_df.AGE0to17_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

tazControl4['AGE18to24_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE18to24_S':roundsum(_df.AGE18to24_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

tazControl4['AGE25to34_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE25to34_S':roundsum(_df.AGE25to34_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

tazControl4['AGE35to49_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE35to49_S':roundsum(_df.AGE35to49_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

tazControl4['AGE50to64_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE50to64_S':roundsum(_df.AGE50to64_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

tazControl4['AGE65to79_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE65to79_S':roundsum(_df.AGE65to79_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

tazControl4['AGE80M_S'] = reindex(tazControl4.groupby('COUNTY', as_index=False).\
	apply(lambda _df: pd.DataFrame({'AGE80M_S':roundsum(_df.AGE80M_S)}, index=_df.TAZ)).droplevel(0),
	tazControl4.TAZ)

print("difference from rounding age", tazControl4['TOTPOP_WGT'].sum()-tazControl4[['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']].sum().sum(), sep=' ')
print("difference from rounding age", (tazControl4['TOTPOP_WGT']-tazControl4[['AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']].sum(axis=1)).sum(), sep=' ')

# Check if any person controls are NA or zero
for idx,col in enumerate(['MALE_S','FEMALE_S','AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']):
	print('Column ' + col + ' has NA values: ', tazControl4[tazControl4[col].isna()].shape[0], ' and Negative values: ', tazControl4[tazControl4[col]<0].shape[0])



tazControl4[['MALE_S','FEMALE_S','AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']] = tazControl4[['MALE_S','FEMALE_S','AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S']].astype(int)
                                         
tazControl4['TOTPOP_S'] = tazControl4['AGE0to17_S']+tazControl4['AGE18to24_S']+tazControl4['AGE25to34_S']+tazControl4['AGE35to49_S']+tazControl4['AGE50to64_S']+tazControl4['AGE65to79_S']+tazControl4['AGE80M_S']
tazControl4.rename(columns = {'PUMA_x':'PUMA'})[['TAZ','HHS','GQ','PUMA','COUNTY','HHSIZE1_S3','HHSIZE2_S3','HHSIZE3_S3','HHSIZE4M_S3','HHAGE1_S3','HHAGE2_S3','HHAGE3_S3',
 'HHWRK1_S3','HHWRK2_S3','HHWRK3_S3','HHWRK4_S3','HHINC1_S3','HHINC2_S3','HHINC3_S3','HHINC4_S3','HHINC5_S3','HHINC6_S3','HH_WIKID_S3','HH_WOKID_S3','HHFAMMAR_S3','HHFAMNOWIFE_S3','HHFAMNOHUSBAND_S3','HHFAMNON_S3','HHFAMNONALONE_S3','HHUNITSINGLE_S3','HHUNITMULTI_S3','HHUNITMOBILE_S3',
		 'TOTPOP_S','MALE_S','FEMALE_S','AGE0to17_S','AGE18to24_S','AGE25to34_S','AGE35to49_S','AGE50to64_S','AGE65to79_S','AGE80M_S', 'MALE','FEMALE', 'TOT_POP_CENSUS','TOTPOP_WGT']].to_csv(os.path.join(outputDir, "control_totals_taz.csv"), index = False)
		 
totpop = tazControl4[['TOTPOP_S']].sum().sum()
ctrl_region = pd.DataFrame(data = {'REGION':[3],'TOTPOP':[totpop]}).to_csv(os.path.join(outputDir,"control_totals_region.csv"), index = False)

		 
