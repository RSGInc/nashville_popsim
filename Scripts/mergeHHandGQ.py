import pandas as pd
import numpy as np
import os, sys

#read properties from parameters file
parameters_file = sys.argv[1]
# parameters_file=r'E:\Projects\Clients\NashvilleMPO\ModelUpdate2023\Tasks\Task4_Enhancements\Update_PopulationSim_Software\GitHub_William\Data\parameters.csv'
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']
WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'output')
XWALK_DIR = parameters[parameters.Key == 'XWALK_DIR']['Value'].item().strip(' ')

puma_veh = pd.read_csv(os.path.join(WORKING_DIR,'Setup','data','seed_households.csv'))[['hh_id','VEH','TEN','MV']]
puma_veh['tenure']  = np.where(puma_veh['TEN']<3,1,2)
puma_veh['recent_mover'] = np.where(puma_veh['MV'] < 4, 1,np.where(puma_veh['MV'] >= 4, 0, np.nan))

puma_per = pd.read_csv(os.path.join(WORKING_DIR,'Setup','data','seed_persons.csv'))


children = puma_per[puma_per.AGEP < 18]
children = pd.DataFrame(children.groupby('hh_id',as_index = False)['PWGTP'].count())
children.columns = ['hh_id','children']
heads = puma_per[puma_per.RELSHIPP == 20][['hh_id','RAC1P']].rename(columns = {'RAC1P':'race_of_head'})
puma_veh = puma_veh.merge(children, how = 'left', on = 'hh_id').merge(heads, how = 'left', on = 'hh_id').fillna(0)
puma_veh['HH'] = 1
print(puma_veh.children.describe())
gqhh = pd.read_csv(os.path.join(outputDir,'GQ','synthetic_households.csv'))
print(gqhh.head())
gqpop = pd.read_csv(os.path.join(outputDir,'GQ','synthetic_persons.csv'))

xwalk = pd.read_csv(os.path.join(XWALK_DIR,'geo_crosswalks.csv'))
gqpop = gqpop.merge(xwalk[['MAZ','TAZ']], how = 'left', on = 'MAZ')
gqhh = gqhh.merge(xwalk[['MAZ','TAZ']], how = 'left', on = 'MAZ')

hh = pd.read_csv(os.path.join(outputDir,'HH','synthetic_households.csv'))
pop = pd.read_csv(os.path.join(outputDir,'HH','synthetic_persons.csv'))

gqhh['household_id_old'] = gqhh['household_id'] #save old household_id to merge with GQ persons
gqhh['household_id'] = np.arange(hh.household_id.max() + 1, len(gqhh) + hh.household_id.max() +1)
gqhh['HH'] = 0

print("GQ HHs")
print(len(hh))
print(len(hh), hh.household_id.min(), hh.household_id.max() )
print(len(gqhh), gqhh.household_id.min(), gqhh.household_id.max() )

print("GQ Persons")
print(len(gqpop))

gqpop['household_id_old'] = gqpop['household_id'] #save old household_id to merge with GQ hhs
#gqpop = gqpop.drop('household_id',axis = 1).merge(gqhh[['hh_id','household_id']], how = 'left', on = 'hh_id')
gqpop = gqpop.drop('household_id',axis = 1).merge(gqhh[['household_id_old','household_id']], how = 'left', on = 'household_id_old')

print(len(gqpop))

hh_expand_id = pd.read_csv(os.path.join(outputDir,'HH','final_expanded_household_ids.csv'))
gq_expand_id = pd.read_csv(os.path.join(outputDir,'GQ','final_expanded_household_ids.csv'))
pd.concat([hh_expand_id, gq_expand_id], ignore_index=True).to_csv(os.path.join(outputDir,'combined','expanded_household_ids.csv'),index = False)

hh_summary_id = pd.read_csv(os.path.join(outputDir,'HH','final_summary_MAZ_PUMA.csv'))
gq_summary_id = pd.read_csv(os.path.join(outputDir,'GQ','final_summary_MAZ_PUMA.csv'))
hh_summary_id.merge(gq_summary_id, how = 'left', on = ['geography','id']).to_csv(os.path.join(outputDir,'combined','summary_MAZ_PUMA.csv'), index = False)

hh_summary_taz = pd.read_csv(os.path.join(outputDir,'HH','final_summary_TAZ.csv'))
# gq_summary_taz = pd.read_csv(os.path.join(outputDir,'GQ','summary_TAZ.csv'))
# hh_summary_taz.merge(gq_summary_taz, how = 'left', on = ['geography','id']).to_csv(os.path.join(outputDir,'combined','summary_TAZ.csv'), index = False)
hh_summary_taz.to_csv(os.path.join(outputDir,'combined','final_summary_TAZ.csv'), index = False)

hh_summary_MAZ = pd.read_csv(os.path.join(outputDir,'HH','final_summary_MAZ.csv'))
gq_summary_MAZ = pd.read_csv(os.path.join(outputDir,'GQ','final_summary_MAZ.csv'))
hh_summary_MAZ.merge(gq_summary_MAZ, how = 'left', on = ['geography','id']).to_csv(os.path.join(outputDir,'combined','summary_MAZ.csv'), index = False)


gqhh.to_csv(os.path.join(outputDir,'Combined','newhhidcheck.csv'), index = False)
pd.concat([hh.merge(puma_veh, how = 'left', on = 'hh_id').rename(columns = {'hh_id':'hh_id_pums'}), gqhh.drop('household_id_old',axis = 1).rename(columns = {'hh_id':'hh_id_pums'})],ignore_index=True)[['household_id','hh_id_pums','HH','MAZ','TAZ','PUMA','NP','AGEHOH','HHINCADJ','workers','children','VEH','tenure','recent_mover','race_of_head']].fillna(-1).to_csv(os.path.join(outputDir,'Combined','synthetic_households.csv'), index = False)
pd.concat([pop, gqpop.drop('household_id_old',axis = 1)], ignore_index=True).rename(columns = {'hh_id':'hh_id_pums'})[['hh_id_pums','household_id','MAZ','PUMA','per_num','AGEP']].to_csv(os.path.join(outputDir,'Combined','synthetic_persons.csv'), index = False)

# Merge Seed Data for validation
# seed_hh = pd.read_csv(os.path.join(WORKING_DIR,'Setup','data','seed_households.csv'))
# seed_gq = pd.read_csv(os.path.join(WORKING_DIR,'Setup','data','seed_households_GQ.csv')).rename(columns = {'GQWGTP':'WGTP'})
# pd.concat([seed_hh[[col for col in seed_hh.columns if col in seed_gq.columns]],seed_gq[[col for col in seed_gq.columns if col in seed_hh.columns]]], sort = False).to_csv(os.path.join(WORKING_DIR,'Setup','data','seed_households_all.csv'),index = False)


