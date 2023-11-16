import pandas as pd
import numpy as np
import os, sys

#read properties from parameters file
parameters_file = sys.argv[1]
#parameters_file = r'E:\Projects\Clients\Nashville\Tasks\Task2_PopSim\Data\parameters.csv'
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']
WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'output')
XWALK_DIR = parameters[parameters.Key == 'XWALK_DIR']['Value'].item().strip(' ')

puma_veh = pd.read_csv(os.path.join(WORKING_DIR,'Setup','data','seed_households.csv'))[['hh_id','VEH','TEN','MV']]
puma_per = pd.read_csv(os.path.join(WORKING_DIR,'Setup','data','seed_persons.csv'))
children = puma_per[puma_per.AGEP < 18]
children = pd.DataFrame(children.groupby('hh_id',as_index = False)['PWGTP'].count())
print("Children check", len(children))
children.columns = ['hh_id','children']
heads = puma_per[puma_per.RELP == 0][['hh_id','RAC1P']]
print("Heads check", len(heads))
print("Veh, ten,mv check", len(puma_veh))
puma_veh = puma_veh.merge(children, how = 'left', on = 'hh_id').merge(heads, how = 'left', on = 'hh_id').fillna(0)
print(puma_veh.children.describe())
#gqhh = pd.read_csv(os.path.join(outputDir,'GQ','synthetic_households.csv'))
#gqpop = pd.read_csv(os.path.join(outputDir,'GQ','synthetic_persons.csv'))

xwalk = pd.read_csv(os.path.join(XWALK_DIR,'geo_crosswalks.csv'))
# gqpop = gqpop.merge(xwalk[['MAZ','TAZ']], how = 'left', on = 'MAZ')
# gqhh = gqhh.merge(xwalk[['MAZ','TAZ']], how = 'left', on = 'MAZ')

hh = pd.read_csv(os.path.join(outputDir,'HH','synthetic_households.csv'))
pop = pd.read_csv(os.path.join(outputDir,'HH','synthetic_persons.csv'))
print('synthetic hh', len(hh))
# gqhh['household_id_old'] = gqhh['household_id'] #save old household_id to merge with GQ persons
# gqhh['household_id'] = np.arange(hh.household_id.max() + 1, len(gqhh) + hh.household_id.max() +1)
# print("GQ HHs")
# print(len(hh))
# print(len(hh), hh.household_id.min(), hh.household_id.max() )
# print(len(gqhh), gqhh.household_id.min(), gqhh.household_id.max() )

# print("GQ Persons")
# print(len(gqpop))

# gqpop['household_id_old'] = gqpop['household_id'] #save old household_id to merge with GQ hhs
# gqpop = gqpop.drop('household_id',axis = 1).merge(gqhh[['hh_id','household_id']], how = 'left', on = 'hh_id')
# gqpop = gqpop.drop('household_id',axis = 1).merge(gqhh[['household_id_old','household_id']], how = 'left', on = 'household_id_old')

# print(len(gqpop))

# gqhh.to_csv(os.path.join(outputDir,'Combined','newhhidcheck.csv'), index = False)
hh.merge(puma_veh, how = 'left', on = 'hh_id').fillna(0).to_csv(os.path.join(outputDir,'HHplus','synthetic_households.csv'), index = False)
# gqpop.drop('household_id_old',axis = 1).append(pop).to_csv(os.path.join(outputDir,'Combined','synthetic_persons.csv'), index = False)





