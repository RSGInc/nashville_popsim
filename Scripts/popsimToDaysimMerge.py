#A small script to combine the household and group quarter daysim format outputs
import sys
import os
import numpy as np
import pandas as pd

parameters_file = sys.argv[1]
parameters = pd.read_csv(parameters_file)
parameters.columns = ['Key', 'Value']
WORKING_DIR = parameters[parameters.Key == 'WORKING_DIR']['Value'].item().strip(' ')
outputDir = os.path.join(WORKING_DIR, 'Setup', 'output','DaySimFormat')
xwalkDir = os.path.join(WORKING_DIR, 'Setup', 'data')

xwalk = pd.read_csv(os.path.join(xwalkDir,'geo_crosswalks.csv'))

#hh

hh = pd.read_csv(os.path.join(outputDir, 'household_2017_HH.dat'), sep = ' ')
hhper = pd.read_csv(os.path.join(outputDir,'person_2017_HH.dat'), sep = ' ')

#gq

gq = pd.read_csv(os.path.join(outputDir, 'household_2017_GQ.dat'), sep = ' ')
gq = gq.merge(xwalk[['MAZ','TAZ']], left_on = 'hhparcel', right_on = 'MAZ').drop('MAZ', axis = 1).rename(columns = {'TAZ':'hhtaz'})

gq['new_id'] = np.arange(hh.hhno.max() +1, hh.hhno.max()+1 +len(gq))

idmap = gq[['hhno','new_id']]

gq['hhno'] = gq['new_id']
gq = gq[list(hh.columns)]

gqper = pd.read_csv(os.path.join(outputDir, 'person_2017_GQ.dat'), sep = ' ')
gqper = gqper.merge(idmap, how = 'left', on = 'hhno').drop('hhno',axis = 1).rename(columns = {'new_id':'hhno'})
gqper = gqper[list(hhper.columns)]

#append

finalhh = hh.append(gq)
finalhh.to_csv(os.path.join(outputDir, 'household_2017.dat'), sep = ' ', index = False)

finalper = hhper.append(gqper)
finalper.to_csv(os.path.join(outputDir, 'person_2017.dat'), sep = ' ', index = False)

