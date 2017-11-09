import pandas as pd
import datetime
import calendar
import copy

# read in data
array = 'cabled'
data_file = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/daily_checks/'+array+'/no_data_today.csv'
refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/daily_checks/'+array+'/no_data_yesterday.csv'
data = pd.read_csv(data_file)
refdes_streams_df = pd.read_csv(refdes_streams)

# extract only refdes
data = pd.DataFrame(data, columns=['refdes'])
refdes_streams_df = pd.DataFrame(refdes_streams_df, columns=['refdes'])

# append refdes for which no data was returned
data_temp = list(set(refdes_streams_df['refdes'].values) - set(data['refdes'].values))
data_temp = pd.DataFrame(data_temp, columns=['refdes']).sort_values(by=['refdes'])
print(data_temp)
