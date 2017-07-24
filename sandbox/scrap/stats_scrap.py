import datetime
import calendar
import requests
import json
import pandas as pd
import concurrent.futures
import numpy as np
import csv



# add back any refdes for which no data was returned
dfa = ooi_data.drop(['timestamp'],axis = 1)
dfa = dfa['refdes'].values
dfb = refdes_streams_df['refdes'].values
dfc = list(set(dfb) - set(dfa))
dfc = pd.DataFrame(dfc,columns=['refdes'])

dfa = ooi_data.drop(['timestamp'],axis = 1)
dfd = dfa["refdes"].map(str) + '-' + dfa["stream"]
dfe = refdes_streams_df['refdes'].map(str) + '-' + refdes_streams_df["stream"]
dfe = list(set(dfe) - set(dfd))
dfe = pd.DataFrame(dfe,columns=['refdes'])
dfe = dfe.rename(index=str, columns={'refdes':'stream'})

#write time table for all streams to file
timetable_streams = ooi_data
timetable_streams["stream"] = timetable_streams["refdes"].map(str) + '-' + timetable_streams["stream"]
timetable_streams = timetable_streams.drop(['refdes'],axis =1)
timetable_streams = pd.concat([timetable_streams, dfe])
timetable_streams['data'] = timetable_streams['timestamp'].notnull()
timetable_streams = timetable_streams.drop_duplicates()
timetable_streams = timetable_streams.pivot(index='stream', columns='timestamp', values='data')
timetable_streams.to_csv('output/'+array+'/timetables/timetable_streams.csv', index=True)

#write time table for all instruments to file
timetable_refdes = pd.concat([ooi_data, dfc])
timetable_refdes['data'] = timetable_refdes['timestamp'].notnull()
timetable_refdes = timetable_refdes.drop_duplicates()
timetable_refdes = timetable_refdes.pivot(index='refdes', columns='timestamp', values='data')
timetable_refdes.to_csv('output/'+array+'/timetables/timetable_refdes.csv', index=True)

#write time table for all nodes to file
timetable_node = pd.concat([ooi_data, dfc])
timetable_node['data'] = timetable_node['timestamp'].notnull()
timetable_node['node'] = timetable_node['refdes'].str[:14]
timetable_node = timetable_node.drop(['refdes','stream'],axis = 1)
timetable_node = timetable_node.drop_duplicates()
timetable_node = timetable_node.pivot(index='node', columns='timestamp', values='data')
timetable_node.to_csv('output/'+array+'/timetables/timetable_node.csv', index=True)

#write time table for all platforms to file
timetable_platform = pd.concat([ooi_data, dfc])
timetable_platform['data'] = timetable_platform['timestamp'].notnull()
timetable_platform['platform'] = timetable_platform['refdes'].str[:8]
timetable_platform = timetable_platform.drop(['refdes','stream'],axis = 1)
timetable_platform = timetable_platform.drop_duplicates()
timetable_platform = timetable_platform.pivot(index='platform', columns='timestamp', values='data')
timetable_platform.to_csv('output/'+array+'/timetables/timetable_platform.csv', index=True)

# # calculate month stats based on total days of data requestes
# timetable_months = pd.concat([ooi_data, dfc])
# timetable_months['timestamp'] = pd.to_datetime(timetable_months['timestamp'])
# timetable_months['month'] = timetable_months['timestamp'].dt.strftime('%Y-%m')
# timetable_months = timetable_months.groupby(['refdes', 'month'])['timestamp'].nunique().reset_index(name="days_of_data")

# # create denominator
# def to_integer(dt_time):
#     return 10000*dt_time.year + 100*dt_time.month + dt_time.day

# def diff_month(d1, d2):
#     return (d1.year - d2.year) * 12 + d1.month - d2.month

# periods = diff_month(now.date(),begin_time_set.date())
# start_time = to_integer(begin_time_set)

# total_months = pd.DataFrame({'date' : pd.date_range(str(start_time),periods=periods,freq='M') })
# total_months['year'] = total_months['date'].dt.year
# total_months['month'] = total_months['date'].dt.month
# total_months['days_in_month'] = total_months.apply(lambda x: calendar.monthrange(x['year'],x['month'])[1], axis=1)
# total_months['month'] = total_months['date'].dt.strftime('%Y-%m')
# total_months = total_months[['month','days_in_month']]

# timetable_months = pd.merge(timetable_months, total_months, on='month', how='outer')
# timetable_months['percent'] = timetable_months['days_of_data'] / timetable_months['days_in_month']
# timetable_months = timetable_months.pivot(index='refdes', columns='month', values='percent')
# timetable_months.to_csv('output/'+array+'/stats/timetable_refdes_monthly.csv', index=True)