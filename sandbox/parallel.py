import datetime
import calendar
import requests
import json
import pandas as pd
import concurrent.futures
import numpy as np
import csv

username = 'OOIAPI-9N9UMLHV9W5GOP'
token = 'SJN6HXHH116OZ8'
begin_time_set = datetime.datetime(2017, 6, 28, 0,0,0)
array = 'test2'
refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/' + array + '.csv'

# setup the base url for the request that will be built using the inputs above.
BASE_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'

# request only parameter 7 (time)
parameter = '7'

# read in csv (for now)
refdes_streams_df = pd.read_csv(refdes_streams)

# prepare time stamp manipulators and range of data requests
begin_time_str = begin_time_set.strftime('%Y-%m-%dT%H:%M:%S.000Z')
ntp_epoch = datetime.datetime(1900, 1, 1)
unix_epoch = datetime.datetime(1970, 1, 1)
ntp_delta = (unix_epoch - ntp_epoch).total_seconds()
now = datetime.datetime.now()
days = abs(begin_time_set.date() - now.date())
days = int(days.days)
print(days,"days of data since", begin_time_str, "will be requested for each refdes+stream.")

# iterate over reference designators, delivery methods and streams to build request url lists 
requests_dict = {}
for index, row in refdes_streams_df.iterrows():

    # start at the begin time set above
    begin_time = begin_time_set
    begin_time_str = begin_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    #step forward by 1 day (86400 seconds) with each new request
    end_time = begin_time + datetime.timedelta(seconds=86400)
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    ref_des =  row['refdes']
    sub_site = ref_des[:8]
    platform = ref_des[9:14]
    instrument = ref_des[15:27]
    stream = row['stream']
    delivery_method = row['method']
    
    ref_des_list = []

    for i in range(days):
        request_url = '/'.join((BASE_URL, sub_site, platform, instrument, delivery_method, stream))
        request_url = request_url+'?beginDT='+begin_time_str+'&endDT='+end_time_str+'&limit=1000&parameters='+parameter
        
        ref_des_list.append(request_url)

        begin_time = begin_time + datetime.timedelta(seconds=86400)
        begin_time_str = begin_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        end_time = end_time + datetime.timedelta(seconds=86400)
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
    requests_dict[ref_des] = ref_des_list

# set up threads pool and execute requests
pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
session = requests.session()

# set up function to send requests
def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)


# create some empty lists as inputs for your final data frame output
ref_des_list = []
stream_list = []
timestamp_list = []


for key, values in requests_dict.items():
    print("requesting data for",key)
    future_to_url = {pool.submit(request_data, url, username, token): url for url in values}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:    
            data = future.result()
            data = data.json()
            stream = data[0]['pk']['stream']
            timestamp = data[0]['time']
            timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
            timestamp = timestamp.date()
            # print(timestamp)

            ref_des_list.append(key)
            stream_list.append(stream)
            timestamp_list.append(timestamp)
            
        except:
            data = future.result()
#             data = data.json()
#             print(data['status'])

# convert lists to data frame
data_dict = {
    'refdes':ref_des_list,
    'stream':stream_list,
    'timestamp':timestamp_list}
ooi_data = pd.DataFrame(data_dict, columns = ['refdes', 'stream', 'timestamp'])
# ooi_data = ooi_data.drop_duplicates()
ooi_data = ooi_data[ooi_data.timestamp >= begin_time_set.date()]

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
timetable_streams = timetable_streams.pivot(index='stream', columns='timestamp', values='data')
timetable_streams.to_csv('output/timetable_streams.csv', index=True)

#write time table for all instruments to file
timetable_refdes = pd.concat([ooi_data, dfc])
timetable_refdes['data'] = timetable_refdes['timestamp'].notnull()
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

# calculate month stats based on total days of data requestes
timetable_months = pd.concat([ooi_data, dfc])
timetable_months['timestamp'] = pd.to_datetime(timetable_months['timestamp'])
timetable_months['month'] = timetable_months['timestamp'].dt.strftime('%Y-%m')
timetable_months = timetable_months.groupby(['refdes', 'month'])['timestamp'].nunique().reset_index(name="days_of_data")

# create denominator
def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

periods = diff_month(now.date(),begin_time_set.date())
start_time = to_integer(begin_time_set)

total_months = pd.DataFrame({'date' : pd.date_range(str(start_time),periods=periods,freq='M') })
total_months['year'] = total_months['date'].dt.year
total_months['month'] = total_months['date'].dt.month
total_months['days_in_month'] = total_months.apply(lambda x: calendar.monthrange(x['year'],x['month'])[1], axis=1)
total_months['month'] = total_months['date'].dt.strftime('%Y-%m')
total_months = total_months[['month','days_in_month']]

timetable_months = pd.merge(timetable_months, total_months, on='month', how='outer')
timetable_months['percent'] = timetable_months['days_of_data'] / timetable_months['days_in_month']
timetable_months = timetable_months.pivot(index='refdes', columns='month', values='percent')
timetable_months.to_csv('output/'+array+'/stats/timetable_refdes_monthly.csv', index=True)