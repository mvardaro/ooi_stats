import datetime
import requests
import json
import pandas as pd
import concurrent.futures

import pandas as pd
import calendar
import copy

# this script is written in python 3, because python 2 has some SSL issues when threading
# concurrent requests.

# specify you api credentials, begin time and inputs
username = 'OOIAPI-30AZZ33CYL06XZ'
token = '77CEUHU3VZ9'
array = 'cabled'
input_path = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/'
begin_time_set = datetime.datetime.utcnow() - datetime.timedelta(seconds=86400)
last_checked_file = '/no_data_2017-11-02.csv'


# base url for the request that will be built using the inputs above.
BASE_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'

# request only parameter 7 (time)
parameter = '7'

# read in csv
refdes_streams = input_path + array + '.csv'
refdes_streams_df = pd.read_csv(refdes_streams)

# prepare time stamp manipulators and range of data requests
begin_time_str = begin_time_set.strftime('%Y-%m-%dT%H:%M:%S.000Z')
ntp_epoch = datetime.datetime(1900, 1, 1)
unix_epoch = datetime.datetime(1970, 1, 1)
ntp_delta = (unix_epoch - ntp_epoch).total_seconds()

print("Building instrument requests...")
requests_list = []
for index, row in refdes_streams_df.iterrows():

    ref_des =  row['refdes'] + '   ' + row['stream']
    sub_site = ref_des[:8]
    platform = ref_des[9:14]
    instrument = ref_des[15:27]
    stream = row['stream']
    delivery_method = row['method']
    
    request_url = '/'.join((BASE_URL, sub_site, platform, instrument, delivery_method, stream))
    request_url = request_url+'?beginDT='+begin_time_str+'&limit=1000&parameters='+parameter
    requests_list.append(request_url)



print("Instrument request urls built. Requesting data for...")
ref_des_list = []
stream_list = []
timestamp_list = []

# set up threads pool and execute requests
pool = concurrent.futures.ThreadPoolExecutor(max_workers=12)
session = requests.session()

# set up function to send requests
def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)




future_to_url = {pool.submit(request_data, url, username, token): url for url in requests_list}
for future in concurrent.futures.as_completed(future_to_url):
    try:    
        data = future.result()
        data = data.json()
        stream = data[-1]['pk']['stream']
        reference_designator = data[-1]['pk']['subsite'] + '-' + data[-1]['pk']['node'] + '-' + data[-1]['pk']['sensor']
        timestamp = data[-1]['time']
        timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
        timestamp = timestamp.date()
        print(reference_designator, timestamp)

        ref_des_list.append(reference_designator)
        stream_list.append(stream)
        timestamp_list.append(timestamp)

    except:
        data = future.result()
#             data = data.json()
#             print(future)


# convert lists to data frame
data_dict = {
    'refdes':ref_des_list,
    'stream':stream_list,
    'timestamp':timestamp_list}
ooi_data = pd.DataFrame(data_dict, columns = ['refdes', 'stream', 'timestamp'])
ooi_data = ooi_data[ooi_data.timestamp >= begin_time_set.date()]
ooi_data.to_csv('output/daily_checks/'+array+'/data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.csv', index=False)






data_file = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/daily_checks/'+array+'/data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.csv'
refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/'+array+'.csv'
data = pd.read_csv(data_file)
refdes_streams_df = pd.read_csv(refdes_streams)

# extract only refdes
data = pd.DataFrame(data, columns=['refdes'])
refdes_streams_df = pd.DataFrame(refdes_streams_df, columns=['refdes'])

# append refdes for which no data was returned
data_temp = list(set(refdes_streams_df['refdes'].values) - set(data['refdes'].values))
data_temp = pd.DataFrame(data_temp, columns=['refdes']).sort_values(by=['refdes'])
data_temp.to_csv('output/daily_checks/'+array+'/no_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.csv', index=False)

data_temp['available'] = 'FALSE'
data['available'] = 'TRUE'
data = pd.concat([data, data_temp])
data.sort_values(by=['refdes'])
data = data.drop_duplicates()
data.to_csv('output/daily_checks/'+array+'/summary_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.csv', index=False)






data_file = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/daily_checks/'+array+'/no_data_'+ datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.csv'
refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/daily_checks/'+array+last_checked_file
data = pd.read_csv(data_file)
refdes_streams_df = pd.read_csv(refdes_streams)

# extract only refdes
data = pd.DataFrame(data, columns=['refdes'])
refdes_streams_df = pd.DataFrame(refdes_streams_df, columns=['refdes'])

# append refdes for which no data was returned
data_temp = list(set(refdes_streams_df['refdes'].values) ^ set(data['refdes'].values))
data_temp = pd.DataFrame(data_temp, columns=['refdes']).sort_values(by=['refdes'])
print(data_temp)
