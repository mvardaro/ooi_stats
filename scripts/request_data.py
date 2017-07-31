import datetime
import requests
import json
import pandas as pd
import concurrent.futures

# this script is written in python 3, because python 2 has some SSL issues when threading
# concurrent requests.

# specify you api credentials, begin time and inputs
username = 'OOIAPI-9N9UMLHV9W5GOP'
token = 'SJN6HXHH116OZ8'
begin_time_set = datetime.datetime(2013, 7, 1, 0,0,0)
array = 'test3'
input_path = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/'




# set up threads pool and execute requests
pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
session = requests.session()

# set up function to send requests
def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)

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
now = datetime.datetime.now()
days = abs(begin_time_set.date() - now.date())
days = int(days.days)
print(str(days) + " days of data since " + str(begin_time_str) + " will be requested for each refdes+stream.")

# iterate over reference designators, delivery methods and streams in csv to build request urls by refdes.
# the urls are stored in a dictonary.

print("Building instrument requests...")
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

# request data and store entry for refdes, stream and first data point returned
print("Instrument request urls built. Requesting data for...")

ref_des_list = []
stream_list = []
timestamp_list = []


for key, values in requests_dict.items():
    print(key)
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
            print(timestamp)

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
ooi_data = ooi_data[ooi_data.timestamp >= begin_time_set.date()]
ooi_data.to_csv('output/'+array+'/data.csv', index=False)
