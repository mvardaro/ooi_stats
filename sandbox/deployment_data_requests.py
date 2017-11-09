import matplotlib.pyplot as plt
import requests
from requests.packages.urllib3.util.retry import Retry
import json
import datetime
import pandas as pd
import concurrent.futures
import logging


username = 'OOIAPI-30AZZ33CYL06XZ'
token = '77CEUHU3VZ9'
# array = 'endurance'
arrays = ['cabled','endurance','global','pioneer']
input_path = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/'

logging.basicConfig(filename=array+'_requests.log',level=logging.DEBUG)


ntp_epoch = datetime.datetime(1900, 1, 1)
unix_epoch = datetime.datetime(1970, 1, 1)
ntp_delta = (unix_epoch - ntp_epoch).total_seconds()


# set up threads pool and execute requests
pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
session = requests.session()

retry = Retry(
        total=10,
        backoff_factor=0.3,
    )
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100,max_retries=retry,pool_block=True)
session.mount('http://', adapter)


# set up function to send requests
def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)

# base url for the request that will be built using the inputs above.
DEPLOYEMENT_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/inv/'
DATA_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'
parameter = '7'

# read in csv
refdes = input_path + array + '.csv'
refdes_list = pd.read_csv(refdes)
refdes_list = refdes_list['refdes']
refdes_list = refdes_list.drop_duplicates()






print("Building deployment info requests...")

asset_requests = []
for i in refdes_list:
    sub_site = i[:8]
    platform = i[9:14]
    instrument = i[15:27]
    asset_url_inputs = '/'.join((sub_site, platform, instrument))
    request_url = DEPLOYEMENT_URL+asset_url_inputs+'/-1'
    asset_requests.append(request_url)









print("Sending deployment info requests...")
ref_des_list = []
start_time_list = []
end_time_list = []
deployment_list = []

future_to_url = {pool.submit(request_data, url, username, token): url for url in asset_requests}
for future in concurrent.futures.as_completed(future_to_url):
    try:
        asset_info = future.result()
        asset_info = asset_info.json()
        
        for i in range(len(asset_info)):
            refdes = asset_info[i]['referenceDesignator']
            ref_des_list.append(refdes)
            
            deployment = asset_info[i]['deploymentNumber']
            deployment_list.append(deployment)
            
            start = asset_info[i]['eventStartTime']
            end = asset_info[i]['eventStopTime']
            
            try:
                start_time = datetime.datetime.utcfromtimestamp(start/1000.0)
                start_time_list.append(start_time)
                
                end_time = datetime.datetime.utcfromtimestamp(end/1000.0)
                end_time_list.append(end_time)
                
            except:
                end_time = datetime.datetime.utcnow()
                end_time_list.append(end_time)
                
    except:
        pass


data_dict = {
    'refdes':ref_des_list,
    'deployment':deployment_list,
    'start_time':start_time_list,
    'end_time':end_time_list}
deployment_data = pd.DataFrame(data_dict, columns = ['refdes', 'deployment','start_time', 'end_time'])








print("Calculating days between deployment dates...")

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

def diff_days(d1,d2):
    return (d2 - d1).days

# create empty dataframe
deployment_data_days = pd.DataFrame(columns = ['refdes', 'deployment','date'])

# calculate days between deployment dates
for index, row in deployment_data.iterrows():
    start_time = row['start_time']
    end_time = row['end_time']
    periods = diff_days(start_time, end_time)
    start_time = to_integer(start_time)
    total_days = pd.DataFrame({'date' : pd.date_range(str(start_time),periods=periods,freq='D')})
    
    total_days['refdes'] = row['refdes']
    total_days['deployment'] = row['deployment']
    deployment_data_days = deployment_data_days.append(total_days)

    # re-order data frame columns
deployment_data_days = deployment_data_days[['refdes', 'deployment','date']]







print("Building data request urls...")
deployment_data_days['start_date'] = deployment_data_days['date']
deployment_data_days['end_date'] = deployment_data_days['date'] + datetime.timedelta(seconds=86400)
# deployment_file_df = deployment_file_df[['refdes', 'start_date','end_date']]



refdes_streams = input_path + array + '.csv'
refdes_streams_df = pd.read_csv(refdes_streams)

request_inputs = pd.merge(refdes_streams_df,deployment_data_days, on='refdes')

request_inputs['subsite'] = request_inputs.refdes.str[:8]
request_inputs['platform'] = request_inputs.refdes.str[9:14]
request_inputs['instrument'] = request_inputs.refdes.str[15:27]
request_inputs['start_date'] = pd.to_datetime(request_inputs['start_date'])
request_inputs['start_date'] = request_inputs.start_date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
request_inputs['end_date'] = pd.to_datetime(request_inputs['end_date'])
request_inputs['end_date'] = request_inputs.end_date.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

request_inputs['urls'] = DATA_URL+\
                        request_inputs.subsite+\
                        '/'+request_inputs.platform+\
                        '/'+request_inputs.instrument+\
                        '/'+request_inputs.method+\
                        '/'+request_inputs.stream+\
                        '?beginDT='+request_inputs.start_date+\
                        '&endDT='+request_inputs.end_date+\
                        '&limit=1000&parameters='+parameter

request_urls = request_inputs['urls'].values.tolist()








print("Sending data requests...")
print("Current time:", datetime.datetime.now())
print(len(request_urls ),"data requests being sent.")
print("Check log file in working directory for progress.")


ref_des_list = []
deployment_list = []
method_list = []
stream_list = []
timestamp_list = []


future_to_url = {pool.submit(request_data, url, username, token): url for url in request_urls}
for future in concurrent.futures.as_completed(future_to_url):
    # url = future_to_url[future]
    try: 
        data = future.result() 
        data = data.json()
        reference_designator = data[0]['pk']['subsite'] + '-' + data[0]['pk']['node'] + '-' + data[0]['pk']['sensor']
        deployment = data[0]['pk']['deployment']
        method = data[0]['pk']['method']
        stream = data[0]['pk']['stream']
        timestamp = data[0]['time']
        timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
        timestamp = timestamp.date()
        # print(reference_designator, deployment, method, stream, timestamp, future.result())

        ref_des_list.append(reference_designator)
        deployment_list.append(deployment)
        method_list.append(method)
        stream_list.append(stream)
        timestamp_list.append(timestamp)

    except:
        # print('no data for ', url)
        pass






# convert lists to data frame
data_dict = {
    'refdes':ref_des_list,
    'deployment':deployment_list,
    'method':method_list,
    'stream':stream_list,
    'date':timestamp_list}
    # add in deployment numbers
ooi_data = pd.DataFrame(data_dict, columns = ['refdes','deployment','method','stream', 'date'])
ooi_data['date'] = pd.to_datetime(ooi_data['date'])
ooi_data['date'] = ooi_data.date.dt.strftime('%Y-%m-%d')

request_inputs = request_inputs[['refdes','deployment','method','stream', 'date']]
request_inputs['date'] = pd.to_datetime(request_inputs['date'])
request_inputs['date'] = request_inputs.date.dt.strftime('%Y-%m-%d')

deployed_but_no_data = ooi_data.merge(request_inputs,indicator=True, how='outer')
deployed_but_no_data = deployed_but_no_data[deployed_but_no_data['_merge'] == 'right_only']
del deployed_but_no_data['_merge']

deployed_but_no_data['value'] = 0
ooi_data['value'] = 1

output = pd.concat([ooi_data, deployed_but_no_data])

# ooi_data = ooi_data[ooi_data.timestamp >= begin_time_set.date()]
output.to_csv(array+'.csv', index=False)

print("All requests completed.")
print("Completed at:", datetime.datetime.now())
