import matplotlib.pyplot as plt
import requests
import json
import datetime
import pandas as pd
import concurrent.futures
import logging


username = 'OOIAPI-30AZZ33CYL06XZ'
token = '77CEUHU3VZ9'
array = 'pioneer'
input_path = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/'

# set up threads pool and execute requests
pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)
session = requests.session()

# set up function to send requests
def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)

# base url for the request that will be built using the inputs above.
BASE_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/inv/'

# request only parameter 7 (time)
parameter = '7'

# read in csv
refdes = input_path + array + '.csv'
refdes_list = pd.read_csv(refdes)
refdes_list = refdes_list['refdes']
refdes_list = refdes_list.drop_duplicates()


print("Building instrument requests...")

asset_requests = []
for i in refdes_list:
    sub_site = i[:8]
    platform = i[9:14]
    instrument = i[15:27]
    asset_url_inputs = '/'.join((sub_site, platform, instrument))
    request_url = BASE_URL+asset_url_inputs+'/-1'
    asset_requests.append(request_url)



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
            
            deployment_number = asset_info[i]['deploymentNumber']
            deployment_list.append(deployment_number)
            
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
    'deployment_number':deployment_list,
    'start_time':start_time_list,
    'end_time':end_time_list}
deployment_data = pd.DataFrame(data_dict, columns = ['refdes', 'deployment_number','start_time', 'end_time'])


def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

def diff_days(d1,d2):
    return (d2 - d1).days


# create empty dataframe
deployment_data_days = pd.DataFrame(columns = ['refdes', 'deployment_number','date'])

# calculate days between deployment dates
for index, row in deployment_data.iterrows():
    start_time = row['start_time']
    end_time = row['end_time']
    periods = diff_days(start_time, end_time)
    start_time = to_integer(start_time)
    total_days = pd.DataFrame({'date' : pd.date_range(str(start_time),periods=periods,freq='D')})
    
    total_days['refdes'] = row['refdes']
    total_days['deployment_number'] = row['deployment_number']
    deployment_data_days = deployment_data_days.append(total_days)

    # re-order data frame columns
deployment_data_days = deployment_data_days[['refdes', 'deployment_number','date']]


deployment_data_days.to_csv('output/overall_stats/'+array+'/deployment_data.csv', index=False)