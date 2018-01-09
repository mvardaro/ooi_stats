import matplotlib.pyplot as plt
import requests
from requests.packages.urllib3.util.retry import Retry
import json
import datetime
import pandas as pd
from math import isnan
import concurrent.futures
import logging
import gc
from pprint import pprint
import os


username = ''
token = ''
# arrays = ['RS','CE','CP','GA','GI','GP','GS']

arrays = ['RS']

QC_PARAMETER_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12578/qcparameters/'
DEPLOYEMENT_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/inv/'
DATA_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'
DATA_TEAM_PORTAL_URL = 'http://ooi.visualocean.net/data-streams/science/'


print("requesting qc data...")
r = requests.get(QC_PARAMETER_URL, auth=(username, token))
data = r.json()

refdes_qc_list = []
parameter_qc_list = []
globalrange_min_qc_list = []

for i in range(len(data)):
    if data[i]['qcParameterPK']['qcId'] == 'dataqc_globalrangetest_minmax' \
    and data[i]['qcParameterPK']['parameter'] == 'dat_min':
        
        refdes = data[i]['qcParameterPK']['refDes']['subsite']+'-'+\
            data[i]['qcParameterPK']['refDes']['node']+'-'+\
            data[i]['qcParameterPK']['refDes']['sensor']
        refdes_qc_list.append(refdes)
        
        parameter = data[i]['qcParameterPK']['streamParameter']
        parameter_qc_list.append(parameter)
        
        globalrange_min = data[i]['value']
        globalrange_min_qc_list.append(globalrange_min)

qc_dict = {
    'refdes':refdes_qc_list,
    'parameter':parameter_qc_list,
    'global_range_min':globalrange_min_qc_list,
}     
        
globalrange_min_qc_data = pd.DataFrame(qc_dict,columns=['refdes','parameter','global_range_min'])

refdes_qc_list = []
parameter_qc_list = []
globalrange_max_qc_list = []

for i in range(len(data)):
    if data[i]['qcParameterPK']['qcId'] == 'dataqc_globalrangetest_minmax' \
    and data[i]['qcParameterPK']['parameter'] == 'dat_max':
        
        refdes = data[i]['qcParameterPK']['refDes']['subsite']+'-'+\
            data[i]['qcParameterPK']['refDes']['node']+'-'+\
            data[i]['qcParameterPK']['refDes']['sensor']
        refdes_qc_list.append(refdes)
        
        parameter = data[i]['qcParameterPK']['streamParameter']
        parameter_qc_list.append(parameter)
        
        globalrange_max = data[i]['value']
        globalrange_max_qc_list.append(globalrange_max)

qc_dict = {
    'refdes':refdes_qc_list,
    'parameter':parameter_qc_list,
    'global_range_max':globalrange_max_qc_list,
}     
        
globalrange_max_qc_data = pd.DataFrame(qc_dict,columns=['refdes','parameter','global_range_max'])

global_ranges = pd.merge(globalrange_min_qc_data,globalrange_max_qc_data, on=['refdes','parameter'], how='outer')


# set up some functions
def request_data(url,username,token):
    auth = (username, token)
    return session.get(url,auth=auth)

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

def diff_days(d1,d2):
    return (d2 - d1).days

ntp_epoch = datetime.datetime(1900, 1, 1)
unix_epoch = datetime.datetime(1970, 1, 1)
ntp_delta = (unix_epoch - ntp_epoch).total_seconds()

pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
session = requests.session()
retry = Retry(
        total=10,
        backoff_factor=0.3,
    )
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100,max_retries=retry,pool_block=True)
session.mount('http://', adapter)

#make output directory
output_dir = datetime.datetime.now().strftime('%Y%m%d') + '/'
new_dir = 'output/'+ output_dir
if not os.path.isdir(new_dir):
    try:
        os.mkdir(new_dir)
    except OSError:
        if os.path.exists(new_dir):
            pass
        else:
            raise

for array in arrays:

    log_filename = array
    logging.basicConfig(filename=log_filename+'_requests.log',level=logging.DEBUG,filemode='w')

    refdes_in = DATA_TEAM_PORTAL_URL + array
    refdes_list = pd.read_csv(refdes_in)
    refdes_list = refdes_list[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_list.columns = ['refdes','method', 'stream','parameter']
    refdes_list = refdes_list['refdes']
    refdes_list = refdes_list.drop_duplicates()

    print('\n'+"working on", array)
    print("building deployment info requests...")
    asset_requests = []
    for i in refdes_list:
        sub_site = i[:8]
        platform = i[9:14]
        instrument = i[15:27]
        asset_url_inputs = '/'.join((sub_site, platform, instrument))
        request_url = DEPLOYEMENT_URL+asset_url_inputs+'/-1'
        asset_requests.append(request_url)

    print("sending deployment info requests...")
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


    print("calculating days between deployment dates...")
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

    print("building data request urls...")
    deployment_data_days['start_date'] = deployment_data_days['date'] + datetime.timedelta(seconds=5)
    deployment_data_days['end_date'] = deployment_data_days['date'] + datetime.timedelta(seconds=86395)

    qc_db_input = DATA_TEAM_PORTAL_URL + array
    qc_db_input = pd.read_csv(qc_db_input)
    refdes_streams_df = qc_db_input[['reference_designator','method', 'stream_name','parameter_name']]
    refdes_streams_df.columns = ['refdes','method', 'stream','parameter']
    refdes_streams_df = refdes_streams_df.drop_duplicates()

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
                            '&limit=50'

    request_urls = request_inputs['urls'].drop_duplicates()
    request_urls = request_urls.tolist()


    print("comparing qc data base reference designator science parameter combinations to production qc global range lookup table...")

    new_dir = 'output/'+ output_dir+'descrepancies/'
    if not os.path.isdir(new_dir):
        try:
            os.mkdir(new_dir)
        except OSError:
            if os.path.exists(new_dir):
                pass
            else:
                raise

    ranges = global_ranges[['refdes','parameter']].drop_duplicates()
    ranges = ranges[ranges.refdes.str.startswith(array)]
    expected = refdes_streams_df[['refdes','parameter']].drop_duplicates()

    not_found = ranges.merge(expected,indicator=True, how='outer')
    missing_GL_QC_values = not_found[not_found['_merge'] == 'right_only']
    del missing_GL_QC_values['_merge']
    missing_GL_QC_values.to_csv('output/'+ output_dir + 'descrepancies/' + array +'_missing_GL_QC_values.csv', index=False)

    missing_science_classification = not_found[not_found['_merge'] == 'left_only']
    del missing_science_classification['_merge']
    missing_science_classification.to_csv('output/'+ output_dir + 'descrepancies/' + array +'_missing_science_classification.csv', index=False)


    print("sending data requests for", array+'...')
    print('\t',"current time:", datetime.datetime.now())
    print('\t',len(request_urls ),"data requests being sent")
    print('\t',"check",array+"_requests.log","file in your working directory for progress")

    finaldf = pd.DataFrame()
    missing = []

    future_to_url = {pool.submit(request_data, url, username, token): url for url in request_urls}
    for future in concurrent.futures.as_completed(future_to_url):
    #     url = future_to_url[future]
        try:
            data = future.result() 
            data = data.json()

            refdes_list = []
            parameter_list = []
            method_list = []
            stream_list = []
            timestamp_list = []
            value_list = []
            
            # use this to speed up the loop
    #         df = pd.DataFrame.from_records(map(json.loads, map(json.dumps,data)))
            
            # iterate through data points to extract time stamps
            for i in range(len(data)):
                timestamp = data[i]['time']
                timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
                timestamp = timestamp.date()
                
          
                # get refdes from the response and create data frame y with the corresponding gloabl range values
                refdes = data[i]['pk']['subsite'] + '-' + data[i]['pk']['node'] + '-' + data[i]['pk']['sensor']
                method = data[i]['pk']['method']
                stream = data[i]['pk']['stream']
                x = global_ranges['refdes'] == refdes
                y = global_ranges[x]

                # check if global range list contains an entry for the refdes
                templist = list(global_ranges['refdes'])
                if refdes not in templist:
                    missing.append(refdes)
                
    #           capture presence of time in stream, even if no science parameters found
                refdes_list.append(refdes)
                method_list.append(method)
                stream_list.append(stream)
                parameter_list.append('time')
                value_list.append(data[i]['time'])
                timestamp_list.append(timestamp)
                    
                # iterate through all variables in global range data frame y, then iterate through keys in data point
                # to find matching keys, then grab values
                for var in y.parameter.values:
                    for j in data[i].keys():
                        if var == j:
                            z = data[i][j]
                            
                            # conditional to handle 2d datasets, in which case the first non nan value is checked
                            if type(z) != list:
                                refdes_list.append(refdes)
                                method_list.append(method)
                                stream_list.append(stream)
                                parameter_list.append(var)
                                value_list.append(z)
                                timestamp_list.append(timestamp)
                            else:
                                u = next(u for u in z if not isnan(u))
                                refdes_list.append(refdes)
                                method_list.append(method)
                                stream_list.append(stream)
                                parameter_list.append(var)
                                value_list.append(u)
                                timestamp_list.append(timestamp)

            # create data frame from lists collected above
            data_dict = {
                'refdes':refdes_list,
                'method':method_list,
                'stream':stream_list,
                'parameter':parameter_list,
                'value':value_list,
                'date':timestamp_list}
            response_data = pd.DataFrame(data_dict, columns = ['refdes','method','stream','parameter','value','date'])
            
            # subset to mode time stamp of response to omit data returned outside time range (day) requested
            response_data = response_data.loc[response_data['date'] == response_data['date'].mode()[0]]
            data_length = len(response_data[response_data['parameter'] == 'time'])

            # merge into data frame with global range values and check if value between global ranges
            df = y.merge(response_data,how='outer')
            df['pass'] = (df['value'] < pd.to_numeric(df['global_range_max'])) & \
                            (df['value'] > pd.to_numeric(df['global_range_min'])) 
            
            # assign true to all time parameter instances
            df.loc[df['parameter'] == 'time', 'pass'] = True

            # collapse the data frame to calculate percent of data points that pass the test for that day
            df2 = df['pass'].groupby([df['refdes'], \
                                      df['method'], \
                                      df['stream'], \
                                      df['parameter'],\
                                      df['date'] \
                                     ]).sum().reset_index()
            df2['percent'] = (df2['pass'] / data_length) * 100
            df2['data_points'] = data_length
    #         df2 = df2[['refdes','method','stream','parameter','date','data_points','percent']]

            # append result for this ref des and day to final data frame
            finaldf = finaldf.append(df2)
                
        except:
    #         print('no data for ', url)
            pass

    request_inputs = request_inputs[['refdes','method','stream', 'parameter','date']]
    request_inputs = request_inputs.drop_duplicates()
    request_inputs['date'] = pd.to_datetime(request_inputs['date'])
    request_inputs['date'] = request_inputs.date.dt.strftime('%Y-%m-%d')

    finaldf['date'] = pd.to_datetime(finaldf['date'])
    finaldf['date'] = finaldf.date.dt.strftime('%Y-%m-%d')

    deployed_but_no_data = finaldf.merge(request_inputs,indicator=True,how='outer',on=['refdes','method','stream','parameter','date'])
    deployed_but_no_data = deployed_but_no_data[deployed_but_no_data['_merge'] == 'right_only']
    del deployed_but_no_data['_merge']

    output = pd.concat([finaldf, deployed_but_no_data])
    output.to_csv('output/'+ output_dir + array+'_quality.csv', index=False)

    gc.collect()