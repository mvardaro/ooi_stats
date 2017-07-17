import requests
import json
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

USERNAME ='OOIAPI-9N9UMLHV9W5GOP'
TOKEN= 'SJN6HXHH116OZ8'


refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/sandbox/cabled.csv'

# need to specify a minute into day, else data from previous day is requested
begin_time_set = datetime.datetime(2017, 1, 1, 0,0,0) 
# days = 5


# request only parameter 7 (time)
parameter = '7' 

# read in csv (for now)
refdes_streams_df = pd.read_csv(refdes_streams, parse_dates=True) # read in csv (for now).

# prepare time stamp converters
ntp_epoch = datetime.datetime(1900, 1, 1)
unix_epoch = datetime.datetime(1970, 1, 1)
ntp_delta = (unix_epoch - ntp_epoch).total_seconds()
now = datetime.datetime.now()
days = abs(begin_time_set.date() - now.date())
days = int(days.days)

# create some empty lists as inputs for your final data frame output
ref_des_list = []
stream_list = []
timestamp_list = []




# interate over reference designators, delivery methods and streams

for index, row in refdes_streams_df.iterrows():
    ref_des =  row['refdes']
    stream = row['stream']
    delivery_method = row['method']
    
    begin_time = begin_time_set
    begin_time_str = begin_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_time = begin_time + datetime.timedelta(seconds=86400)
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    # print ref_des,stream,delivery_method
    
    # while begin_time < now: 
    for i in range(days): 
        
        try:        
            response = requests.get('https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'+
                                    ref_des[:8]+'/'+ref_des[9:14]+'/'+ref_des[15:27]+'/'+
                                    delivery_method+'/'+stream+
                                    '?beginDT='+begin_time_str+'&endDT='+end_time_str+
                                    '&limit=1000&parameters='+parameter, 
                                    auth=(USERNAME, TOKEN))
            data = response.json()
            timestamp = data[0]['time']
            timestamp = datetime.datetime.utcfromtimestamp(timestamp - ntp_delta).replace(microsecond=0)
            timestamp = timestamp.date()

            # print timestamp

            ref_des_list.append(ref_des)
            stream_list.append(stream)
            timestamp_list.append(timestamp)

        except:
            data = response.json()
            # print "\n From", begin_time_str, end_time_str, ":"
            # print data['status']


        # move to next day
        begin_time = begin_time + datetime.timedelta(seconds=86400)
        begin_time_str = begin_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        end_time = end_time + datetime.timedelta(seconds=86400)
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')



# convert lists to data frame
data_dict = {
    'refdes':ref_des_list,
    'stream':stream_list,
    'timestamp':timestamp_list}
ooi_data = pd.DataFrame(data_dict, columns = ['refdes', 'stream', 'timestamp'])


# collapse by unique ref_des and day with data
ooi_data = ooi_data.drop('stream', axis = 1)
ooi_data = ooi_data.drop_duplicates()
ooi_data = ooi_data[ooi_data.timestamp >= begin_time_set.date()]
ooi_data.to_csv('output/ooi_data.csv', index=False)

ooi_data_days = ooi_data.groupby('refdes')['timestamp'].nunique().reset_index(name="days_of_data_count")
# add back any ref_des for which no data was found in the system
refdes_streams_df = refdes_streams_df.drop(['stream','method'], axis = 1)
ooi_data_days = pd.concat([ooi_data_days, refdes_streams_df])
ooi_data_days = ooi_data_days.fillna(value=0)
ooi_data_days = ooi_data_days.groupby('refdes')['days_of_data_count'].sum().reset_index(name='days_of_data_count')
ooi_data_days.to_csv('output/ooi_data_days.csv', index=False)


# count ref_des on platform
ooi_data_days['platform'] = ooi_data_days.refdes.str[:8]
platform = ooi_data_days.groupby('platform')['refdes'].nunique().reset_index(name="inst_on_platform_count")
ooi_data_days = pd.merge(ooi_data_days,platform, on='platform')

# count ref_des on array
ooi_data_days['array'] = ooi_data_days.refdes.str[:2]
array = ooi_data_days.groupby('array')['refdes'].nunique().reset_index(name="inst_on_array_count")
ooi_data_days = pd.merge(ooi_data_days,array, on='array')

# for each ref_des - count days with data and divide by total days over time range requested
ooi_data_inst = pd.DataFrame(ooi_data_days, columns = ['refdes', 'days_of_data_count'])
ooi_data_inst['inst_percent'] = ooi_data_inst['days_of_data_count'] / days *100
ooi_data_inst.to_csv('output/instrument_percent.csv', columns = ['refdes', 'inst_percent'], index=False)


ooi_data_platform = pd.DataFrame(ooi_data_days, columns = ['platform', 'days_of_data_count','inst_on_platform_count'])
ooi_data_platform = ooi_data_platform.groupby(['platform','inst_on_platform_count'])['days_of_data_count'].sum().reset_index(name='days_of_data_count')
ooi_data_platform['platform_percent'] = ooi_data_platform['days_of_data_count'] / (days * ooi_data_platform['inst_on_platform_count']) *100
ooi_data_platform.to_csv('output/platform_percent.csv', columns = ['platform', 'platform_percent'], index=False)


ooi_data_array = pd.DataFrame(ooi_data_days, columns = ['array', 'days_of_data_count','inst_on_array_count'])
ooi_data_array = ooi_data_array.groupby(['array','inst_on_array_count'])['days_of_data_count'].sum().reset_index(name='days_of_data_count')
ooi_data_array['array_percent'] = ooi_data_array['days_of_data_count'] / (days * ooi_data_array['inst_on_array_count']) *100
ooi_data_array.to_csv('output/array_percent.csv', columns = ['array', 'array_percent'], index=False)


# plot instruments
ooi_data_plotting = ooi_data
ooi_data_plotting['platform'] = ooi_data_plotting.refdes.str[:8]
ooi_data_plotting['array'] = ooi_data_plotting.refdes.str[:2]
ooi_data_plotting = pd.concat([ooi_data_plotting, refdes_streams_df])


yticks=[]

for index, row in ooi_data_plotting.iterrows():
    yticks.append(row["refdes"])
    
yticks = pd.unique(yticks)
yticks = yticks[::-1]
y = np.arange(len(yticks))
counter = -1

for x in y:
    for index, row in ooi_data.iterrows():
        if yticks[counter] == row["refdes"]:
            StartTime = row['timestamp']
            EndTime = row['timestamp'] + datetime.timedelta(seconds=86400)
            stream_time = np.array([StartTime,EndTime])
            stream_shape = np.full((stream_time.shape),y[counter])
            plt.plot(stream_time, stream_shape, linewidth=10, color='blue')
    counter = counter -1

plt.yticks(y, yticks)      
plt.xticks(rotation=20)
plt.tight_layout()
plt.grid()
plt.savefig('instruments.png', figsize=(16, 16), dpi=300,)
# plt.show()

# plot platforms
plt.close()
yticks=[]

for index, row in ooi_data_plotting.iterrows():
    yticks.append(row["platform"])
    
yticks = pd.unique(yticks)
yticks = yticks[::-1]
y = np.arange(len(yticks))
counter = -1

for x in y:
    for index, row in ooi_data.iterrows():
        if yticks[counter] == row["refdes"][:8]:
            StartTime = row['timestamp']
            EndTime = row['timestamp'] + datetime.timedelta(seconds=86400)
            stream_time = np.array([StartTime,EndTime])
            stream_shape = np.full((stream_time.shape),y[counter])
            plt.plot(stream_time, stream_shape, linewidth=10, color='blue')
    counter = counter -1

plt.yticks(y, yticks)      
plt.xticks(rotation=20)
plt.tight_layout()
plt.grid()
plt.savefig('platforms.png', figsize=(16, 16), dpi=300,)
# plt.show()

# plot arrays
plt.close()
yticks=[]

for index, row in ooi_data_plotting.iterrows():
    yticks.append(row["array"])
    
yticks = pd.unique(yticks)
yticks = yticks[::-1]
y = np.arange(len(yticks))
counter = -1

for x in y:
    for index, row in ooi_data.iterrows():
        if yticks[counter] == row["refdes"][:2]:
            StartTime = row['timestamp']
            EndTime = row['timestamp'] + datetime.timedelta(seconds=86400)
            stream_time = np.array([StartTime,EndTime])
            stream_shape = np.full((stream_time.shape),y[counter])
            plt.plot(stream_time, stream_shape, linewidth=10, color='blue')
    counter = counter -1

plt.yticks(y, yticks)      
plt.xticks(rotation=20)
plt.tight_layout()
plt.grid()
plt.savefig('arrays.png', figsize=(16, 16), dpi=300,)
# plt.show()

# plot instruments
platforms = []

for index, row in ooi_data_plotting.iterrows():
    platforms.append(row["platform"])
    
platforms = pd.unique(platforms)



for i in platforms:
    plt.close()
    yticks=[]
    
    for index, row in ooi_data_plotting.iterrows():
        if i == row["refdes"][:8]:
            yticks.append(row["refdes"])
    yticks = pd.unique(yticks)
    yticks = yticks[::-1]
    y = np.arange(len(yticks))
    counter = -1
    
    for x in y:
        for index, row in ooi_data.iterrows():
            if yticks[counter] == row["refdes"]:
                StartTime = row['timestamp']
                EndTime = row['timestamp'] + datetime.timedelta(seconds=86400)
                stream_time = np.array([StartTime,EndTime])
                stream_shape = np.full((stream_time.shape),y[counter])
                plt.plot(stream_time, stream_shape, linewidth=10, color='blue')
        counter = counter -1
    
    plt.yticks(y, yticks)      
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.grid()
    plt.savefig(str(i) + '.png', figsize=(16, 16), dpi=300,)
    # plt.show()