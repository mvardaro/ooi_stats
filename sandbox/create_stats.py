import pandas as pd
import datetime
import calendar
import copy

# read in data
array = 'cabled'
data_file = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/'+array+'/data.csv'
refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/input/'+array+'.csv'
data = pd.read_csv(data_file)
refdes_streams_df = pd.read_csv(refdes_streams)

now = datetime.datetime.now()
begin_time_set = datetime.datetime(2013, 7, 1, 0,0,0)


# extract only refdes and drop duplicated dates from multiple streams
data = pd.DataFrame(data, columns=['refdes', 'date'])
data = data.drop_duplicates()
refdes_streams_df = pd.DataFrame(refdes_streams_df, columns=['refdes'])


# append refdes for which no data was returned
data_temp = pd.DataFrame(data, columns=['refdes'])
data_temp = list(set(refdes_streams_df['refdes'].values) - set(data_temp['refdes'].values))
data_temp = pd.DataFrame(data_temp,columns=['refdes'])
data = pd.concat([data, data_temp])


# calculate month stats based on total days of data requestes
timetable_months = data
timetable_months['date'] = pd.to_datetime(timetable_months['date'])
timetable_months['month'] = timetable_months['date'].dt.strftime('%Y-%m')
timetable_months = timetable_months.groupby(['refdes', 'month'])['date'].nunique().reset_index(name="days_of_data")


# create denominator
def to_integer(dt_time):
    return 10000*dt_time.year + 100 * dt_time.month + dt_time.day

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


# create monthly percent by reference designator
timetable_months_refdes = copy.deepcopy(timetable_months)
timetable_months_refdes['percent'] = timetable_months_refdes['days_of_data'] / timetable_months_refdes['days_in_month']
timetable_months_refdes = timetable_months_refdes.pivot(index='refdes', columns='month', values='percent')
timetable_months_refdes.to_csv('output/'+array+'/stats/timetable_refdes_monthly.csv', index=True)


# create monthly percentages by node
timetable_months_node = copy.deepcopy(timetable_months)
timetable_months_node['node'] = timetable_months_node['refdes'].str[:14]
node = timetable_months_node.groupby('node')['refdes'].nunique().reset_index(name="inst_on_node_count")
timetable_months_node = pd.merge(timetable_months_node,node, on='node')


timetable_months_node['refdes_percent'] = timetable_months_node['days_of_data'] / timetable_months_node['days_in_month']
node_percent = timetable_months_node.groupby(['node','month'])['refdes_percent'].sum().reset_index(name="percent_node")
timetable_months_node =  pd.merge(timetable_months_node,node_percent, on=['node','month'])
timetable_months_node = pd.DataFrame(timetable_months_node, columns=['month','node','inst_on_node_count','percent_node'])
timetable_months_node['percent_node'] = timetable_months_node['percent_node'] / timetable_months_node['inst_on_node_count']
timetable_months_node = timetable_months_node.drop_duplicates()
timetable_months_node = timetable_months_node.drop(['inst_on_node_count'],axis=1)
timetable_months_node = timetable_months_node.pivot(index='node', columns='month', values='percent_node')
timetable_months_node.to_csv('output/'+array+'/stats/timetable_node_monthly.csv', index=True)


# create monthly percentages by platform
timetable_months_platform = copy.deepcopy(timetable_months)
timetable_months_platform['platform'] = timetable_months_platform['refdes'].str[:8]
platform = timetable_months_platform.groupby('platform')['refdes'].nunique().reset_index(name="inst_on_platform_count")
timetable_months_platform = pd.merge(timetable_months_platform,platform, on='platform')


timetable_months_platform['refdes_percent'] = timetable_months_platform['days_of_data'] / timetable_months_platform['days_in_month']
platform_percent = timetable_months_platform.groupby(['platform','month'])['refdes_percent'].sum().reset_index(name="percent_platform")
timetable_months_platform =  pd.merge(timetable_months_platform,platform_percent, on=['platform','month'])
timetable_months_platform = pd.DataFrame(timetable_months_platform, columns=['month','platform','inst_on_platform_count','percent_platform'])
timetable_months_platform['percent_platform'] = timetable_months_platform['percent_platform'] / timetable_months_platform['inst_on_platform_count']
timetable_months_platform = timetable_months_platform.drop_duplicates()
timetable_months_platform = timetable_months_platform.drop(['inst_on_platform_count'],axis=1)
timetable_months_platform = timetable_months_platform.pivot(index='platform', columns='month', values='percent_platform')
timetable_months_platform.to_csv('output/'+array+'/stats/timetable_platform_monthly.csv', index=True)


# create monthly percentages by array
timetable_months_array = copy.deepcopy(timetable_months)
timetable_months_array['array'] = timetable_months_array['refdes'].str[:2]
array_df = timetable_months_array.groupby('array')['refdes'].nunique().reset_index(name="inst_on_array_count")
timetable_months_array = pd.merge(timetable_months_array,array_df, on='array')



timetable_months_array['refdes_percent'] = timetable_months_array['days_of_data'] / timetable_months_array['days_in_month']
array_percent = timetable_months_array.groupby(['array','month'])['refdes_percent'].sum().reset_index(name="percent_array")
timetable_months_array =  pd.merge(timetable_months_array,array_percent, on=['array','month'])
timetable_months_array = pd.DataFrame(timetable_months_array, columns=['month','array','inst_on_array_count','percent_array'])
timetable_months_array['percent_array'] = timetable_months_array['percent_array'] / timetable_months_array['inst_on_array_count']
timetable_months_array = timetable_months_array.drop_duplicates()
timetable_months_array = timetable_months_array.drop(['inst_on_array_count'],axis=1)
timetable_months_array = timetable_months_array.pivot(index='array', columns='month', values='percent_array')
timetable_months_array.to_csv('output/'+array+'/stats/timetable_array_monthly.csv', index=True)