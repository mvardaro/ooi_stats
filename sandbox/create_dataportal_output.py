import pandas as pd

# read in data
array = 'cabled'

data_file = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/'+array+'/data.csv'
deployment_file = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/'+array+'/deployment_data.csv'

data = pd.read_csv(data_file)
data['date'] = pd.to_datetime(data['date']).dt.date

deployment_file_df = pd.read_csv(deployment_file)
deployment_file_df['date'] = pd.to_datetime(deployment_file_df['date']).dt.date





data_temp = data[['refdes', 'date']]
data_temp = data_temp.drop_duplicates()
data_temp['value'] = 1

deployment_file_df_temp = deployment_file_df[['refdes', 'date']]
deployment_file_df_temp = deployment_file_df_temp.drop_duplicates()

# right_only = no data, left_only = should be 0 unless system returns data outside deployment time ranges
deployed_but_no_data = data_temp.merge(deployment_file_df_temp,on =['refdes','date'], indicator=True, how='outer')
deployed_but_no_data = deployed_but_no_data[deployed_but_no_data['_merge'] == 'right_only']
del deployed_but_no_data['_merge']
deployed_but_no_data['value'] = 0


# deployed_but_no_data = list(set(zip(deployment_file_df_temp.refdes ,deployment_file_df_temp.date)) - set(zip(data_temp.refdes, data_temp.date)))
# deployed_but_no_data = pd.DataFrame(deployed_but_no_data, columns=['refdes','date'])


output = pd.concat([data_temp, deployed_but_no_data])
output = output[['refdes','date','value']].sort_values(by=['refdes','date'])
output.to_csv('output/overall_stats/'+array+'/'+array+'_refdes_stats_data.csv', index=False)





# cabled_csv = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/cabled/cabled_refdes_stats_data.csv'
# global_csv = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/global/global_refdes_stats_data.csv'
# pioneer_csv ='/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/pioneer/pioneer_refdes_stats_data.csv'
# endurance_csv = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/overall_stats/endurance/endurance_refdes_stats_data.csv'

# cabled_df = pd.read_csv(cabled_csv)
# global_df = pd.read_csv(global_csv)
# pioneer_df = pd.read_csv(pioneer_csv)
# endurance_df = pd.read_csv(endurance_csv)


# final = pd.concat([global_df,cabled_df,pioneer_df,endurance_df]).sort_values(by=['refdes','date'])

# final.to_csv('refdes.csv',index=False)