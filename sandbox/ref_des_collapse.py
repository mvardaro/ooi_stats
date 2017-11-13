import pandas as pd

array = 'endurance'
stats_data = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/'+ array + '.csv'



deployment_data = pd.read_csv(stats_data)
data = pd.read_csv(stats_data)


deployment_data['value'] = 0


data = deployment_data[data.value != 0]


data = data[['refdes','date']]
data = data.drop_duplicates()
deployment_data = deployment_data[['refdes','date']]
deployment_data = deployment_data.drop_duplicates()


deployed_but_no_data = data.merge(deployment_data,indicator=True, how='outer')
deployed_but_no_data = deployed_but_no_data[deployed_but_no_data['_merge'] == 'right_only']



del deployed_but_no_data['_merge']
deployed_but_no_data['value'] = 0
data['value'] = 1


output = pd.concat([data, deployed_but_no_data])


output.to_csv('/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/'+array+'_refdes.csv', index=False)