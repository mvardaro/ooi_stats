import pandas as pd

array = 'old/overall_stats/20171107/cabled'
stats_data = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/'+ array + '.csv'


deployment_data = pd.read_csv(stats_data)
data = pd.read_csv(stats_data)

# a simple drop duplicates after subset to just ['refdes','date','value']
# will not work because there can be both a 0 and 1 value for a reference designator
# creating a conflicting entry. It should only be 1 as 0 is implied.
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

# create method type output
deployment_data = pd.read_csv(stats_data)
data = pd.read_csv(stats_data)

x = list(data.method.values)
y = []
for i in x:
    if 'recovered' in i:
        y.append('recovered')
    elif 'telemetered' in i:
        y.append('telemetered')
    elif 'streamed' in i:
        y.append('streamed')
data['method_type'] = y
deployment_data['method_type'] = y
deployment_data['value'] = 0
data = deployment_data[data.value != 0]
data = data[['refdes','method_type','date']]
data = data.drop_duplicates()
deployment_data = deployment_data[['refdes','method_type','date']]
deployment_data = deployment_data.drop_duplicates()
deployed_but_no_data = data.merge(deployment_data,indicator=True, how='outer')
deployed_but_no_data = deployed_but_no_data[deployed_but_no_data['_merge'] == 'right_only']
del deployed_but_no_data['_merge']
deployed_but_no_data['value'] = 0
data['value'] = 1
output = pd.concat([data, deployed_but_no_data])
output.to_csv('/Users/knuth/Documents/ooi/repos/github/ooi_stats/output/'+array+'_method_type.csv', index=False)