import requests
import json
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


ooi_data = pd.read_csv('/Users/knuth/Documents/ooi/repos/github/ooi_stats/sandbox/output/days.csv', parse_dates=True)
ooi_data_plotting = ooi_data
ooi_data_plotting['platform'] = ooi_data_plotting.refdes.str[:8]
ooi_data_plotting['array'] = ooi_data_plotting.refdes.str[:2]
ooi_data_plotting['timestamp'] = pd.to_datetime(ooi_data_plotting['timestamp'])

refdes_streams = '/Users/knuth/Documents/ooi/repos/github/ooi_stats/sandbox/cabled.csv'
refdes_streams_df = pd.read_csv(refdes_streams, parse_dates=True) # read in csv (for now).
refdes_streams_df = refdes_streams_df.drop(['stream','method'], axis = 1)

ooi_data_plotting = pd.concat([ooi_data_plotting, refdes_streams_df])



platforms = []

for index, row in ooi_data_plotting.iterrows():
    platforms.append(row['platform'])
    
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