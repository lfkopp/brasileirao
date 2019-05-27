

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



short = pd.read_csv('brasileirao_2019.txt',sep=';', index_col=False)
short.data = pd.to_datetime(short.data)
last_short = short[short['data'] == max(short['data'])]
short_final = last_short[last_short.columns[1:]].set_index('time')
short_final.sort_values(by=[str(i) for i in list(range(20,0,-1))], inplace=True)
short_final.plot(kind='bar',stacked=True,figsize=(20,15), colormap='autumn')
plt.savefig('figs\short_final_2019.png')



long = pd.read_csv('brasileirao_long_2019.txt',sep=';', index_col=False, decimal=',')
long.data = pd.to_datetime(long.data)
long['points'] = (20-long['pos']) * long['chance']
long2 = long.groupby(['data','time'])['points'].mean().unstack()
long2.sort_values(long2.columns.max()).plot(kind='area',stacked=True,figsize=(15,20), colormap='brg')
plt.savefig('figs\long2_stacked_2019.png')


long2 = long.groupby(['time','data'])['points'].mean().unstack()
long2.sort_values(long2.columns.max()).plot(kind='barh',stacked=False,figsize=(15,20), colormap='autumn')
plt.savefig('figs\long2_2019.png')


long3 = long[long['data'] == max(long['data'])]
times = long3['time'].unique()
from scipy.ndimage.filters import gaussian_filter1d

fig= plt.figure(figsize=(15,10))

for t in times:
    sublong = long3[long3['time'] == t]
    ysmoothed = gaussian_filter1d(sublong.chance, sigma=3)
    plt.plot(sublong.pos,ysmoothed,label=t)
plt.legend(loc=1,ncol=5,fontsize='medium')
plt.xticks(np.arange(1, 21, 1))
plt.grid()
plt.xlim(1,20)
plt.ylim(0)
plt.ylabel('Chances (%)')
plt.xlabel('Position')
plt.title('Chances for each team falling into x^th position at the end of the Brasileirão')
plt.savefig('figs\long3_2019.png')
