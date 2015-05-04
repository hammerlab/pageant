
from math import log
import matplotlib.pyplot as plt
from matplotlib.ticker import NullFormatter
import numpy as np
import os
import sys

def convert_num(arg):
    if arg[-1] == 'k':
        return int(arg[:-1])*1000
    if arg[-1] == 'm':
        return int(arg[:-1])*1000000
    return int(arg)

num_rows = convert_num(sys.argv[1]) if len(sys.argv) > 1 else 1000
depths_filename = sys.argv[2] if len(sys.argv) > 2 else 'depths-by-max.csv'

data_dir = os.path.join(os.getenv('HOME'), 'pt189')
def data(fn):
    return os.path.join(data_dir, fn)

d = np.loadtxt(data(depths_filename), dtype=int, delimiter=',')
x = d[:num_rows,0]
y = d[:num_rows,1]
v = d[:num_rows,2]

rdh1 = np.loadtxt(data('rdh1full.csv'), delimiter=',', converters={ 0: int, 1: int, 2: float, 3: int, 4: float })
rdh2 = np.loadtxt(data('rdh2full.csv'), delimiter=',', converters={ 0: int, 1: int, 2: float, 3: int, 4: float })

rdh1nums = rdh1[:,0]
rdh1weights = rdh1[:,1]
rdh2nums = rdh2[:,0]
rdh2weights = rdh2[:,1]

# the random data
#x = np.random.randn(1000)
#y = np.random.randn(1000)

nullfmt   = NullFormatter()         # no labels

# definitions for the axes
left, width = 0.1, 0.65
bottom, height = 0.1, 0.65
bottom_h = left_h = left+width+0.02

rect_scatter = [left, bottom, width, height]
rect_histx = [left, bottom_h, width, 0.2]
rect_histy = [left_h, bottom, 0.2, height]

# start with a rectangular Figure
plt.figure(1, figsize=(10,10))

axScatter = plt.axes(rect_scatter)
axHistx = plt.axes(rect_histx)
axHisty = plt.axes(rect_histy)

# no labels
axHistx.xaxis.set_major_formatter(nullfmt)
axHisty.yaxis.set_major_formatter(nullfmt)

# the scatter plot:
axScatter.scatter(x, y, c=v, marker='s', s=[ (1+log(i)**2) for i in v ], alpha=0.002)

# now determine nice limits by hand:
binwidth = 1 #0.25
xmax = np.max(np.fabs(x))
ymax = np.max(np.fabs(y))
#xymax = np.max( [np.max(np.fabs(x)), np.max(np.fabs(y))] )

#lim = ( int(xymax/binwidth) + 1) * binwidth
limx = ( int(xmax/binwidth) + 1) * binwidth
limy = ( int(ymax/binwidth) + 1) * binwidth

llimx = -0.1
llimy = -1

axScatter.set_xlim( (llimx, limx) )
axScatter.set_ylim( (llimy, limy) )

#bins = np.arange(-lim, lim + binwidth, binwidth)
binsx = np.arange(0, limx + binwidth, binwidth)
binsy = np.arange(0, limy + binwidth, binwidth)
axHistx.hist(rdh1nums, bins=binsx, range=(0, xmax), weights=rdh1weights, log=True)
axHisty.hist(rdh2nums, bins=binsy, range=(0, ymax), weights=rdh2weights, log=True, orientation='horizontal')

axHistx.set_xlim( axScatter.get_xlim() )
axHisty.set_ylim( axScatter.get_ylim() )

plt.show()
