# Get this figure: fig = py.get_figure("https://plot.ly/~ryan.blake.williams/27/")
# Get this figure's data: data = py.get_figure("https://plot.ly/~ryan.blake.williams/27/").get_data()
# Add data to this figure: py.plot(Data([Scatter(x=[1, 2], y=[2, 3])]), filename ="GA4534 (copy)", fileopt="extend")
# Get y data of first trace: y1 = py.get_figure("https://plot.ly/~ryan.blake.williams/27/").get_data()[0]["y"]

# Get figure documentation: https://plot.ly/python/get-requests/
# Add data documentation: https://plot.ly/python/file-options/

# If you're using unicode in your file, you may need to specify the encoding.
# You can reproduce this figure in Python with the following code!

# Learn about API authentication here: https://plot.ly/python/getting-started
# Find your api_key here: https://plot.ly/settings/api

import plotly.plotly as py
from plotly.graph_objs import *
#py.sign_in('username', 'api_key')

import sys

in_dir = sys.argv[1]
print("dir: " + in_dir)

in_file = "%s/cdf.csv" % in_dir

xs = []
ys = []
zs = []
with open(in_file, 'r') as fd:
    for line in fd.readlines()[1:]:
        cols = line.strip().split(',')
        xs.append(int(cols[0]))
        ys.append(int(cols[1]))
        zs.append(float(cols[7]))

trace1 = {
    "x": xs,
    "y": ys,
    "z": zs,
    "colorbar": {"title": "Fraction of target loci covered"},
    "name": "Col5",
    "type": "heatmap",
    "uid": "024769",
    #"xsrc": "ryan.blake.williams:24:JE6DIO3T4CMM1OIPIGBXKGBFGDDX0TR2",
    #"ysrc": "ryan.blake.williams:24:O6KEY9AQXYA872567DKMTJN7D42470D0",
    "zmax": 1,
    "zmin": 0
    #"zsrc": "ryan.blake.williams:24:YL3LX4NJ6IFTX8JQDU2FZO8UAZOQRSY8"
}
data = Data([trace1])
layout = {
    "autosize": False,
    "height": 400,
    "margin": {
        "r": 60,
        "t": 60,
        "autoexpand": True,
        "b": 60,
        "l": 60
    },
    "title": "",
    "width": 600,
    "xaxis": {
        "autorange": False,
        "range": [-0.301029995664, 3.0],
        "title": "Tumor depth",
        "type": "log"
    },
    "yaxis": {
        "autorange": False,
        "range": [-0.301029995664, 3.0],
        "title": "Normal depth",
        "type": "log"
    }
}
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig)
