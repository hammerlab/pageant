
import plotly.plotly as py
from plotly.graph_objs import *

import sys

in_dir = sys.argv[1]
print("dir: " + in_dir)

in_file = "%s/cdf.csv" % in_dir

vals = {}
xs = {}
ys = {}
xKeys = {}
yKeys = {}
with open(in_file, 'r') as fd:
    for line in fd.readlines()[1:]:
        cols = line.strip().split(',')
        x = int(cols[0])
        y = int(cols[1])
        z = float(cols[7])

        xKeys[x] = True
        yKeys[y] = True

        if x not in vals:
            vals[x] = {}

        vals[x][y] = z

xKeys = list(sorted(xKeys.keys()))
yKeys = list(sorted(yKeys.keys()))
zs = []
for x in xKeys:
    row = [ ]
    for y in yKeys:
        row.append(vals[x][y] if x in vals and y in vals[x] else 0)
    zs.append(row)

trace1 = {
    "x": xKeys,
    "y": yKeys,
    "z": zs,
    "colorbar": {"title": "Fraction of target loci covered"},
    "name": "Col5",
    "type": "surface",
    "uid": "024769",
    "zmax": 1,
    "zmin": 0
}
data = Data([trace1])

layout = {
    "autosize": True,
    "height": 400,
    "margin": {
        "r": 60,
        "t": 60,
        "autoexpand": True,
        "b": 60,
        "l": 60
    },
    "scene": {
        "aspectratio": {
            "x": 1,
            "y": 1,
            "z": 1
        },
        "camera": {
            "center": {
                "x": 0,
                "y": 0,
                "z": 0
            },
            "eye": {
                "x": 1.29488370702,
                "y": 0.430927434957,
                "z": 1.68079675485
            },
            "up": {
                "x": 0,
                "y": 0,
                "z": 1
            }
        },
        "xaxis": {
            "autorange": True,
            "title": "Normal Depth",
            "type": "log"
        },
        "yaxis": {
            "autorange": True,
            "title": "Tumor Depth",
            "type": "log"
        },
        "zaxis": {"title": "Fraction of target loci covered"}
    },
    "title": "",
    "xaxis": {
        "autorange": False,
        "range": [-0.301029995664, 3],
        "title": "Tumor depth",
        "type": "log"
    },
    "yaxis": {
        "autorange": False,
        "range": [-0.301029995664, 3],
        "title": "Normal depth",
        "type": "log"
    }
}
fig = Figure(data=data, layout=layout)
plot_url = py.plot(fig)
