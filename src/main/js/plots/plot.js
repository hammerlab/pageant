
/**
 * Inputs:
 *
 *    - one optional argument: a sample identifier
 *    - stdin: a `cdf.csv` produced by the CoverageDepth program
 *
 * Build a 3D-surface plot showing the fraction of on-target loci covered for each {normal,tumor} depth.
 */

var args = process.argv.slice(2);
var title = (args.length ? (args[0] + ": ") : "") + "Normal vs. Tumor Coverage";

const fs = require('fs'),
      path = require('path'),
      readline = require('readline'),
      _ = require('underscore');

const credentialsPath = path.join(process.env.HOME, '.plotly', '.credentials');
const credentials = JSON.parse(fs.readFileSync(credentialsPath));

const plotly = require('plotly')(credentials.username, credentials.api_key);

var stdin = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

var xKeys = {},
      yKeys = {},
      values = {};

stdin.on('line', line => {
  const cols = line.split(',');
  const x = parseInt(cols[0]);
  if (isNaN(x)) return;
  const y = parseInt(cols[1]);

  // "fracLociOn"
  const z = parseFloat(cols[7]);

  xKeys[x] = true;
  yKeys[y] = true;

  if (!(x in values)) {
    values[x] = {};
  }

  values[x][y] = z;
});

stdin.on('close', () => {

  xKeys = _.keys(xKeys).map(s => parseInt(s));
  xKeys.sort((a, b) => a - b);

  yKeys = _.keys(yKeys).map(s => parseInt(s));
  yKeys.sort((a, b) => a - b);

  const zs =
        xKeys.map(x => {
          const row = values[x];
          return yKeys.map(y => (y in row) ? row[y] : 0)
        });

  const trace1 = {
    x: xKeys,
    y: yKeys,
    z: zs,
    colorbar: { title: 'Fraction of target loci covered' },
    type: 'surface',
    zmax: 1,
    zmin: 0
  };

  const data = [ trace1 ];

  const layout = {
    autosize: true,
    height: 400,
    margin: {
      r: 60,
      t: 60,
      autoexpand: true,
      b: 60,
      l: 60
    },
    scene: {
      aspectratio: {
        x: 1,
        y: 1,
        z: 1
      },
      camera: {
        center: {
          x: 0,
          y: 0,
          z: 0
        },
        eye: {
          x: 1.29488370702,
          y: 0.430927434957,
          z: 1.68079675485
        },
        up: {
          x: 0,
          y: 0,
          z: 1
        }
      },
      xaxis: {
        autorange: true,
        title: 'Normal Depth',
        type: 'log'
      },
      yaxis: {
        autorange: true,
        title: 'Tumor Depth',
        type: 'log'
      },
      zaxis: { title: 'Fraction of target loci covered' }
    },
    title,
    filename: title
  };

  plotly.plot(
        data,
        {
          layout,
          filename: title
        },
        function(err, msg) {
          console.error(msg);
        }
  );
});

