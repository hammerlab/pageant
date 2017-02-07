# pageant
PArallel GEnomic ANalysis Toolkit

[![Build Status](https://travis-ci.org/hammerlab/pageant.svg?branch=master)](https://travis-ci.org/hammerlab/pageant)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/pageant/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/pageant?branch=master)

Currently: one tool, [`CoverageDepth`](https://github.com/hammerlab/pageant/blob/master/src/main/scala/org/hammerlab/pageant/coverage/CoverageDepth.scala), for analyzing coverage in a BAM file or files, optionally intersected with an "interval file" (e.g. an exome capture kit `.bed`).

## `CoverageDepth`

This tool computes coverage-depth statistics about one or two sets of reads (e.g. `.bam`s), optionally taking an intervals file (e.g. a `.bed`, denoting "targeted loci" of some upstream analysis, e.g. whole-exome sequencing) and generating coverage-depth statistics for on-target loci, off-target loci, and total.

When run on two samples with an interval file, it can plot the fraction of the targeted loci which were covered at at ≥X depth in one sample and ≥Y depth in the other, for all (X,Y):

[![3-D plot preview](https://d3vv6lp55qjaqc.cloudfront.net/items/2q261q1a0U1501381n40/Screen%20Recording%202017-02-06%20at%2008.59%20AM.gif?X-CloudApp-Visitor-Id=486740)](https://plot.ly/~ryan.blake.williams/92.embed?share_key=2XOQGkohwn5UTHEW2F3G07)

### Running
After [setting `$PAGEANT_JAR` to point to a Pageant assembly JAR](#installation):

```bash
$SPARK_HOME/bin/spark-submit \
  --properties-file $spark_props \
  --class org.hammerlab.pageant.coverage.CoverageDepth \
  $PAGEANT_JAR \
  --intervals-file $intervals \
  --out $out_dir \
  $normal $tumor
```

you'll additionally want to fill in:
- `$spark_props`: path to [a Spark properties file](http://spark.apache.org/docs/2.1.0/configuration.html#dynamically-loading-spark-properties)
  - inline Spark-config options will work here as well, per [the Spark docs](http://spark.apache.org/docs/2.1.0/configuration.html).
- `$intervals`: optional path to e.g. a `.bed` file to view on-/off-target stats about
- `$normal`/`$tumor`: paths to `.bam`s (or `.adam` alignment records)
  - one `.bam` can also be passed, resulting in a different and simpler 1-dimensional histogram output.
- `$out`: output directory

A full list of arguments/options can be found by running with `-h`:

```bash
$SPARK_HOME/bin/spark-submit \
  --class org.hammerlab.pageant.coverage.CoverageDepth \
  $PAGEANT_JAR \
  -h
```

### Output
`CoverageDepth` writes out a directory with a few files of note; see [this test-data for a live example](src/test/resources/coverage.intervals.golden2):

- `misc`: plaintext file with high-level stats
- `cdf.csv`: CSV with stats about the number of loci with "normal" depth ≥X and "tumor" depth ≥Y, for (X,Y) filtered to (a relatively dense set of) "round numbers".
- `pdf.csv`: same as above, but stats are about loci with depth ==X and ==Y, resp.
- `pdf`/`cdf`: when `CoverageDepth` is run with the `--persist-distributions` (`-v`) flag, the unfiltered "pdf" and "cdf" above are written out as sharded CSVs.

### Plotting
[`src/main/python/plot.py`](src/main/python/plot.py) can be used to consume the output directory (`$out`) from [above](#Running) and send it to [plot.ly](https://plot.ly):

```bash
# $out argument should be the output directory 
python src/main/python/plot.py $out
```

generating an interactive 2D-histogram like the one shown [above](#CoverageDepth).

## Installation

Download a pre-built assembly-JAR, and set `$PAGEANT_JAR` to point to it:

```bash
wget https://oss.sonatype.org/content/repositories/snapshots/org/hammerlab/pageant_2.11/1.0.0-SNAPSHOT/pageant_2.11-1.0.0-SNAPSHOT-assembly.jar
export PAGEANT_JAR=$PWD/pageant_2.11-1.0.0-SNAPSHOT-assembly.jar
```

or clone and build it yourself:

```bash
git clone git@github.com:hammerlab/pageant.git
cd pageant
sbt assembly
export PAGEANT_JAR=target/scala-2.11/pageant-assembly-1.0.0-SNAPSHOT.jar
```

### Spark Installation
Pageant runs on Apache Spark:

- [Download Spark](http://spark.apache.org/downloads.html)
- Set `$SPARK_HOME` to the Spark installation directory

Pageant currently builds against Spark 2.1.0, but some other versions will also work…
