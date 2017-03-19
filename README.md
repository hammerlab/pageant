# pageant
PArallel GEnomic ANalysis Toolkit

[![Build Status](https://travis-ci.org/hammerlab/pageant.svg?branch=master)](https://travis-ci.org/hammerlab/pageant)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/pageant/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/pageant?branch=master)

Currently: one tool, [`CoverageDepth`][], for analyzing coverage in a BAM file or files, optionally intersected with an "interval file" (e.g. an exome capture kit `.bed`).

## [`CoverageDepth`][]

This tool computes coverage-depth statistics about one or two sets of reads (e.g. `.bam`s), optionally taking an intervals file (e.g. a `.bed`, denoting "targeted loci" of some upstream analysis, e.g. whole-exome sequencing) and generating coverage-depth statistics for on-target loci, off-target loci, and total.

When run on two samples with an interval file, it can plot the fraction of the targeted loci which were covered at at ≥X depth in one sample and ≥Y depth in the other, for all (X,Y):

[![3-D plot preview](https://d3vv6lp55qjaqc.cloudfront.net/items/2q261q1a0U1501381n40/Screen%20Recording%202017-02-06%20at%2008.59%20AM.gif)](https://plot.ly/~ryan.blake.williams/92.embed?share_key=2XOQGkohwn5UTHEW2F3G07)

### Running Locally
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

In the above, you'll want to fill in:
- `$spark_props`: path to [a Spark properties file](http://spark.apache.org/docs/2.1.0/configuration.html#dynamically-loading-spark-properties)
  - inline Spark-config options will work here as well, per [the Spark docs](http://spark.apache.org/docs/2.1.0/configuration.html).
- `$intervals`: optional path to e.g. a `.bed` file to view on-/off-target stats about
- `$normal`/`$tumor`: paths to `.bam`s (or `.adam` alignment records)
  - one `.bam` can also be passed, resulting in a different and simpler 1-dimensional histogram output.
- `$out`: output directory

A full list of arguments/options can be found by running with `-h`:

```
$ $SPARK_HOME/bin/spark-submit \
  --class org.hammerlab.pageant.coverage.CoverageDepth \
  $PAGEANT_JAR \
  -h
 PATHS                             : Paths to sets of reads: FILE1 FILE2 FILE3
 --dir (-d) PATH                   : When set, relative paths will be prefixed with this path (default: None)
 --force (-f)                      : Write result files even if they already exist (default: false)
 --include-duplicates              : Include reads marked as duplicates (default: false)
 --include-failed-quality-checks   : Include reads that failed vendor quality checks (default: false)
 --include-single-end              : Include single-end reads (default: false)
 --interval-partition-bytes (-b) N : Number of bytes per chunk of input interval-file (default: 1048576)
 --intervals-file (-i) PATH        : Intervals file or capture kit; print stats for loci matching this intervals file, not matching, and total.
                                     (default: None)
 --loci VAL                        : If set, loci to include. Either 'all' or 'contig[:start[-end]],contig[:start[-end]],…' (default: None)
 --loci-file VAL                   : Path to file giving loci to include. (default: None)
 --min-alignment-quality INT       : Minimum read mapping quality for a read (Phred-scaled) (default: None)
 --no-sequence-dictionary          : If set, get contigs and lengths directly from reads instead of from sequence dictionary. (default: false)
 --only-mapped-reads               : Include only mapped reads (default: false)
 --out (-o) DIR                    : Directory to write results to
 --persist-distributions (-v)      : When set, persist full PDF and CDF of coverage-depth histogram (default: false)
 --persist-joint-histogram (-jh)   : When set, save the computed joint-histogram; if one already exists, skip reading it, recompute it, and overwrite
                                     it (default: false)
 --sample-names STRING[]           : name1,…,nameN
 --split-size VAL                  : Maximum HDFS split size (default: None)
 -h (-help, --help, -?)            : Print help (default: true)
 -print_metrics                    : Print metrics to the log on completion (default: false)
```

### Output
[`CoverageDepth`][] writes out a directory with a few files of note; see [this test-data for a live example](src/test/resources/coverage.intervals.golden2):

- `misc`: plaintext file with high-level stats
- `cdf.csv`: CSV with stats about the number of loci with "normal" depth ≥X and "tumor" depth ≥Y, for (X,Y) filtered to (a relatively dense set of) "round numbers".
- `pdf.csv`: same as above, but stats are about loci with depth ==X and ==Y, resp.
- `pdf`/`cdf`: when [`CoverageDepth`][] is run with the `--persist-distributions` (`-v`) flag, the unfiltered "pdf" and "cdf" above are written out as sharded CSVs.

### Plotting
The [`plot.js`][] script in this repo can be used to consume the `cdf.csv` produced [above](#Running) and send it to [plot.ly](https://plot.ly):

#### Install JS dependencies
```bash
cd src/main/js/plots
npm install
```

#### Pipe `cdf.csv` to [`plot.js`][]

```bash
# $out argument should be the output directory from above
cat $out/cdf.csv | node plot.js
```

If `$out` is in a gcloud bucket (`gs://…`), use `gsutil` to pipe the file to the plot script:

```bash
gsutil cat $out/cdf.csv | node plot.js
```

generating an interactive 2D-histogram like the one shown [above](#CoverageDepth).

### Running on GCloud
Running on an ephemeral Google Cloud Dataproc cluster is easy and cheap (~$0.02/cpu-hr using predominantly pre-emptible nodes, as of current writing).

You'll want to [install the `gcloud` command-line utility](https://cloud.google.com/sdk/docs/#install_the_latest_cloud_tools_version_cloudsdk_current_version) and then follow the steps below.

#### Create a cluster
e.g. with 51 4-core nodes (2 reserved and 49 pre-emptible), pointing at a GCloud bucket with your data:

```bash
gcloud dataproc clusters create pageant \
	--master-machine-type n1-standard-4 \
	--worker-machine-type n1-standard-4 \
	--num-workers 2 \
	--num-preemptible-workers 49
```

#### Submit a job

```bash
gcloud dataproc jobs submit spark \
	--cluster pageant \
	--class org.hammerlab.pageant.coverage.CoverageDepth \
	--jars gs://hammerlab-lib/pageant-c482335.jar \
	-- \
	--intervals-file <path to .bed> \
	--out <out directory> \
	<path to normal .bam> \
	<path to tumor .bam>
```

This uses a Pageant JAR that's already on GCloud storage, so that no bandwidth- or time-cost is incurred uploading a JAR.

#### Optional: extra Spark configs

You may wish to include some Spark configs in either the cluster-creation step (to set defaults across multiple jobs that may be run before the cluster is torn down):

```
--properties spark:spark.speculation=true,spark:spark.speculation.interval=1000,spark:spark.speculation.multiplier=1.3,spark:spark.yarn.maxAppAttempts=1,spark:spark.eventLog.enabled=true,spark:spark.eventLog.dir=hdfs:///user/spark/eventlog
```

or in the job-creation step:

```
--properties spark.speculation=true,spark.speculation.interval=1000,spark.speculation.multiplier=1.3,spark.yarn.maxAppAttempts=1,spark.eventLog.enabled=true,spark.eventLog.dir=hdfs:///user/spark/eventlog
```

#### Tear down the cluster

```bash
gcloud dataproc clusters delete pageant
```

Alternatively, you can just resize it down to the minimum 2 reserved nodes:

```bash
gcloud dataproc clusters update pageant --num-preemptible-workers 0
```

## Local Installation

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


[`CoverageDepth`]: src/main/scala/org/hammerlab/pageant/coverage/CoverageDepth.scala
[`plot.js`]: src/main/js/plots/plot.js
