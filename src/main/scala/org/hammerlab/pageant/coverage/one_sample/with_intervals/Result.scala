package org.hammerlab.pageant.coverage.one_sample.with_intervals

import java.io.PrintWriter

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.hammerlab.csv.ProductsToCSV._
import org.hammerlab.genomics.reference.{ ContigLengths, NumLoci }
import org.hammerlab.magic.rdd.scan.ScanRightByKeyRDD._
import org.hammerlab.math.Steps.roundNumbers
import org.hammerlab.pageant.coverage.CoverageDepth.getJointHistogramPath
import org.hammerlab.pageant.coverage.ReadSetStats
import org.hammerlab.pageant.coverage.one_sample.with_intervals.Result.DC
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram._
import org.hammerlab.pageant.utils.{ WriteLines, WriteRDD }

/**
 * Statistics about one set of reads' coverage of a set of intervals.
 *
 * @param jh joint-histogram of read coverage vs. interval coverage (the latter being 1 or 0 everywhere).
 * @param pdf ([[Depth]], [[Count]]) tuples indicating on-target and off-target coverage at all observed depths.
 * @param cdf CDF of the PDF above; tuples represent numbers of on- and off-target loci with depth *at least* a given
 *            number.
 * @param readsStats summary depth/coverage stats about the reads-set.
 * @param filteredCDF summary CDF, filtered to a few logarithmically-spaced round-numbers.
 * @param totalOnLoci total number of on-target loci.
 * @param totalOffLoci total number of off-target loci observed to have non-zero coverage.
 */
case class Result(jh: JointHistogram,
                  pdf: RDD[DC],
                  cdf: RDD[DC],
                  readsStats: ReadSetStats,
                  filteredCDF: Array[DC],
                  totalOnLoci: NumLoci,
                  totalOffLoci: NumLoci) {

  @transient lazy val ReadSetStats(maxDepth, totalBases, onBases) = readsStats

  def toCSVRow(depthCounts: DC): CSVRow =
    CSVRow(depthCounts, totalBases, totalOnLoci, totalOffLoci)

  def save(dir: String,
           force: Boolean = false,
           writeFullDistributions: Boolean = false,
           writeJointHistogram: Boolean = false): this.type = {

    val fs = new Path(dir).getFileSystem(jh)

    if (writeFullDistributions) {
      WriteRDD(dir, s"pdf", pdf.map(toCSVRow), force, jh)
      WriteRDD(dir, s"cdf", cdf.map(toCSVRow), force, jh)
    }

    if (writeJointHistogram) {
      val jhPath = getJointHistogramPath(dir)

      if (fs.exists(jhPath)) {
        fs.delete(jhPath, true)
      }

      jh.write(jhPath)
    }

    WriteLines(dir, s"cdf.csv", filteredCDF.map(CSVRow(_, totalBases, totalOnLoci, totalOffLoci)).toCSV(), force, jh)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      pw.println(s"Max depth: $maxDepth")
      pw.println(s"Total mapped bases: $totalBases")
      pw.println(s"Total on-target bases: $onBases")
      pw.println(s"Total on-target loci: $totalOnLoci")
      pw.close()
    }

    this
  }
}

object Result {
  type DC = (Depth, Counts)

  def apply(jh: JointHistogram, contigLengths: ContigLengths, hasIntervals: Boolean): Result = {
    val j = jh.jh
    val keys = j.map(Key(_))

    val (totalOnLoci, totalOffLoci) = jh.coveredLociCounts(idx = 1)

    val pdf =
      keys
        .map(key => key.depth -> Counts(key))
        .reduceByKey(_ + _)
        .sortByKey()

    val sc = jh.sc

    val cdf = pdf.scanRightByKey(Counts.empty)(_ + _)

    val maxDepth = pdf.keys.reduce(math.max)

    val depthSteps = roundNumbers(maxDepth)

    val stepsBC = sc.broadcast(depthSteps)

    val filteredCDF =
      (for {
        (depth, count) ← cdf
        depthFilter = stepsBC.value
        if depthFilter(depth)
      } yield
        depth → count
      )
      .collect
      .sortBy(_._1)

    val (firstDepth, firstCounts) = filteredCDF.take(1)(0)
    if (firstDepth != 0) {
      throw new Exception(s"Bad first firstDepth: $firstDepth (count: $firstCounts)")
    }

    val totalBases = firstCounts.all.bp
    val onBases = firstCounts.on.bp

    Result(
      jh,
      pdf.sortByKey(),
      cdf.sortByKey(),
      ReadSetStats(maxDepth, totalBases, onBases),
      filteredCDF,
      totalOnLoci,
      totalOffLoci
    )
  }
}
