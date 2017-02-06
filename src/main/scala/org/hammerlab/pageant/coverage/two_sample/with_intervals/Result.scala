package org.hammerlab.pageant.coverage.two_sample.with_intervals

import java.io.PrintWriter

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.csv._
import org.hammerlab.genomics.reference.{ ContigLengths, NumLoci }
import org.hammerlab.magic.rdd.grid.PartialSumGridRDD
import org.hammerlab.magic.rdd.grid.PartialSumGridRDD.{ Col, Row }
import org.hammerlab.math.Steps.roundNumbers
import org.hammerlab.pageant.coverage.CoverageDepth.getJointHistogramPath
import org.hammerlab.pageant.coverage.one_sample.with_intervals.ReadSetStats
import org.hammerlab.pageant.coverage.two_sample.with_intervals.Result.D2C
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import org.hammerlab.pageant.utils.{ WriteLines, WriteRDD }

/**
 * Statistics about one set of reads' coverage of a set of intervals.
 *
 * @param jh joint-histogram of read coverage vs. interval coverage (the latter being 1 or 0 everywhere).
 * @param pdf (([[Depth]], [[Depth]], [[Counts]]) tuples indicating on-target and off-target coverage at all observed depths.
 * @param cdf CDF of the PDF above; tuples represent numbers of on- and off-target loci with depth *at least* a given
 *            number.
 * @param sample1Stats summary depth/coverage stats about the first reads-set.
 * @param sample2Stats summary depth/coverage stats about the second reads-set.
 * @param filteredPDF summary PDF, filtered to a few logarithmically-spaced round-numbers.
 * @param filteredCDF summary CDF, filtered to a few logarithmically-spaced round-numbers.
 * @param totalOnLoci total number of on-target loci.
 * @param totalOffLoci total number of off-target loci observed to have non-zero coverage.
 */
case class Result(jh: JointHistogram,
                  pdf: RDD[D2C],
                  cdf: RDD[D2C],
                  sample1Stats: ReadSetStats,
                  sample2Stats: ReadSetStats,
                  filteredPDF: Array[D2C],
                  filteredCDF: Array[D2C],
                  totalOnLoci: NumLoci,
                  totalOffLoci: NumLoci) {

  @transient lazy val ReadSetStats(maxDepth1, totalBases1, onBases1) = sample1Stats
  @transient lazy val ReadSetStats(maxDepth2, totalBases2, onBases2) = sample2Stats

  implicit def toCSVRow(d2c: D2C): CSVRow = CSVRow(d2c, totalBases1, totalBases2, totalOnLoci, totalOffLoci)

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

    WriteLines(dir, s"pdf.csv", filteredPDF.map(toCSVRow).toCSV(), force, jh)
    WriteLines(dir, s"cdf.csv", filteredCDF.map(toCSVRow).toCSV(), force, jh)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      pw.println(s"Max depths: $maxDepth1,$maxDepth2")
      pw.println(s"Total mapped bases: $totalBases1,$totalBases2")
      pw.println(s"Total on-target bases: $onBases1,$onBases2")
      pw.println(s"Total on-target loci: $totalOnLoci")
      pw.close()
    }

    this
  }
}

object Result {

  type D2C = ((Depth, Depth), Counts)

  def filterDistribution(distribution: RDD[((Row, Col), Counts)],
                         filtersBroadcast: Broadcast[(Set[Int], Set[Int])]): Array[D2C] =
    (for {
      ((d1, d2), count) ← distribution
      (d1Filter, d2Filter) = filtersBroadcast.value
      if d1Filter(d1) && d2Filter(d2)
    } yield {
      (d1, d2) → count
    })
    .collect
    .sortBy(_._1)

  def apply(jh: JointHistogram, contigLengths: ContigLengths, hasIntervals: Boolean): Result = {
    val j = jh.jh
    val keys = j.map(Key(_))

    val (totalOnLoci, totalOffLoci) = jh.coveredLociCounts(idx = 2)

    val keyedCounts =
      for {
        key ← keys
      } yield
        key.depth1 → key.depth2 → Counts(key)

    implicit val countsMonoid = Counts

    val (pdf, cdf, maxD1, maxD2) = PartialSumGridRDD[Counts](keyedCounts)

    val d1Steps = roundNumbers(maxD1)
    val d2Steps = roundNumbers(maxD2)

    val stepsBroadcast = jh.sc.broadcast((d1Steps, d2Steps))

    val filteredPDF = filterDistribution(pdf, stepsBroadcast)
    val filteredCDF = filterDistribution(cdf, stepsBroadcast)

    val firstElem = filteredCDF.take(1)(0)
    if (firstElem._1 != (0, 0)) {
      throw new Exception(s"Bad first elem: $firstElem")
    }

    val firstCounts = firstElem._2
    val totalBases1 = firstCounts.all.bp1
    val totalBases2 = firstCounts.all.bp2
    val onBases1 = firstCounts.on.bp1
    val onBases2 = firstCounts.on.bp2

    Result(
      jh,
      pdf.sortByKey(),
      cdf.sortByKey(),
      ReadSetStats(maxD1, totalBases1, onBases1),
      ReadSetStats(maxD2, totalBases2, onBases2),
      filteredPDF,
      filteredCDF,
      totalOnLoci,
      totalOffLoci
    )
  }
}

