package org.hammerlab.pageant.coverage.one

import java.io.PrintWriter

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.hammerlab.csv.ProductsToCSV._
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.math.Steps
import org.hammerlab.pageant.coverage.ReadSetStats
import org.hammerlab.pageant.coverage.one.Result.DC
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
 * @param totalIntervalLoci total number of on-target loci.
 */
case class Result(jh: JointHistogram,
                  pdf: RDD[DC],
                  cdf: RDD[DC],
                  readsStats: ReadSetStats,
                  filteredCDF: Array[DC],
                  totalIntervalLoci: NumLoci) {

  @transient lazy val ReadSetStats(maxDepth, totalBases, onBases) = readsStats

  def toCSVRow(depthCounts: DC): CSVRow = CSVRow(depthCounts, totalBases, totalIntervalLoci)

  def save(dir: String, force: Boolean = false, writeFullDistributions: Boolean = false): this.type = {
    val fs = new Path(dir).getFileSystem(jh)

    if (writeFullDistributions) {
      WriteRDD(dir, s"pdf", pdf.map(toCSVRow), force, jh)
      WriteRDD(dir, s"cdf", cdf.map(toCSVRow), force, jh)

      val jhPath = new Path(dir, s"jh")
      if (!fs.exists(jhPath)) {
        jh.write(jhPath)
      }
    }

    WriteLines(dir, s"cdf.csv", filteredCDF.map(CSVRow(_, totalBases, totalIntervalLoci)).toCSV(), force, jh)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      pw.println(s"Max depth: $maxDepth")
      pw.println(s"Total sequenced bases: $totalBases")
      pw.println(s"Total on-target bases: $onBases")
      pw.println(s"Total on-target loci: $totalIntervalLoci")
      pw.close()
    }

    this
  }
}

object Result {
  type DC = (Depth, Counts)

  def apply(jh: JointHistogram): Result = {
    val j = jh.jh
    val fks = j.map(Key.make)

    val totalIntervalLoci = j.filter(_._1._2(1).get == 1).values.sum.toLong

    val pdf = fks.map(fk => fk.d -> Counts(fk)).reduceByKey(_ + _)

    val sc = jh.sc

    val partitionSums =
      pdf
        .values
        .mapPartitions(iter =>
          Iterator(
            iter.foldLeft(Counts.empty)(_ + _)
          )
        )
        .collect
        .drop(1)
        .scanRight(Counts.empty)(_ + _)

    val partitionSumsRDD = sc.parallelize(partitionSums, partitionSums.length)

    val cdf = pdf.zipPartitions(partitionSumsRDD)((iter, sumIter) => {
      val sum = sumIter.next()
      for {
        (depth, counts) <- iter
      } yield
        depth -> (counts + sum)
    })

    val maxDepth = pdf.keys.reduce(math.max)

    val depthSteps = Steps.roundNumbers(maxDepth)

    val stepsBC = sc.broadcast(depthSteps)

    val filteredCDF =
      (for {
        (depth, count) ← cdf
        depthFilter = stepsBC.value
        if depthFilter(depth)
      } yield {
        depth → count
      }).collect.sortBy(_._1)

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
      totalIntervalLoci
    )
  }
}
