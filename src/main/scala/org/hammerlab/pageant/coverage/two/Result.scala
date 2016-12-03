package org.hammerlab.pageant.coverage.two

import java.io.PrintWriter

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.magic.rdd.grid.PartialSumGridRDD
import org.hammerlab.math.Steps.roundNumbers
import org.hammerlab.pageant.coverage.ReadSetStats
import org.hammerlab.pageant.coverage.two.Result.D2C
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth

/**
 * Statistics about one set of reads' coverage of a set of intervals.
 *
 * @param jh joint-histogram of read coverage vs. interval coverage (the latter being 1 or 0 everywhere).
 * @param pdf ([[Depth]], [[Count]]) tuples indicating on-target and off-target coverage at all observed depths.
 * @param cdf CDF of the PDF above; tuples represent numbers of on- and off-target loci with depth *at least* a given
 *            number.
 * @param sample1Stats summary depth/coverage stats about the first reads-set.
 * @param sample2Stats summary depth/coverage stats about the second reads-set.
 * @param filteredCDF summary CDF, filtered to a few logarithmically-spaced round-numbers.
 * @param totalIntervalLoci total number of on-target loci.
 */
case class Result(jh: JointHistogram,
                  pdf: RDD[D2C],
                  cdf: RDD[D2C],
                  sample1Stats: ReadSetStats,
                  sample2Stats: ReadSetStats,
                  filteredCDF: Array[D2C],
                  totalIntervalLoci: NumLoci) {

  @transient lazy val sc = jh.sc
  @transient lazy val fs = FileSystem.get(sc.hadoopConfiguration)

  @transient lazy val ReadSetStats(maxDepth1, totalBases1, onBases1) = sample1Stats
  @transient lazy val ReadSetStats(maxDepth2, totalBases2, onBases2) = sample2Stats

  def save(dir: String, force: Boolean = false, writeFullDistributions: Boolean = false): this.type = {
    if (writeFullDistributions) {
      writeRDD(dir, s"pdf", pdf, force)
      writeRDD(dir, s"cdf", cdf, force)

      val jhPath = new Path(dir, s"jh")
      if (!fs.exists(jhPath)) {
        jh.write(jhPath)
      }
    }

    writeCSV(dir, s"cdf.csv", filteredCDF.map(d2cString), force)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      pw.println(s"$maxDepth1,$maxDepth2")
      pw.println(s"$totalBases1,$totalBases2")
      pw.println(s"$onBases1,$onBases2")
      pw.println(s"$totalIntervalLoci")
      pw.close()
    }

    this
  }

  private def writeCSV(dir: String, fn: String, strs: Iterable[String], force: Boolean): Unit = {
    val path = new Path(dir, fn)
    if (!force && fs.exists(path)) {
      println(s"Skipping $path, already exists")
    } else {
      val os = fs.create(path)
      os.writeBytes(strs.mkString("", "\n", "\n"))
      os.close()
    }
  }

  private def writeCSV(dir: String, fn: String, v: Vector[(Depth, NumLoci)], force: Boolean): Unit = {
    writeCSV(dir, fn, v.map(t => s"${t._1},${t._2}"), force)
  }

  private def d2cString(t: D2C): String = {
    val ((d1, d2), counts) = t
    val Counts(on, off) = counts
    List(
      d1, d2,
      on.bp1, on.bp2, on.n,
      on.bp1 * 1.0 / totalBases1, on.bp2 * 1.0 / totalBases2, on.n * 1.0 / totalIntervalLoci,
      off.bp1, off.bp2, off.n,
      off.bp1 * 1.0 / totalBases1, off.bp2 * 1.0 / totalBases2, off.n * 1.0 / totalIntervalLoci
    ).mkString(",")
  }

  private def writeRDD(dir: String, fn: String, rdd: RDD[D2C], force: Boolean): Unit = {
    val path = new Path(dir, fn)
    (fs.exists(path), force) match {
      case (true, true) ⇒
        println(s"Removing $path")
        fs.delete(path, true)
        rdd.map(d2cString).saveAsTextFile(path.toString)
      case (true, false) ⇒
        println(s"Skipping $path, already exists")
      case _ ⇒
        rdd.map(d2cString).saveAsTextFile(path.toString)
    }
  }
}

object Result {

  type D2C = ((Depth, Depth), Counts)

  def apply(jh: JointHistogram): Result = {
    val j = jh.jh
    val fks = j.map(Key.make)

    val totalIntervalLoci = j.filter(_._1._2(2).get == 1).values.sum.toLong

    val keyedCounts =
      for {
        fk <- fks
      } yield
        fk.depth1 -> fk.depth2 -> Counts(fk)

    implicit val countsMonoid = Counts

    val (pdf, cdf, maxD1, maxD2) = PartialSumGridRDD[Counts](keyedCounts)

    val d1Steps = roundNumbers(maxD1)
    val d2Steps = roundNumbers(maxD2)

    val sc = jh.sc
    val stepsBC = sc.broadcast((d1Steps, d2Steps))

    val filteredCDF =
      (for {
        ((d1, d2), count) ← cdf
        (d1Filter, d2Filter) = stepsBC.value
        if d1Filter(d1) && d2Filter(d2)
      } yield {
        (d1, d2) → count
      }).collect.sortBy(_._1)

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
      filteredCDF,
      totalIntervalLoci
    )
  }
}

