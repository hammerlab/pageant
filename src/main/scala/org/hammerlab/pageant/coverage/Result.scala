package org.hammerlab.pageant.coverage

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.coverage.Count.NumLoci
import org.hammerlab.pageant.coverage.Result.D2C
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import org.hammerlab.pageant.rdd.GridCDFRDD
import org.hammerlab.pageant.utils.RoundNumbers

case class Result(name: String,
                  jh: JointHistogram,
                  dir: String,
                  pdf: RDD[D2C],
                  cdf: RDD[D2C],
                  maxD1: Int,
                  maxD2: Int,
                  filteredCDF: Array[D2C],
                  totalBases1: Long,
                  totalBases2: Long,
                  onBases1: Long,
                  onBases2: Long,
                  totalIntervalLoci: Long) {

  @transient lazy val sc = jh.sc
  @transient lazy val fs = FileSystem.get(sc.hadoopConfiguration)

  def writeCSV(fn: String, strs: Iterable[String], force: Boolean): Unit = {
    val path = new Path(dir, fn)
    if (!force && fs.exists(path)) {
      println(s"Skipping $path, already exists")
    } else {
      val os = fs.create(path)
      os.writeBytes(strs.mkString("", "\n", "\n"))
      os.close()
    }
  }

  def writeCSV(fn: String, v: Vector[(Depth, NumLoci)], force: Boolean): Unit = {
    writeCSV(fn, v.map(t => s"${t._1},${t._2}"), force)
  }

  def d2cString(t: D2C): String = {
    val ((d1, d2), counts) = t
    val Counts(on, off) = counts
    List(
      d1, d2,
      on.bp1, on.bp2, on.n,
      on.bp1 * 1.0 / totalBases1, on.bp2 * 1.0 / totalBases2, on.n * 1.0 / totalIntervalLoci,
      off.bp1, off.bp2, off.n
    ).mkString(",")
  }

  def writeRDD(fn: String, rdd: RDD[D2C], force: Boolean): Unit = {
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

  def save(force: Boolean = false): this.type = {
    writeRDD(s"$name-pdf", pdf, force)
    writeRDD(s"$name-cdf", cdf, force)

    writeCSV(s"$name-filtered-cdf.csv", filteredCDF.map(d2cString), force)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      pw.println(s"$maxD1,$maxD2")
      pw.println(s"$totalBases1,$totalBases2")
      pw.println(s"$onBases1,$onBases2")
      pw.println(s"$totalIntervalLoci")
      pw.close()
    }

    val jhPath = new Path(dir, s"$name-jh")
    if (!fs.exists(jhPath)) {
      jh.write(jhPath)
    }

    this
  }
}

object Result {

  type D2C = ((Depth, Depth), Counts)

  // Divide [0, maxDepth] into N geometrically-evenly-spaced steps (of size maxDepth^(1/100)),
  // including all sequential integers until those steps are separated by at least 1.
  def geometricEvenSteps(maxDepth: Int, N: Int = 100): Set[Int] = {
    import math.{exp, log, max, min}
    val logMaxDepth = log(maxDepth)

    val steps =
      (for {
        i ← 1 until N
      } yield
        min(maxDepth, max(i, exp((i - 1) * logMaxDepth / (N - 2)).toInt))
        ).toSet ++ Set(0)

    println(s"teps: ${steps.toList.sorted.mkString(",")}")

    steps
  }

  //  0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
  // 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
  // 20, 22, 24, 26, 28,
  // 30, 32, 34, 36, 38,
  // 40, 42, 44, 46, 48,
  // 50, 55,
  // 60, 65,
  // 70, 75,
  // 80, 85,
  // 90, 95,
  // repeat * [powers of 100] (sans initial 0)
  def roundNumberSteps(maxDepth: Int): Set[Int] = {
    RoundNumbers(
      (0 until 20) ++ (20 until 50 by 2) ++ (50 until 100 by 5),
      maxDepth,
      100
    ).toSet
  }

  def apply(name: String, jh: JointHistogram, dir: String): Result = {
    val j = jh.jh
    val fks = j.map(FK.make)

    val totalIntervalLoci = j.filter(_._1._2(2).get == 1).values.sum.toLong

    val (pdf, cdf, maxD1, maxD2) = GridCDFRDD[Counts, FK](fks, _.d1, _.d2, Counts(_), _ + _, Counts.empty)

    val d1Steps = roundNumberSteps(maxD1)
    val d2Steps = roundNumberSteps(maxD2)

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
      name, jh, dir,
      pdf.sortByKey(), cdf.sortByKey(),
      maxD1, maxD2,
      filteredCDF,
      totalBases1, totalBases2,
      onBases1, onBases2,
      totalIntervalLoci
    )
  }
}

