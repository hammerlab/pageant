package org.hammerlab.pageant.histogram

import java.io.{ObjectOutputStream, PrintWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.hammerlab.pageant.histogram.DepthMain.{Depth, NumBP, NumLoci}
import org.hammerlab.pageant.histogram.JointHistogram.{Depths, L, OS}
import org.hammerlab.pageant.histogram.Result.D2C
import org.hammerlab.pageant.rdd.GridCDFRDD
import org.hammerlab.pageant.utils.Args

import scala.collection.Map

case class FK(c: String, d1: Depth, d2: Depth, on: Long, off: Long)
object FK {
  def make(t: ((OS, Depths), L)): FK = {
    val (on, off) =
      if (t._1._2(2).get == 1)
        (t._2, 0L)
      else
        (0L, t._2)
    new FK(t._1._1.get, t._1._2(0).get, t._1._2(1).get, on, off)
  }
}

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

  def apply(name: String, jh: JointHistogram, dir: String): Result = {
    val j = jh.jh
    val fks = j.map(FK.make)

    val totalIntervalLoci = j.filter(_._1._2(2).get == 1).values.sum.toLong

    val (pdf, cdf, maxD1, maxD2) = GridCDFRDD[Counts, FK](fks, _.d1, _.d2, Counts(_), _ + _, Counts.empty)

    val N = 100

    import math.{max, min, exp, log}

    val logMaxD1 = log(maxD1)
    val logMaxD2 = log(maxD2)

    val d1Steps = (1 until N).map(i ⇒ min(maxD1, max(i, exp((i - 1) * logMaxD1 / (N - 2)).toInt))).toSet ++ Set(0)
    val d2Steps = (1 until N).map(i ⇒ min(maxD2, max(i, exp((i - 1) * logMaxD2 / (N - 2)).toInt))).toSet ++ Set(0)

    println(s"d1Steps: ${d1Steps.toList.sorted.mkString(",")}")
    println(s"d2Steps: ${d2Steps.toList.sorted.mkString(",")}")

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

    Result(name, jh, dir, pdf.sortByKey(), cdf.sortByKey(), maxD1, maxD2, filteredCDF, totalBases1, totalBases2, onBases1, onBases2, totalIntervalLoci)
  }
}

case class Counts(on: Count, off: Count) {
  def +(o: Counts): Counts = Counts(on + o.on, off + o.off)
  @transient lazy val all: Count = on + off
}

object Counts {
  def apply(fk: FK): Counts = Counts(
    Count(fk.on * fk.d1, fk.on * fk.d2, fk.on),
    Count(fk.off * fk.d1, fk.off * fk.d2, fk.off)
  )
  val empty = Counts(Count.empty, Count.empty)
}

case class Count(bp1: NumBP, bp2: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp1 + o.bp1, bp2 + o.bp2, n + o.n)
}

object Count {
  val empty = Count(0, 0, 0)
}

object DepthMain {

  type Depth = Int
  type NumLoci = Long
  type NumBP = Long

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("pageant")
    val sc = new SparkContext(conf)

    val Args(strings, _, bools, arguments) =
      Args(
        args,
        defaults = Map("force" → false),
        aliases = Map(
          'n' → "normal",
          't' → "tumor",
          'i' → "intervals",
          'j' → "joint-hist",
          'f' → "force"
        )
      )

    val outPath = arguments(0)

    val force = bools("force")
    val forceStr = if (force) " (forcing)" else ""

    val jh = strings.get("joint-hist") match {
      case Some(jhPath) ⇒
        println(s"Loading JointHistogram: $jhPath$forceStr")
        JointHistogram.load(sc, jhPath)
      case _ ⇒
        val normalPath = strings("normal")
        val tumorPath = strings("tumor")
        val intervalPath = strings("intervals")

        println(
          s"Analyzing ($normalPath, $tumorPath) against $intervalPath and writing to $outPath$forceStr"
        )

        JointHistogram.fromFiles(sc, Seq(normalPath, tumorPath), Seq(intervalPath))
    }

    Result("all", jh, outPath).save(force)
  }
}
