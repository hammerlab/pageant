package org.hammerlab.pageant.coverage.one

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.coverage.Steps
import org.hammerlab.pageant.coverage.one.Result.DC
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram._

case class Result(jh: JointHistogram,
                  dir: String,
                  rawOutput: Boolean,
                  pdf: RDD[DC],
                  cdf: RDD[DC],
                  maxDepth: Int,
                  filteredCDF: Array[DC],
                  totalBases: Long,
                  onBases: Long,
                  totalIntervalLoci: Long) {

  @transient lazy val sc = jh.sc
  @transient lazy val fs = FileSystem.get(sc.hadoopConfiguration)

  def dcString(t: DC): String = {
    val (depth, counts) = t
    val Counts(on, off) = counts
    List(
      depth,
      on.bp, on.n,
      on.bp * 1.0 / totalBases, on.n * 1.0 / totalIntervalLoci,
      off.bp, off.n,
      off.bp * 1.0 / totalBases, off.n * 1.0 / totalIntervalLoci
    ).mkString(",")
  }

  def writeRDD(fn: String, rdd: RDD[DC], force: Boolean): Unit = {
    val path = new Path(dir, fn)
    (fs.exists(path), force) match {
      case (true, true) ⇒
        println(s"Removing $path")
        fs.delete(path, true)
        rdd.map(dcString).saveAsTextFile(path.toString)
      case (true, false) ⇒
        println(s"Skipping $path, already exists")
      case _ ⇒
        rdd.map(dcString).saveAsTextFile(path.toString)
    }
  }

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

  def save(force: Boolean = false): this.type = {
    if (rawOutput) {
      writeRDD(s"pdf", pdf, force)
      writeRDD(s"cdf", cdf, force)

      val jhPath = new Path(dir, s"jh")
      if (!fs.exists(jhPath)) {
        jh.write(jhPath)
      }
    }

    writeCSV(s"cdf.csv", filteredCDF.map(dcString), force)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      pw.println(s"$maxDepth")
      pw.println(s"$totalBases")
      pw.println(s"$onBases")
      pw.println(s"$totalIntervalLoci")
      pw.close()
    }

    this
  }
}

object Result {
  type DC = (Depth, Counts)

  def apply(jh: JointHistogram, dir: String, rawOutput: Boolean): Result = {
    val j = jh.jh
    val fks = j.map(FK.make)

    val totalIntervalLoci = j.filter(_._1._2(1).get == 1).values.sum.toLong

    val pdf = fks.map(fk => fk.d -> Counts(fk)).reduceByKey(_ + _)

    val sc = jh.sc

    val partitionSums =
      pdf
        .values
        .mapPartitionsWithIndex((idx, iter) => {
          val sum = iter.foldLeft(Counts.empty)(_ + _)
          Iterator(sum)
        })
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

    val depthSteps = Steps.roundNumberSteps(maxDepth)

    val stepsBC = sc.broadcast(depthSteps)

    val filteredCDF =
      (for {
        (depth, count) ← cdf
        depthFilter = stepsBC.value
        if depthFilter(depth)
      } yield {
        depth → count
      }).collect.sortBy(_._1)

    val firstElem = filteredCDF.take(1)(0)
    if (firstElem._1 != 0) {
      throw new Exception(s"Bad first elem: $firstElem")
    }

    val firstCounts = firstElem._2
    val totalBases = firstCounts.all.bp
    val onBases = firstCounts.on.bp

    Result(
      jh,
      dir,
      rawOutput,
      pdf.sortByKey(),
      cdf.sortByKey(),
      maxDepth,
      filteredCDF,
      totalBases,
      onBases,
      totalIntervalLoci
    )
  }
}
