package org.hammerlab.pageant.qc

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._

object BasesByPos {

  def main(args: Array[String]): Unit = {
    val inputPaths = args.slice(0, args.length - 1)
    val outputPath = args.last

    val outputPathPieces = outputPath.split("/")
    val appName = outputPathPieces.slice(outputPathPieces.length - 2, outputPathPieces.length).mkString("/")

    val conf = new SparkConf()
    conf.setAppName(s"QC:quals:$appName")
    val sc = new SparkContext(conf)

    writeBasesByPos(sc, inputPaths, outputPath)
  }

  def writeBasesByPos(sc: SparkContext, inputPaths: Array[String], outputPath: String, oneLinePerPos: Boolean = true) = {
    val reads = sc.union(inputPaths.map(sc.loadAlignments(_).rdd))
    val basesByPos =
      (for {
        read ← reads
        (base, i) ← read.getSequence.zipWithIndex
      } yield {
        (i, base) → 1L
      })
      .reduceByKey(_ + _)
      .map(p ⇒ p._1._1 → (p._1._2 → p._2))
      .groupByKey()
      .mapValues(_.toMap)
      .sortByKey()
      .collect

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val basesFilename = s"$outputPath"
    val basesPW = new PrintWriter(fs.create(new Path(basesFilename)))
    if (oneLinePerPos)
      for {
        (i, map) ← basesByPos
      } {
        basesPW.println(s"$i,${"ACGTN".map(c ⇒ map.getOrElse(c, 0L)).mkString(",")}")
      }
    else
      for {
        (i, map) ← basesByPos
        (base, count) ← map
      } {
        basesPW.println(List(i, base, count).mkString(","))
      }

    basesPW.close()
  }
}
