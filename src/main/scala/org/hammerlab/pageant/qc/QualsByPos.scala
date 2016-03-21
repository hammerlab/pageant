package org.hammerlab.pageant.qc

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._

object QualsByPos {
  def main(args: Array[String]): Unit = {
    val inputPaths = args.slice(0, args.length - 1)
    val outputPath = args.last

    val outputPathPieces = outputPath.split("/")
    val appName = outputPathPieces.slice(outputPathPieces.length - 2, outputPathPieces.length).mkString("/")

    val conf = new SparkConf()
    conf.setAppName(s"QC:quals:$appName")
    val sc = new SparkContext(conf)

    writeQualsByPos(sc, inputPaths, outputPath)
  }

  def writeQualsByPos(sc: SparkContext, inputPaths: Array[String], outputPath: String) = {
    val reads = sc.union(inputPaths.map(sc.loadAlignments(_).rdd))
    val qualsByPos =
      (for {
        read ← reads
        (qual, i) ← read.getQual.zipWithIndex
      } yield {
        (i, qual) → 1L
      })
      .reduceByKey(_ + _)
      .map(p ⇒ p._1._1 → (p._1._2 → p._2))
      .groupByKey()
      .mapValues(_.toMap)
      .sortByKey()
      .collect

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val qualsPW = new PrintWriter(fs.create(new Path(outputPath)))
    for {
      (i, map) ← qualsByPos
      (qual, count) ← map.toList.sortBy(_._1)
    } {
      qualsPW.println(List(i, qual, count).mkString(","))
    }
    qualsPW.close()
  }
}
