package org.hammerlab.pageant.coverage

import org.apache.spark.{SparkConf, SparkContext}
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.utils.Args

import scala.collection.Map

object CoverageDepth {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CoverageDepth")
    val sc = new SparkContext(conf)

    val Args(strings, longs, bools, arguments) =
      Args(
        args,
        defaults = Map(
          "force" → false,
          "raw" → false,
          "interval-partition-bytes" → (1 << 20)
        ),
        aliases = Map(
          'n' → "normal",
          't' → "tumor",
          'r' → "reads",
          'i' → "intervals",
          'b' → "interval-partition-bytes",
          'j' → "joint-hist",
          'f' → "force",
          'w' → "raw"
        )
      )

    val outPath = arguments(0)

    val force = bools("force")
    val forceStr = if (force) " (forcing)" else ""

    val jh =
      (strings.get("joint-hist"), strings.get("reads")) match {
//        case (Some(jhPath), _) ⇒
//          println(s"Loading JointHistogram: $jhPath$forceStr")
//          JointHistogram.load(sc, jhPath)
        case (_, Some(readsPath)) =>
          val intervalPath = strings("intervals")

          println(s"Analyzing $readsPath against $intervalPath and writing to $outPath$forceStr")

          val jh =
            JointHistogram.fromFiles(
              sc,
              Seq(readsPath),
              Seq(intervalPath),
              bytesPerIntervalPartition = longs("interval-partition-bytes").toInt
            )

          one.Result(jh, outPath, bools("raw")).save(force)
        case _ ⇒
          val normalPath = strings("normal")
          val tumorPath = strings("tumor")
          val intervalPath = strings("intervals")

          println(s"Analyzing ($normalPath, $tumorPath) against $intervalPath and writing to $outPath$forceStr")

          val jh =
            JointHistogram.fromFiles(
              sc,
              Seq(normalPath, tumorPath),
              Seq(intervalPath),
              bytesPerIntervalPartition = longs("interval-partition-bytes").toInt
            )

          two.Result(jh, outPath, bools("raw")).save(force)
      }

  }
}
