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
