package org.hammerlab.pageant.utils

import org.apache.spark.{ SparkConf, SparkContext }

object FilterCSV {

  def parseRanges(s: String): ((Long, Long), (Long, Long)) = {
    val comma = s.indexOf(',')
    if (comma >= 0) {
      (parseRange(s.substring(0, comma)), parseRange(s.substring(comma + 1)))
    } else {
      val r = parseRange(s)
      (r, r)
    }
  }

  def parseRange(s: String): (Long, Long) = {

    def parse(str: String, default: Long = Long.MaxValue): Long = {
      if (str.isEmpty)
        default
      else
        str.toLong
    }

    val pieces = s.split(":")
    pieces match {
      case Array(e) ⇒ (0, parse(e))
      case Array(st, e) ⇒ (parse(st, 0L), parse(e))

    }
  }

  def parseCols(s: String): (Int, Int) = {
    val pieces = s.split(',').map(_.toInt)
    (pieces(0), pieces(1))
  }

  def main(unparsed: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("pageant")
    val sc = new SparkContext(conf)

    val Args(strs, longs, bools, args) =
      Args(
        unparsed,
        defaults = Map(
          "range" → "100",
          "partitions" → 1,
          "cols" → "0,1",
          "stable" → false
        ),
        aliases = Map(
          'p' → "partitions",
          'r' → "range",
          'c' → "cols",
          's' → "stable"
        )
      )

    val ((lo1, hi1), (lo2, hi2)) = parseRanges(strs("range"))
    val (col1, col2) = parseCols(strs("cols"))
    val csvPath = args(0)
    val outPath = args(1)
    println(s"Filtering $csvPath: col $col1: [$lo1,$hi1),  col $col2: [$lo2,$hi2); writing to $outPath")
    val processed =
      sc
        .textFile(csvPath)
        .map(_.split(','))
        .filter(t ⇒ {
          val v1 = t(col1).toLong
          val v2 = t(col2).toLong
          lo1 <= v1 && v1 < hi1 && lo2 <= v2 && v2 < hi2
        })
        .map(_.mkString(","))

    val numPartitions = longs("partitions").toInt

    val repartitioned =
      if (bools("stable")) {
        processed
          .zipWithIndex()
          .map(_.swap)
          .repartition(numPartitions)
          .sortByKey()
          .values
      } else {
        processed.repartition(numPartitions)
      }

    repartitioned.saveAsTextFile(outPath)
  }
}
