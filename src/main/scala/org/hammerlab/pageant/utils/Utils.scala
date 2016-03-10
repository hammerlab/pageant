package org.hammerlab.pageant.utils

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.utils.Utils.{toI, rc}
import org.hammerlab.pageant.rdd.OrderedRepartitionRDD._
import org.hammerlab.pageant.rdd.IfRDD._

object Utils {
  def byteToHex(b: Byte) = {
    val s = b.toInt.toHexString.takeRight(2)
    if (s.length == 1) "0" + s else s
  }
  def b2h(byte: Byte) = byteToHex(byte)

  def byteToBinary(byte: Byte) = {
    var b = 0xff & byte
    (0 until 8).map(i => {
      val r = b % 2
      b >>= 1
      r
    }).reverse.mkString("")
  }
  def b2b(byte: Byte) = byteToBinary(byte)

  def bytesToHex(a: Array[Byte]) = a.map(byteToHex).mkString(",")
  def pt(a: Array[Byte], cols: Int = 10, binary: Boolean = true) =
    a
      .grouped(cols)
      .map(
        _.map(
          b => (if (binary) byteToBinary _ else byteToHex _)(b)
        ).mkString(" ")
      )
      .mkString("\n")

  def resourcePath(fn: String): String = ClassLoader.getSystemClassLoader.getResource(fn).getFile

  def loadBam(sc: SparkContext,
              name: String,
              includeRC: Boolean = false,
              numPartitions: Int = 0): RDD[Byte] = {
    val reads = sc.loadAlignments(name).iff(numPartitions > 0, _.orderedRepartition(numPartitions))
    for {
      read <- reads
      seq = s"${read.getSequence}$$" + (if (includeRC) s"${rc(read.getSequence)}$$" else "")
      bp <- seq
    } yield {
      toI(bp)
    }
  }

  def longToCmpFnReturn(l: Long) =
    if (l < 0) -1
    else if (l > 0) 1
    else 0

}
