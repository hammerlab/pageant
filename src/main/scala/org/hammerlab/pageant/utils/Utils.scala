package org.hammerlab.pageant.utils

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.utils.Utils.{toI, rc}

object Utils {
  def byteToHex(b: Byte) = {
    val s = b.toInt.toHexString.takeRight(2)
    if (s.length == 1) "0" + s else s
  }

  def bytesToHex(a: Array[Byte]) = a.map(byteToHex).mkString(",")
  def rev[T, U](t: (T, U)): (U, T) = (t._2, t._1)

  def resourcePath(fn: String): String = ClassLoader.getSystemClassLoader.getResource(fn).getFile

  def loadBam(sc: SparkContext, name: String, includeRC: Boolean = false): RDD[Byte] = {
    for {
      read <- sc.loadAlignments(resourcePath(name))
      seq = s"${read.getSequence}$" + (if (includeRC) s"${rc(read.getSequence)}$" else "")
      bp <- seq
    } yield {
      toI(bp)
    }
  }
}
