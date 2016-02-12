package org.hammerlab.pageant.fm.utils

import org.apache.spark.SparkContext
import Utils._
import org.hammerlab.pageant.fm.index.SparkFM
import org.hammerlab.pageant.suffixes.KarkainnenSuffixArray
import org.hammerlab.pageant.utils.Utils._

trait SmallFMSuite extends FMSuite {
  def saPartitions: Int
  def ts: String
  def tsPartitions: Int
  def blockSize: Int

  var sa: Array[Int] = _
  var bwt: Array[Int] = _

  override def initFM(sc: SparkContext): SparkFM = {
    sa = KarkainnenSuffixArray.make(ts.map(toI).toArray, 6)

    val bwtu =
      sa
        .map(x => if (x == 0) ts.length - 1 else x - 1)
        .zipWithIndex
        .sortBy(_._1)
        .map(_._2)
        .zip(ts)
        .sortBy(_._1)
        .map(_._2)

    bwt = bwtu.map(toI)

    val saZipped = sc.parallelize(sa.map(_.toLong), saPartitions).zipWithIndex()
    val tZipped = sc.parallelize(ts.map(toI), tsPartitions).zipWithIndex().map(rev)

    SparkFM(saZipped, tZipped, ts.length, N = 6)
  }
}
