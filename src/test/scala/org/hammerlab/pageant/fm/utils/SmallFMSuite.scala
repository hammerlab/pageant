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
    val (sa2, bwt2, fm2) = SmallFMSuite.initFM(sc, saPartitions, ts, tsPartitions, blockSize)
    sa = sa2
    bwt = bwt2
    fm2
  }
}

object SmallFMSuite {
  def initFM(sc: SparkContext,
             saPartitions: Int,
             ts: String,
             tsPartitions: Int,
             blockSize: Int,
             N: Int = 6): (Array[Int], Array[Int], SparkFM) = {
    val sa = KarkainnenSuffixArray.make(ts.map(toI).toArray, 6)

    val bwtu =
      sa
      .map(x => if (x == 0) ts.length - 1 else x - 1)
      .zipWithIndex
      .sortBy(_._1)
      .map(_._2)
      .zip(ts)
      .sortBy(_._1)
      .map(_._2)

    val bwt = bwtu.map(toI)

    val saZipped = sc.parallelize(sa.map(_.toLong), saPartitions).zipWithIndex()
    val tZipped = sc.parallelize(ts.map(toI), tsPartitions).zipWithIndex().map(rev)

    (sa, bwt, SparkFM(saZipped, tZipped, ts.length, N = N, blockSize = blockSize))
  }
}
