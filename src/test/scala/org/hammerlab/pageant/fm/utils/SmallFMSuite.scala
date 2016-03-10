package org.hammerlab.pageant.fm.utils

import org.apache.spark.SparkContext
import Utils._
import org.hammerlab.pageant.fm.index.FMIndex.FMI
import org.hammerlab.pageant.fm.index.PDC3FMBuilder
import org.hammerlab.pageant.suffixes.dc3.DC3
import org.hammerlab.pageant.utils.Utils._

trait SmallFMSuite extends FMSuite {
  def saPartitions: Int
  def ts: String
  def tsPartitions: Int
  def blockSize: Int

  var sa: Array[Int] = _
  var bwt: Array[T] = _

  override def initFM(sc: SparkContext): FMI = {
    val (sa2, bwt2, fm2) = SmallFMSuite.initFM(sc, saPartitions, ts, tsPartitions, blockSize)
    sa = sa2
    bwt = bwt2
    fm2
  }
}

object SmallFMSuite {
  def initBWT(ts: String): (Array[Int], AT) = {
    val sa = DC3.make(ts.map(toI(_).toInt).toArray, 6)

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
    (sa, bwt)
  }

  def initFM(sc: SparkContext,
             saPartitions: Int,
             ts: String,
             tsPartitions: Int,
             blockSize: Int,
             N: Int = 6): (Array[Int], Array[T], FMI) = {
    val (sa, bwt) = initBWT(ts)
    (sa, bwt, initFM(sc, saPartitions, ts, tsPartitions, sa, bwt, blockSize, N))
  }

  def initFM(sc: SparkContext,
             saPartitions: Int,
             tsStr: String,
             tsPartitions: Int,
             saArr: Array[Int],
             bwt: AT,
             blockSize: Int,
             N: Int): FMI = {

    val sa = sc.parallelize(saArr.map(_.toLong), saPartitions)
    val ts = sc.parallelize(tsStr.map(toI), tsPartitions)

    PDC3FMBuilder.withSA(sa, ts, tsStr.length, N = N, blockSize = blockSize)
  }
}
