package org.hammerlab.pageant.fm.blocks

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.utils.{Counts, Utils ⇒ FMUtils}
import org.hammerlab.pageant.fm.utils.Utils.{VT, toI}

import scala.reflect.ClassTag

object Utils {

  def order[U: ClassTag](rdd: RDD[(Long, U)]): Array[U] = rdd.collect.sortBy(_._1).map(_._2)

  def counts(s: String): Counts = Counts(s.trim().split("\\s+").map(_.toLong))

  def runs(str: String): Seq[BWTRun] = {
    str.split(" ").map(s ⇒ {
      val t = toI(s.last)
      val n = s.dropRight(1).toInt
      BWTRun(t, n)
    })
  }

  def makeBlocks(blockSize: Int, ss: Seq[(String, String)]): Array[RunLengthBWTBlock] = {
    (for {
      ((c, rs), i) <- ss.zipWithIndex
    } yield {
      RunLengthBWTBlock(blockSize * i, counts(c), runs(rs))
    }).toArray
  }

  def makeRLBlocks(blockSize: Int, ss: Seq[(String, String)]): Array[RunLengthBWTBlock] = {
    (for {
      ((c, ts), i) <- ss.zipWithIndex
    } yield {
      RunLengthBWTBlock.fromTs(blockSize * i, counts(c), ts.map(toI))
    }).toArray
  }

  def sToTs(s: String): VT = s.map(FMUtils.toI).toVector
}
