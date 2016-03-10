package org.hammerlab.pageant.fm.index

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTBlock, BWTRun, BWTRunsIterator, MergeInsertsIterator, RunLengthBWTBlock}
import org.hammerlab.pageant.fm.utils.Counts
import org.hammerlab.pageant.fm.utils.Utils.{AT, BlockIdx, N, T, VT}

import scala.collection.mutable.ArrayBuffer

class BoundPartitioner(bounds: Seq[(Int, Long)]) extends Partitioner {
  override def numPartitions: Int = bounds.length

  def keyToBound(key: Any): Long = key.asInstanceOf[Long]

  override def getPartition(key: Any): Int = {
    val k = keyToBound(key)
    var i = 0
    while (i < bounds.length && bounds(i)._2 <= k) i += 1
    if (i == bounds.length)
      if (bounds(i - 1)._2 == k)
        bounds(i - 1)._1
      else
        throw new Exception(s"Can't find partition for $k in bounds ${bounds.mkString(",")}")
    else
      bounds(i)._1
  }
}

class DualBoundPartitioner(bounds: Seq[(Int, Long)]) extends BoundPartitioner(bounds) {
  override def keyToBound(key: Any): Long = key.asInstanceOf[Tuple2[Long, Long]]._2
}

