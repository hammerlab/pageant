package org.hammerlab.pageant.fm.index

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTBlock, BWTRun, BWTRunIterator, BWTRunsIterator, MergeInsertsIterator, RunLengthBWTBlock}
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
      throw new Exception(s"Can't find partition for $k in bounds ${bounds.mkString(",")}")
    bounds(i)._1
  }
}

class DualBoundPartitioner(bounds: Seq[(Int, Long)]) extends BoundPartitioner(bounds) {
  override def keyToBound(key: Any): Long = key.asInstanceOf[Tuple2[Long, Long]]._2
}

object DirectFM {
//  def apply(ts: RDD[VT]): RDD[BWTBlock] = {
//
//  }

//  def rec(tss: RDD[(BlockIdx, (VT, Long))],
//          idx: Int,
//          blocks: RDD[(BlockIdx, RunLengthBWTBlock)],
//          partitionBounds: Seq[(Int, Long)],
//          totals: Map[T, Long],
//          blockSize: Int): RDD[RunLengthBWTBlock] = {
//
//    val partitioner = new BoundPartitioner(partitionBounds)
//
//    val inserts: RDD[(Long, VT)] =
//      (for {
//        (blockIdx, ((ts, pos), block)) <- tss.join(blocks)
//        last = ts.last
//        occ = block.occ(last, pos)
//        count = totals(last)
//        newPos = occ + count
//      } yield {
//        newPos -> ts.dropRight(1)
//      }).repartitionAndSortWithinPartitions(partitioner)
//
//    val partitionInsertCounts = inserts.mapPartitions(iter => {
//      val counts = Array.fill(N)(0)
//      for {
//        (pos, ts) <- iter
//        last = ts.last
//      } {
//        counts(last) += 1
//      }
//      Iterator(counts)
//    }).collect
//
//    val curCounts = Array.fill(N)(0L)
//    var partitionInsertSums: ArrayBuffer[Counts] = ArrayBuffer(curCounts.clone())
//    partitionInsertCounts.foreach(counts => {
//      (0 until N).foreach(i => curCounts(i) += counts(i))
//      partitionInsertSums.append(curCounts.clone())
//    })
//
//    val sc = tss.context
//    val insertCountsRDD = sc.parallelize(partitionInsertSums, partitionBounds.length)
//
//    blocks.values.zipPartitions(inserts, insertCountsRDD)((blocksIter, insertsIter, insertCountsIter) => {
//      val insertCounts = insertCountsIter.next()
//      if (insertCountsIter.hasNext) {
//        throw new Exception(
//          s"Got multiple insertCounts:\n${(insertCountsIter ++ Iterator(insertCounts)).map(_.mkString(",")).mkString("\n")}"
//        )
//      }
//      new MergeInsertsIterator(blocksIter, insertsIter, insertCounts, blockSize)
//    })
//
//    for {
//      (blockIdx, (blocksIter, insertsIter)) <- blocks.cogroup(inserts)
//    } yield {
//      val block = blocksIter.head
//      if (blocksIter.size != 1) {
//        throw new Exception(s"Got more than one block for idx $blockIdx:\n${blocksIter.mkString("\n")}")
//      }
//    }
//  }
}
