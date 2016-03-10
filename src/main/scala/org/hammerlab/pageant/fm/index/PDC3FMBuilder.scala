package org.hammerlab.pageant.fm.index

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTBlock, FullBWTBlock, RunLengthBWTBlock}
import org.hammerlab.pageant.fm.utils.Counts
import org.hammerlab.pageant.fm.utils.Utils.{BlockIdx, Idx, T, V}
import org.hammerlab.pageant.suffixes.pdc3.PDC3

import scala.collection.mutable.ArrayBuffer

object PDC3FMBuilder {

  def makeBwtBlocks(indexedBwtt: RDD[(Idx, T)],
                    startCountsRDD: RDD[Counts],
                    blockSize: Int,
                    runLengthEncode: Boolean = true): RDD[(BlockIdx, BWTBlock)] = {
    indexedBwtt.zipPartitions(startCountsRDD)((bwtIter, startCountIter) => {
      var startCounts = startCountIter.next()
      assert(
        startCountIter.isEmpty,
        s"Got more than one summed-count in partition starting from $startCounts"
      )

      var data: ArrayBuffer[T] = ArrayBuffer()
      var rets: ArrayBuffer[Array[Int]] = ArrayBuffer()
      var blocks: ArrayBuffer[(BlockIdx, BWTBlock)] = ArrayBuffer()
      var blockIdx = -1L
      var startIdx = -1L
      var idx = -1L
      var counts: Counts = null

      for {
        (idx, t) <- bwtIter
      } {
        if (blockIdx == -1L) {
          blockIdx = idx / blockSize
          startIdx = idx
          counts = startCounts.copy()
        } else if (idx % blockSize == 0) {
          val block = FullBWTBlock(startIdx, startCounts, data.toArray)
          blocks.append((blockIdx, block))
          blockIdx = idx / blockSize
          startIdx = idx
          data.clear()
          startCounts = counts.copy()
        }
        counts(t) += 1
        data.append(t)
      }

      if (data.nonEmpty) {
        blocks.append((blockIdx, FullBWTBlock(startIdx, startCounts, data.toArray)))
      }
      blocks.toIterator
    }).groupByKey.mapValues(iter => {
      val data: ArrayBuffer[T] = ArrayBuffer()
      val blocks = iter.toArray.sortBy(_.startIdx)
      val first = blocks.head
      for {block <- blocks} {
        data ++= block.data
      }
      (
        if (runLengthEncode)
          RunLengthBWTBlock.fromTs(first.startIdx, first.startCounts, data)
        else
          FullBWTBlock(first.startIdx, first.startCounts, data)
      ): BWTBlock
    }).sortByKey().setName("BWTBlocks")
  }

  def getStartCountsRDD(sc: SparkContext,
                        bwtt: RDD[T],
                        N: Int): (RDD[Counts], Counts) = {
    @transient val partitionCounts: RDD[Counts] =
      bwtt
        .mapPartitions(iter => Iterator(Counts(iter)))
        .setName("partitionCounts")

    @transient val lastCounts: Array[Counts] = partitionCounts.collect

    @transient val (startCounts, totalCounts) = Counts.partialSums(lastCounts)
    @transient val totalSums = totalCounts.partialSum()

    (
      sc.parallelize(startCounts, startCounts.length),
      totalSums
    )
  }

  def apply(ts: RDD[T],
            N: Int,
            blockSize: Int = 100): FMIndex = {
    val count = ts.count()
    @transient val sa = PDC3(ts.map(_.toLong), count)
    withSA(sa, ts, count, N, blockSize)
  }

  def withSA(sa: RDD[V],
             t: RDD[T],
             N: Int,
             blockSize: Int,
             runLengthEncode: Boolean): FMIndex = {
    withSA(sa, t, sa.count, N, blockSize, runLengthEncode)
  }

  def withSA(sa: RDD[V],
             t: RDD[T],
             count: Long,
             N: Int,
             blockSize: Int = 100,
             runLengthEncode: Boolean = true): FMIndex = {
    fromZipped(sa.zipWithIndex(), t.zipWithIndex().map(_.swap), count, N, blockSize, runLengthEncode)
  }

  def fromZipped(saZipped: RDD[(V, Idx)],
                 tZipped: RDD[(Idx, T)],
                 count: Long,
                 N: Int,
                 blockSize: Int = 100,
                 runLengthEncode: Boolean = true): FMIndex = {
    @transient val sc = saZipped.sparkContext

    @transient val tShifted: RDD[(Idx, T)] =
      tZipped
      .map(p =>
        (
          if (p._1 + 1 == count)
            0L
          else
            p._1 + 1,
          p._2
        )
      )

    @transient val indexedBwtt: RDD[(Idx, T)] =
      saZipped
        .join(tShifted)
        .map(p => {
          val (sufPos, (idx, t)) = p
          (idx, t)
        })
        .sortByKey().setName("indexedBwtt")

    indexedBwtt.cache()

    @transient val bwtt: RDD[T] = indexedBwtt.map(_._2).setName("bwtt")

    val (startCountsRDD, totalSums) = getStartCountsRDD(sc, bwtt, N)
    val bwtBlocks: RDD[(BlockIdx, BWTBlock)] =
      makeBwtBlocks(
        indexedBwtt,
        startCountsRDD,
        blockSize,
        runLengthEncode
      )

    bwtBlocks.cache()

    FMIndex(bwtBlocks, totalSums, count, blockSize, runLengthEncode)
  }
}
