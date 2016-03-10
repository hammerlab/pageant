package org.hammerlab.pageant.fm.index

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTBlock, BWTRun, BWTRunsIterator, BlockIterator, RunLengthBWTBlock}
import org.hammerlab.pageant.fm.index.FMIndex.RunLengthFMIndex
import org.hammerlab.pageant.fm.utils.Pos
import org.hammerlab.pageant.fm.utils.Utils.T
import org.hammerlab.pageant.rdd.SlidingRDDPartitioner
import org.hammerlab.pageant.suffixes.sentinel.SentinelSA

object SentinelFM {
  type Rank = Long
  type SARank = Long
  def apply(tss: RDD[T], blockSize: Int): RunLengthFMIndex = {
    val sa: RDD[Rank] = SentinelSA.fromBytes(tss)
    val count = sa.count

    val saIdxs: RDD[(Rank, SARank)] =
      sa.map(t ⇒
        if (t == 0)
          count - 1
        else
          t - 1
      ).zipWithIndex

    val tssi: RDD[(Rank, T)] = tss.zipWithIndex().map(_.swap)

    val bwt: RDD[T] =
      (for {
        (idx, (t, saIdx)) ← tssi.join(saIdxs)
      } yield {
        saIdx → t
      }).sortByKey().values

    val rlBwtFirst: RDD[BWTRun] = bwt.mapPartitions(it ⇒ new RunLengthIterator(it))
    val numPartitions = rlBwtFirst.getNumPartitions
    val firstElems: RDD[(Int, BWTRun)] =
      rlBwtFirst
        .mapPartitionsWithIndex((idx, it) ⇒ {
          Iterator(
            (
              if (idx > 0)
                idx - 1
              else
                numPartitions - 1,
              it.next()
            )
          )
        })
        .partitionBy(
          new SlidingRDDPartitioner(rlBwtFirst.getNumPartitions)
        )

    val rlBwt: RDD[BWTRun] = rlBwtFirst.zipPartitions(firstElems)((origRLIter, lastElemIter) ⇒ {

      val (partitionIdx, lastElem) = lastElemIter.next()

      val rlIter =
        if (partitionIdx == 0) {
          origRLIter
        } else {
          origRLIter.drop(1)
        }

      val appendedRLIter =
        if (partitionIdx == numPartitions - 1) {
          rlIter
        } else {
          rlIter ++ Iterator(lastElem)
        }

      new BWTRunsIterator(appendedRLIter)
    })

    val partitionCounts = rlBwt.mapPartitions(it ⇒ {
      var pos = Pos()
      it.foreach(pos += _)
      Iterator(pos)
    }).collect()

    val (partialSums, totalSums) = Pos.partialSums(partitionCounts)
    val sc = tss.context
    val partialSumsRDD = sc.parallelize(partialSums, numPartitions)

    val bwtBlocks =
      rlBwt.zipPartitions(partialSumsRDD)((rlIter, partialSumIter) ⇒ {
        val partialSum = partialSumIter.next()
        new BlockIterator(partialSum, blockSize, rlIter)
      })
      .groupByKey
      .mapValues(blocksIter ⇒ {
        val blocks = blocksIter.toArray.sortBy(_.startIdx)
        val first = blocks.head
        RunLengthBWTBlock(first.startIdx, first.startCounts, BWTRunsIterator(blocks.toIterator).toSeq): BWTBlock
      })
      .sortByKey()

    FMIndex(
      bwtBlocks,
      totalSums.counts.partialSum(),
      totalSums.idx,
      blockSize,
      runLengthEncoded = true
    )
  }
}
