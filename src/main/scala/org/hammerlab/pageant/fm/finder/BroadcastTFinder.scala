package org.hammerlab.pageant.fm.finder

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.finder.BroadcastTFinder.TMap
import org.hammerlab.pageant.fm.index.FMIndex.FMI
import org.hammerlab.pageant.fm.index.PDC3FMBuilder
import org.hammerlab.pageant.fm.utils.{Bound, Bounds, BoundsMap, HiBound, LoBound}
import org.hammerlab.pageant.fm.utils.Utils.{AT, BlockIdx, Idx, T, TPos}

/**
  * FMFinder implementation that performs LF-mappings using a broadcasted Map of all target sequences.
  *
  * If the target sequences are (in aggregate) larger than makes sense to broadcast, this can run in to problems.
  */
case class BroadcastTFinder(fm: FMI) extends FMFinder[PosNeedle](fm) with Serializable {

  def occAll(tssRdd: RDD[AT]): RDD[(AT, BoundsMap)] = {
    val (tssi, tsBC) = tssToIndexAndBroadcast(tssRdd)

    val cur: RDD[(BlockIdx, PosNeedle)] =
      for {
        (tIdx, ts) <- tssi
        end <- ts.indices
        bound: Bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
        blockIdx = bound.blockIdx(blockSize)
      } yield
        (
          blockIdx,
          PosNeedle(tIdx, end+1, end+1, bound)
        )

    val occs = occRec(
      cur,
      sc.emptyRDD[Needle],
      tsBC,
      emitIntermediateRanges = true
    )

    joinBounds(tssi, occsToBoundsMap(occs))
  }

  def occ(tssRdd: RDD[AT]): RDD[(AT, Bounds)] = {
    val (tssi, tsBC) = tssToIndexAndBroadcast(tssRdd)

    val cur: RDD[(BlockIdx, PosNeedle)] =
      for {
        (tIdx, ts) <- tssi
        bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
        blockIdx = bound.blockIdx(blockSize)
      } yield
        (
          blockIdx,
          PosNeedle(tIdx, ts.length, ts.length, bound)
        )

    val occs = occRec(
      cur,
      sc.emptyRDD[Needle],
      tsBC,
      emitIntermediateRanges = false
    )

    joinBounds(tssi, occsToBounds(occs))
  }

  def occBidi(tssRdd: RDD[(AT, TPos, TPos)]): RDD[(AT, BoundsMap)] = {
    val tssi = tssRdd.zipWithIndex().map(_.swap).setName("tssi")
    val tsBC = tssToBroadcast(tssi.map(p => (p._1, p._2._1)))

    val cur: RDD[(BlockIdx, PosNeedle)] =
      for {
        (tIdx, (ts, l, r)) <- tssi
        end <- r to ts.length
        bound: Bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
        blockIdx = bound.blockIdx(blockSize)
      } yield
        (
          blockIdx,
          PosNeedle(tIdx, end, end, bound)
        )

    val occs = occRec(
      cur,
      sc.emptyRDD[Needle],
      tsBC,
      emitIntermediateRanges = true
    )

    for {
      ((ts, l, r), boundsMap) <- joinBounds(tssi, occsToBoundsMap(occs))
    } yield {
      ts -> boundsMap.filter(l, r)
    }
  }

  def occRec(cur: RDD[(BlockIdx, PosNeedle)],
             finished: RDD[Needle],
             tsBC: Broadcast[Map[Idx, Map[TPos, T]]],
             emitIntermediateRanges: Boolean = true): RDD[Needle] = {
    val next: RDD[(BlockIdx, PosNeedle)] =
      cur.cogroup(bwtBlocks).flatMap {
        case (blockIdx, (tuples, blocks)) =>
          assert(blocks.size == 1, s"Got ${blocks.size} blocks for block idx $blockIdx")
          val block = blocks.head
          val totalSumsV = totalSumsBC.value
          val tss = tsBC.value
          for {
            PosNeedle(idx, start, end, bound) <- tuples
            lastT = tss(idx)(start - 1)
            c = totalSumsV(lastT)
            o = block.occ(lastT, bound)
            newBound = bound.move(c + o)
          } yield {
            (
              newBound.blockIdx(blockSize),
              PosNeedle(idx, start - 1, end, newBound)
            )
          }
      }

    val (newFinished, notFinished, numLeft) = findFinished(next, emitIntermediateRanges)
    if (numLeft > 0) {
      occRec(notFinished, finished ++ newFinished, tsBC, emitIntermediateRanges)
    } else {
      finished ++ newFinished
    }
  }

  private def tssToIndexAndBroadcast(tssRdd: RDD[AT]): (RDD[(Idx, AT)], Broadcast[TMap]) = {
    val tssi = tssRdd.zipWithIndex().map(_.swap).setName("tssi")
    (tssi, tssToBroadcast(tssi))
  }

  private def tssToBroadcast(tssi: RDD[(Idx, AT)]): Broadcast[TMap] = {
    sc.broadcast(
      (for {
        (tIdx, ts) <- tssi.collect
      } yield {
        tIdx -> ts.zipWithIndex.map(_.swap).toMap
      }).toMap
    )
  }
}

object BroadcastTFinder {
  type TMap = Map[Idx, Map[TPos, T]]
}
