package org.hammerlab.pageant.fm.finder

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.index.FMIndex.FMI
import org.hammerlab.pageant.fm.index.SparkFMBuilder
import org.hammerlab.pageant.fm.utils.Utils.{AT, BlockIdx, Idx, TPos}
import org.hammerlab.pageant.fm.utils._

import scala.collection.mutable.ArrayBuffer

case class AllTFinder(fm: FMI) extends FMFinder[TNeedle](fm) {
  def occAll(tss: RDD[AT]): RDD[(AT, BoundsMap)] = {
    val tssi = tss.zipWithIndex().map(_.swap).setName("tssi")
    val tssPrefixes: RDD[(Idx, TPos, AT)] =
      tssi.flatMap { case (tIdx, ts) =>
        var cur = ts
        var i = ts.length
        var prefixes: ArrayBuffer[(Idx, TPos, AT)] = ArrayBuffer()
        ts.foreach(t => {
          prefixes.append((tIdx, i, cur))
          cur = cur.dropRight(1)
          i -= 1
        })
        prefixes
      }

    val cur: RDD[(BlockIdx, TNeedle)] =
      for {
        (tIdx, end, ts) <- tssPrefixes
        bound: Bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
        blockIdx = bound.blockIdx(blockSize)
      } yield
        (
          blockIdx,
          TNeedle(tIdx, end, ts, bound)
        )

    val occs = occRec(
      cur,
      sc.emptyRDD[Needle],
      emitIntermediateRanges = true
    )

    joinBounds(tssi, occsToBoundsMap(occs))
  }

  def occBidi(tss: RDD[(AT, TPos, TPos)]): RDD[(AT, BoundsMap)] = {
    val tssi = tss.zipWithIndex().map(_.swap).setName("tssi")
    val tssPrefixes: RDD[(Idx, TPos, AT)] =
      tssi.flatMap { case (tIdx, (ts, l, r)) =>
        var cur = ts
        var prefixes: ArrayBuffer[(Idx, TPos, AT)] = ArrayBuffer()
        (ts.length to r by -1).foreach(i => {
          prefixes.append((tIdx, i, cur))
          cur = cur.dropRight(1)
        })
        prefixes
      }

    val cur: RDD[(BlockIdx, TNeedle)] =
      for {
        (tIdx, end, ts) <- tssPrefixes
        bound: Bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
        blockIdx = bound.blockIdx(blockSize)
      } yield
        (
          blockIdx,
          TNeedle(tIdx, end, ts, bound)
        )

    val occs = occRec(
      cur,
      sc.emptyRDD[Needle],
      emitIntermediateRanges = true
    )

    for {
      ((ts, l, r), boundsMap) <- joinBounds(tssi, occsToBoundsMap(occs))
    } yield {
      ts -> boundsMap.filter(l, r)
    }
  }

  def occ(tss: RDD[AT]): RDD[(AT, Bounds)] = {
    val tssi = tss.zipWithIndex().map(_.swap).setName("tssi")
    tssi.cache()

    val cur: RDD[(BlockIdx, TNeedle)] =
      for {
        (tIdx, ts) <- tssi
        bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
        blockIdx = bound.blockIdx(blockSize)
      } yield
        (
          blockIdx,
          TNeedle(tIdx, ts.length, ts, bound)
        )

    val occs = occRec(
      cur,
      sc.emptyRDD[Needle],
      emitIntermediateRanges = false
    )
    joinBounds(tssi, occsToBounds(occs))
  }

  def occRec(cur: RDD[(BlockIdx, TNeedle)],
             finished: RDD[Needle],
             emitIntermediateRanges: Boolean = true): RDD[Needle] = {
    val next: RDD[(BlockIdx, TNeedle)] =
      cur.cogroup(fm.bwtBlocks).flatMap {
        case (blockIdx, (tuples, blocks)) =>
          assert(blocks.size == 1, s"Got ${blocks.size} blocks for block idx $blockIdx")
          val block = blocks.head
          val totalSumsV = fm.totalSumsBC.value
          for {
            TNeedle(idx, end, ts, bound) <- tuples
            lastT = ts.last
            newTs = ts.dropRight(1)
            c = totalSumsV(lastT)
            o = block.occ(lastT, bound)
            newBound = bound.move(c + o)
          } yield {
            (
              newBound.blockIdx(blockSize),
              TNeedle(idx, end, newTs, newBound)
            )
          }
      }

    next.checkpoint()

    val (newFinished, notFinished, numLeft) = findFinished(next, emitIntermediateRanges)
    if (numLeft > 0) {
      occRec(notFinished, finished ++ newFinished, emitIntermediateRanges)
    } else {
      finished ++ newFinished
    }
  }
}
