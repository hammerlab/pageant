package org.hammerlab.pageant.fmi

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fmi.SparkFM._
import org.hammerlab.pageant.utils.Utils._

import scala.collection.mutable.ArrayBuffer

case class AllTFinder(fm: SparkFM) extends FMFinder[TNeedle](fm) {
  def occAll(tss: RDD[AT]): RDD[(AT, BoundsMap)] = {
    val tssi = tss.zipWithIndex().map(rev).setName("tssi")
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

    joinBounds(occsToBoundsMap(occs), tssi)
  }

  def occ(tss: RDD[AT]): RDD[(AT, Bounds)] = {
    val tssi = tss.zipWithIndex().map(rev).setName("tssi")
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
    joinBounds(occsToBounds(occs), tssi)
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
