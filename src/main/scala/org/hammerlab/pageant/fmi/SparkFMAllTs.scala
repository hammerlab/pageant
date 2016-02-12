package org.hammerlab.pageant.fmi

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fmi.SparkFM._
import org.hammerlab.pageant.suffixes.PDC3

import scala.collection.mutable.ArrayBuffer
import org.hammerlab.pageant.utils.Utils.rev

/**
  * SparkFM implementation that supports occAll by generating all prefixes of the query sequences and doing LF-mappings on them.
  *
  * This can explode the size of the query RDD (e.g. by ~50x for 100bp query sequences), so is not advisable with large query RDDs.
  */
case class SparkFMAllTs(saZipped: RDD[(V, Idx)],
                        tZipped: RDD[(Idx, T)],
                        count: Long,
                        N: Int,
                        blockSize: Int = 100) extends SparkFM[TNeedle](saZipped, tZipped, count, N, blockSize) {

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
      cur.cogroup(bwtBlocks).flatMap {
        case (blockIdx, (tuples, blocks)) =>
          assert(blocks.size == 1, s"Got ${blocks.size} blocks for block idx $blockIdx")
          val block = blocks.head
          val totalSumsV = totalSumsBC.value
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

object SparkFMAllTs {
  def apply[U](us: RDD[U],
               N: Int,
               toT: (U) => T,
               blockSize: Int = 100): SparkFMAllTs = {
    @transient val sc = us.context
    us.cache()
    val count = us.count
    @transient val t: RDD[T] = us.map(toT)
    t.cache()
    @transient val tZipped: RDD[(Idx, T)] = t.zipWithIndex().map(rev)
    @transient val sa = PDC3(t.map(_.toLong), count)
    @transient val saZipped: RDD[(V, Idx)] = sa.zipWithIndex()

    SparkFMAllTs(saZipped, tZipped, count, N, blockSize)
  }
}
