package org.hammerlab.pageant.fmi

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fmi.SparkFM._
import org.hammerlab.pageant.suffixes.PDC3
import Utils._

import scala.collection.mutable.ArrayBuffer

case class PartialSum(a: Array[Long], n: Long)
object PartialSum {
  def apply(a: Array[Long]): PartialSum = PartialSum(a, a.sum)
}

case class BWTBlock(startIdx: Long, endIdx: Long, startCounts: Array[Long], data: AT) {
  def occ(t: T, bound: Bound): Long = {
    startCounts(t) + data.take((bound.v - startIdx).toInt).count(_ == t)
  }

  override def toString: String = {
    s"BWTC([$startIdx,$endIdx): ${startCounts.mkString(",")}, ${data.mkString(",")})"
  }

  override def equals(other: Any): Boolean = {
    other match {
      case b: BWTBlock =>
        startIdx == b.startIdx &&
        endIdx == b.endIdx &&
        startCounts.sameElements(b.startCounts) &&
        data.sameElements(b.data)
      case _ => false
    }
  }
}

trait Needle {
  def idx: Idx
  def start: TPos
  def end: TPos
  def bound: Bound
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty
  def keyByPos: ((Idx, TPos, TPos), Bound) = (idx, start, end) -> bound
}

case class TNeedle(idx: Idx, end: TPos, remainingTs: AT, bound: Bound) extends Needle {
  def start = remainingTs.length
  def isEmpty: Boolean = remainingTs.isEmpty
  override def toString: String = {
    s"Needle($idx($end), ${remainingTs.map(toC).mkString("")}, $bound)"
  }
}

case class PosNeedle(idx: Idx, start: TPos, end: TPos, bound: Bound) extends Needle {
  def isEmpty: Boolean = start == 0
  override def toString: String = {
    s"Needle($idx[$start,$end): $bound)"
  }
}

trait Bound {
  def v: Long
  def blockIdx(blockSize: Long): Long
  def move(n: Long): Bound
}

case class LoBound(v: Long) extends Bound {
  def blockIdx(blockSize: Long) = v / blockSize
  def move(n: Long) = LoBound(n)
  override def toString: String = s"$v↓"
}
case class HiBound(v: Long) extends Bound {
  def blockIdx(blockSize: Long) = (v - 1) / blockSize
  def move(n: Long) = HiBound(n)
  override def toString: String = s"$v↑"
}

case class Bounds(lo: LoBound, hi: HiBound) {
  override def toString: String = s"Bounds(${lo.v}, ${hi.v})"
  def toTuple: (Long, Long) = (lo.v, hi.v)
}
object Bounds {
  def apply(lo: Long, hi: Long): Bounds = Bounds(LoBound(lo), HiBound(hi))
  def merge(bounds: Iterable[Bound]): Bounds = {
    bounds.toArray match {
      case Array(l: LoBound, h: HiBound) => Bounds(l, h)
      case Array(h: HiBound, l: LoBound) => Bounds(l, h)
      case _ =>
        throw new Exception(s"Bad bounds: ${bounds.mkString(",")}")
    }
  }
}

abstract class SparkFM[NT <: Needle](saZipped: RDD[(V, Idx)],
                                     tZipped: RDD[(Idx, T)],
                                     count: Long,
                                     N: Int,
                                     blockSize: Int = 100) extends Serializable {
  @transient protected val sc = saZipped.sparkContext

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
    saZipped.join(tShifted).map(p => {
      val (sufPos, (idx, t)) = p
      (idx, t)
    })
    .sortByKey().setName("indexedBwtt")

  indexedBwtt.cache()

  @transient val bwtt: RDD[T] = indexedBwtt.map(_._2).setName("bwtt")

  @transient val partitionCounts: RDD[Array[Int]] =
    bwtt.mapPartitions(
      iter => {
        var counts: Array[Int] = Array.fill(N)(0)
        iter.foreach(t => {
          counts(t) += 1
        })
        Array(counts).iterator
      },
      preservesPartitioning = false
    ).setName("partitionCounts")

  @transient val lastCounts: Array[Array[Int]] = partitionCounts.collect

  @transient val summedCountsBuf: ArrayBuffer[(Array[Long], Long)] = ArrayBuffer()
  @transient var curSummedCounts = Array.fill(N)(0L)
  @transient var total = 0L
  lastCounts.foreach(lastCount => {
    summedCountsBuf.append((curSummedCounts.clone(), total))
    var i = 0
    lastCount.foreach(c => {
      curSummedCounts(i) += c
      i += 1
      total += c
    })
  })

  var totalSums = Array.fill(N)(0L)
  for {i <- 1 until N} {
    totalSums(i) = totalSums(i - 1) + curSummedCounts(i - 1)
  }
  val totalSumsBC = sc.broadcast(totalSums)

  val summedCounts: Array[(Array[Long], Long)] = summedCountsBuf.toArray
  val summedCountsRDD = sc.parallelize(summedCounts, summedCounts.length)

  val bwtBlocks: RDD[(BlockIdx, BWTBlock)] =
    indexedBwtt.zipPartitions(summedCountsRDD)((bwtIter, summedCountIter) => {
      var (startCounts, total) = summedCountIter.next()
      assert(
        summedCountIter.isEmpty,
        s"Got more than one summed-count in partition starting from $startCounts $total"
      )

      var data: ArrayBuffer[T] = ArrayBuffer()
      var rets: ArrayBuffer[Array[Int]] = ArrayBuffer()
      var blocks: ArrayBuffer[(BlockIdx, BWTBlock)] = ArrayBuffer()
      var blockIdx = -1L
      var startIdx = -1L
      var idx = -1L
      var counts: Array[Long] = null

      for {
        (i, t) <- bwtIter
      } {
        if (blockIdx == -1L) {
          idx = i
          blockIdx = idx / blockSize
          startIdx = idx
          counts = startCounts.clone()
        } else if (idx % blockSize == 0) {
          val block = BWTBlock(startIdx, idx, startCounts.clone(), data.toArray)
          blocks.append((blockIdx, block))
          blockIdx = idx / blockSize
          startIdx = idx
          data.clear()
          startCounts = counts.clone()
        }
        counts(t) += 1
        data.append(t)
        idx += 1
      }

      if (data.nonEmpty) {
        blocks.append((blockIdx, BWTBlock(startIdx, idx, startCounts.clone(), data.toArray)))
      }
      blocks.toIterator
    }).groupByKey.mapValues(iter => {
      val data: ArrayBuffer[T] = ArrayBuffer()
      val blocks = iter.toArray.sortBy(_.endIdx)
      val first = blocks.head
      val last = blocks.last
      for {block <- blocks} {
        data ++= block.data
      }
      BWTBlock(first.startIdx, last.endIdx, first.startCounts, data.toArray)
    }).setName("BWTBlocks")

  bwtBlocks.cache()

  def occAll(tss: RDD[AT]): RDD[(AT, BoundsMap)]
  def occ(tss: RDD[AT]): RDD[(AT, Bounds)]

  def occsToBoundsMap(occs: RDD[Needle]): RDD[(Idx, BoundsMap)] = {
    occs
      .map(_.keyByPos)
      .groupByKey()
      .mapValues(Bounds.merge)
      .map({
        case ((tIdx, start, end), bounds) => ((tIdx, start), (end, bounds))
      })
      .groupByKey()
      .mapValues(_.toMap)
      .map({
        case ((tIdx, start), endMap) => (tIdx, (start, endMap))
      })
      .groupByKey()
      .mapValues(_.toMap)
  }

  def occsToBounds(occs: RDD[Needle]): RDD[(Idx, Bounds)] = {
    occs
      .map(n => (n.idx, n.bound))
      .groupByKey()
      .mapValues(Bounds.merge)
  }

  def findFinished(next: RDD[(BlockIdx, NT)],
                   emitIntermediateRanges: Boolean): (RDD[Needle], RDD[(BlockIdx, NT)], Long) = {
    next.checkpoint()
    val newFinished: RDD[Needle] =
      if (emitIntermediateRanges)
        next.map(_._2)
      else
        next
        .map(_._2: Needle)
        .filter(_.isEmpty)
    newFinished.cache()

    val notFinished = next.filter(_._2.nonEmpty).setName("leftover")
    notFinished.cache()
    val numLeft = notFinished.count()
    (newFinished, notFinished, numLeft)
  }

  def joinBounds[T](finished: RDD[(Idx, T)], tssi: RDD[(Idx, AT)]): RDD[(AT, T)] = {
    (for {
      (idx, (tsIter, boundsIter)) <- tssi.cogroup(finished)
    } yield {

      assert(tsIter.size == 1, s"Found ${tsIter.size} ts with idx $idx")
      val ts = tsIter.head

      idx -> (ts -> boundsIter.head)
    }).sortByKey().map(_._2)
  }
}

object SparkFM {
  type T = Int
  type AT = Array[T]
  type V = Long
  type Idx = Long
  type TPos = Int
  type BlockIdx = Long
  type BoundsMap = Map[TPos, Map[TPos, Bounds]]
  type PartitionIdx = Int
}
