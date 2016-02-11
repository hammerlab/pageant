package org.hammerlab.pageant.suffixes

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

import org.hammerlab.pageant.suffixes.SparkFM._

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

case class Needle(idx: Idx, end: TPos, ts: AT, bound: Bound) {
  override def toString: String = {
    s"Needle($idx($end), ${ts.map(toC).mkString("")}, $bound)"
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

case class SparkFM(saZipped: RDD[(V, Idx)],
                   tZipped: RDD[(Idx, T)],
                   count: Long,
                   N: Int,
                   countInterval: Int = 100) extends Serializable {
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
  for { i <- 1 until N } {
    totalSums(i) = totalSums(i-1) + curSummedCounts(i-1)
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
          blockIdx = idx / countInterval
          startIdx = idx
          counts = startCounts.clone()
        } else if (idx % countInterval == 0) {
          val block = BWTBlock(startIdx, idx, startCounts.clone(), data.toArray)
          blocks.append((blockIdx, block))
          blockIdx = idx / countInterval
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
      for { block <- blocks } { data ++= block.data }
      BWTBlock(first.startIdx, last.endIdx, first.startCounts, data.toArray)
    }).setName("BWTBlocks")

  bwtBlocks.cache()


  def occAll(tss: RDD[AT]): RDD[(AT, BoundsMap)] = {
    val tssi = tss.zipWithIndex().map(p => (p._2, p._1)).setName("tssi")
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

    val cur: RDD[(BlockIdx, Needle)] =
      tssPrefixes.mapPartitions(
        iter => {
          for {
            (tIdx, end, ts) <- iter
            bound: Bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
            blockIdx = bound.blockIdx(countInterval)
          } yield
            (
              blockIdx,
              Needle(tIdx, end, ts, bound)
            )
        }
      )

    val occs =
      occRec(
        cur,
        sc.emptyRDD[((Idx, TPos, TPos), Bound)],
        emitIntermediateRanges = true
      )

    val finished =
      occs
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

    (for {
      (idx, (tsIter, boundsIter)) <- tssi.cogroup(finished)
    } yield {

      assert(tsIter.size == 1, s"Found ${tsIter.size} ts with idx $idx")
      val ts = tsIter.head

      assert(boundsIter.size == 1, s"Found ${boundsIter.size} bounds with idx $idx")
      val bounds = boundsIter.head

      idx -> (ts -> bounds)
    }).sortByKey().map(_._2)
  }

  def occ(tss: RDD[AT]): RDD[(AT, Bounds)] = {
    val tssi = tss.zipWithIndex().map(p => (p._2, p._1)).setName("tssi")
    tssi.cache()

    val cur: RDD[(BlockIdx, Needle)] =
      tssi.mapPartitions(
        iter => {
          for {
            (tIdx, ts) <- iter
            bound <- List(LoBound(0L), HiBound(count))   // Starting bounds
            blockIdx = bound.blockIdx(countInterval)
          } yield
            (
              blockIdx,
              Needle(tIdx, ts.length, ts, bound)
            )
        }
      )

    val finished: RDD[(Idx, Bounds)] =
      occRec(
        cur,
        sc.emptyRDD[((Idx, TPos, TPos), Bound)],
        emitIntermediateRanges = false
      )
        .map {
          case ((idx, _, _), bound) => (idx, bound)
        }
        .groupByKey()
        .mapValues(Bounds.merge)

    (for {
      (idx, (tsIter, boundsIter)) <- tssi.cogroup(finished)
    } yield {

      assert(tsIter.size == 1, s"Found ${tsIter.size} ts with idx $idx")
      val ts = tsIter.head

      idx -> (ts -> boundsIter.head)
    }).sortByKey().map(_._2)
  }

  def occRec(cur: RDD[(BlockIdx, Needle)],
             finished: RDD[((Idx, TPos, TPos), Bound)],
             emitIntermediateRanges: Boolean = true): RDD[((Idx, TPos, TPos), Bound)] = {
    val next: RDD[(BlockIdx, Needle)] =
      cur.cogroup(bwtBlocks).flatMap {
        case (blockIdx, (tuples, blocks)) =>
          assert(blocks.size == 1, s"Got ${blocks.size} blocks for block idx $blockIdx")
          val block = blocks.head
          val totalSumsV = totalSumsBC.value
          for {
            Needle(idx, end, ts, bound) <- tuples
            lastT = ts.last
            newTs = ts.dropRight(1)
            c = totalSumsV(lastT)
            o = block.occ(lastT, bound)
            newBound = bound.move(c + o)
          } yield {
            (
              newBound.blockIdx(countInterval),
              Needle(idx, end, newTs, newBound)
            )
          }
      }

    next.checkpoint()

    val newFinished =
      for {
        (blockIdx, Needle(idx, end, ts, bound)) <- next
        if emitIntermediateRanges || ts.isEmpty
      } yield {
        ((idx, ts.length, end), bound)
      }
    newFinished.cache()
    val numNew = newFinished.count()

    val notFinished = next.filter(_._2.ts.nonEmpty).setName("leftover")
    notFinished.cache()
    val numLeft = notFinished.count()
    if (numLeft > 0) {
      occRec(notFinished, finished ++ newFinished, emitIntermediateRanges)
    } else {
      finished ++ newFinished
    }
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
//  type Bound = Long
  type PartitionIdx = Int

  def apply[U](us: RDD[U],
               N: Int,
               countInterval: Int = 100,
               toT: (U) => T): SparkFM = {
    @transient val sc = us.context
    us.cache()
    val count = us.count
    @transient val t: RDD[T] = us.map(toT)
    t.cache()
    @transient val tZipped: RDD[(Idx, T)] = t.zipWithIndex().map(p => (p._2, p._1))
    @transient val sa = PDC3(t.map(_.toLong), count)
    @transient val saZipped: RDD[(V, Idx)] = sa.zipWithIndex()

    SparkFM(saZipped, tZipped, count, N, countInterval)
  }

  val toI = "$ACGT".zipWithIndex.toMap
  val toC = toI.map(p => (p._2, p._1))
}
