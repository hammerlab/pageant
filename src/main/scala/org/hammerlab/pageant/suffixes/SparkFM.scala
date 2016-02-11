package org.hammerlab.pageant.suffixes

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

import org.hammerlab.pageant.suffixes.SparkFM._

case class PartialSum(a: Array[Long], n: Long)
object PartialSum {
  def apply(a: Array[Long]): PartialSum = PartialSum(a, a.sum)
}

case class BWTBlock(startIdx: Long, endIdx: Long, startCounts: Array[Long], data: Array[T]) {
  def occ(t: T, bound: Bound): Long = {
    startCounts(t) + data.take((bound - startIdx).toInt).count(_ == t)
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

case class Needle(idx: Idx, ts: Array[T], bound: Bound, isLow: Boolean) {
  override def toString: String = {
    s"Needle($idx, ${ts.mkString(",")}, $bound, $isLow)"
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
          //println(s"new block: $block, ${startCounts.mkString(",")}, ${counts.mkString(",")}")
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

  
  def occ(tss: RDD[Array[T]]): RDD[(Array[T], Bound, Bound)] = {
    val tssi = tss.zipWithIndex().map(p => (p._2, p._1)).setName("tssi")
    tssi.cache()

    val cur: RDD[(BlockIdx, Needle)] =
      tssi.mapPartitions(
        iter => {
          for {
            (tIdx, ts) <- iter
            (blockIdx, bound, isLow) <- List(
              (0L, 0L, true),                              // Starting low bound
              ((count - 1) / countInterval, count, false)  // Starting high bound
            )
          } yield
            (
              blockIdx,
              Needle(tIdx, ts, bound, isLow)
            )
        }
      )

    val finished = occRec(cur, sc.emptyRDD[(Idx, (Bound, Boolean))])

    (for {
      (idx, (tsIter, bounds)) <- tssi.cogroup(finished)
    } yield {

      assert(tsIter.size == 1, s"Found ${tsIter.size} ts with idx $idx")
      val ts = tsIter.head

      assert(bounds.size == 2, s"Found ${bounds.size} bounds for idx $idx, ts $ts")
      val (first, second) = (bounds.head, bounds.tail.head)
      val (lo, hi) = (first, second) match {
        case ((l, true), (h, false)) => (l, h)
        case ((h, false), (l, true)) => (l, h)
        case _ =>
          throw new Exception(
            s"Bad bounds: ${first._1},${first._2} ${second._1},${second._2}"
          )
      }

      (idx, (ts, lo, hi))
    }).sortByKey().map(_._2)
  }

  def occRec(cur: RDD[(BlockIdx, Needle)],
             finished: RDD[(Idx, (Bound, Boolean))]): RDD[(Idx, (Bound, Boolean))] = {
    val next: RDD[(BlockIdx, Needle)] =
      cur.cogroup(bwtBlocks).flatMap {
        case (blockIdx, (tuples, blocks)) =>
          assert(blocks.size == 1, s"Got ${blocks.size} blocks for block idx $blockIdx")
          val block = blocks.head
          val totalSumsV = totalSumsBC.value
          for {
            Needle(idx, ts, bound, isLow) <- tuples
            lastT = ts.last
            newTs = ts.dropRight(1)
            c = totalSumsV(lastT)
            o = block.occ(lastT, bound)
            newBound = c + o
          } yield {
            (
              (if (isLow) newBound else newBound - 1) / countInterval,
              Needle(idx, newTs, newBound, isLow)
            )
          }
      }

    next.checkpoint()

    val newFinished =
      for {
        (blockIdx, Needle(idx, ts, bound, isLow)) <- next
        if ts.isEmpty
      } yield {
        (idx, (bound, isLow))
      }

    val notFinished = next.filter(_._2.ts.nonEmpty).setName("leftover")
    notFinished.cache()
    val numLeft = notFinished.count()
    if (numLeft > 0) {
      occRec(notFinished, finished ++ newFinished)
    } else {
      newFinished
    }
  }
}

object SparkFM {
  type T = Int
  type V = Long
  type Idx = Long
  type BlockIdx = Long
  type Bound = Long
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
}
