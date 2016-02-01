package org.hammerlab.pageant.suffixes

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//import PDC3.T
import SparkFM.{T, V, Idx, PartitionIdx}

case class PartialSum(a: Array[Long], n: Long)
object PartialSum {
  def apply(a: Array[Long]): PartialSum = PartialSum(a, a.sum)
}

case class BWTChunk(idx: Long, startCounts: Array[Long], data: Array[T]) {
  def occ(t: T, bound: Long): Long = {
    startCounts(t) + data.take((bound - idx).toInt).count(_ == t)
  }
}

class SparkFM[U](us: RDD[U], N: Int, countInterval: Int = 100, implicit val toT: (U) => T) {
  val sc = us.context
  val count = us.count
  val t: RDD[T] = us.map(toT)
  val tZipped: RDD[(Idx, T)] = t.zipWithIndex().map(p => (p._2, p._1))
  val sa = PDC3(t.map(_.toLong), count)
  val saZipped: RDD[(Idx, V)] = sa.zipWithIndex().map(p => (p._2, p._1))
  val uZipped: RDD[(Idx, U)] = us.zipWithIndex().map(p => (p._2, p._1))

  val indexedBwtt: RDD[(Idx, T)] =
    saZipped
      .map(p =>
        (
          if (p._1 == count - 1)
            0L
          else
            p._1 + 1,
          p._2
        )
      )
      .join(tZipped).map(p => {
        val (idx, (sufPos, t)) = p
        (sufPos, t)
      })
      .sortByKey()

  val bwtt: RDD[T] = indexedBwtt.map(_._2)


//  val counts = us.mapPartitions(iter => {
//    val c: mutable.Map[U, Long] = mutable.Map()
//    iter.foreach(u => c(u) = c.getOrElse(u, 0L) + 1)
//    c.toIterator
//  }).reduceByKey(_ + _).collectAsMap()

//  indexedBwt.mapPartitionsWithIndex((idx, iter) => {
//    val c: mutable.Map[U, Long] = mutable.Map()
//    iter.foreach(u => c(u) = c.getOrElse(u, 0L) + 1)
//    c.toIterator.map(p => (idx, p))
//  })

  val partitionCounts: RDD[Array[Int]] =
    bwtt.mapPartitions(iter => {
      var counts: Array[Int] = Array.fill(N)(0)
      var rets: ArrayBuffer[Array[Int]] = ArrayBuffer()
      var i = 0
      iter.foreach(t => {
        counts(t) += 1
        if (i == countInterval) {
          rets.append(counts.clone)
          i = 0
        } else {
          i += 1
        }
      })
      rets.toIterator
    }).setName("interval counts")

  partitionCounts.cache()

  val lastCounts: Array[Array[Int]] =
    partitionCounts.mapPartitions(
      iter => List(iter.toList.last).toIterator
    ).collect

  val summedCountsBuf: ArrayBuffer[(Array[Long], Long)] = ArrayBuffer()
  var curSummedCounts = Array.fill(N)(0L)
  var total = 0L
//  var partitionIdx = 0
  lastCounts.foreach(lastCount => {
    summedCountsBuf.append((curSummedCounts.clone(), total))
    var i = 0
    lastCount.foreach(c => {
      curSummedCounts(i) += c
      i += 1
      total += c
    })
//    partitionIdx += 1
  })
  val totalCounts = curSummedCounts.clone()
  var totalSums = Array.fill(N)(0L)
  for {
    (c, i) <- totalCounts.zipWithIndex
  } {
    totalSums(i) = (if (i == 0) 0L else totalSums(i-1)) + c
  }

  val totalSumsBC = sc.broadcast(totalSums)

  val summedCounts = summedCountsBuf.toArray
  val summedCountsRDD = sc.parallelize(summedCounts, summedCounts.length)

  val bwtChunks =
    indexedBwtt.zipPartitions(summedCountsRDD)((bwtIter, summedCountIter) => {
      var (counts, total) = summedCountIter.next()
      assert(
        summedCountIter.isEmpty,
        s"Got more than one summed-count in partition starting from index ${bwtIter.next()._1}"
      )

      var data: ArrayBuffer[T] = ArrayBuffer()
      var rets: ArrayBuffer[Array[Int]] = ArrayBuffer()
      var chunks: ArrayBuffer[(Long, BWTChunk)] = ArrayBuffer()
      var chunkIdx = -1L

      for { (idx, t) <- bwtIter } {
        if (chunkIdx == -1L) {
          chunkIdx = idx / countInterval
        } else if (idx % countInterval == 0) {
          chunks.append((chunkIdx, BWTChunk(idx, counts.clone(), data.toArray)))
          chunkIdx = idx / countInterval
        }
        counts(t) += 1
        data.append(t)
      }

      chunks.toIterator
    }).setName("BWTChunks")

  bwtChunks.cache()

  //  val partitioner = new Partitioner {
  //    override def numPartitions: Int = {
  //      summedCounts.length
  //    }
  //
  //    override def getPartition(key: Any): Int = {
  //      key.asInstanceOf[Int]
  //    }
  //  }
  //
  //  val totalCounts: Array[Long] = summedCounts.last._2
  //  val totalCountsBC: Broadcast[Array[Long]] = sc.broadcast(totalCounts)

  //  def getPartitionIdx(idx: Long): Int = {
  //    summedCounts.find(_._3 > idx).getOrElse(summedCounts.last)._1
  //  }

  //def findUs(uss: RDD[Array[U]]): RDD[(Array[T], Long, Long)] = occ(uss.map(_.map(toT)))
  def occ(tss: RDD[Array[T]]): RDD[(Array[T], Long, Long)] = {
    val tssi = tss.zipWithIndex().map(p => (p._2, p._1))
    val cur: RDD[(Long, (Idx, Array[T], Long, Boolean))] =
      tssi.mapPartitions(iter => {
        for {
          (tIdx, ts) <- iter
          bound <- List((0L, true), (count, false))
        } yield
          (bound._1 / countInterval, (tIdx, ts, bound._1, bound._2))
      })

    val finished = occRec(cur, sc.emptyRDD[(Idx, (Long, Boolean))])
    (for {
      (idx, (tsIter, bounds)) <- tssi.cogroup(finished)
    } yield {

      assert(tsIter.size == 1, s"Found ${tsIter.size} ts with idx $idx")
      val ts = tsIter.head

      assert(bounds.size == 2, s"Found ${bounds.size} bounds for idx $idx, t $t")
      val (first, second) = (bounds.head, bounds.tail.head)
      val (lo, hi) = (first, second) match {
        case ((h, true), (l, false)) => (l, h)
        case ((l, false), (h, true)) => (l, h)
        case _ =>
          throw new Exception(
            s"Bad bounds: ${first._1},${first._2} ${second._1},${second._2}"
          )
      }

      (idx, (ts, lo, hi))
    }).sortByKey().map(_._2)
  }

  def occRec(cur: RDD[(Long, (Idx, Array[T], Long, Boolean))],
             finished: RDD[(Idx, (Long, Boolean))]): RDD[(Idx, (Long, Boolean))] = {
    val next: RDD[(Long, (Idx, Array[T], Long, Boolean))] =
      cur.cogroup(bwtChunks).flatMap {
        case (chunkIdx, (tuples, chunks)) =>
          assert(chunks.size == 1, s"Got ${chunks.size} chunks for chunk idx $chunkIdx")
          val chunk = chunks.head
          val totalSumsV = totalSumsBC.value
          for {
            (idx, ts, bound, isLow) <- tuples
            lastT = ts.last
            newTs = ts.dropRight(1)
            newBound = totalSumsV(lastT) + chunk.occ(lastT, bound)
          } yield {
            (newBound / countInterval, (idx, newTs, newBound, isLow))
          }
      }

    next.checkpoint()

    val newFinished =
      for {
        (chunkIdx, (idx, ts, bound, isLow)) <- next
        if ts.isEmpty
      } yield {
        (idx, (bound, isLow))
      }

    val notFinished = next.filter(_._2._2.nonEmpty).setName("leftover")
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
  type PartitionIdx = Int
}
