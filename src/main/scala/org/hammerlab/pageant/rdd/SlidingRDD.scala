package org.hammerlab.pageant.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SlidingRDDPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
}

class SlidingRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {
  def sliding3(fill: T): RDD[(T, T, T)] = sliding3(Some(fill))
  def sliding3(fillOpt: Option[T] = None): RDD[(T, T, T)] = {
    val N = rdd.getNumPartitions
    val firstSplit: RDD[(T, T)] =
      rdd.mapPartitionsWithIndex((idx, iter) =>
        if (idx == 0)
          fillOpt.map(fill =>
            (N - 1) → (fill, fill)
          ).toIterator
        else
          Iterator(
            (idx - 1) → (iter.next(), iter.next())
          )
      ).partitionBy(new SlidingRDDPartitioner(N)).values

    rdd.zipPartitions(firstSplit)((iter, tailIter) ⇒ {
      val tail =
        if (tailIter.hasNext) {
          val (tail1, tail2) = tailIter.next()
          Iterator(tail1, tail2)
        } else {
          Iterator()
        }
      (iter ++ tail).sliding(3).withPartial(false).map(l => (l(0), l(1), l(2)))
    })
  }

  def sliding(n: Int, fill: T): RDD[Seq[T]] = sliding(n, Some(fill))
  def sliding(n: Int, fillOpt: Option[T] = None): RDD[Seq[T]] = {
    val N = rdd.getNumPartitions
    val firstSplit: RDD[Array[T]] =
      rdd.mapPartitionsWithIndex((idx, iter) =>
        if (idx == 0)
          fillOpt.map(fill =>
            (N - 1) → Array.fill(n - 1)(fill)
          ).toIterator
        else
          Iterator(
            (idx - 1) → iter.take(n - 1).toArray
          )
      ).partitionBy(new SlidingRDDPartitioner(N)).values

    rdd.zipPartitions(firstSplit)((iter, tailIter) ⇒ {
      val tail =
        if (tailIter.hasNext) {
          tailIter.next().toIterator
        } else {
          Iterator()
        }
      (iter ++ tail).sliding(n).withPartial(false)
    })
  }

  def slideUntil(sentinel: T): RDD[Seq[T]] = {
    val N = rdd.getNumPartitions
    val firstSplit: RDD[(Int, ArrayBuffer[T])] =
      rdd.mapPartitionsWithIndex((idx, iter) =>
          if (idx == 0)
            Iterator()
          else {
            val toSentinel: ArrayBuffer[T] = ArrayBuffer()
            var filling = true
            while (iter.hasNext && filling) {
              val next = iter.next()
              if (next == sentinel)
                filling = false
              else
                toSentinel.append(next)
            }
            Iterator(
              (idx - 1) → toSentinel
            )
          }
      ).partitionBy(new SlidingRDDPartitioner(N))

    rdd.zipPartitions(firstSplit)((iter, tailIter) ⇒ {
      val (partitionIdx, tail) =
        if (tailIter.hasNext) {
          val (pIdx, buf) = tailIter.next()
          (pIdx, buf.toIterator)
        } else {
          (N - 1, Iterator())
        }

      val it =
        (if (partitionIdx == 0)
          iter
        else
          iter.dropWhile(_ != sentinel)
        ) ++ tail

      new TakeUntilIterator[T](it, sentinel)
    })
  }

}

object SlidingRDD {
  implicit def toSlidingRDD[T: ClassTag](rdd: RDD[T]): SlidingRDD[T] = new SlidingRDD[T](rdd)
}
