package org.hammerlab.pageant.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SlidingRDDPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[(Int, Boolean)]._1
  }
}

class SlidingRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {
  def sliding3(fill: T): RDD[(T, T, T)] = sliding3(Some(fill))
  def sliding3(fillOpt: Option[T] = None): RDD[(T, T, T)] = {
    val n = rdd.getNumPartitions
    val firstSplit: RDD[((Int, Boolean), Vector[T])] =
      rdd.mapPartitionsWithIndex((idx, iter) => {
        val arr = iter.toVector
        if (idx == 0)
          Iterator(((0, false), arr)) ++ fillOpt.map(fill => ((n - 1, true), Vector(fill, fill))).toIterator
        else {
          val first = arr(0)
          val second = arr(1)

          val prevPartition: ((Int, Boolean), Vector[T]) = ((idx - 1, true), Vector(first, second))
          val nextPartition: ((Int, Boolean), Vector[T]) = ((idx, false), arr)

          Iterator(prevPartition, nextPartition)
        }
      })

    firstSplit
      .repartitionAndSortWithinPartitions(new SlidingRDDPartitioner(rdd.getNumPartitions))
      .values
      .mapPartitions(iter => {
        val elems = iter.next
        (if (iter.hasNext) {
          elems ++ iter.next
        } else {
          elems
        }).sliding(3)
          .filter(_.length == 3)
          .map(l => (l(0), l(1), l(2)))
      })
  }
}

object SlidingRDD {
  implicit def toSlidingRDD[T: ClassTag](rdd: RDD[T]): SlidingRDD[T] = new SlidingRDD[T](rdd)
}
