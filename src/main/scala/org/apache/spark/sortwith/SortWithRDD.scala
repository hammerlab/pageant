package org.apache.spark.sortwith

import java.util.Comparator

import org.apache.spark.rdd.{ShuffledRDD, RDD}

import scala.reflect.ClassTag

class SortWithRDD[T: ClassTag](rdd: RDD[T]) extends Serializable {

  def sortWithCmp(comparator: Comparator[T],
                  numPartitions: Int = rdd.partitions.length,
                  ascending: Boolean = true): RDD[T] = {
    sortWith(comparator.compare, numPartitions, ascending)
  }

  def sortWith(cmpFn: (T, T) => Int,
               numPartitions: Int = rdd.partitions.length,
               ascending: Boolean = true): RDD[T] = {

    val partitioner = new ElementRangePartitioner[T](numPartitions, rdd, cmpFn, ascending)

    def boolCmpFn(t1: T, t2: T): Boolean =
      if (ascending)
        cmpFn(t1, t2) < 0
      else
        cmpFn(t1, t2) > 0

    new ShuffledRDD[T, T, T](rdd.keyBy(x => x), partitioner).mapPartitions(iter => {
      iter.map(_._2).toArray.sortWith(boolCmpFn).toIterator
    })
  }
}

object SortWithRDD {
  implicit def rddToSortWithRdd[T: ClassTag](rdd: RDD[T]): SortWithRDD[T] = new SortWithRDD[T](rdd)
}
