package org.hammerlab.pageant.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class OrderedRepartitionRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def orderedRepartition(n: Int): RDD[T] = rdd.zipWithIndex.map(_.swap).repartition(n).sortByKey().values
  def zipRepartition(n: Int): RDD[(Long, T)] = rdd.zipWithIndex.map(_.swap).repartition(n).sortByKey()
  def zipRepartitionL(n: Int): RDD[(T, Long)] = rdd.zipWithIndex.map(_.swap).repartition(n).sortByKey().map(_.swap)
}

object OrderedRepartitionRDD {
  implicit def toOrderedRepartitionRDD[T: ClassTag](rdd: RDD[T]): OrderedRepartitionRDD[T] = new OrderedRepartitionRDD(rdd)
}

