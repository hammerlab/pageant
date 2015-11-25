package org.hammerlab.pageant.scratch

import org.hammerlab.pageant.reads.Bases
import org.hammerlab.pageant.scratch.CountsRDD._

object BasesRDD {

  import org.apache.spark.rdd.RDD

  type BasesRDD = RDD[((Bases, Boolean), Long)]

  def stepsFn(k: Int) = s"$filePrefix.${k}mers.steps"

  def loadSteps(k: Int): BasesRDD =
    sc.objectFile(stepsFn(k)).setName(s"${k}mers")

  def reduce(rdd: BasesRDD, numPartitions: Int = 10000): CountsRDD = {
    rdd.map(p => (p._1._1, p._2)).reduceByKey(_ + _, numPartitions)
  }

  def writeReduced(k: Int, numPartitions: Int = 10000): CountsRDD = {
    val r = reduce(loadSteps(k), numPartitions)
    r.saveAsObjectFile(countsFn(k))
    r
  }

  def stepKmers(rdd: BasesRDD,
                k: Int,
                numPartitions: Int = 5000,
                cache: Boolean = true): BasesRDD = {
    val ks = (for {
      ((bases, isFirst), num) <- rdd
      startIdx = if (isFirst) 0 else bases.length - k
      i <- startIdx to bases.length - k
      sub = bases.slice(i, i + k)
    } yield {
      ((sub, isFirst && i == 0), num)
    }).reduceByKey(_ + _, numPartitions).setName(s"${k}mers")
    if (cache) ks.cache
    else ks
  }

  def nextStep(rdd: BasesRDD,
               k: Int, numPartitions: Int = 5000,
               cache: Boolean = true,
               writeFile: Boolean = true): (BasesRDD, Long) = {
    val ks = stepKmers(rdd, k, numPartitions, cache)
    ks.saveAsObjectFile(stepsFn(k))
    val count = ks.map(_._1._1).distinct.count
    (ks, count)
  }

  def steps(fromK: Int, toK: Int, numPartitions: Int = 10000): BasesRDD =
    (fromK to toK by -1).foldLeft(loadSteps(fromK + 1))((rdd, k) => {
      val next = stepKmers(rdd, k, numPartitions)
      next.saveAsObjectFile(stepsFn(k))
      rdd.unpersist()
      next
    })

}
