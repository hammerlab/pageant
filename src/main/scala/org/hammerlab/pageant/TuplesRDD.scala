package org.hammerlab.pageant

object TuplesRDD {

  import org.hammerlab.pageant.BasesRDD._
  import org.apache.spark.rdd.RDD

  type TuplesRDD = RDD[(Bases, (Long, Long))]

  def tuplesFn(k: Int) = s"$filePrefix.${k}mers.tuples"

  def loadTuples(k: Int): TuplesRDD =
    sc.objectFile(tuplesFn(k)).setName(s"${k}tuples")

  def sumTuples(ta: (Long, Long), tb: (Long, Long)): (Long, Long) = (ta._1 + tb._1, ta._2 + tb._2)

  def basesRddToTuplesRdd(rdd: BasesRDD, numPartitions: Int = 10000): TuplesRDD = {
    (for {
      ((bases, isFirst), num) <- rdd
    } yield {
      (bases, if (isFirst) (num, 0L) else (0L, num))
    }).reduceByKey(sumTuples(_, _), numPartitions)
  }

  def stepTupleRdd(rdd: TuplesRDD,
                   k: Int,
                   numPartitions: Int = 10000,
                   cache: Boolean = true): TuplesRDD = {
    val ks = (for {
      (bases, (numFirsts, num)) <- rdd
      i <- 0 to bases.length - k
      if numFirsts > 0 || (i == bases.length - k && num > 0)
      sub = bases.slice(i, i + k)
    } yield {
      (
        sub,
        (
          if (i == 0) numFirsts else 0L,
          (if (i > 0) numFirsts else 0L) + (if (i == bases.length - k) num else 0L)
          )
        )
    }).reduceByKey(sumTuples(_, _), numPartitions).setName(s"${k}tuples")
    if (cache) ks.cache
    else ks
  }
}
