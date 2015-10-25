package org.hammerlab.pageant

object CountsRDD {

  import org.apache.spark.rdd.RDD

  type CountsRDD = RDD[(Bases, Long)]

  def countsFn(k: Int) = s"$filePrefix.${k}mers.counts"

  def loadCounts(k: Int): CountsRDD = sc.objectFile(countsFn(k)).setName(s"${k}counts")

  def kmers(rdd: RDD[Bases],
            k: Int,
            numPartitions: Int = 5000,
            cache: Boolean = true): CountsRDD = {
    val ks = (for {
      bases <- rdd
      i <- 0 to bases.length - k
      sub = bases.slice(i, i + k)
    } yield {
      sub -> 1L
    }).reduceByKey(_ + _, numPartitions).setName(s"${k}mers")
    if (cache) ks.cache
    else ks
  }
}
