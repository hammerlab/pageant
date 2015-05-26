package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.Histogram.Hist
import org.hammerlab.pageant.PerContigJointHistogram.PerContigJointHist
import org.hammerlab.pageant.avro.{
  Histogram => H,
  HistogramEntry
}

case class Histogram(l: Hist, n: Long) {
  val sc = l.context

  lazy val h: H =
    H.newBuilder()
      .setReads(
        l.map(
          e => HistogramEntry.newBuilder().setDepth(e._1).setNumLoci(e._2).build()
        ).collect().toList
      ).setEntropy(entropy)
       .build()

  lazy val entropy =
    (for {
      (depth,numLoci) <- l
      p = numLoci * 1.0 / n
    } yield {
      -p * math.log(p) / math.log(2)
    }).reduce(_ + _)

}

object Histogram {
  type Hist = RDD[(Long, Long)]

  def apply(l: PerContigJointHist,
            indexFn: ((Long, Long)) => Long,
            n: Long,
            name: String): Histogram = {
    val rdh =
      l
        .map(p => (indexFn(p._1), p._2))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .setName(name)
        .persist()

    Histogram(rdh, n)
  }
}

