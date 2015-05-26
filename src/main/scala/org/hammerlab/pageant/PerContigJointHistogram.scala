
package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.PerContigJointHistogram.PerContigJointHist
import org.hammerlab.pageant.avro.{
  PerContigJointHistogram => PCJH,
  JointHistogramEntry,
  RegressionWeights => RW
}

case class PerContigJointHistogram(l: PerContigJointHist) {

  @transient
  val sc = l.context

  lazy val h = {
    PCJH.newBuilder()
      .setEntries(collectEntries)
      .setTotalLoci(n)
      .setHist1(readDepthHist1.h)
      .setHist2(readDepthHist2.h)
      .setXWeights(regressionWeights)
      .setYWeights(regression2Weights)
      .setDemingWeights1(demingWeights._1)
      .setDemingWeights2(demingWeights._2)
      .setMutualInformation(mutualInformation)
      .build()
  }

  lazy val collectEntries: java.util.List[JointHistogramEntry] = {
    (for {
      ((depth1, depth2), numLoci) <- l
    } yield {
        JointHistogramEntry.newBuilder().setDepth1(depth1).setDepth2(depth2).setNumLoci(numLoci).build()
      }).collect().toList
  }

  lazy val totalLoci = l.map(_._2).reduce(_+_)
  lazy val n = totalLoci

  lazy val readDepthHist1 = Histogram(l, _._1, n, "hist1")
  lazy val readDepthHist2 = Histogram(l, _._2, n, "hist2")
  lazy val r1 = readDepthHist1
  lazy val r2 = readDepthHist2

  lazy val lociCountsByMaxDepth: PerContigJointHist =
    l.sortBy(p => (math.max(p._1._1, p._1._2), p._1._1, p._1._2))

  lazy val diffs = l.map {
    case ((depth1, depth2), numLoci) => (depth1 - depth2, numLoci)
  }.reduceByKey(_ + _).sortBy(_._2, ascending = false).setName("diffs").persist()

  lazy val absDiffs = l.map {
    case ((depth1, depth2), numLoci) => (math.abs(depth1 - depth2), numLoci)
  }.reduceByKey(_ + _).sortBy(_._2, ascending = false).setName("abs diffs").persist()

  lazy val numDiffs = diffs.count()
  lazy val numAbsDiffs = absDiffs.count()

  lazy val ds = diffs.collect
  lazy val ads = absDiffs.collect

  lazy val sortedAbsDiffs = ads.sortBy(_._1)
  lazy val (cumulativeAbsDiffs, _) = sortedAbsDiffs.foldLeft((List[(Long,Long)](), 0L))((soFar, p) => {
    (soFar._1 :+ (p._1, soFar._2 + p._2), soFar._2 + p._2)
  })

  lazy val cumulativeAbsDiffFractions = cumulativeAbsDiffs.map(p => (p._1, p._2 * 1.0 / totalLoci))

  lazy val lociWithAZeroDepth =
    l
      .filter(p => p._1._1 == 0 || p._1._2 == 0)
      .map(p => (p._1._1 - p._1._2, p._2))
      .collect()

  lazy val depthRatioLogs =
    l
      .filter(p => p._1._1 != 0 && p._1._2 != 0)
      .map(p => (math.log(p._1._1 * 1.0 / p._1._2)/math.log(2), p._2))
      .reduceByKey(_ + _)

  lazy val roundedRatioLogs =
    depthRatioLogs
      .map(p => (math.round(p._1), p._2))
      .reduceByKey(_ + _)
      .collect()

  lazy val readsDot =
    (for {
      ((depth1, depth2), numLoci) <- l
    } yield {
      numLoci * depth1 * depth2
    }).reduce(_ + _)

  lazy val xy = readsDot
  lazy val xx =
    (for {
      ((depth1, _), numLoci) <- l
    } yield {
      numLoci * depth1 * depth1
    }).reduce(_ + _)

  lazy val yy =
    (for {
      ((_, depth2), numLoci) <- l
    } yield {
      numLoci * depth2 * depth2
    }).reduce(_ + _)

  lazy val regressionWeights =
    RW.newBuilder
      .setSlope((xy - n) * 1.0 / (xx - n))
      .setIntercept((xx - xy) * 1.0 / (xx - n))
      .build()

  lazy val regression2Weights =
    RW.newBuilder()
      .setSlope((xy - n) * 1.0 / (yy - n))
      .setIntercept((yy - xy) * 1.0 / (yy - n))
      .build()

  lazy val demingWeights = {
    val a: Double = xy - n
    val b: Double = xx - yy
    val c: Double = -a
    val m1 = (-b + math.sqrt(b*b - 4*a*c)) / (2*a)
    val b1 = 1 - m1
    val m2 = (-b - math.sqrt(b*b - 4*a*c)) / (2*a)
    val b2 = 1 - m2
    (
      RW.newBuilder().setSlope(m1).setIntercept(b1).build(),
      RW.newBuilder().setSlope(m2).setIntercept(b2).build()
    )
  }

  lazy val mutualInformation = {
    val bc1 = sc.broadcast(readDepthHist1.l.collectAsMap())
    val bc2 = sc.broadcast(readDepthHist2.l.collectAsMap())
    val n = totalLoci
    (for {
      ((depth1, depth2), numLoci) <- l
    } yield {
      val pxy = numLoci * 1.0 / n
      val px = bc1.value.getOrElse(depth1, {
        throw new Exception(s"Depth $depth1 not found in RDH1 map")
      }) * 1.0 / n
      val py = bc2.value.getOrElse(depth2, {
        throw new Exception(s"Depth $depth2 not found in RDH2 map")
      }) * 1.0 / n
      pxy * math.log(pxy / px / py) / math.log(2)
    }).reduce(_ + _)
  }

}

object PerContigJointHistogram {
  type PerContigJointHist = RDD[((Long, Long), Long)]

  def collect(l: PerContigJointHist): PCJH = {
    PerContigJointHistogram(l).h
  }

}
