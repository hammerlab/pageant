
package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.PerContigJointHistogram.PerContigJointHist
import org.hammerlab.pageant.avro.{
  PerContigJointHistogram => PCJH,
  JointHistogramEntry,
  PrincipalComponent,
  LinearRegressionWeights => LRW
}

case class PerContigJointHistogram(l: PerContigJointHist) {

  @transient val sc = l.context

  lazy val h = {
    PCJH.newBuilder()
      .setEntries(collectEntries)
      .setTotalLoci(n)
      .setHist1(readDepthHist1.h)
      .setHist2(readDepthHist2.h)
      .setXWeights(regressionWeights)
      .setYWeights(regression2Weights)
      .setPcs(pcs)
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

  lazy val readsDot: Double =
    (for {
      ((depth1, depth2), numLoci) <- l
    } yield {
      numLoci * depth1 * depth2
    }).reduce(_ + _)

  lazy val xy = readsDot

  def addTuples(a: (Long, Long), b: (Long, Long)) = (a._1+b._1, a._2+b._2)

  lazy val (sx: Double, xx: Double) = {
    val (sx, xx) =
      (for {
        ((depth1, _), numLoci) <- l
      } yield {
        val xi = numLoci * depth1
        (xi, xi * depth1)
      }).reduce(addTuples)

    (sx.toDouble, xx.toDouble)
  }

  lazy val (sy: Double, yy: Double) = {
    val (sy, yy) =
      (for {
        ((_, depth2), numLoci) <- l
      } yield {
        val yi = numLoci * depth2
        (yi, yi * depth2)
      }).reduce(addTuples)

    (sy.toDouble, yy.toDouble)
  }

  lazy val vx = (xx - sx*sx*1.0/n) / (n-1)
  lazy val vy = (yy - sy*sy*1.0/n) / (n-1)
  lazy val vxy = (xy - sx*sy*1.0/n) / (n-1)

  lazy val ((e1,e2), (v1,v2)) = {
    val T = (vx + vy) / 2
    val D = vx*vy - vxy*vxy

    val e1 = T + math.sqrt(T*T - D)
    val e2 = T - math.sqrt(T*T - D)

    val d1 = math.sqrt((e1-vy)*(e1-vy) + vxy*vxy)
    val v1 = ((e1 - vy) / d1, vxy / d1)

    val d2 = math.sqrt((e2-vy)*(e2-vy) + vxy*vxy)
    val v2 = ((e2 - vy) / d2, vxy / d2)

    ((e1,e2), (v1,v2))
  }

  lazy val sum_es = e1 + e2

  lazy val pcs = for {
    (e,v) <- List((e1,v1), (e2,v2))
  } yield {
    PrincipalComponent.newBuilder()
      .setValue(e)
      .setVector(List(v._1, v._2).map(double2Double))
      .setVarianceFraction(e / sum_es)
      .build()
  }

  def mse(xx: Double, yy: Double, m: Double, b: Double): Double = {
    yy + m*m*xx + b*b*n - 2*m*xy - 2*b*sy + 2*b*m*sx
  }

  def weights(xx: Double, yy: Double): LRW = {
    val den = n*xx - sx*sx
    val m = (n*xy - sx*sy) * 1.0 / den
    val b = (sy*xx - sx*xy) * 1.0 / den
    val err = mse(xx, yy, m, b)
    val num = sx*sy - n*xy
    val rSquared = num * 1.0 / (sx*sx - n*xx) * num / (sy*sy - n*yy)
    LRW.newBuilder
      .setSlope(m)
      .setIntercept(b)
      .setMse(err)
      .setRSquared(rSquared)
      .build()
  }

  lazy val regressionWeights = weights(xx, yy)
  lazy val regression2Weights = weights(yy, xx)

  lazy val mutualInformation = {
    val bc1 = sc.broadcast(r1.lMap)
    val bc2 = sc.broadcast(r2.lMap)
    (for {
      ((depth1, depth2), numLoci) <- l
    } yield {

      val vx = bc1.value.getOrElse(
        depth1,
        throw new Exception(s"Depth $depth1 not found in RDH1 map")
      )

      val vy = bc2.value.getOrElse(
        depth2,
        throw new Exception(s"Depth $depth2 not found in RDH2 map")
      )

      numLoci * (math.log(numLoci) + math.log(n) - math.log(vx) - math.log(vy))
    }).reduce(_ + _) / math.log(2) / n
  }

}

object PerContigJointHistogram {
  type PerContigJointHist = RDD[((Long, Long), Long)]

  def collect(l: PerContigJointHist): PCJH = {
    PerContigJointHistogram(l).h
  }

}
