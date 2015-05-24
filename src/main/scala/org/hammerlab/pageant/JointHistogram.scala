package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.avro.JointHistogramEntry

case class JointHistogram(lociPerReadDepthPair: RDD[((Long,Long), Long)]) {

  @transient
  val sc = lociPerReadDepthPair.context

  lociPerReadDepthPair.setName("JointHist").persist()

  lazy val l = lociPerReadDepthPair

  def getReadDepthHistAndTotal(indexFn: ((Long, Long)) => Long): RDD[(Long, Long)] = {
    val readDepthHist =
      lociPerReadDepthPair
        .filter(p => indexFn(p._1) != 0)
        .map(p => (indexFn(p._1), p._2))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .persist()

    val withZeros = readDepthHist ++ sc.parallelize(List((0L, totalLoci - readDepthHist.map(_._2).reduce(_ + _))))

    val curTotalLoci = withZeros.map(_._2).reduce(_ + _)
    assert(curTotalLoci == totalLoci, s"ReadDepthHist size didn't match total: $curTotalLoci vs. $totalLoci")

    withZeros
  }

  lazy val totalLoci = lociPerReadDepthPair.map(_._2).reduce(_ + _)

  lazy val readDepthHist1 = getReadDepthHistAndTotal(_._1).setName("hist1")
  lazy val readDepthHist2 = getReadDepthHistAndTotal(_._2).setName("hist2")
  lazy val r1 = readDepthHist1
  lazy val r2 = readDepthHist2

  def getEntropy(rdh: RDD[(Long, Long)]): Double = {
    (for {
      (depth,numLoci) <- r1
      p = numLoci * 1.0 / n
    } yield {
      -p * math.log(p) / math.log(2)
    }).reduce(_ + _)
  }

  lazy val entropy1 = getEntropy(r1)
  lazy val entropy2 = getEntropy(r2)

  lazy val e1 = entropy1
  lazy val e2 = entropy2

  lazy val lociCountsByMaxDepth: RDD[((Long,Long), Long)] =
    lociPerReadDepthPair.sortBy(p => (math.max(p._1._1, p._1._2), p._1._1, p._1._2))

  lazy val diffs = lociPerReadDepthPair.map {
    case ((depth1, depth2), numLoci) => (depth1 - depth2, numLoci)
  }.reduceByKey(_ + _).sortBy(_._2, ascending = false).setName("diffs").persist()

  lazy val absDiffs = lociPerReadDepthPair.map {
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

  lazy val lociWithAZeroDepth = lociPerReadDepthPair.filter(p => p._1._1 == 0 || p._1._2 == 0).map(p => (p._1._1 - p._1._2, p._2)).collect()

  lazy val depthRatioLogs = lociPerReadDepthPair.filter(p => p._1._1 != 0 && p._1._2 != 0).map(p => (math.log(p._1._1 * 1.0 / p._1._2)/math.log(2), p._2)).reduceByKey(_ + _)

  lazy val roundedRatioLogs = depthRatioLogs.map(p => (math.round(p._1), p._2)).reduceByKey(_ + _).collect

  def write(filename: String): Unit = {
    val entries =
      for {
        ((depth1, depth2), numLoci) <- lociPerReadDepthPair
      } yield {
        JointHistogramEntry.newBuilder().setDepth1(depth1).setDepth2(depth2).setNumLoci(numLoci).build()
      }

    entries.adamParquetSave(filename)
  }

  lazy val readsDot =
    (for {
      ((depth1, depth2), numLoci) <- lociPerReadDepthPair
    } yield {
        numLoci * depth1 * depth2
      }).reduce(_ + _)

  lazy val xy = readsDot
  val n = totalLoci
  lazy val xx =
    (for {
      ((depth1, _), numLoci) <- lociPerReadDepthPair
    } yield {
        numLoci * depth1 * depth1
      }).reduce(_ + _)

  lazy val yy =
    (for {
      ((_, depth2), numLoci) <- lociPerReadDepthPair
    } yield {
        numLoci * depth2 * depth2
      }).reduce(_ + _)

  lazy val regressionWeights = {
    (
      (xy - n) * 1.0 / (xx - n),
      (xx - xy) * 1.0 / (xx - n)
    )
  }

  lazy val regression2Weights = {
    (
      (xy - n) * 1.0 / (yy - n),
      (yy - xy) * 1.0 / (yy - n)
    )
  }

  lazy val demingWeights = {
    val a: Double = xy - n
    val b: Double = xx - yy
    val c: Double = -a
    val m1 = (-b + math.sqrt(b*b - 4*a*c)) / (2*a)
    val b1 = 1 - m1
    val m2 = (-b - math.sqrt(b*b - 4*a*c)) / (2*a)
    val b2 = 1 - m2
    ((m1,b1), (m2,b2))
  }

  lazy val mutualInformation = {
    val bc1 = sc.broadcast(readDepthHist1.collectAsMap())
    val bc2 = sc.broadcast(readDepthHist2.collectAsMap())
    val n = totalLoci
    (for {
      ((depth1, depth2), numLoci) <- lociPerReadDepthPair
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

object JointHistogram {

  def apply(sc: SparkContext, file1: String, file2: String): JointHistogram = {
    val projectionOpt =
      Some(
        Projection(
          AlignmentRecordField.readMapped,
          AlignmentRecordField.sequence,
          AlignmentRecordField.contig,
          AlignmentRecordField.start
        )
      )
    val reads = sc.loadAlignments(file1, None, projectionOpt).setName("reads1")
    val reads2 = sc.loadAlignments(file2, None, projectionOpt).setName("reads2")
    JointHistogram(sc, reads, reads2)
  }

  def apply(sc: SparkContext, reads: RDD[AlignmentRecord], reads2: RDD[AlignmentRecord]): JointHistogram = {
    lazy val numReads = reads.count()
    lazy val numReads2 = reads2.count()

    lazy val readDepthPerLocus: RDD[((String, Long), Long)] = ReadDepthHistogram.getReadDepthPerLocus(reads)
    lazy val readDepthPerLocus2: RDD[((String, Long), Long)] = ReadDepthHistogram.getReadDepthPerLocus(reads2)

    lazy val original1Loci = readDepthPerLocus.count()
    lazy val original2Loci = readDepthPerLocus2.count()

    lazy val joinedReadDepthPerLocus: RDD[((String, Long), (Long, Long))] =
      readDepthPerLocus.fullOuterJoin(readDepthPerLocus2).map {
        case (locus, (count1Opt, count2Opt)) => (locus, (count1Opt.getOrElse(0L), count2Opt.getOrElse(0L)))
      }

    lazy val lociPerReadDepthPair: RDD[((Long,Long), Long)] =
      joinedReadDepthPerLocus.map({
        case (locus, counts) => (counts, 1L)
      }).reduceByKey(_ + _).sortBy(_._1).setName("joined hist").persist()

    JointHistogram(lociPerReadDepthPair)
  }

  def load(sc: SparkContext, fn: String): JointHistogram = {
    val rdd: RDD[JointHistogramEntry] = sc.adamLoad(fn)
    lazy val lociPerReadDepthPair: RDD[((Long,Long), Long)] =
      rdd.map(e => ((e.getDepth1, e.getDepth2), e.getNumLoci))

    JointHistogram(lociPerReadDepthPair)
  }
}

