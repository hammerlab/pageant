package org.hammerlab.pageant

import org.hammerlab.pageant.JointHistogram.JointHist

import scala.collection.JavaConversions._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.avro.JointHistogramEntry

case class PerContigJointHistogram(l: JointHist) {
  @transient
  val sc = l.context

  def getReadDepthHistAndTotal(indexFn: ((Long, Long)) => Long): RDD[(Long, Long)] = {
    l
    .map(p => (indexFn(p._1), p._2))
    .reduceByKey(_+_)
    .sortBy(_._2, ascending = false)
    .persist()
  }

  lazy val totalLoci = l.map(_._2).reduce(_+_)
  lazy val n = totalLoci

  lazy val readDepthHist1 = getReadDepthHistAndTotal(_._1).setName("hist1")
  lazy val readDepthHist2 = getReadDepthHistAndTotal(_._2).setName("hist2")
  lazy val r1 = readDepthHist1
  lazy val r2 = readDepthHist2

  def getEntropy(rdh: RDD[(Long, Long)]): Double = {
    (for {
      (depth, numLoci) <- r1
      p = numLoci * 1.0 / n
    } yield {
        -p * math.log(p) / math.log(2)
      }).reduce(_ + _)
  }

  lazy val entropy1 = getEntropy(r1)
  lazy val entropy2 = getEntropy(r2)

  lazy val e1 = entropy1
  lazy val e2 = entropy2

  lazy val lociCountsByMaxDepth: JointHist =
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

  lazy val lociWithAZeroDepth = l.filter(p => p._1._1 == 0 || p._1._2 == 0).map(p => (p._1._1 - p._1._2, p._2)).collect()

  lazy val depthRatioLogs = l.filter(p => p._1._1 != 0 && p._1._2 != 0).map(p => (math.log(p._1._1 * 1.0 / p._1._2)/math.log(2), p._2)).reduceByKey(_ + _)

  lazy val roundedRatioLogs = depthRatioLogs.map(p => (math.round(p._1), p._2)).reduceByKey(_ + _).collect

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

case class JointHistogram(total: PerContigJointHistogram,
                          perContigs: Map[String, PerContigJointHistogram]) {
  @transient
  val sc = total.l.context

  def write(filename: String): Unit = {
    val entries =
      for {
        ((depth1, depth2), (numLoci, perContig)) <- JointHistogram.joinContigs(total.l, perContigs)
      } yield {
        JointHistogramEntry
        .newBuilder()
          .setDepth1(depth1)
          .setDepth2(depth2)
          .setNumLoci(numLoci)
          .setLociPerContig(JointHistogram.s2j(perContig))
          .build()
      }

    entries.adamParquetSave(filename)
  }

}

object JointHistogram {

  type JointHistWithContigs = RDD[((Long,Long), (Long, Map[String, Long]))]
  type JointHist = RDD[((Long, Long), Long)]

  def splitContigs(l: JointHistWithContigs): (JointHist, Map[String, PerContigJointHistogram]) = {
    val total: JointHist = l.map(p => (p._1, p._2._1))

    val keys = l.flatMap(_._2._2.keys).distinct().collect()

    val perContig: Map[String, PerContigJointHistogram] =
      (for {
        key <- keys
      } yield {
          key -> PerContigJointHistogram(
            for {
              ((d1,d2), (_,m)) <- l
              nl <- m.get(key)
            } yield {
              ((d1, d2), nl)
            }
          )
        }).toMap

    (total, perContig)
  }

  def joinContigs(l: JointHist, perContigs: Map[String, PerContigJointHistogram]): JointHistWithContigs = {
    val fromL =
      for {
        ((d1,d2), nl) <- l
      } yield {
        ((d1,d2), (nl, Map[String, Long]()))
      }

    val rdds =
      l.context.union(
        (for {
          (key, h) <- perContigs
        } yield {
          for {
            ((d1,d2), nl) <- h.l
          } yield {
            ((d1, d2), (0L, Map(key -> nl)))
          }
        }).toSeq :+ fromL
      )

    rdds.reduceByKey(mergeCounts)
  }

  def apply(l: JointHist, perContig: Map[String, PerContigJointHistogram]): JointHistogram =
    new JointHistogram(PerContigJointHistogram(l), perContig)

  def apply(l: JointHistWithContigs): JointHistogram = {
    val (total, perContig) = splitContigs(l)
    JointHistogram(total, perContig)
  }

  def j2s(m: java.util.Map[String, java.lang.Long]): Map[String, Long] = mapAsScalaMap(m).toMap.mapValues(Long2long)
  def s2j(m: Map[String, Long]): java.util.Map[String, java.lang.Long] = mapToJavaMap(m.mapValues(long2Long))

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


  def getReadDepthPerLocus(reads: RDD[AlignmentRecord]): RDD[((String, Long), Long)] = {
    (for {
      read <- reads if read.getReadMapped
      offset <- (0 until read.getSequence.size)  // TODO(ryan): handle indels correctly
      contig <- Option(read.getContig)
      name <- Option(contig.getContigName)
      start <- Option(read.getStart)
      pos = start + offset
    } yield {
      ((name, pos), 1L)
    }).reduceByKey(_ + _)
  }

  def mergeCounts(a: (Long, Map[String, Long]),
                  b: (Long, Map[String, Long])): (Long, Map[String, Long]) =
    (a._1 + b._1, mergeMaps(a._2, b._2))

  def mergeMaps(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] = {
    (a.keySet ++ b.keySet).map(i => (i,a.getOrElse(i, 0L) + b.getOrElse(i, 0L))).toMap
  }

  def apply(sc: SparkContext, reads: RDD[AlignmentRecord], reads2: RDD[AlignmentRecord]): JointHistogram = {

    lazy val readDepthPerLocus: RDD[((String, Long), Long)] = getReadDepthPerLocus(reads)
    lazy val readDepthPerLocus2: RDD[((String, Long), Long)] = getReadDepthPerLocus(reads2)

    lazy val joinedReadDepthPerLocus: RDD[((String, Long), (Long, Long))] =
      readDepthPerLocus.fullOuterJoin(readDepthPerLocus2).map {
        case (locus, (count1Opt, count2Opt)) => (locus, (count1Opt.getOrElse(0L), count2Opt.getOrElse(0L)))
      }

    lazy val l: JointHistWithContigs =
      joinedReadDepthPerLocus.map({
        case ((contig, locus), count) => (count, (1L, Map(contig -> 1L)))
      }).reduceByKey(mergeCounts)
        .sortBy(_._1)
        .setName("joined hist")
        .persist()

    JointHistogram(l)
  }

  def load(sc: SparkContext, fn: String): JointHistogram = {
    val rdd: RDD[JointHistogramEntry] = sc.adamLoad(fn)
    lazy val l: JointHistWithContigs =
      rdd.map(e => {
        val d1: Long = e.getDepth1
        val d2: Long = e.getDepth2
        val nl: Long = e.getNumLoci
        val m: Map[String, Long] = mapAsScalaMap(e.getLociPerContig).toMap.mapValues(Long2long)
        ((d1, d2), (nl, m))
      })

    JointHistogram(l)
  }
}

