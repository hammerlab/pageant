
package org.hammerlab.pageant

import java.io.{File, FileWriter, BufferedWriter}

import org.apache.hadoop.fs.{Path, FileSystem}
import org.bdgenomics.adam.predicates.{AlignmentRecordConditions, ADAMPredicate}
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.{ADAMSaveArgs, ADAMContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.avro.{JointHistogram, JointHistogramEntry}
import parquet.hadoop.metadata.CompressionCodecName

import scala.collection.immutable.StringOps

case class Histogram(h: Seq[(Long,Long)]) {
  lazy val (cumulative, totalLoci): (List[(Long,Long)], Long) =
    h.foldLeft((List[(Long,Long)](), 0L))((soFar, pair) => {
      (soFar._1 :+ (pair._1, soFar._2 + pair._2), soFar._2 + pair._2)
    })

  case class HistogramRow(depth: Int, numLoci: Long, cumulativeLoci: Long) {
    val fraction: Double = numLoci * 1.0 / totalLoci
    val cumulativeFraction: Double = cumulativeLoci * 1.0 / totalLoci

    override def toString: String = {
      "%d:\t%d\t%.2f\t%d\t%.2f".format(depth, numLoci, fraction, cumulativeLoci, cumulativeFraction)
    }

    def toCsv: String = {
      List[Any](depth, numLoci, fraction, cumulativeLoci, cumulativeFraction).map(_.toString).mkString(",")
    }
  }

  val hist: Seq[HistogramRow] = h.zipWithIndex.map {
    case ((depth, numLoci), idx) => HistogramRow(depth.toInt, numLoci, cumulative(idx)._2)
  }

  def print(n: Int = 20): Unit = {
    println(hist.take(20).mkString("\t", "\n\t", ""))
  }

  def writeCsv(filename: String) = {
    val bw = new BufferedWriter(new FileWriter(new File(filename)))
    bw.write(hist.map(_.toCsv).mkString("\n"))
    bw.close
  }

  def writeHadoopCsv(filename: String, sc: SparkContext) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val os = fs.create(new Path(filename))
    os.write(new StringOps(hist.map(_.toCsv).mkString("\n")).getBytes)
    fs.close()
  }
}

object Histogram {
  def make(rdd: RDD[(Long, Long)]): Histogram = {
    new Histogram(rdd.collect.sortBy(_._1))
  }
}

object ReadDepthHistogram {

  case class Histogram2(reads: RDD[AlignmentRecord], reads2: RDD[AlignmentRecord]) {
    lazy val numReads = reads.count()
    lazy val numReads2 = reads2.count()

    lazy val readDepthPerLocus: RDD[((String, Long), Long)] = getReadDepthPerLocus(reads)
    lazy val readDepthPerLocus2: RDD[((String, Long), Long)] = getReadDepthPerLocus(reads2)

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

    lazy val lociCountsByMaxDepth: RDD[((Long,Long), Long)] =
      lociPerReadDepthPair.sortBy(p => (math.max(p._1._1, p._1._2), p._1._1, p._1._2))

    def getReadDepthHistAndTotal(indexFn: ((Long, Long)) => Long,
                                 originalLoci: Long,
                                 idx: Int): (RDD[(Long, Long)], Long) = {
      val readDepthHist =
        lociPerReadDepthPair
          .filter(p => indexFn(p._1) != 0)
          .map(p => (p._1._1, p._2))
          .reduceByKey(_ + _)
          .sortBy(_._2, ascending = false)
          .setName("hist1")
          .persist()

      val totalLoci = readDepthHist.map(_._2).reduce(_ + _)

      if (totalLoci != originalLoci) {
        println(
          s"WARNING: original number of loci from reads $idx doesn't match joined number: $originalLoci vs. $totalLoci"
        )
      }

      (readDepthHist, totalLoci)
    }

    lazy val (readDepthHist1, total1Loci) = getReadDepthHistAndTotal(_._1, original1Loci, 1)
    lazy val (readDepthHist2, total2Loci) = getReadDepthHistAndTotal(_._2, original2Loci, 2)

    lazy val totalLoci = lociPerReadDepthPair.map(_._2).reduce(_ + _)

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

    def write(sc: SparkContext, filename: String): Unit = {
      val entries =
        for {
          ((depth1, depth2), numLoci) <- lociPerReadDepthPair
        } yield {
          JointHistogramEntry.newBuilder().setDepth1(depth1).setDepth2(depth2).setNumLoci(numLoci).build()
        }

      entries.adamParquetSave(filename)
    }
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

  def run(sc: SparkContext, file1: String, file2Opt: Option[String] = None): Histogram = {
    val reads = sc.loadAlignments(file1, None, None)
    run(reads)
  }

  def run(reads: RDD[AlignmentRecord]): Histogram = {
    val lociPerReadDepth: List[(Long, Long)] =
      getReadDepthPerLocus(reads).map({
        case (locus, count) => (count, 1L)
      }).reduceByKeyLocally(_ + _).toList.sortBy(_._1)

    Histogram(lociPerReadDepth)
  }

  class MappedReadPredicate extends ADAMPredicate[AlignmentRecord] {
    override val recordCondition = AlignmentRecordConditions.isMapped
  }

  def run2(sc: SparkContext, file1: String, file2: String): Histogram2 = {
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
    Histogram2(reads, reads2)
  }

  def print(p: ((Long, Long), Long)): String = {
    "%d,%d:\t%d".format(p._1._1, p._1._2, p._2)
  }

}
