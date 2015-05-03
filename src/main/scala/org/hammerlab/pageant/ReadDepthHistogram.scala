
package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro.AlignmentRecord

case class Histogram(h: List[(Long,Long)]) {
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
  }

  val hist: List[HistogramRow] = h.zipWithIndex.map {
    case ((depth, numLoci), idx) => HistogramRow(depth.toInt, numLoci, cumulative(idx)._2)
  }

  def print(n: Int = 20): Unit = {
    println(hist.take(20).mkString("\t", "\n\t", ""))
  }
}

object ReadDepthHistogram {

  case class Histogram2(h: RDD[((Long,Long),Long)])

  def getReadDepthPerLocus(reads: RDD[AlignmentRecord]): RDD[((String, Long), Long)] = {
    (for {
      read <- reads
      offset <- (0 until read.getSequence.size)
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

  def run2(sc: SparkContext, file1: String, file2: String): RDD[((Long,Long), Long)] = {
    val reads = sc.loadAlignments(file1, None, None)
    val reads2 = sc.loadAlignments(file2, None, None)
    run2(reads, reads2)
  }

  def print(p: ((Long, Long), Long)): String = {
    "%d,%d:\t%d".format(p._1._1, p._1._2, p._2)
  }

  def run2(reads: RDD[AlignmentRecord], reads2: RDD[AlignmentRecord]): RDD[((Long,Long), Long)] = {
    val readDepthPerLocus: RDD[((String, Long), Long)] = getReadDepthPerLocus(reads)
    val readDepthPerLocus2: RDD[((String, Long), Long)] = getReadDepthPerLocus(reads2)

    val joinedReadDepthPerLocus: RDD[((String, Long), (Long, Long))] =
      readDepthPerLocus.fullOuterJoin(readDepthPerLocus2).map {
        case (locus, (count1Opt, count2Opt)) => (locus, (count1Opt.getOrElse(0L), count2Opt.getOrElse(0L)))
      }

    val lociPerReadDepthPair: RDD[((Long,Long), Long)] =
      joinedReadDepthPerLocus.map({
        case (locus, counts) => (counts, 1L)
      }).reduceByKey(_ + _).sortBy(_._1)

    lociPerReadDepthPair
  }
}
