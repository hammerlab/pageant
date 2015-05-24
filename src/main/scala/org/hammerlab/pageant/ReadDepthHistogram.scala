
package org.hammerlab.pageant

import java.io.{File, FileWriter, BufferedWriter}

import org.apache.hadoop.fs.{Path, FileSystem}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro.AlignmentRecord

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

  def print(p: ((Long, Long), Long)): String = {
    "%d,%d:\t%d".format(p._1._1, p._1._2, p._2)
  }

}
