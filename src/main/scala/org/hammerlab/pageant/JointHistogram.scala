package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.InterleavedJointHistogram.InterleavedJointHist
import org.hammerlab.pageant.avro.{JointHistogram => JH}


case class JointHistogram(total: PerContigJointHistogram,
                          perContigs: Map[String, PerContigJointHistogram],
                          lOpt: Option[InterleavedJointHist] = None) {

  lazy val l = lOpt.getOrElse(InterleavedJointHistogram.joinContigs(total.l, perContigs))

  lazy val h = JH.newBuilder().setTotal(total.h).setContigHistograms(perContigs.mapValues(_.h)).build()

  def write(filename: String): Unit = {
    InterleavedJointHistogram.write(l, filename)
  }
}

object JointHistogram {

  def apply(l: InterleavedJointHist): JointHistogram = {
    val (total, perContig) = InterleavedJointHistogram.splitContigs(l)
    JointHistogram(total, perContig, Some(l))
  }

  def j2s(m: java.util.Map[String, java.lang.Long]): Map[String, Long] = m.toMap.mapValues(Long2long)
  def s2j(m: Map[String, Long]): java.util.Map[String, java.lang.Long] = mapToJavaMap(m.mapValues(long2Long))

  def fromAlignmentFiles(sc: SparkContext,
                         file1: String,
                         file2: String): JointHistogram = {
    JointHistogram(InterleavedJointHistogram.fromAlignmentFiles(sc, file1, file2))
  }

  def fromAlignments(sc: SparkContext, 
                     reads: RDD[AlignmentRecord], 
                     reads2: RDD[AlignmentRecord]): JointHistogram = {
    JointHistogram(InterleavedJointHistogram.fromAlignments(sc, reads, reads2))
  }

  def load(sc: SparkContext, fn: String): JointHistogram = {
    JointHistogram(InterleavedJointHistogram.load(sc, fn))
  }

  def loadFromAvro(sc: SparkContext, fn: String): RDD[JH] = {
    sc.adamLoad(fn)
  }

}

