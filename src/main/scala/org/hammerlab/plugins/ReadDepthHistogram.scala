
package org.hammerlab.guacamole.commands

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._

object ReadDepthHistogram {
  def run(sc: SparkContext, file1: String, file2: Option[String] = None): Unit = {
    val reads = sc.loadAlignments(file1)

    val readDepthPerLocus: RDD[((String, Long), Long)] =
      reads.flatMap(read => {
        (0 until read.getSequence.size).map(offset =>
          ((read.getContig.getContigName, read.getStart + offset), 1L)
        )
      }).reduceByKey(_ + _)

    val lociPerReadDepth: List[(Long, Long)] =
      readDepthPerLocus.map({
        case (locus, count) => (count, 1L)
      }).reduceByKeyLocally(_ + _).toList.sortBy(_._1)

    println("Loci per read depth:\n\n%s".format(
      lociPerReadDepth.map({
        case (depth, numLoci) => "%8d: %d".format(depth, numLoci)
      }).mkString("\t", "\n\t", "")
    ))

  }
}
