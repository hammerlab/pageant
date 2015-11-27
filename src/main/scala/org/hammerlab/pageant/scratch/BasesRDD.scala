package org.hammerlab.pageant.scratch

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.Projection
import org.hammerlab.pageant.reads.Bases
import org.bdgenomics.adam.projections.AlignmentRecordField.sequence
import org.bdgenomics.adam.rdd.ADAMContext._

object BasesRDD {

  type BasesRDD = RDD[Bases]

  def loadAlignments(fn: String): BasesRDD = {
    val proj = Projection(sequence)
    c.loadAlignments(fn, projection = Some(proj)).flatMap(Bases.apply)
  }
}
