package org.hammerlab.pageant.coverage.one_sample

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.coverage
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import spire.algebra.Monoid
import org.hammerlab.magic.rdd.scan.ScanRightByKeyRDD._

import scala.reflect.ClassTag

abstract class PDF[C: Monoid: ClassTag] extends coverage.PDF[C] {
  def rdd: RDD[(Depth, C)]
  val m = implicitly[Monoid[C]]
  override def cdf: CDF[C] = new CDF(rdd.scanRightByKey(m.id)(m.op))
}

object PDF {
  implicit def unwrapPDF[C: Monoid](pdf: PDF[C]): RDD[(Depth, C)] = pdf.rdd
}

class CDF[C: Monoid](val rdd: RDD[(Depth, C)]) extends coverage.CDF[C]

object CDF {
  implicit def unwrapCDF[C: Monoid](cdf: CDF[C]): RDD[(Depth, C)] = cdf.rdd
}
