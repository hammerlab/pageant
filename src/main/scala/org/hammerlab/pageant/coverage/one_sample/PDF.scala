package org.hammerlab.pageant.coverage.one_sample

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.coverage
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import spire.algebra.Monoid
import org.hammerlab.magic.rdd.scan.ScanRightByKeyRDD._

import scala.reflect.ClassTag

abstract class PDF[T: Monoid: ClassTag] extends coverage.PDF[T] {
  def rdd: RDD[(Depth, T)]
  val m = implicitly[Monoid[T]]
  override def cdf: CDF[T] = new CDF(rdd.scanRightByKey(m.id)(m.op))
}

object PDF {
  implicit def unwrapPDF[T: Monoid](pdf: PDF[T]): RDD[(Depth, T)] = pdf.rdd
}

class CDF[T: Monoid](val rdd: RDD[(Depth, T)]) extends coverage.CDF[T]

object CDF {
  implicit def unwrapCDF[T: Monoid](cdf: CDF[T]): RDD[(Depth, T)] = cdf.rdd
}
