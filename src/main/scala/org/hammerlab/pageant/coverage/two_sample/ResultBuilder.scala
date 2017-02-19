package org.hammerlab.pageant.coverage.two_sample

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.magic.rdd.grid.PartialSumGridRDD
import org.hammerlab.math.Steps.roundNumbers
import org.hammerlab.pageant.coverage.IsKey
import org.hammerlab.pageant.coverage.two_sample.with_intervals.Counts
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import spire.algebra.Monoid

import scala.reflect.ClassTag

abstract class ResultBuilder[K <: Key[C] : ClassTag : IsKey, Result, C : ClassTag : Monoid] {

  def make(jh: JointHistogram,
           pdf: PDF[C],
           cdf: CDF[C],
           firstCounts: C,
           totalReferenceLoci: NumLoci): Result

  def make(jh: JointHistogram, totalReferenceLoci: NumLoci): Result = {
    val j = jh.jh
    val ik = implicitly[IsKey[K]]
    val keys = j.map(ik.make)

    val keyedCounts =
      for {
        key ← keys
      } yield
        key.depth → key.toCounts

    val (pdfRDD, cdfRDD, maxDepth1, maxDepth2) = PartialSumGridRDD(keyedCounts)

    val d1Steps = roundNumbers(maxDepth1)
    val d2Steps = roundNumbers(maxDepth2)

    val stepsBroadcast = jh.sc.broadcast((d1Steps, d2Steps))

    val cdf = CDF[C](cdfRDD.sortByKey(), stepsBroadcast)
    val pdf = PDF(pdfRDD.sortByKey(), stepsBroadcast, maxDepth1, maxDepth2)

    val firstElem = cdf.filtered.take(1)(0)
    if (firstElem._1 != (0, 0)) {
      throw new Exception(s"Bad first elem: $firstElem")
    }

    val firstCounts = firstElem._2

    make(
      jh,
      pdf,
      cdf,
      firstCounts,
      totalReferenceLoci
    )
  }
}

object ResultBuilder {

  type D2C = ((Depth, Depth), Counts)

}
