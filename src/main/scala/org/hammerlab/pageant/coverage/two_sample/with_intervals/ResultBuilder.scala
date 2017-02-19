package org.hammerlab.pageant.coverage.two_sample.with_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.one_sample.with_intervals.ReadSetStats
import org.hammerlab.pageant.coverage.two_sample
import org.hammerlab.pageant.coverage.two_sample.Count
import org.hammerlab.pageant.histogram.JointHistogram

object ResultBuilder
  extends two_sample.ResultBuilder[Key, Result, Counts] {
  override def make(jh: JointHistogram,
                    pdf: PDF,
                    cdf: CDF,
                    firstCounts: Counts,
                    totalReferenceLoci: NumLoci): Result = {

    val Counts(Count(onBases1, onBases2, totalOnLoci), Count(_, _, totalOffLoci)) = firstCounts
    val Count(totalBases1, totalBases2, _) = firstCounts.all

    val stats1 = ReadSetStats(pdf.maxDepth1, totalBases1, onBases1)
    val stats2 = ReadSetStats(pdf.maxDepth2, totalBases2, onBases2)

    Result(
      jh,
      pdf,
      cdf,
      stats1,
      stats2,
      totalOnLoci,
      totalOffLoci
    )
  }
}
