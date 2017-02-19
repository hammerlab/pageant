package org.hammerlab.pageant.coverage.two_sample.without_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.two_sample
import org.hammerlab.pageant.coverage.two_sample.Count
import org.hammerlab.pageant.histogram.JointHistogram

object ResultBuilder extends two_sample.ResultBuilder[Key, Result, Count] {
  override def make(jh: JointHistogram,
                    pdf: PDF,
                    cdf: CDF,
                    firstCounts: Count,
                    totalReferenceLoci: NumLoci): Result = {
    val Count(totalBases1, totalBases2, totalCoveredLoci) = firstCounts
    Result(
      jh,
      pdf,
      cdf,
      ReadSetStats(pdf.maxDepth1, totalBases1),
      ReadSetStats(pdf.maxDepth2, totalBases2),
      totalCoveredLoci,
      totalReferenceLoci
    )
  }
}
