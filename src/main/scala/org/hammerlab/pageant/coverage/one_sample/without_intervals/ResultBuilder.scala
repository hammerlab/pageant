package org.hammerlab.pageant.coverage.one_sample.without_intervals

import org.hammerlab.pageant.coverage.one_sample.{ CDF, Count, PDF }
import org.hammerlab.pageant.coverage.one_sample
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth

object ResultBuilder extends one_sample.ResultBuilder[Key, Count, Result] {
  type DC = (Depth, Count)

  override def make(jh: JointHistogram,
                    pdf: PDF[Count],
                    cdf: CDF[Count],
                    filteredCDF: Array[(Depth, Count)],
                    maxDepth: Depth,
                    firstCounts: Count): Result =
    Result(jh, pdf, cdf, maxDepth, firstCounts.bp, filteredCDF, firstCounts.n)
}
