package org.hammerlab.pageant.coverage.one_sample.with_intervals

import org.hammerlab.pageant.coverage.one_sample.{ CDF, PDF }
import org.hammerlab.pageant.coverage.one_sample
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram._

object ResultBuilder extends one_sample.ResultBuilder[Key, Counts, Result] {

  type DC = (Depth, Counts)

  override def make(jh: JointHistogram,
                    pdf: PDF[Counts],
                    cdf: CDF[Counts],
                    filteredCDF: Array[(Depth, Counts)],
                    maxDepth: Depth,
                    firstCounts: Counts): Result = {

    val totalBases = firstCounts.all.bp
    val onBases = firstCounts.on.bp

    val (totalOnLoci, totalOffLoci) = jh.coveredLociCounts(idx = 1)

    Result(
      jh,
      pdf.rdd.sortByKey(),
      cdf.rdd.sortByKey(),
      ReadSetStats(maxDepth, totalBases, onBases),
      filteredCDF,
      totalOnLoci,
      totalOffLoci
    )
  }
}
