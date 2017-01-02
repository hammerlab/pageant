package org.hammerlab.pageant.coverage.one_sample.with_intervals

import java.io.PrintWriter

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.one_sample
import org.hammerlab.pageant.coverage.one_sample.with_intervals.ResultBuilder.DC
import org.hammerlab.pageant.coverage.one_sample.{ CDF, Count, PDF }
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth

/**
 * Statistics about one set of reads' coverage of a set of intervals.
 *
 * @param jh joint-histogram of read coverage vs. interval coverage (the latter being 1 or 0 everywhere).
 * @param pdf ([[Depth]], [[Count]]) tuples indicating on-target and off-target coverage at all observed depths.
 * @param cdf CDF of the PDF above; tuples represent numbers of on- and off-target loci with depth *at least* a given
 *            number.
 * @param readsStats summary depth/coverage stats about the reads-set.
 * @param filteredCDF summary CDF, filtered to a few logarithmically-spaced round-numbers.
 * @param totalOnLoci total number of on-target loci.
 * @param totalOffLoci total number of off-target loci observed to have non-zero coverage.
 */
case class Result(jh: JointHistogram,
                  pdf: PDF[Counts],
                  cdf: CDF[Counts],
                  readsStats: ReadSetStats,
                  filteredCDF: Array[DC],
                  totalOnLoci: NumLoci,
                  totalOffLoci: NumLoci)
  extends one_sample.Result[Counts, CSVRow] {

  @transient lazy val ReadSetStats(maxDepth, totalBases, onBases) = readsStats

  override def toCSVRow(depthCounts: DC): CSVRow =
    CSVRow(depthCounts, totalBases, totalOnLoci, totalOffLoci)

  override def writeMisc(pw: PrintWriter): Unit = {
    pw.println(s"Max depth: $maxDepth")
    pw.println(s"Total mapped bases: $totalBases")
    pw.println(s"Total on-target bases: $onBases")
    pw.println(s"Total on-target loci: $totalOnLoci")
  }
}


