package org.hammerlab.pageant.coverage.two_sample.with_intervals

import java.io.PrintWriter

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.one_sample.with_intervals.ReadSetStats
import org.hammerlab.pageant.coverage.two_sample
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth

/**
 * Statistics about one set of reads' coverage of a set of intervals.
 *
 * @param jh joint-histogram of read coverage vs. interval coverage (the latter being 1 or 0 everywhere).
 * @param pdf (([[Depth]], [[Depth]], [[Counts]]) tuples indicating on-target and off-target coverage at all observed depths.
 * @param cdf CDF of the PDF above; tuples represent numbers of on- and off-target loci with depth *at least* a given
 *            number.
 * @param sample1Stats summary depth/coverage stats about the first reads-set.
 * @param sample2Stats summary depth/coverage stats about the second reads-set.
 * @param totalOnLoci total number of on-target loci.
 * @param totalOffLoci total number of off-target loci observed to have non-zero coverage.
 */
case class Result(jh: JointHistogram,
                  pdf: PDF,
                  cdf: CDF,
                  sample1Stats: ReadSetStats,
                  sample2Stats: ReadSetStats,
                  totalOnLoci: NumLoci,
                  totalOffLoci: NumLoci)
  extends two_sample.Result[Counts, CSVRow] {

  @transient lazy val ReadSetStats(maxDepth1, totalBases1, onBases1) = sample1Stats
  @transient lazy val ReadSetStats(maxDepth2, totalBases2, onBases2) = sample2Stats

  override def toCSVRow(d2c: ((Depth, Depth), Counts)): CSVRow =
    CSVRow(
      d2c,
      totalBases1,
      totalBases2,
      totalOnLoci,
      totalOffLoci
    )

  override def writeMisc(pw: PrintWriter): Unit = {
    pw.println(s"Max depths: $maxDepth1,$maxDepth2")
    pw.println(s"Total mapped bases: $totalBases1,$totalBases2")
    pw.println(s"Total on-target bases: $onBases1,$onBases2")
    pw.println(s"Total on-target loci: $totalOnLoci")
  }
}
