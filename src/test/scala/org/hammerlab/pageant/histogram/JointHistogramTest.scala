package org.hammerlab.pageant.histogram

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{AlignmentRecord, Contig, Feature}
import org.scalatest.Matchers

class JointHistogramTest extends ADAMFunSuite with Matchers {

  val chr2 = Contig.newBuilder().setContigName("chr2").setContigLength(10000000L).build()
  val chr11 = Contig.newBuilder().setContigName("chr11").setContigLength(2000000L).build()

  def read(start: Long, end: Long, cigar: Option[String] = None, contig: Contig = chr2): AlignmentRecord = {
    AlignmentRecord.newBuilder()
      .setContig(contig)
      .setReadMapped(true)
      .setStart(start)
      .setEnd(end)
      .setCigar(cigar.getOrElse("%dM".format(end - start)))
      .build()
  }

  def feature(start: Long, end: Long, contig: Contig = chr2): Feature = {
    Feature.newBuilder().setContig(contig).setStart(start).setEnd(end).build()
  }

  sparkTest("simple") {

    val f = feature(100, 110)

    val read1 = read(90, 103)
    val read2 = read(107, 210)

    val reads1 = sc.parallelize(List(read1))
    val reads2 = sc.parallelize(List(read2))
    val features = sc.parallelize(List(f))

    val j = JointHistogram.fromReadsAndFeatures(List(reads1, reads2), List(features))

    j.jh.collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(Some(1), Some(0), Some(1))) -> 3,
        (Some("chr2"), List(Some(1), Some(0), Some(0))) -> 10,
        (Some("chr2"), List(Some(0), Some(1), Some(0))) -> 100,
        (Some("chr2"), List(Some(0), Some(1), Some(1))) -> 3,
        (Some("chr2"), List(Some(0), Some(0), Some(1))) -> 4
      )
    )

    j.totalLoci should be(
      Map(
        Some("chr2") -> 120L,
        None -> 120L
      )
    )

    j.sums.get(0, 1).collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(None, None, Some(1))) -> 3.0,
        (Some("chr2"), List(None, None, Some(0))) -> 10.0
      )
    )

    j.sqsums.get(0, 1).collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(None, None, Some(1))) -> 3.0,
        (Some("chr2"), List(None, None, Some(0))) -> 10.0
      )
    )

    j.dots.get(0, 1).collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(None, None, Some(1))) -> 0.0,
        (Some("chr2"), List(None, None, Some(0))) -> 0.0
      )
    )

    j.ns.get(0, 1).collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(None, None, Some(1))) -> 10.0,
        (Some("chr2"), List(None, None, Some(0))) -> 110.0
      )
    )

    j.weights(0, 1).collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(None, None, Some(1))) -> (RegressionWeights(-0.42857142857142855, 0.42857142857142855, 1.7142857142857144, 0.18367346938775508), RegressionWeights(-0.42857142857142855, 0.42857142857142855, 1.7142857142857144, 0.18367346938775508)),
        (Some("chr2"), List(None, None, Some(0))) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0))
      )
    )

    j.eigens(0, 1).collectAsMap().toMap should be(
      Map(
        (Some("chr2"), List(None, None, Some(1))) -> (
          Eigen(0.33333333333333337, (0.7071067811865477, -0.7071067811865475), 0.7142857142857143),
          Eigen(0.13333333333333333, (-0.7071067811865475, -0.7071067811865475), 0.2857142857142857)
        ),
        (Some("chr2"), List(None, None, Some(0))) -> (
          Eigen(0.16680567139282737, (0.7071067811865475, -0.7071067811865476), 1.0),
          Eigen(0.0, (-0.7071067811865476, -0.7071067811865475), 0.0)
        )
      )
    )
  }

  def makeRead(start: Long, sequence: String, cigar: String): AlignmentRecord = {
    AlignmentRecord.newBuilder()
      .setContig(Contig.newBuilder().setContigName("chr1").build())
      .setSequence(sequence)
      .setStart(start)
      .setCigar(cigar)
      .setReadMapped(true)
      .build()
  }

  sparkTest("skipInsertionCoverage") {

    /*

       idx:  123456789012
        r1:  ACGTAACCGGTT
        r2:      AA----TT
        r3:   CGTAACCGG
                 ^
                AAA
     depth:  122233333322
     */

    val reads = sc.parallelize(
      List(
        makeRead(1, "ACGTAACCGGTT", "12M"),
        makeRead(5, "AATT", "2M4D2M"),
        makeRead(2, "CGTAAAAACCGG", "3M3I6M")
      )
    )

    val l: Array[(Long, Int)] = JointHistogram.readsToDepthMap(reads).collect().map(p => (p._1._2, p._2)).sortBy(_._1)

    l should be(
      Array(
        (1,1),
        (2,2),
        (3,2),
        (4,2),
        (5,3),
        (6,3),
        (7,3),
        (8,3),
        (9,3),
        (10,3),
        (11,2),
        (12,2)
      )
    )
  }

}
