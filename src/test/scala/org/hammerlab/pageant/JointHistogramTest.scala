package org.hammerlab.pageant

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{Contig, Feature, AlignmentRecord}
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

    val j = JointHistogram.fromAlignments(reads1, reads2, Some((features, 3000000L)))
    j.totalLoci should be(Map(Some(false) -> 110L, Some(true) -> 6L, None -> 116L))

    j.readsDot should be(
      Map(
        (Some(true), Some("chr2")) -> 0.0,
        (Some(false), Some("chr2")) -> 0.0,
        (None, Some("chr2")) -> 0.0,
        (Some(true), None) -> 0.0,
        (Some(false), None) -> 0.0,
        (None, None) -> 0.0
      )
    )

    j.perContigTotals should be(
      Map(
        (None, Some("chr2")) -> 116,
        (Some(true), Some("chr2")) -> 6,
        (Some(false), Some("chr2")) -> 110
      )
    )

    j.sample1Bases should be(
      Map(
        (None, Some("chr2")) -> 13,
        (Some(true), Some("chr2")) -> 3,
        (Some(false), Some("chr2")) -> 10,
        (None, None) -> 13,
        (Some(true), None) -> 3,
        (Some(false), None) -> 10
      )
    )

    j.sample2Bases should be(
      Map(
        (None, Some("chr2")) -> 103,
        (Some(true), Some("chr2")) -> 3,
        (Some(false), Some("chr2")) -> 100,
        (None, None) -> 103,
        (Some(true), None) -> 3,
        (Some(false), None) -> 100
      )
    )

    j.stats should be(
      Map(
        (Some(true), Some("chr2")) -> ((3.0,3.0,0.0),(3.0,3.0)),
        (Some(false), Some("chr2")) -> ((10.0,100.0,0.0),(10.0,100.0)),
        (None, Some("chr2")) -> ((13.0,103.0,0.0),(13.0,103.0)),

        (Some(true), None) -> ((3.0,3.0,0.0),(3.0,3.0)),
        (Some(false), None) -> ((10.0,100.0,0.0),(10.0,100.0)),
        (None, None) -> ((13.0,103.0,0.0),(13.0,103.0))
      )
    )

    j.weights should be(
      Map(
        (Some(true), Some("chr2")) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0)),
        (Some(false), Some("chr2")) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0)),
        (None, Some("chr2")) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0)),

        (Some(true), None) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0)),
        (Some(false), None) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0)),
        (None, None) -> (RegressionWeights(-1.0, 1.0, 0.0, 1.0), RegressionWeights(-1.0, 1.0, 0.0, 1.0))
      )
    )

    j.cov should be(
      Map(
        (Some(true), Some("chr2")) -> (0.3, 0.3, -0.3),
        (Some(false), Some("chr2")) -> (0.08340283569641369, 0.0834028356964137, -0.08340283569641369),
        (None, Some("chr2")) -> (0.10037481259370314, 0.10037481259370311, -0.10037481259370314),

        (Some(true), None) -> (0.3, 0.3, -0.3),
        (Some(false), None) -> (0.08340283569641369, 0.0834028356964137, -0.08340283569641369),
        (None, None) -> (0.10037481259370314, 0.10037481259370311, -0.10037481259370314)
      )
    )

    j.eigens((Some(true), Some("chr2"))) should be(List((0.6, (0.7071067811865475, -0.7071067811865475)), (0.0, (-0.7071067811865475, -0.7071067811865475))))
    j.eigens((Some(false), Some("chr2"))) should be(List((0.16680567139282737, (0.7071067811865475, -0.7071067811865476)), (0.0, (-0.7071067811865476, -0.7071067811865475))))
    j.eigens((None, Some("chr2"))) should be(List((0.20074962518740624, (0.7071067811865475, -0.7071067811865475)), (-1.3877787807814457E-17, (-0.7071067811865475, -0.7071067811865476))))

    j.eigens((Some(true), None)) should be(List((0.6, (0.7071067811865475, -0.7071067811865475)), (0.0, (-0.7071067811865475, -0.7071067811865475))))
    j.eigens((Some(false), None)) should be(List((0.16680567139282737, (0.7071067811865475, -0.7071067811865476)), (0.0, (-0.7071067811865476, -0.7071067811865475))))
    j.eigens((None, None)) should be(List((0.20074962518740624, (0.7071067811865475, -0.7071067811865475)), (-1.3877787807814457E-17, (-0.7071067811865475, -0.7071067811865476))))

    j.pcsc should be(
      Map(
        (Some(true), Some("chr2")) -> List(PrincipalComponent(0.0, (-0.7071067811865475, -0.7071067811865475), 0.0)),
        (Some(false), Some("chr2")) -> List(PrincipalComponent(0.0, (-0.7071067811865476, -0.7071067811865475), 0.0)),
        (None, Some("chr2")) -> List(PrincipalComponent(-1.3877787807814457E-17, (-0.7071067811865475, -0.7071067811865476), -6.912983172376583E-17)),

        (Some(true), None) -> List(PrincipalComponent(0.0, (-0.7071067811865475, -0.7071067811865475), 0.0)),
        (Some(false), None) -> List(PrincipalComponent(0.0, (-0.7071067811865476, -0.7071067811865475), 0.0)),
        (None, None) -> List(PrincipalComponent(-1.3877787807814457E-17, (-0.7071067811865475, -0.7071067811865476), -6.912983172376583E-17))      )
    )

    j.mutualInformation should be(
      Map(
        (Some(true), Some("chr2")) -> 0.9999999999999996,
        (Some(false), Some("chr2")) -> 0.43949698692151445,
        (None, Some("chr2")) -> 0.5061252137852826,

        (Some(true), None) -> 0.9999999999999996,
        (Some(false), None) -> 0.43949698692151445,
        (None, None) -> 0.5061252137852826
      )
    )

  }

}
