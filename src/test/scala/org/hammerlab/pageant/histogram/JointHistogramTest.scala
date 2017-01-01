package org.hammerlab.pageant.histogram

import htsjdk.samtools.TextCigarCodec
import org.bdgenomics.adam.models.{ SequenceDictionary, SequenceRecord }
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature }
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus, NumLoci }
import org.hammerlab.pageant.histogram.JointHistogram.{ Depths, JointHistKey, OCN, fromReadsAndFeatures }
import org.hammerlab.pageant.utils.PageantSuite

class JointHistogramTest
  extends PageantSuite
    with LocusUtil {

  import JointHistogram.jointHistKeyOrd

  val sd =
    SequenceDictionary(
      SequenceRecord("chr2", 10000000L),
      SequenceRecord("chr11", 2000000L)
    )

  def read(start: Locus, end: Locus, cigar: Option[String] = None, contigName: ContigName = "chr2"): AlignmentRecord =
    AlignmentRecord
      .newBuilder()
      .setContigName(contigName.name)
      .setReadMapped(true)
      .setStart(start.locus)
      .setEnd(end.locus)
      .setCigar(cigar.getOrElse("%sM".format(end - start)))
      .build()

  def feature(start: Locus, end: Locus, contigName: ContigName = "chr2"): Feature =
    Feature
      .newBuilder()
      .setContigName(contigName.name)
      .setStart(start.locus)
      .setEnd(end.locus)
      .build()

  implicit val convertContigName = convertOpt[String, ContigName] _
  implicit val convertSomeJHK = convertTuple2[Some[String], Depths, OCN, Depths] _
  implicit val convertSomeJHKDouble = convertTuple2[(Some[String], Depths), Double, JointHistKey, Double] _
  implicit val convSomeNumLociMap = convertMap[(Some[String], Depths), Int, JointHistKey, NumLoci] _

  implicit val convertJHK = convertTuple2[Option[String], Depths, OCN, Depths] _
  implicit val convNumLociMap = convertMap[JointHistKey, Int, JointHistKey, NumLoci] _
  implicit val convSumsMap = convertMap[(Option[String], Depths), Double, JointHistKey, Double] _

  implicit val convTotalLociMap = convertMap[Option[String], Int, OCN, NumLoci] _

  type DoubleMap = Map[JointHistKey, Double]

  def Key(contigName: ContigName, depths: Option[Int]*): JointHistKey = (Some(contigName), depths)

  test("simple") {

    val f = feature(100, 110)

    val read1 = read(90, 103)
    val read2 = read(107, 210)

    val reads1 = sc.parallelize(List(read1))
    val reads2 = sc.parallelize(List(read2))
    val features = FeatureRDD(sc.parallelize(List(f)), sd)

    val j = fromReadsAndFeatures(List(reads1, reads2), List(features))

    j.jh.collectAsMap().toMap should ===(
      Map(
        Key("chr2", Some(1), Some(0), Some(1)) →   3,
        Key("chr2", Some(1), Some(0), Some(0)) →  10,
        Key("chr2", Some(0), Some(1), Some(0)) → 100,
        Key("chr2", Some(0), Some(1), Some(1)) →   3,
        Key("chr2", Some(0), Some(0), Some(1)) →   4
      )
    )

    j.totalLoci === (
      Map(
        Some("chr2") → 120,
        None → 120
      )
    )

    j.sums.get(0, 1).collectAsMap().toMap should === (
      Map(
        Key("chr2", None, None, Some(1)) → 3.0,
        Key("chr2", None, None, Some(0)) → 10.0
      )
    )

    j.sqsums.get(0, 1).collectAsMap().toMap should === (
      Map(
        Key("chr2", None, None, Some(1)) → 3.0,
        Key("chr2", None, None, Some(0)) → 10.0
      )
    )

    j.dots.get(0, 1).collectAsMap().toMap should === (
      Map(
        Key("chr2", None, None, Some(1)) → 0.0,
        Key("chr2", None, None, Some(0)) → 0.0
      )
    )

    j.ns.get(0, 1).collectAsMap().toMap should === (
      Map(
        Key("chr2", None, None, Some(1)) → 10.0,
        Key("chr2", None, None, Some(0)) → 110.0
      )
    )

    j.weights(0, 1).collectAsMap().toMap should === (
      Map(
        Key("chr2", None, None, Some(1)) →
            (
              RegressionWeights(
                -0.42857142857142855,
                0.42857142857142855,
                1.7142857142857144,
                0.18367346938775508
              ),
              RegressionWeights(
                -0.42857142857142855,
                0.42857142857142855,
                1.7142857142857144,
                0.18367346938775508
              )
            ),
        Key("chr2", None, None, Some(0)) →
            (
              RegressionWeights(-1.0, 1.0, 0.0, 1.0),
              RegressionWeights(-1.0, 1.0, 0.0, 1.0)
            )
      )
    )

    j.eigens(0, 1).collectAsMap().toMap should === (
      Map(
        Key("chr2", None, None, Some(1)) → (
          Eigen(0.33333333333333337, (0.7071067811865477, -0.7071067811865475), 0.7142857142857143),
          Eigen(0.13333333333333333, (-0.7071067811865475, -0.7071067811865475), 0.2857142857142857)
        ),
        Key("chr2", None, None, Some(0)) → (
          Eigen(0.16680567139282737, (0.7071067811865475, -0.7071067811865476), 1.0),
          Eigen(0.0, (-0.7071067811865476, -0.7071067811865475), 0.0)
        )
      )
    )
  }

  def makeRead(start: Long, sequence: String, cigar: String): AlignmentRecord =
    AlignmentRecord.newBuilder()
      .setContigName("1")
      .setSequence(sequence)
      .setStart(start)
      .setEnd(start + TextCigarCodec.decode(cigar).getReferenceLength)
      .setCigar(cigar)
      .setReadMapped(true)
      .build()

  test("skipInsertionCoverage") {

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

    implicit val toLocusInt = toLocusKey[Int] _
    implicit val toLocusIntArray = convertArray[(Int, Int), (Locus, Int)] _

    val l: Array[(Locus, Int)] =
      JointHistogram
        .readsToDepthMap(reads)
        .collect()
        .map(p => (p._1.locus, p._2))
        .sortBy(_._1)

    l should === (
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
