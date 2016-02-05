package org.hammerlab.pageant.suffixes

import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.{BeforeAndAfterAll, Matchers}

object Test {
  //val ref: Seq[Char] = "CCTGAGGCATGATCCTGACTTCGGGTGCAGGACATTAATTGGTAGCTAATCCAGGCATTAAACGCGGTCGGGAAGTATGAAATCCTGAATGCTCCGTGCC"
  val toI = "$ACGT".zipWithIndex.toMap
  val toC = toI.map(p => (p._2, p._1))
  val ref: Seq[Char] = "ACGTTGCA$"
}

class SparkFMTest extends SparkFunSuite with Matchers with Serializable {

  import Test.{ref, toC, toI}

  @transient var refRdd: RDD[Char] = _
  @transient var fm: SparkFM[Char] = _

  sparkTest("simple") {

    println(s"test: $sc")
    sc.setCheckpointDir("tmp")

    refRdd = sc.parallelize(ref, 2)
    fm = new SparkFM(refRdd, 5, 2, toI.apply)

    fm.count should be(9)

    fm.sa.getNumPartitions should be(6)
    val sa = fm.sa.collect
    sa should be(Array(8, 7, 0, 6, 1, 5, 2, 4, 3))

    fm.tShifted.getNumPartitions should be(2)
    val tShifted = fm.tShifted.collect
    tShifted should be(
      Array(
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 4),
        (6, 3),
        (7, 2),
        (8, 1),
        (0, 0)
      )
    )

    fm.indexedBwtt.getNumPartitions should be(6)
    val ibwt = fm.indexedBwtt.collect
    ibwt should be(
      Array(
        (0, 1),
        (1, 2),
        (2, 0),
        (3, 3),
        (4, 1),
        (5, 4),
        (6, 2),
        (7, 4),
        (8, 3)
      )
    )

    fm.bwtt.getNumPartitions should be(6)
    val bwt = fm.bwtt.collect
    bwt should be(Array(1, 2, 0, 3, 1, 4, 2, 4, 3))

    bwt.map(toC).mkString("") should be("AC$GATCTG")

    fm.lastCounts should be(
      Array(
        Array(0, 1, 1, 0, 0),  // AC
        Array(1, 0, 0, 0, 0),  // $
        Array(0, 1, 0, 1, 0),  // GA
        Array(0, 0, 0, 0, 1),  // T
        Array(0, 0, 1, 0, 1),  // CT
        Array(0, 0, 0, 1, 0)   // G
      )
    )

    fm.totalSums should be(Array(0L, 1L, 3L, 5L, 7L))

    fm.summedCounts.map(_._2) should be(Array(0, 2, 3, 5, 6, 8))
    fm.summedCounts.map(_._1) should be(
      Array(
        Array(0, 0, 0, 0, 0),
        Array(0, 1, 1, 0, 0),
        Array(1, 1, 1, 0, 0),
        Array(1, 2, 1, 1, 0),
        Array(1, 2, 1, 1, 1),
        Array(1, 2, 2, 1, 2)
      )
    )

    val chunks = fm.bwtChunks.collect.map(p => (p._1, p._2.toString()))
    val expected = Array(
      (0, BWTChunk(0, 2, Array(0,0,0,0,0), Array(1,2))),
      (1, BWTChunk(2, 4, Array(0,1,1,0,0), Array(0,3))),
      (2, BWTChunk(4, 6, Array(1,1,1,1,0), Array(1,4))),
      (3, BWTChunk(6, 8, Array(1,2,1,1,1), Array(2,4))),
      (4, BWTChunk(8, 9, Array(1,2,2,1,2), Array(3)))
    ).map(p => (p._1, p._2.toString()))

    chunks should be(expected)

    val needles: List[Array[Int]] = List("A", "C", "G", "T"/*, "CAT"*/).map(_.toArray.map(toI))

    val needlesRdd = sc.parallelize(needles, 2)
    val actual = fm.occ(needlesRdd).collect.map(p => (p._1.map(toC).mkString(""), p._2, p._3))

    actual should be(
      Array(
        ("A", 1, 3),
        ("C", 3, 5),
        ("G", 5, 7),
        ("T", 7, 9)
      )
    )
  }
}


/*

gca$
gca$
aca$
tca$
gta$
ata$
tta$
tta$
tag$
cag$
acg$
gtg$
ggc$
tcc$
aac$
ctc$

$gca
$gca
$aca
$tca
$gta
$ata
$tta
$tta
$tag
$cag
$acg
$gtg
$ggc
$tcc
$aac
$ctc

a$gc
a$gc
a$ac
a$tc
a$gt
a$at
a$tt
a$tt
aac$
ac$a
ag$c
ag$t
ata$

c$gg
c$tc
c$aa
c$ct
ca$a
ca$g
ca$g
ca$t
cc$t
cg$a

g$ta
g$ca
g$ac
g$gt
gc$g

ta$g
ta$a
ta$t
ta$t
tc$c
tg$g

 */
