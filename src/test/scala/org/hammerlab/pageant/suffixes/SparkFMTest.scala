package org.hammerlab.pageant.suffixes

import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers

import scala.collection.mutable.ArrayBuffer

class SparkFMTest extends SparkFunSuite with Matchers with Serializable {

  val toI = "$ACGT".zipWithIndex.toMap
  val toC = toI.map(p => (p._2, p._1))

  def fmTest(name: String,
             sa: Array[Int],
             saPartitions: Int,
             ts: String,
             tsPartitions: Int,
             countInterval: Int)(body: SparkFM => Unit): Unit = {
    sparkTest(name) {
      sa.length should be(ts.length)

      val saZipped = sc.parallelize(sa.map(_.toLong), saPartitions).zipWithIndex()
      val tZipped = sc.parallelize(ts.map(toI), tsPartitions).zipWithIndex().map(p => (p._2, p._1))

      sc.setCheckpointDir("tmp")

      body(
        SparkFM(saZipped, tZipped, count = sa.length, N = 5, countInterval = countInterval)
      )
    }
  }

  def fmTest(name: String,
             saPartitions: Int,
             ts: String,
             tsPartitions: Int,
             countInterval: Int)(body: SparkFM => Unit): Unit = {
    val sa = KarkainnenSuffixArray.make(ts.map(toI).toArray, 5)
    fmTest(name, sa, saPartitions, ts, tsPartitions, countInterval)(body)
  }


  val sequence = "ACGTTGCA$"

  val sa = KarkainnenSuffixArray.make(sequence.map(toI).toArray, 5)
  sa should be(Array(8, 7, 0, 6, 1, 5, 2, 4, 3))

  val bwtu =
    sa
    .map(x => if (x == 0) sequence.length - 1 else x - 1)
    .zipWithIndex
    .sortBy(_._1)
    .map(_._2)
    .zip(sequence)
    .sortBy(_._1)
    .map(_._2)

  val bwt = bwtu.map(toI)
  bwt should be(Array(1, 2, 0, 3, 1, 4, 2, 4, 3))

  def BWTChunksTest(saPartitions: Int,
                    tsPartitions: Int,
                    countInterval: Int): Unit = {

    fmTest(
      s"bwtchunks-$saPartitions-$tsPartitions-$countInterval",
      sa,
      saPartitions,
      sequence,
      tsPartitions,
      countInterval
    )((fm) => {

      fm.tShifted.getNumPartitions should be(tsPartitions)
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

      val maxPartitions = math.max(saPartitions, tsPartitions)

      fm.indexedBwtt.getNumPartitions should be(maxPartitions)
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

      fm.bwtt.getNumPartitions should be(maxPartitions)
      val bwtt = fm.bwtt.collect
      bwtt should be(bwt)

      bwt.map(toC).mkString("") should be("AC$GATCTG")

      fm.partitionCounts.getNumPartitions should be(maxPartitions)

      fm.totalSums should be(Array(0L, 1L, 3L, 5L, 7L))

      val curCounts = Array.fill(5)(0)
      val counts = ArrayBuffer[Array[Int]]()
      for { i <- bwt } {
        counts.append(curCounts.clone())
        curCounts(i) += 1
      }

      val chunks =
        (for {
          start <- bwt.indices by countInterval
        } yield {
          val end = math.min(start + countInterval, bwt.length)
          BWTChunk(start, end, counts(start).map(_.toLong), bwt.slice(start, end))
        }).toArray.zipWithIndex.map(p => (p._2, p._1))

      fm.bwtChunks.collect.sortBy(_._1) should be(chunks)
    })
  }

  for {
    saPartitions <- List(1, 3, 6)
    tsPartitions <- List(1, 3, 6)
    countInterval <- 1 to 6
  } {
    BWTChunksTest(saPartitions, tsPartitions, countInterval)
  }


  def testLF(strs: List[String], tuples: (String, Int, Int)*): Unit = {
    fmTest(s"occ-${strs.mkString(",")}", 3, sequence, 2, 4)((fm) => {
      val needles: List[Array[Int]] = strs.map(_.toArray.map(toI))

      val needlesRdd = sc.parallelize(needles, 2)
      val actual = fm.occ(needlesRdd).collect.map(p => (p._1.map(toC).mkString(""), p._2, p._3))

      actual should be(tuples.toArray)
    })
  }

  testLF(
    List("$", "A", "C", "G", "T"),
    ("$", 0, 1),
    ("A", 1, 3),
    ("C", 3, 5),
    ("G", 5, 7),
    ("T", 7, 9)
  )

  testLF(
    List("A$", "AA", "AC", "AG", "AT"),
    ("A$", 1, 2),
    ("AA", 2, 2),
    ("AC", 2, 3),
    ("AG", 3, 3),
    ("AT", 3, 3)
  )

  testLF(
    List("C$", "CA", "CC", "CG", "CT"),
    ("C$", 3, 3),
    ("CA", 3, 4),
    ("CC", 4, 4),
    ("CG", 4, 5),
    ("CT", 5, 5)
  )

  testLF(
    List("G$", "GA", "GC", "GG", "GT"),
    ("G$", 5, 5),
    ("GA", 5, 5),
    ("GC", 5, 6),
    ("GG", 6, 6),
    ("GT", 6, 7)
  )

  testLF(
    List("T$", "TA", "TC", "TG", "TT"),
    ("T$", 7, 7),
    ("TA", 7, 7),
    ("TC", 7, 7),
    ("TG", 7, 8),
    ("TT", 8, 9)
  )

  testLF(
    List("ACG", "CGT", "GTT", "TTG", "TGC", "GCA", "CA$"),
    ("ACG", 2, 3),
    ("CGT", 4, 5),
    ("GTT", 6, 7),
    ("TTG", 8, 9),
    ("TGC", 7, 8),
    ("GCA", 5, 6),
    ("CA$", 3, 4)
  )

  testLF(
    List("ACGT", "CGTT", "GTTG", "TTGC", "TGCA", "GCA$"),
    ("ACGT", 2, 3),
    ("CGTT", 4, 5),
    ("GTTG", 6, 7),
    ("TTGC", 8, 9),
    ("TGCA", 7, 8),
    ("GCA$", 5, 6)
  )

  testLF(
    List("ACGTT", "CGTTG", "GTTGC", "TTGCA", "TGCA$"),
    ("ACGTT", 2, 3),
    ("CGTTG", 4, 5),
    ("GTTGC", 6, 7),
    ("TTGCA", 8, 9),
    ("TGCA$", 7, 8)
  )

  testLF(
    List("ACGTTG", "CGTTGC", "GTTGCA", "TTGCA$"),
    ("ACGTTG", 2, 3),
    ("CGTTGC", 4, 5),
    ("GTTGCA", 6, 7),
    ("TTGCA$", 8, 9)
  )

  testLF(
    List("ACGTTGC", "CGTTGCA", "GTTGCA$"),
    ("ACGTTGC", 2, 3),
    ("CGTTGCA", 4, 5),
    ("GTTGCA$", 6, 7)
  )

  testLF(
    List("ACGTTGCA", "CGTTGCA$"),
    ("ACGTTGCA", 2, 3),
    ("CGTTGCA$", 4, 5)
  )

  testLF(
    List("ACGTTGCA$"),
    ("ACGTTGCA$", 2, 3)
  )

}
