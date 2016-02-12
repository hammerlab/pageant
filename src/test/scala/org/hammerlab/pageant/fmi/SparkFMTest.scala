package org.hammerlab.pageant.fmi

import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.SparkFunSuite
import org.hammerlab.pageant.fmi.SparkFM.{V, Idx, T}
import org.hammerlab.pageant.suffixes.KarkainnenSuffixArray
import org.scalatest.Matchers

import scala.collection.mutable.ArrayBuffer
import Utils._
import org.hammerlab.pageant.utils.Utils.rev

abstract class SparkFMTest[NT <: Needle] extends SparkFunSuite with Matchers with Serializable {

  def makeSparkFM(saZipped: RDD[(V, Idx)],
                  tZipped: RDD[(Idx, T)],
                  count: Long,
                  N: Int,
                  blockSize: Int = 100): SparkFM[NT]

  def fmTest(name: String,
             sa: Array[Int],
             saPartitions: Int,
             ts: String,
             tsPartitions: Int,
             blockSize: Int)(body: SparkFM[NT] => Unit): Unit = {
    sparkTest(name) {
      sa.length should be(ts.length)

      val saZipped = sc.parallelize(sa.map(_.toLong), saPartitions).zipWithIndex()
      val tZipped = sc.parallelize(ts.map(toI), tsPartitions).zipWithIndex().map(rev)

      sc.setCheckpointDir("tmp")

      body(
        makeSparkFM(saZipped, tZipped, count = sa.length, N = 5, blockSize = blockSize)
      )
    }
  }

  def fmTest(name: String,
             saPartitions: Int,
             ts: String,
             tsPartitions: Int,
             blockSize: Int)(body: SparkFM[NT] => Unit): Unit = {
    val sa = KarkainnenSuffixArray.make(ts.map(toI).toArray, 5)
    fmTest(name, sa, saPartitions, ts, tsPartitions, blockSize)(body)
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

  def BWTBlocksTest(saPartitions: Int,
                    tsPartitions: Int,
                    blockSize: Int): Unit = {

    fmTest(
      s"bwtblocks-$saPartitions-$tsPartitions-$blockSize",
      sa,
      saPartitions,
      sequence,
      tsPartitions,
      blockSize
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

      val blocks =
        (for {
          start <- bwt.indices by blockSize
        } yield {
          val end = math.min(start + blockSize, bwt.length)
          BWTBlock(start, end, counts(start).map(_.toLong), bwt.slice(start, end))
        }).toArray.zipWithIndex.map(rev)

      fm.bwtBlocks.collect.sortBy(_._1) should be(blocks)
    })
  }

  for {
    saPartitions <- List(1, 3, 6)
    tsPartitions <- List(1, 3, 6)
    blockSize <- 1 to 6
  } {
    BWTBlocksTest(saPartitions, tsPartitions, blockSize)
  }


  def testLF(tuples: (String, Int, Int)*): Unit = {
    val strs = tuples.map(_._1)
    fmTest(s"occ-${strs.mkString(",")}", 3, sequence, 2, 4)((fm) => {
      val needles: Seq[Array[Int]] = strs.map(_.toArray.map(toI))

      val needlesRdd = sc.parallelize(needles, 2)
      val actual = fm.occ(needlesRdd).collect.map(p => (p._1.map(toC).mkString(""), p._2))

      actual should be(tuples.toArray.map(t => (t._1, Bounds(t._2, t._3))))
    })
  }

  testLF(
    ("$", 0, 1),
    ("A", 1, 3),
    ("C", 3, 5),
    ("G", 5, 7),
    ("T", 7, 9)
  )

  testLF(
    ("A$", 1, 2),
    ("AA", 2, 2),
    ("AC", 2, 3),
    ("AG", 3, 3),
    ("AT", 3, 3)
  )

  testLF(
    ("C$", 3, 3),
    ("CA", 3, 4),
    ("CC", 4, 4),
    ("CG", 4, 5),
    ("CT", 5, 5)
  )

  testLF(
    ("G$", 5, 5),
    ("GA", 5, 5),
    ("GC", 5, 6),
    ("GG", 6, 6),
    ("GT", 6, 7)
  )

  testLF(
    ("T$", 7, 7),
    ("TA", 7, 7),
    ("TC", 7, 7),
    ("TG", 7, 8),
    ("TT", 8, 9)
  )

  testLF(
    ("ACG", 2, 3),
    ("CGT", 4, 5),
    ("GTT", 6, 7),
    ("TTG", 8, 9),
    ("TGC", 7, 8),
    ("GCA", 5, 6),
    ("CA$", 3, 4)
  )

  testLF(
    ("ACGT", 2, 3),
    ("CGTT", 4, 5),
    ("GTTG", 6, 7),
    ("TTGC", 8, 9),
    ("TGCA", 7, 8),
    ("GCA$", 5, 6)
  )

  testLF(
    ("ACGTT", 2, 3),
    ("CGTTG", 4, 5),
    ("GTTGC", 6, 7),
    ("TTGCA", 8, 9),
    ("TGCA$", 7, 8)
  )

  testLF(
    ("ACGTTG", 2, 3),
    ("CGTTGC", 4, 5),
    ("GTTGCA", 6, 7),
    ("TTGCA$", 8, 9)
  )

  testLF(
    ("ACGTTGC", 2, 3),
    ("CGTTGCA", 4, 5),
    ("GTTGCA$", 6, 7)
  )

  testLF(
    ("ACGTTGCA", 2, 3),
    ("CGTTGCA$", 4, 5)
  )

  testLF(
    ("ACGTTGCA$", 2, 3)
  )

  def testOccAll(tuples: (String, List[((Int, Int), (Int, Int))])*): Unit = {
    val strs = tuples.map(_._1)
    fmTest(s"occAll-${strs.mkString(",")}", 3, sequence, 2, 4)((fm) => {
      val needles: Seq[Array[Int]] = strs.map(_.toArray.map(toI))

      val needlesRdd = sc.parallelize(needles, 2)
      val actual = for {
        (ts, map) <- fm.occAll(needlesRdd).collect
        str = ts.map(toC).mkString("")
      } yield {
        str -> (for {
          (r, rm) <- map.toList
          (c, Bounds(lb, hb)) <- rm.toList
        } yield {
          (r, c) -> (lb.v.toInt, hb.v.toInt)
        }).sortBy(_._1)
      }

      actual should be(tuples.toArray.map(t => (t._1, t._2.sortBy(_._1))))
    })
  }

  testOccAll(
    "TGT" -> List((0,1) -> (7,9), (0,2) -> (7,8), (0,3) -> (8,8), (1,2) -> (5,7), (1,3) -> (6,7), (2,3) -> (7,9))
  )
}
