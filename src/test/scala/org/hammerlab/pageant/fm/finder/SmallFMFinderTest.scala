package org.hammerlab.pageant.fm.finder

import org.hammerlab.pageant.fm.utils.{Bounds, Utils, SmallFMSuite}
import Utils._

sealed abstract class SmallFMFinderTest extends SmallFMSuite with FMFinderTest {

  val saPartitions = 3
  val ts = "ACGTTGCA$"
  val tsPartitions = 2
  val blockSize = 4

  def testLF(tuples: (String, Int, Int)*): Unit = {
    val strs = tuples.map(_._1)
    test(s"LF-${strs.mkString(",")}") {
      val needles: Seq[Array[Int]] = strs.map(_.toArray.map(toI))

      val needlesRdd = sc.parallelize(needles, 2)
      val actual = fmf.occ(needlesRdd).collect.map(p => (p._1.map(toC).mkString(""), p._2))

      actual should be(tuples.toArray.map(t => (t._1, Bounds(t._2, t._3))))
    }
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
    test(s"occAll-${strs.mkString(",")}") {
      val needles: Seq[Array[Int]] = strs.map(_.toArray.map(toI))

      val needlesRdd = sc.parallelize(needles, 2)
      val actual = for {
        (ts, map) <- fmf.occAll(needlesRdd).collect
        str = ts.map(toC).mkString("")
      } yield {
        str -> (for {
          (r, rm) <- map.m.toList
          (c, Bounds(lb, hb)) <- rm.toList
        } yield {
          (r, c) ->(lb.v.toInt, hb.v.toInt)
        }).sortBy(_._1)
      }

      actual should be(tuples.toArray.map(t => (t._1, t._2.sortBy(_._1))))
    }
  }

  testOccAll(
    "TGT" -> List((0,1) -> (7,9), (0,2) -> (7,8), (0,3) -> (8,8), (1,2) -> (5,7), (1,3) -> (6,7), (2,3) -> (7,9))
  )
}

class SmallBroadcastFinderTest extends SmallFMFinderTest with BroadcastFinderTest
class SmallAllTFinderTest extends SmallFMFinderTest with AllTFinderTest
