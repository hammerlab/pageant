package org.hammerlab.pageant.fm.bwt

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.RunLengthBWTBlock
import org.hammerlab.pageant.fm.bwt.BWT.{NextStepInfo, NextStringPos, StepInfo, StringPos}
import org.hammerlab.pageant.fm.utils.Utils
import org.hammerlab.pageant.fm.blocks.Utils.{counts, runs}
import org.hammerlab.pageant.fm.utils.Utils.{N, VT}
import org.hammerlab.pageant.utils.PageantSuite

import scala.reflect.ClassTag

class BWTTest extends PageantSuite {

  def opt(l: Long): Option[Long] = if (l > 0) Some(l) else None
  def sToTs(s: String): VT = s.map(Utils.toI).toVector

  def sp(s: String, curPos: Long, nextInsertPos: Long = -1): StringPos = {
    StringPos(sToTs(s), curPos, opt(nextInsertPos))
  }

  def nsp(s: String, nextInsertPos: Long, nextPos: Long, next2InsertPos: Long = -1): NextStringPos = {
    NextStringPos(sToTs(s), nextInsertPos, nextPos, opt(next2InsertPos))
  }

  def order[U: ClassTag](rdd: RDD[(Long, U)]): Array[U] = rdd.collect.sortBy(_._1).map(_._2)

  def testFn(
    blockSize: Int,
    blocksPerPartition: Int = 1
  )(
    pb1: (Int, Int)*
  )(
    e1: (String, String)*
  )(
    pb2: (Int, Int)*
  )(
    e2: (String, String)*
  )(
    pb3: (Int, Int)*
  )(
    e3: (String, String)*
  )(
    pb4: (Int, Int)*
  )(
    e4: (String, String)*
  )(
    pb5: (Int, Int)*
  )(
    e5: (String, String)*
  ): Unit = {

    def expected(ss: Seq[(String, String)]): Array[RunLengthBWTBlock] = {
      (for {
        ((c, rs), i) <- ss.zipWithIndex
      } yield {
        RunLengthBWTBlock(blockSize * i, counts(c), runs(rs))
      }).toArray
    }

    test(s"simple-$blockSize") {
      val tss: RDD[VT] =
        sc.parallelize(
          List(
            "ACAG$",
            "TCAG$",
            "CCGA$",
            "AGTC$"
          ).map(sToTs)
        )

      var nsi = NextStepInfo(tss, blockSize, blocksPerPartition)

      var si = BWT.toNextStep(nsi)

      si.counts should be (counts("0 4 4 4 4 4"))
      si.partitionBounds should be (pb1.map(pb ⇒ (pb._1, pb._2.toLong)).toArray)

      order(si.stringPoss) should be(
        Array(
          sp("ACAG", 0, 4),
          sp("TCAG", 1, 4),
          sp("CCGA", 2, 4),
          sp("AGTC", 3, 4)
        )
      )

      order(si.bwt) should be(expected(e1))

      nsi = BWT.primeNextStep(si)
      nsi.counts should be(counts("0 4 5 6 8 8"))
      order(nsi.nextStringPoss) should be(
        Array(
          nsp("ACA", 4, 6, 5),
          nsp("TCA", 4, 7, 5),
          nsp("CCG", 4, 4, 8),
          nsp("AGT", 4, 5, 8)
        )
      )

      si = BWT.toNextStep(nsi)
      si.partitionBounds should be (pb2.map(pb ⇒ (pb._1, pb._2.toLong)).toArray)
      order(si.stringPoss) should be(
        Array(
          sp("ACA", 6, 5),
          sp("TCA", 7, 5),
          sp("CCG", 4, 8),
          sp("AGT", 5, 8)
        )
      )
      order(si.bwt) should be(expected(e2))

      nsi = BWT.primeNextStep(si)
      nsi.counts should be(counts("0  4  7  8 11 12"))
      order(nsi.nextStringPoss) should be(
        Array(
          nsp("AC", 5,  5,  8),
          nsp("TC", 5,  6,  8),
          nsp("CC", 8, 10,  8),
          nsp("AG", 8, 11, 11)
        )
      )

      si = BWT.toNextStep(nsi)
      si.partitionBounds should be (pb3.map(pb ⇒ (pb._1, pb._2.toLong)).toArray)
      order(si.stringPoss) should be(
        Array(
          sp("AC",  5,  8),
          sp("TC",  6,  8),
          sp("CC", 10,  8),
          sp("AG", 11, 11)
        )
      )
      order(si.bwt) should be(expected(e3))

      nsi = BWT.primeNextStep(si)
      nsi.counts should be(counts("0  4  7 11 15 16"))
      order(nsi.nextStringPoss) should be(
        Array(
          nsp("A",  8,  8,  5),
          nsp("T",  8,  9, 16),
          nsp("C",  8, 10, 10),
          nsp("A", 11, 14,  7)
        )
      )

      si = BWT.toNextStep(nsi)
      si.partitionBounds should be (pb4.map(pb ⇒ (pb._1, pb._2.toLong)).toArray)
      order(si.stringPoss) should be(
        Array(
          sp("A",  8,  5),
          sp("T",  9, 16),
          sp("C", 10, 10),
          sp("A", 14,  7)
        )
      )
      order(si.bwt) should be(expected(e4))

      nsi = BWT.primeNextStep(si)
      nsi.counts should be(counts("0  4  9 14 18 20"))
      order(nsi.nextStringPoss) should be(
        Array(
          nsp("",  5,  5),
          nsp("", 16, 19),
          nsp("", 10, 12),
          nsp("",  7,  8)
        )
      )

      si = BWT.toNextStep(nsi)
      si.partitionBounds should be (pb5.map(pb ⇒ (pb._1, pb._2.toLong)).toArray)
      order(si.stringPoss) should be(
        Array(
          sp("",  5),
          sp("", 19),
          sp("", 12),
          sp("",  8)
        )
      )
      order(si.bwt) should be(expected(e5))
    }
  }

  testFn(
    100
  )(
    (0, 100)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C")
  )(
    (0, 100)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 1T 2A")
  )(
    (0, 100)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 2C 1T 2A 1C 1G")
  )(
    (0, 100)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 2C 1T 1A 1T 1C 2A 1C 1A 1G")
  )(
    (0, 100)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 1$ 2C 1$ 1T 1A 1T 1$ 1C 2A 1C 1A 1G 1$")
  )

  testFn(
    10
  )(
    (0, 10)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C")
  )(
    (0, 10)
)(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 1T 2A")
  )(
    (0, 10), (1, 20)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 2C 1T 2A"),
    ("0 3 3 3 1 0", "1C 1G")
  )(
    (0, 10), (1, 20)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 2C 1T 1A 1T"),
    ("0 2 3 3 2 0", "1C 2A 1C 1A 1G")
  )(
    (0, 10), (1, 20)
  )(
    ("0 0 0 0 0 0", "2G 1A 1C 1G 1$ 2C 1$ 1T"),
    ("2 1 3 3 1 0", "1A 1T 1$ 1C 2A 1C 1A 1G 1$")
  )


}

/*

String: ACAG$TCAG$CCGA$ACTC$

""

$  A  C  G  T  N
0  0  0  0  0  0

 0  0 ACAG $       * 0        0   0   4
 1  0 TCAG $       * 1        0   1   4
 2  0 CCGA $       * 2        0   2   4
 3  0 AGTC $       * 3        0   3   4

$  A  C  G  T  N
0  4  4  4  4  4

 0  0  ACA G $     * 0        4   6   5
 1  0  TCA G $     * 1        4   7   5
 2  0  CCG A $     * 2        4   4   8
 3  0  AGT C $     * 3        4   5   8

2G 1A 1C

$  A  C  G  T  N
0  4  5  6  8  8

 0  0  ACA G $          - 0
 1  1  TCA G $          - 1
 2  2  CCG A $          - 2
 3  3  AGT C $          - 3
 4      CC G A$    * 2        8  10   8
 5      AG T C$    * 3        8  11  11
 6      AC A G$    * 0        5   5   8
 7      TC A G$    * 1        5   6   8

2G 1A 1C 1G 1T 2A

$  A  C  G  T  N
0  4  7  8 11 12

 0  0  ACA G $
 1  1  TCA G $
 2  2  CCG A $
 3  3  AGT C $
 4  4   CC G A$         - 2
 5       A C AG$   * 0        8   8   5
 6       T C AG$   * 1        8   9  16
 7  5   AG T C$         - 3
 8  6   AC A G$         - 0
 9  7   TC A G$         - 1
10       C C GA$   * 2        8  10  10
11       A G TC$   * 3       11  14   7

2G 1A 1C 1G 2C 1T 2A 1C 1G

$  A  C  G  T  N
0  4  7 11 15 16

 0  0  ACA G $
 1  1  TCA G $
 2  2  CCG A $
 3  3  AGT C $
 4  4   CC G A$
 5  5    A C AG$        - 0
 6  6    T C AG$        - 1
 7  7   AG T C$
 8       $ A CAG$  * 0        5   5
 9       $ T CAG$  * 1       16  19
10       $ C CGA$  * 2       10  12
11  8   AC A G$
12  9   TC A G$
13 10    C C GA$        - 2
14       $ A GTC$  * 3        7   8
15 11    A G TC$        - 3

2G 1A 1C 1G 2C 1T 1A 1T 1C 2A 1C 1A 1G

$  A  C  G  T  N
0  4  9 14 18 20

 0  0  ACA G $
 1  1  TCA G $
 2  2  CCG A $
 3  3  AGT C $
 4  4   CC G A$
 5         $ ACAG$ * 0
 6  5    C C AG$
 7  6    A C AG$
 8         $ AGTC$ * 3
 9  7   AG T C$
10  8    $ A CAG$      - 0
11  9    $ T CAG$      - 1
12         $ CCGA$ * 2
13 10    $ C CGA$      - 2   C
14 11   AC A G$              A
15 12   TC A G$              A
16 13    C C GA$             C
17 14    $ A GTC$      - 3   A
18 15    A G TC$             G
19         $ TCAG$ * 1       $

2G 1A 1C 1G 1$ 2C 1$ 1T 1A 1T 1$ 1C 2A 1C 1A 1G 1$


 */
