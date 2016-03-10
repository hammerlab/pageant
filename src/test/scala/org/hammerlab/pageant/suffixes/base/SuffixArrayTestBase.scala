package org.hammerlab.pageant.suffixes.base

import org.bdgenomics.adam.rdd.ADAMContext._
import org.hammerlab.pageant.rdd.OrderedRepartitionRDD._
import org.hammerlab.pageant.rdd.IfRDD._
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.utils.{KryoNoReferenceTracking, PageantSuite}
import org.hammerlab.pageant.utils.Utils.loadBam
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.fm.utils.Utils.toI

trait SuffixArrayTestBase extends FunSuite with Matchers {
  def arr(a: Array[Int], n: Int): Array[Int]

  def intsFromFile(file: String): Array[Int] = {
    val inPath = ClassLoader.getSystemClassLoader.getResource(file).getFile
    (for {
      line <- scala.io.Source.fromFile(inPath).getLines()
      if line.trim.nonEmpty
      s <- line.split(",")
      i = s.trim().toInt
    } yield {
      i
    }).toArray
  }
}

trait SuffixArrayRDDTest
  extends SuffixArrayTestBase
    with PageantSuite
    with KryoNoReferenceTracking {

  def rdd(r: RDD[Byte]): RDD[Int]

}

trait SuffixArrayBAMTest extends SuffixArrayRDDTest {

  def checkArrays(actual: Array[Long], expected: Array[Long]): Unit = {
    actual.length should be(expected.length)
    for {
      idx ← actual.indices
      actualElem = actual(idx)
      expectedElem = expected(idx)
    } {
      withClue(
        s", idx $idx: ${actual.slice(idx - 5, idx + 5).mkString(",")} vs. ${expected.slice(idx - 5, idx + 5).mkString(",")}"
      ) {
        actualElem should be(expectedElem)
      }
    }
  }

  def testBam(num: Int, numPartitions: Int): Unit = {
    val name = s"$num-$numPartitions"
    test(name) {

      val ots =
        sc.parallelize(
          sc
            .textFile(s"src/test/resources/1000.reads", numPartitions)
            .take(num)
            .flatMap(_ + '$')
            .map(toI),
          numPartitions
        )

      ots.getNumPartitions should be(numPartitions)
      ots.take(10) should be(Array(1, 4, 4, 4, 4, 4, 1, 1, 3, 1))

      val ts = ots.zipWithIndex().map(_.swap).sortByKey(numPartitions = numPartitions).map(_._2)

      ts.getNumPartitions should be(numPartitions)
      ts.take(10) should be(Array(1, 4, 4, 4, 4, 4, 1, 1, 3, 1))

      val totalLength = 102 * num
      ts.count should be(totalLength)

      val sa = rdd(ts)
      sa.count should be(totalLength)

      sa.take(num) should be(1 to num map (_ * 102 - 1) toArray)

      sa.getNumPartitions should be(numPartitions)

      // Set to true to overwrite the existing "expected" file that SAs will be vetted against.
      val writeMode = false

      if (writeMode) {
        sa.saveAsTextFile(s"src/test/resources/$num.sa.ints")
        ts.saveAsTextFile(s"src/test/resources/$num.ts.ints")
      } else {
        val expectedSAFilename = s"src/test/resources/$num.sa.ints"
        val expectedTSFilename = s"src/test/resources/$num.ts.ints"

        val expectedSA = sc.textFile(expectedSAFilename).collect.map(_.toLong)
        val actualSA = sa.collect.map(_.toLong)
        checkArrays(actualSA, expectedSA)

        val expectedTS = sc.textFile(expectedTSFilename).collect.map(_.toLong)
        val actualTS = ts.collect.map(_.toLong)
        checkArrays(actualTS, expectedTS)
      }
    }
  }

  testBam(10, 4)
  testBam(100, 10)
  testBam(1000, 10)
}

trait SuffixArrayDNATest extends SuffixArrayTestBase {
  test(s"SA 1") {
    arr(Array(0, 1, 2, 0, 1, 1), 4) should be(Array(0, 3, 5, 4, 1, 2))
  }

  test(s"SA 2") {
    // Inserting elements at the end of the above array.
    arr(Array(0, 1, 2, 0, 1, 1, 0), 4) should be(Array(0, 3, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1), 4) should be(Array(0, 3, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 2), 4) should be(Array(0, 3, 4, 1, 5, 2, 6))
    arr(Array(0, 1, 2, 0, 1, 1, 3), 4) should be(Array(0, 3, 4, 1, 5, 2, 6))
  }

  test(s"SA 3") {
    // Inserting elements at index 3 in the last array above.
    arr(Array(0, 1, 2, 0, 0, 1, 1, 3), 4) should be(Array(0, 3, 4, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 1, 0, 1, 1, 3), 4) should be(Array(0, 4, 3, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 2, 0, 1, 1, 3), 4) should be(Array(0, 4, 5, 1, 6, 3, 2, 7))
    arr(Array(0, 1, 2, 3, 0, 1, 1, 3), 4) should be(Array(0, 4, 5, 1, 6, 2, 3, 7))
  }

  test(s"SA 4") {
    // Inserting elements at index 5 in the first array in the second block above.
    arr(Array(0, 1, 2, 0, 1, 0, 1, 0), 4) should be(Array(0, 3, 5, 7, 4, 6, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1, 0), 4) should be(Array(0, 3, 7, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 2, 1, 0), 4) should be(Array(0, 3, 7, 6, 1, 4, 2, 5))
    arr(Array(0, 1, 2, 0, 1, 3, 1, 0), 4) should be(Array(0, 3, 7, 6, 1, 4, 2, 5))
  }

  test(s"SA 5: zeroes") {
    for { i <- 0 to 16 } {
      withClue(s"$i zeroes:") {
        arr(Array.fill(i+1)(0), 4) should be((0 to i).toArray)
      }
    }
  }

}

trait SuffixArrayOtherTest extends SuffixArrayTestBase {
  test(s"SA 6") {
    arr(Array(5, 1, 3, 0, 4, 5, 2), 7) should be(Array(3, 1, 6, 2, 4, 0, 5))
    arr(Array(2, 2, 2, 2, 0, 2, 2, 2, 1), 9) should be(Array(4, 8, 3, 7, 2, 6, 1, 5, 0))
  }

  test(s"random 100") {
    val a = Array(
      2, 7, 8, 7, 5, 5, 2, 3, 8, 1,  // 0
      2, 9, 2, 2, 6, 2, 9, 9, 6, 7,  // 1
      1, 8, 5, 1, 1, 7, 8, 7, 4, 6,  // 2
      8, 1, 5, 1, 6, 3, 9, 3, 7, 8,  // 3
      4, 1, 3, 7, 9, 8, 2, 4, 8, 1,  // 4
      5, 8, 1, 1, 6, 7, 2, 1, 8, 2,  // 5
      4, 9, 9, 2, 5, 6, 8, 2, 6, 8,  // 6
      7, 8, 1, 8, 1, 3, 3, 7, 7, 5,  // 7
      6, 1, 1, 2, 3, 3, 7, 3, 1, 9,  // 8
      8, 8, 8, 6, 9, 5, 5, 9, 4, 8   // 9
    )
    val expected =
      Array(
        81, 52, 23, 82, 9, 74, 41, 31, 49, 33, 53, 24, 72, 57, 20, 88,         // 1's
        56, 12, 83, 6, 46, 59, 63, 13, 67, 0, 10, 15,                          // 2's
        87, 84, 75, 85, 76, 37, 42, 7, 35,                                     // 3's
        40, 28, 98, 47, 60,                                                    // 4's
        22, 32, 5, 4, 95, 79, 64, 50, 96,                                      // 5's
        80, 14, 34, 18, 54, 29, 65, 68, 93,                                    // 6's
        19, 55, 86, 27, 3, 78, 77, 70, 38, 25, 1, 43,                          // 7's
        99, 51, 8, 73, 30, 48, 71, 45, 58, 66, 39, 21, 92, 26, 2, 69, 91, 90,  // 8's
        11, 62, 36, 97, 94, 17, 44, 89, 61, 16                                 // 9's
      )
    arr(a, 10) should be (expected)
  }

  test(s"random 1000") {
    val a = intsFromFile("random1000.in")
    val expected = intsFromFile("random1000.expected")
    arr(a, 10) should be(expected)
  }
}

trait SuffixArrayTest extends SuffixArrayDNATest with SuffixArrayOtherTest
