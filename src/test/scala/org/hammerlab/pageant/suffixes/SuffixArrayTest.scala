package org.hammerlab.pageant.suffixes

import org.scalatest.{Ignore, FunSuite, Matchers}

trait SuffixArrayTestBase extends FunSuite with Matchers {
  def arr(a: Array[Int], n: Int): Array[Int]
  def name: String

  def testFn(name: String)(testFun: => Unit): Unit

  testFn(s"$name SA 1") {
    arr(Array(0, 1, 2, 0, 1, 1), 4) should be(Array(3, 0, 5, 4, 1, 2))
  }

  testFn(s"$name SA 2") {
    // Inserting elements at the end of the above array.
    arr(Array(0, 1, 2, 0, 1, 1, 0), 4) should be(Array(6, 3, 0, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1), 4) should be(Array(3, 0, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 2), 4) should be(Array(3, 0, 4, 5, 1, 6, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 3), 4) should be(Array(3, 0, 4, 1, 5, 2, 6))
  }

  testFn(s"$name SA 3") {
    // Inserting elements at index 3 in the last array above.
    arr(Array(0, 1, 2, 0, 0, 1, 1, 3), 4) should be(Array(3, 4, 0, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 1, 0, 1, 1, 3), 4) should be(Array(4, 0, 3, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 2, 0, 1, 1, 3), 4) should be(Array(4, 0, 5, 1, 6, 3, 2, 7))
    arr(Array(0, 1, 2, 3, 0, 1, 1, 3), 4) should be(Array(4, 0, 5, 1, 6, 2, 7, 3))
  }

  testFn(s"$name SA 4") {
    // Inserting elements at index 5 in the first array in the second block above.
    arr(Array(0, 1, 2, 0, 1, 0, 1, 0), 4) should be(Array(7, 5, 3, 0, 6, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1, 0), 4) should be(Array(7, 3, 0, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 2, 1, 0), 4) should be(Array(7, 0, 3, 6, 1, 4, 2, 5))
    arr(Array(0, 1, 2, 0, 1, 3, 1, 0), 4) should be(Array(7, 0, 3, 6, 1, 4, 2, 5))
  }

  testFn(s"$name SA 5: zeroes") {
    for { i <- 0 to 16 } {
      arr(Array.fill(i+1)(0), 4) should be((i to 0 by -1).toArray)
    }
  }

  testFn(s"$name SA 6") {
    arr(Array(5,1,3,0,4,5,2), 7) should be(Array(3, 1, 6, 2, 4, 0, 5))
    arr(Array(2,2,2,2,0,2,2,2,1), 9) should be(Array(4, 8, 3, 7, 2, 6, 1, 5, 0))
  }

  testFn(s"$name random 100") {
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

  testFn(s"$name random 1000") {
    val a = intsFromFile("random1000.in")
    val expected = intsFromFile("random1000.expected")
    arr(a, 10) should be(expected)
  }
}

trait SuffixArrayLocalTestBase extends SuffixArrayTestBase {
  override def testFn(name: String)(testFun: => Unit): Unit = test(name)(testFun)
}

@Ignore
class WIPSuffixArrayTest extends SuffixArrayLocalTestBase {
  override def arr(a: Array[Int], n: Int): Array[Int] = WIPSuffixArray.make(a, n)
  override def name = "SuffixArrayTest"
}

class KarkkainenTest extends SuffixArrayLocalTestBase {
  override def arr(a: Array[Int], n: Int): Array[Int] = KarkainnenSuffixArray.make(a, n)
  override def name = "KarkkainenTest"
}
