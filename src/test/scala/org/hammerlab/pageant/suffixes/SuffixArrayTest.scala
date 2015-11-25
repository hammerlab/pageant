package org.hammerlab.pageant.suffixes

import org.scalatest.{FunSuite, Matchers}

trait SuffixArrayTestBase extends FunSuite with Matchers {
  def arr: (Array[Int], Int) => Array[Int]
  def name: String

  test(s"$name SA 1") {
    arr(Array(0, 1, 2, 0, 1, 1), 4) should be(Array(3, 0, 5, 4, 1, 2))
  }

  test(s"$name SA 2") {
    // Inserting elements at the end of the above array.
    arr(Array(0, 1, 2, 0, 1, 1, 0), 4) should be(Array(6, 3, 0, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1), 4) should be(Array(3, 0, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 2), 4) should be(Array(3, 0, 4, 5, 1, 6, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 3), 4) should be(Array(3, 0, 4, 1, 5, 2, 6))
  }

  test(s"$name SA 3") {
    // Inserting elements at index 3 in the last array above.
    arr(Array(0, 1, 2, 0, 0, 1, 1, 3), 4) should be(Array(3, 4, 0, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 1, 0, 1, 1, 3), 4) should be(Array(4, 0, 3, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 2, 0, 1, 1, 3), 4) should be(Array(4, 0, 5, 1, 6, 3, 2, 7))
    arr(Array(0, 1, 2, 3, 0, 1, 1, 3), 4) should be(Array(4, 0, 5, 1, 6, 2, 7, 3))
  }

  test(s"$name SA 4") {
    // Inserting elements at index 5 in the first array in the second block above.
    arr(Array(0, 1, 2, 0, 1, 0, 1, 0), 4) should be(Array(7, 5, 3, 0, 6, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1, 0), 4) should be(Array(7, 3, 0, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 2, 1, 0), 4) should be(Array(7, 0, 3, 6, 1, 4, 2, 5))
    arr(Array(0, 1, 2, 0, 1, 3, 1, 0), 4) should be(Array(7, 0, 3, 6, 1, 4, 2, 5))
  }

  test(s"$name SA 5: zeroes") {
    for { i <- 0 to 16 } {
      arr(Array.fill(i+1)(0), 4) should be((i to 0 by -1).toArray)
    }
  }

  test(s"$name SA 6") {
    arr(Array(5,1,3,0,4,5,2), 7) should be(Array(3, 1, 6, 2, 4, 0, 5))
    arr(Array(2,2,2,2,0,2,2,2,1), 9) should be(Array(4, 8, 3, 7, 2, 6, 1, 5, 0))
  }
}

class SuffixArrayTest extends SuffixArrayTestBase {
  override def arr = SuffixArray.apply _
  override def name = "SuffixArrayTest"
}

class KarkkainenTest extends SuffixArrayTestBase {
  override def arr = SuffixArray.make _
  override def name = "KarkkainenTest"
}
