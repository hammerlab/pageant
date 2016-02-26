package org.hammerlab.pageant.rdd

import org.hammerlab.pageant.utils.SparkSuite

import SlidingRDD._

class SlidingRDDTest extends SparkSuite {

  def lToT(l: IndexedSeq[Int]): (Int, Int, Int) = (l(0), l(1), l(2))

  def testN(n: Int): Unit = {
    test(s"$n") {
      val range = 1 to n
      var expectedSlid = range.sliding(3).map(lToT).toArray

      sc.parallelize(range).sliding3().collect should be(expectedSlid)

      expectedSlid ++= Array((n - 1, n, 0), (n, 0, 0))

      sc.parallelize(range).sliding3(0).collect should be(expectedSlid)
    }
  }

  testN(100)
  testN(12)
  testN(11)
  testN(10)
  testN(9)
  testN(8)
}
