package org.hammerlab.pageant.rdd

import org.hammerlab.pageant.utils.SparkSuite

import SlidingRDD._

class SlidingRDDTest extends SparkSuite {

  def lToT(l: IndexedSeq[Int]): (Int, Int, Int) = (l(0), l(1), l(2))

  def testN(n: Int): Unit = {
    test(s"$n") {
      sc.parallelize(1 to n).sliding3.collect should be((1 to n).sliding(3).map(lToT).toArray)
    }
  }

  testN(100)
  testN(12)
  testN(11)
  testN(10)
  testN(9)
  testN(8)
}
