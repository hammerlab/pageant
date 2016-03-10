package org.hammerlab.pageant.suffixes.sentinel

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.utils.Utils
import org.hammerlab.pageant.suffixes.base.SuffixArrayBAMTest

class SentinelSATest extends SuffixArrayBAMTest {
  override def rdd(r: RDD[Byte]): RDD[Int] = {
    SentinelSA(r.map(Utils.toC)).map(_.toInt)
  }

  override def arr(a: Array[Int], n: Int): Array[Int] = {
    SentinelSA(sc.parallelize(a.map(i ⇒ Utils.toC(i.toByte)))).map(_.toInt).collect
  }

  test(s"SA 5: zeroes") {
    for { i <- 4 to 16 } {
      withClue(s"$i zeroes:") {
        arr(Array.fill(i+1)(0), 4) should be((0 to i).toArray)
      }
    }
  }

}
