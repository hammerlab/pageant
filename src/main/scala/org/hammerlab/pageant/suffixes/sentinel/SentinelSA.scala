package org.hammerlab.pageant.suffixes.sentinel

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.suffixes.pdc3.PDC3.L
import org.hammerlab.pageant.rdd.SlidingRDD._
import org.apache.spark.sortwith.SortWithRDD._
import org.hammerlab.pageant.fm.utils.Utils
import org.hammerlab.pageant.fm.utils.Utils.T
import org.hammerlab.pageant.reads.Bases5
import org.hammerlab.pageant.utils.Utils.longToCmpFnReturn

object SentinelSA {

  def cmp(t1: (Bases5, Long), t2: (Bases5, Long)): Int = {
    val b1 = t1._1
    val b2 = t2._1
    var i = 0
    while (i < b1.length && i < b2.length) {
      val b1i = Bases5.cToT(b1(i))
      val b2i = Bases5.cToT(b2(i))
      if (b1i < b2i) return -1
      if (b1i > b2i) return 1
      i += 1
    }
    if (i < b2.length) -1
    else if (i < b1.length) 1
    else longToCmpFnReturn(t1._2 - t2._2)
  }

  def fromBytes(ts: RDD[T]): RDD[L] = {
    apply(ts.map(Utils.toC))
  }

  def apply(ts: RDD[Char]): RDD[L] = {
    ts.slideUntil('$').map(ts ⇒ {
      Bases5(ts)
    }).zipWithIndex().sortWith(cmp).values
  }
}
