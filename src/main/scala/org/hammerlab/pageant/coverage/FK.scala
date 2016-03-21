package org.hammerlab.pageant.coverage

import org.hammerlab.pageant.histogram.JointHistogram._

case class FK(c: String, d1: Depth, d2: Depth, on: Long, off: Long)
object FK {
  def make(t: ((OS, Depths), L)): FK = {
    val (on, off) =
      if (t._1._2(2).get == 1)
        (t._2, 0L)
      else
        (0L, t._2)
    new FK(t._1._1.get, t._1._2(0).get, t._1._2(1).get, on, off)
  }
}

