package org.hammerlab.pageant.utils

import scala.collection.mutable.ArrayBuffer

class RoundNumberIterator(steps: Seq[Int], base: Int = 10, limitOpt: Option[Int] = None) extends Iterator[Int] {
  var idx = 0
  var basePow = 1
  var nextInt = steps(idx)

  override def hasNext: Boolean = {
    limitOpt match {
      case Some(limit) => next < limit
      case _ => true
    }
  }

  override def next(): Int = {
    val n = nextInt
    idx = (idx + 1) % steps.size
    if (idx == 0) {
      basePow *= base
    }
    nextInt = steps(idx) * basePow
    n
  }
}

object RoundNumbers {
  def apply(steps: Seq[Int], limit: Long, base: Int = 10): Seq[Long] = {
    var idx = 0
    var basePow = 1
    var next = steps(idx)
    val ret = ArrayBuffer[Long](0)
    while (next < limit) {
      ret.append(next)
      idx = (idx + 1) % steps.size
      if (idx == 0) {
        basePow *= base
      }
      next = steps(idx) * basePow
    }
    ret
  }
}
