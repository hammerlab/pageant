package org.hammerlab.pageant.coverage

import org.hammerlab.pageant.coverage.Count.{NumBP, NumLoci}

case class Count(bp1: NumBP, bp2: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp1 + o.bp1, bp2 + o.bp2, n + o.n)
}

object Count {
  type NumBP = Long
  type NumLoci = Long
  val empty = Count(0, 0, 0)
}

