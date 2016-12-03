package org.hammerlab.pageant.coverage.two

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP

case class Count(bp1: NumBP, bp2: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp1 + o.bp1, bp2 + o.bp2, n + o.n)
}

object Count {
  val empty = Count(0, 0, 0)
}

