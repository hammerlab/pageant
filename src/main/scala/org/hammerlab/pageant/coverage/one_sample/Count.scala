package org.hammerlab.pageant.coverage.one_sample

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP

case class Count(bp: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp + o.bp, n + o.n)
}

object Count {
  val empty = Count(0, 0)
}
