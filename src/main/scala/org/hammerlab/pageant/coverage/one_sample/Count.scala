package org.hammerlab.pageant.coverage.one_sample

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.utils.Monoid

case class Count(bp: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp + o.bp, n + o.n)
}

object Count {
  val empty = Count(0, NumLoci(0))

  implicit val monoid =
    new Monoid[Count] {
      override def id: Count = empty
      override def op(x: Count, y: Count): Count = x + y
    }
}
