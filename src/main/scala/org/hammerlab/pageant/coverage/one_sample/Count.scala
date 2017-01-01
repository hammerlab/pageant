package org.hammerlab.pageant.coverage.one_sample

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import spire.algebra.Monoid

case class Count(bp: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp + o.bp, n + o.n)
}

object Count {
  val empty = Count(0, NumLoci(0))

  implicit val monoid =
    new Monoid[Count]
      with Serializable {
      override def id: Count = empty
      override def op(x: Count, y: Count): Count = x + y
    }
}
