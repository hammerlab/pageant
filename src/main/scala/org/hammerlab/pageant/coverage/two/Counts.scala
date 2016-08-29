package org.hammerlab.pageant.coverage.two

import spire.algebra.Monoid

case class Counts(on: Count, off: Count) {
  @transient lazy val all: Count = on + off
}

object Counts extends Monoid[Counts] {

  // "Combine" operation.
  override def op(x: Counts, y: Counts): Counts = Counts(x.on + y.on, x.off + y.off)

  // Identity.
  override def id: Counts = Counts(Count.empty, Count.empty)

  def apply(fk: FK): Counts = Counts(
    Count(fk.on * fk.d1, fk.on * fk.d2, fk.on),
    Count(fk.off * fk.d1, fk.off * fk.d2, fk.off)
  )
}

