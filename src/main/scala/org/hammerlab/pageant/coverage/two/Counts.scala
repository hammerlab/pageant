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

  def apply(fk: Key): Counts =
    Counts(
      on = Count(fk.numLociOn * fk.depth1, fk.numLociOn * fk.depth2, fk.numLociOn),
      off = Count(fk.numLociOff * fk.depth1, fk.numLociOff * fk.depth2, fk.numLociOff)
    )
}

