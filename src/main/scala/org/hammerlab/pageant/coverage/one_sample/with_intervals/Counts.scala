package org.hammerlab.pageant.coverage.one_sample.with_intervals

import org.hammerlab.pageant.coverage.one_sample.Count
import spire.algebra.Monoid

case class Counts(on: Count, off: Count) {
  def +(o: Counts): Counts = Counts(on + o.on, off + o.off)
  @transient lazy val all: Count = on + off
}

object Counts {
  val empty = Counts(Count.empty, Count.empty)

  implicit val monoid =
    new Monoid[Counts]
      with Serializable {
      override def id: Counts = empty
      override def op(x: Counts, y: Counts): Counts = x + y
    }
}

