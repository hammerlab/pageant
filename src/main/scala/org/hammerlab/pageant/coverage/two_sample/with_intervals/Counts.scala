package org.hammerlab.pageant.coverage.two_sample.with_intervals

import org.hammerlab.pageant.coverage.two_sample.Count
import org.hammerlab.pageant.utils.Monoid

case class Counts(on: Count, off: Count) {
  @transient lazy val all: Count = on + off
}

object Counts {
  implicit val m =
    new Monoid[Counts] {
      override def id: Counts = Counts(Count.empty, Count.empty)
      override def op(x: Counts, y: Counts): Counts =
        Counts(
          x.on + y.on,
          x.off + y.off
        )
    }
}
