package org.hammerlab.pageant.coverage

case class Counts(on: Count, off: Count) {
  def +(o: Counts): Counts = Counts(on + o.on, off + o.off)
  @transient lazy val all: Count = on + off
}

object Counts {
  def apply(fk: FK): Counts = Counts(
    Count(fk.on * fk.d1, fk.on * fk.d2, fk.on),
    Count(fk.off * fk.d1, fk.off * fk.d2, fk.off)
  )
  val empty = Counts(Count.empty, Count.empty)
}

