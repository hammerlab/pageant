package org.hammerlab.pageant.coverage.one

case class Counts(on: Count, off: Count) {
  def +(o: Counts): Counts = Counts(on + o.on, off + o.off)
  @transient lazy val all: Count = on + off
}

object Counts {
  def apply(fk: Key): Counts = Counts(
    Count(fk.on * fk.d, fk.on),
    Count(fk.off * fk.d, fk.off)
  )
  val empty = Counts(Count.empty, Count.empty)
}

