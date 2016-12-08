package org.hammerlab.pageant.coverage.one

case class Counts(on: Count, off: Count) {
  def +(o: Counts): Counts = Counts(on + o.on, off + o.off)
  @transient lazy val all: Count = on + off
}

object Counts {
  def apply(key: Key): Counts = Counts(
    Count(key.numLociOn * key.depth, key.numLociOn),
    Count(key.numLociOff * key.depth, key.numLociOff)
  )
  val empty = Counts(Count.empty, Count.empty)
}

