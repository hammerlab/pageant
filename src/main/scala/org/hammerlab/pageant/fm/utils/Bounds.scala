package org.hammerlab.pageant.fm.utils

case class Bounds(lo: LoBound, hi: HiBound) {
  override def toString: String = s"Bounds(${lo.v}, ${hi.v})"
  def toTuple: (Long, Long) = (lo.v, hi.v)
  def count: Long = hi.v - lo.v
}
object Bounds {
  def apply(lo: Long, hi: Long): Bounds = Bounds(LoBound(lo), HiBound(hi))
  def merge(bounds: Iterable[Bound]): Bounds = {
    bounds.toArray match {
      case Array(l: LoBound, h: HiBound) => Bounds(l, h)
      case Array(h: HiBound, l: LoBound) => Bounds(l, h)
      case _ =>
        throw new Exception(s"Bad bounds: ${bounds.mkString(",")}")
    }
  }
}

