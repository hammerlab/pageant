package org.hammerlab.pageant.fm.utils

object Utils {
  val toI: Map[Char, Byte] = "$ACGTN".zipWithIndex.toMap.map(p => (p._1, p._2.toByte))
  val toC = toI.map(_.swap)

  val N = 6

  def rc(s: String): String = s.map(c => "$TGCAN"(toI(c))).reverse

  def last[T](it: Iterator[T]): T = {
    var l: T = it.next()
    while (it.hasNext) l = it.next()
    l
  }

  def lastOption[T](it: Iterator[T]): Option[T] = {
    if (it.isEmpty) None
    else {
      var l = it.next()
      while (it.hasNext) l = it.next()
      Some(l)
    }
  }

  type T = Byte
  type AT = Array[T]
  type ST = Seq[T]
  type VT = Vector[T]
  type V = Long
  type Idx = Long
  type TPos = Int
  type BlockIdx = Long
  type PartitionIdx = Int
}
