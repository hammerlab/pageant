package org.hammerlab.pageant.fm.utils

import org.hammerlab.pageant.utils.Utils.rev

object Utils {
  val toI: Map[Char, Byte] = "$ACGTN".zipWithIndex.toMap.map(p => (p._1, p._2.toByte))
  val toC = toI.map(rev)

  val N = 6

  def rc(s: String): String = s.map(c => "$TGCAN"(toI(c))).reverse

  type T = Byte
  type AT = Array[T]
  type V = Long
  type Idx = Long
  type TPos = Int
  type BlockIdx = Long
  type PartitionIdx = Int
}
