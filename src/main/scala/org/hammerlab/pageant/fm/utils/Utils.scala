package org.hammerlab.pageant.fm.utils

import org.hammerlab.pageant.utils.Utils.rev

object Utils {
  val toI = "$ACGTN".zipWithIndex.toMap
  val toC = toI.map(rev)

  type T = Int
  type AT = Array[T]
  type V = Long
  type Idx = Long
  type TPos = Int
  type BlockIdx = Long
  type PartitionIdx = Int
}
