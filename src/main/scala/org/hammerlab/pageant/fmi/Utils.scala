package org.hammerlab.pageant.fmi

import org.hammerlab.pageant.utils.Utils.rev

object Utils {
  val toI = "$ACGTN".zipWithIndex.toMap
  val toC = toI.map(rev)
}
