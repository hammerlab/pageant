package org.hammerlab.pageant.fm.blocks

import com.esotericsoftware.kryo.io.{Input, Output}
import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.utils.VarNum
import org.hammerlab.pageant.fm.utils.Utils.N

trait BWTBlockSerializer {
  def write(output: Output, o: BWTBlock, length: Int): Unit = {
    VarNum.write(output, o.startIdx)
    for { i <- 0 until N } {
      VarNum.write(output, o.startCounts(i))
    }
    VarNum.write(output, length)
  }

  def read(input: Input): (Long, Counts, Int) = {
    val startIdx = VarNum.read(input)
    val startCounts = Array.fill(N)(0L)
    for { i <- 0 until N } {
      startCounts(i) = VarNum.read(input)
    }
    val length = VarNum.read(input).toInt
    (startIdx, startCounts, length)
  }
}
