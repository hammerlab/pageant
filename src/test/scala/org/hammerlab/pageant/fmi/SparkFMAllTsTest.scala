package org.hammerlab.pageant.fmi

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fmi.SparkFM.{T, Idx, V}

class SparkFMAllTsTest extends SparkFMTest[TNeedle] {
  override def makeSparkFM(saZipped: RDD[(V, Idx)],
                           tZipped: RDD[(Idx, T)],
                           count: Idx,
                           N: T,
                           blockSize: T): SparkFMAllTs = {
    SparkFMAllTs(saZipped, tZipped, count, N, blockSize)
  }
}
