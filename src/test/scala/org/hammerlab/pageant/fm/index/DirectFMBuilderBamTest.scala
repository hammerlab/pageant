package org.hammerlab.pageant.fm.index

import org.apache.spark.SparkContext
import org.hammerlab.pageant.fm.utils.Utils

abstract class DirectFMBuilderBamTest(blocksPerPartition: Int) extends FMBamTest {
  def name = "direct"
  override def generateFM(sc: SparkContext) = {
    val bases =
      sc.parallelize(
        sc
        .textFile(s"src/test/resources/1000.reads")
        .take(num)
        .map(_.map(Utils.toI).toVector :+ 0.toByte),
        numPartitions
      )

    DirectFMBuilder(bases, blockSize = blockSize, blocksPerPartition = blocksPerPartition)
  }
}

class DirectFMBuilderBamTest1000 extends DirectFMBuilderBamTest(102) with ThousandReadTest
class DirectFMBuilderBamTest100 extends DirectFMBuilderBamTest(11) with HundredReadTest
class DirectFMBuilderBamTest10 extends DirectFMBuilderBamTest(3) with TenReadTest
