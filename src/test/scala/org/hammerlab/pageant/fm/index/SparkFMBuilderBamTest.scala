package org.hammerlab.pageant.fm.index

import org.apache.spark.SparkContext
import org.hammerlab.pageant.fm.utils.Utils

trait SparkFMBuilderBamTest extends FMBamTest {
  def name = "sparkfm"
  override def generateFM(sc: SparkContext) = {
    val count = total

    // Written once upon a time by PDC3Test
    val ts = sc.textFile(s"src/test/resources/$num.ts.ints").map(_.toByte)
    ts.getNumPartitions should be(numPartitions)
    ts.count should be(count)

    val sa = sc.textFile(s"src/test/resources/$num.sa.ints").map(_.toLong)
    sa.getNumPartitions should be(numPartitions)
    sa.count should be(count)

    SparkFMBuilder(sa.zipWithIndex(), ts.zipWithIndex().map(_.swap), count, Utils.N)
  }
}

class SparkFMBuilderBamTest1000 extends SparkFMBuilderBamTest with ThousandReadTest
class SparkFMBuilderBamTest100 extends SparkFMBuilderBamTest with HundredReadTest
class SparkFMBuilderBamTest10 extends SparkFMBuilderBamTest with TenReadTest

