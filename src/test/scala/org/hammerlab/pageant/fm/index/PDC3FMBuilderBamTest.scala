package org.hammerlab.pageant.fm.index

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.utils.Utils
import org.hammerlab.pageant.fm.utils.Utils.T

trait PDC3FMBuilderBamTest extends FMBamTest {
  def name = "pdc3"
  override def generateFM(sc: SparkContext) = {
    val count = total

    // Written once upon a time by PDC3Test
    val ts: RDD[T] = sc.textFile(s"src/test/resources/$num.ts.ints").map(_.toByte)
    ts.getNumPartitions should be(numPartitions)
    ts.count should be(count)

    val sa: RDD[Long] = sc.textFile(s"src/test/resources/$num.sa.ints").map(_.toLong)
    sa.getNumPartitions should be(numPartitions)
    sa.count should be(count)

    PDC3FMBuilder.withSA(sa, ts, count, Utils.N)
  }
}

class PDC3FMBamTest1000 extends PDC3FMBuilderBamTest with ThousandReadTest
class PDC3FMBamTest100 extends PDC3FMBuilderBamTest with HundredReadTest
class PDC3FMBamTest10 extends PDC3FMBuilderBamTest with TenReadTest

