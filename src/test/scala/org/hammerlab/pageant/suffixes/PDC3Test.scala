package org.hammerlab.pageant.suffixes

import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.utils.SparkSuite
import org.hammerlab.pageant.utils.Utils.loadBam
import org.scalatest.{FunSuite, Matchers}

class PDC3Test extends SuffixArrayTestBase with SparkSuite {

  override def testFn(name: String)(testFun: => Unit): Unit = test(name)(testFun)

  import PDC3.{ItoT, TtoI}

  override def arr(a: Array[Int], n: Int): Array[Int] = {
    PDC3.apply(sc.parallelize(a.map(ItoT(_) + 1))).map(TtoI).collect
  }
  override def name = "PDC3Test"

  test("normal") {
    val ts = loadBam(sc, "normal.bam").repartition(4)

    val sa = PDC3.apply(ts.map(_.toLong))
    sa.count should be(102000)

    //ts.saveAsDirectFile("src/test/resources/normal.bam.ts")
    //sa.saveAsDirectFile("src/test/resources/normal.bam.sa")
  }
}

class CmpFnTest extends FunSuite with Matchers {
  import PDC3.{Joined, cmpFn, zero}

  test("basic 1-1 cmp") {
    cmpFn(
      (1, Joined(t0O = Some(2), n0O = Some(3), n1O = Some(4))),
      (4, Joined(t0O = Some(2), n0O = Some(2), n1O = Some(1)))
    ) should be > 0
  }

  test("basic 2-0 cmp") {
    val t1 = (zero + 5, Joined(t0O = Some(2), n0O = Some(1)))
    val t2 = (zero + 3, Joined(t0O = Some(1), t1O = Some(2), n0O = Some(2), n1O = Some(1)))

    cmpFn(t1, t2) should be > 0
    cmpFn(t2, t1) should be < 0
  }
}
