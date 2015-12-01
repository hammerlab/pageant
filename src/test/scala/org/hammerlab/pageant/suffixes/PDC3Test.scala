package org.hammerlab.pageant.suffixes

import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.{FunSuite, Matchers}

class PDC3Test extends SuffixArrayTestBase with SparkFunSuite {

  override def testFn(name: String)(testFun: => Unit): Unit = sparkTest(name)(testFun)

  override def arr(a: Array[Int], n: Int): Array[Int] = {
    PDC3.apply(sc.parallelize(a.map(_.toLong + 1))).map(_.toInt).collect
  }
  override def name = "PDC3Test"
}

class CmpFnTest extends FunSuite with Matchers {
  import PDC3.{cmpFn, Joined}
  test("basic 1-1 cmp") {
    cmpFn(
      (1, Joined(t0O = Some(2), n0O = Some(3), n1O = Some(4))),
      (4, Joined(t0O = Some(2), n0O = Some(2), n1O = Some(1)))
    ) should be > 0
  }

  test("basic 2-0 cmp") {
    val t1 = (5L, Joined(t0O = Some(2), n0O = Some(1)))
    val t2 = (3L, Joined(t0O = Some(1), t1O = Some(2), n0O = Some(2), n1O = Some(1)))

    cmpFn(t1, t2) should be > 0
    cmpFn(t2, t1) should be < 0
  }
}
