package org.hammerlab.pageant.bases

import org.scalatest.{FunSuite, Matchers}

class BasesTest
  extends FunSuite
    with Matchers {

  test("reverse-complement, equality") {
    Bases("C").toString() should be("C")
    val s = "CATGCCAATTGGCATCATCAT"
    val b1 = Bases(s)
    b1.toString() should be(s)
    b1.rc.toString() should be("ATGATGATGCCAATTGGCATG")

    val b2 = Bases(s)

    b1 should equal(b2)

    b1.drop(1)
  }
}
