package org.hammerlab.pageant

import org.scalatest.{Matchers, FunSuite}

class BasesTest extends FunSuite with Matchers {
  test("Bases") {
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
