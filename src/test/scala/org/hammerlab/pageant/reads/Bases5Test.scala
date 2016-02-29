package org.hammerlab.pageant.reads

import org.scalatest.{FunSuite, Matchers}

class Bases5Test extends FunSuite with Matchers {
  test("Bases") {
    val str = "CATGCCAATTNGGCANTCATCAT"
    for {
      i ← 0 until str.length
      s = str.substring(0, i)
      bases = Bases5(s)
      bases2 = Bases5(s)
    } {
      withClue(s"substring $i ($s): ") {
        bases.toString() should be(s)
        bases should equal(bases2)
      }
    }
  }
}
