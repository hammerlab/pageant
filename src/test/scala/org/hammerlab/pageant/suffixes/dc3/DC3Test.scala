package org.hammerlab.pageant.suffixes.dc3

import org.hammerlab.pageant.suffixes.base.SuffixArrayTest

class DC3Test extends SuffixArrayTest {
  override def arr(a: Array[Int], n: Int): Array[Int] = DC3.make(a, n)
}
