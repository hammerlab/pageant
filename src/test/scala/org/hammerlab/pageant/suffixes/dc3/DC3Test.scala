package org.hammerlab.pageant.suffixes.dc3

import org.hammerlab.pageant.suffixes.base.SuffixArrayLocalTestBase

class DC3Test extends SuffixArrayLocalTestBase {
  override def arr(a: Array[Int], n: Int): Array[Int] = DC3.make(a, n)
}
