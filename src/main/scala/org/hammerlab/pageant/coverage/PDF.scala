package org.hammerlab.pageant.coverage

import spire.algebra.Monoid

abstract class PDF[T: Monoid]
  extends Serializable {
  def cdf: CDF[T]
}

class CDF[T] extends Serializable
