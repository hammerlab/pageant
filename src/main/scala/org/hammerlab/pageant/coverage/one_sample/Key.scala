package org.hammerlab.pageant.coverage.one_sample

import org.hammerlab.pageant.coverage
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import spire.algebra.Monoid

abstract class Key[C: Monoid]
  extends coverage.Key[C, Depth]
