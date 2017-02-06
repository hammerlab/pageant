package org.hammerlab.pageant.coverage.one_sample

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.Key
import org.hammerlab.pageant.histogram.JointHistogram.JointHistKey

abstract class IsKey[K <: Key[_, _]]
  extends Serializable {
  def make(kv: (JointHistKey, NumLoci)): K
}
