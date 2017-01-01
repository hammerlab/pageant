package org.hammerlab.pageant.coverage.one_sample

trait Result {
  def save(dir: String,
           force: Boolean = false,
           writeFullDistributions: Boolean = false,
           writeJointHistogram: Boolean = false): Unit
}
