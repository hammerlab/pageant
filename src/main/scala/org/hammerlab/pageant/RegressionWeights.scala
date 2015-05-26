package org.hammerlab.pageant

import org.hammerlab.pageant.avro.{
  RegressionWeights => RW
}

case class RegressionWeights(slope: Double, intercept: Double, error: Double) {
  lazy val h = RW.newBuilder().setSlope(slope).setIntercept(intercept).setErr(error).build()
}
