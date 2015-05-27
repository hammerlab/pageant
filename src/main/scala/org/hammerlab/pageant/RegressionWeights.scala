package org.hammerlab.pageant

import org.hammerlab.pageant.avro.{
  LinearRegressionWeights => RW
}

case class LinearRegressionWeights(slope: Double, intercept: Double, mse: Double) {
  lazy val h = RW.newBuilder().setSlope(slope).setIntercept(intercept).setMse(mse).build()
}
