package org.hammerlab.pageant.fmi

import org.apache.spark.SparkContext
import org.hammerlab.pageant.utils.SparkSuite

import scala.collection.mutable.ArrayBuffer

abstract class FMSuite extends SparkSuite {

  var fmInits: ArrayBuffer[(SparkContext, SparkFM) => Unit] = ArrayBuffer()

  var fm: SparkFM = _
  def initFM(sc: SparkContext): SparkFM

  inits.append((sc) => {
    fm = initFM(sc)
    fmInits.foreach(_(sc, fm))
  })

}
