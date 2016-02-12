package org.hammerlab.pageant.fm.utils

import org.apache.spark.SparkContext
import org.hammerlab.pageant.fm.index.SparkFM
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
