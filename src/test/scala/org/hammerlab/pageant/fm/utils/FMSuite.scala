package org.hammerlab.pageant.fm.utils

import org.apache.spark.SparkContext
import org.hammerlab.pageant.fm.blocks.BWTBlock
import org.hammerlab.pageant.fm.index.FMIndex.FMI
import org.hammerlab.pageant.fm.index.{FMIndex, SparkFMBuilder}
import org.hammerlab.pageant.utils.PageantSuite

import scala.collection.mutable.ArrayBuffer

trait FMSuite extends PageantSuite {

  var fmInits: ArrayBuffer[(SparkContext, FMI) => Unit] = ArrayBuffer()

  var fm: FMIndex = _
  def initFM(sc: SparkContext): FMIndex

  inits.append((sc) => {
    fm = initFM(sc)
    fmInits.foreach(_(sc, fm))
  })

}
