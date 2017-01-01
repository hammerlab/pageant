package org.hammerlab.pageant.utils

import org.hammerlab.pageant.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.scalactic.ConversionCheckedTripleEquals

class PageantSuite
  extends KryoSparkSuite(classOf[Registrar])
    with ConversionCheckedTripleEquals

