package org.hammerlab.pageant.utils

import org.hammerlab.pageant.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSparkSuite

class PageantSuite
  extends KryoSparkSuite(classOf[Registrar])

