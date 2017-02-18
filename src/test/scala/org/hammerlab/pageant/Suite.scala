package org.hammerlab.pageant

import org.hammerlab.pageant.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSparkSuite

abstract class Suite
  extends KryoSparkSuite(classOf[Registrar])

