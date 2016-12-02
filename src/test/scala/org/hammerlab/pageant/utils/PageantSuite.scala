package org.hammerlab.pageant.utils

import org.hammerlab.pageant.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSerializerSuite

class PageantSuite
  extends KryoSerializerSuite(classOf[Registrar])

