package org.hammerlab.pageant.utils

trait PageantSuite
  extends SparkSuite
    with KryoSuite
    with PageantRegistrar
    with KryoRegistrationRequired
    with KryoNoReferenceTracking
