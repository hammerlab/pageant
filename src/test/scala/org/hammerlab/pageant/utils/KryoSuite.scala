package org.hammerlab.pageant.utils

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoSerializer

trait KryoSuite {
  self: SparkSuite =>

  var ks: KryoSerializer = _
  implicit var kryo: Kryo = _

  inits.append((sc) => {
    ks = new KryoSerializer(sc.getConf)
    kryo = ks.newKryo()
  })
}

trait NoKryoReferenceTracking {
  self: SparkSuite =>
  props +:= "spark.kryo.referenceTracking" -> "false"
}

trait KryoRegistrationRequired {
  self: SparkSuite =>
  props +:= "spark.kryo.registrationRequired" -> "true"
}

