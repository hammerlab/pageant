package org.hammerlab.pageant.utils

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoSerializer

trait KryoSuite extends SparkSuite {
  var ks: KryoSerializer = _
  implicit var kryo: Kryo = _

  props +:=  "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"

  inits.append((sc) => {
    ks = new KryoSerializer(sc.getConf)
    kryo = ks.newKryo()
  })
}

trait KryoNoReferenceTracking extends SparkSuite {
  props +:= "spark.kryo.referenceTracking" -> "false"
}

trait KryoRegistrationRequired extends SparkSuite {
  props +:= "spark.kryo.registrationRequired" -> "true"
}

trait PageantRegistrar extends SparkSuite {
  props +:= "spark.kryo.registrator" -> "org.hammerlab.pageant.kryo.PageantKryoRegistrar"
}

trait KryoSerdePageantRegistrar extends KryoSuite with PageantRegistrar

trait KryoSerdePageantRegistrarNoReferences extends KryoSerdePageantRegistrar with KryoNoReferenceTracking

trait JavaSerialization extends SparkSuite {
  props +:= "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer"
}
