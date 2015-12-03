package org.bdgenomics.adam.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.converters.FastaConverter

class AdamWorkAroundKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[FastaConverter.FastaDescriptionLine])
  }
}
