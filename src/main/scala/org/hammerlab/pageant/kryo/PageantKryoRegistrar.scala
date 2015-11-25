package org.hammerlab.pageant.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.pageant.reads.{BasesSerializer, Bases}

class PageantKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Bases], new BasesSerializer)
  }
}
