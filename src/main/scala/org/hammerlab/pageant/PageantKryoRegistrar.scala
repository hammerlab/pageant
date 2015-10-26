package org.hammerlab.pageant

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class PageantKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Bases], new BasesSerializer)
  }
}
