package org.hammerlab.pageant.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.pageant.reads.{BasesSerializer, Bases}
import org.hammerlab.pageant.scratch.{CountsSerializer, Counts, BasesTupleSerializer, BasesTuple}

class PageantKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Bases], new BasesSerializer)
    kryo.register(classOf[Counts], new CountsSerializer)
    kryo.register(classOf[BasesTuple], new BasesTupleSerializer)
  }
}
