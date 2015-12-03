package org.hammerlab.pageant.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.kryo.AdamWorkAroundKryoRegistrar
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.pageant.reads.{Bases, BasesSerializer}
import org.hammerlab.pageant.scratch.{BasesTuple, BasesTupleSerializer, Counts, CountsSerializer}
import org.hammerlab.pageant.suffixes.PDC3

import scala.collection.mutable.ArrayBuffer

class PageantKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Bases], new BasesSerializer)
    kryo.register(classOf[Counts], new CountsSerializer)
    kryo.register(classOf[BasesTuple], new BasesTupleSerializer)

    new ADAMKryoRegistrator().registerClasses(kryo)
    new AdamWorkAroundKryoRegistrar().registerClasses(kryo)

    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Int]])

    // https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(classOf[scala.reflect.ClassTag$$anon$1])
    kryo.register(classOf[java.lang.Class[_]])
    kryo.register(classOf[PDC3.Joined])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    kryo.register(classOf[ReferenceRegion])
    kryo.register(classOf[Array[ReferenceRegion]])

    kryo.register(classOf[org.bdgenomics.formats.avro.Strand])
    kryo.register(classOf[org.bdgenomics.adam.models.MultiContigNonoverlappingRegions])
    kryo.register(classOf[org.bdgenomics.adam.models.NonoverlappingRegions])

    // Added to Spark in 1.6.0; needed here for Spark < 1.6.0.
    kryo.register(classOf[Array[Tuple1[Any]]])
    kryo.register(classOf[Array[Tuple2[Any, Any]]])
    kryo.register(classOf[Array[Tuple3[Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple4[Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple5[Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple6[Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple7[Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(classOf[Array[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]]])
    kryo.register(None.getClass)
    kryo.register(Nil.getClass)
    kryo.register(classOf[ArrayBuffer[Any]])

  }
}
