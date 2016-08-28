package org.hammerlab.pageant.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.bdgenomics.formats.avro.AlignmentRecord

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Vector[_]])
    kryo.register(classOf[Array[Vector[_]]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofChar])
    kryo.register(classOf[Array[Char]])

    // Tuple2[Long, Any], afaict?
    // "J" == Long (obviously). https://github.com/twitter/chill/blob/6d03f6976f33f6e2e16b8e254fead1625720c281/chill-scala/src/main/scala/com/twitter/chill/TupleSerializers.scala#L861
    kryo.register(Class.forName("scala.Tuple2$mcJZ$sp"))
    kryo.register(Class.forName("scala.Tuple2$mcIZ$sp"))

    new ADAMKryoRegistrator().registerClasses(kryo)

    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Int]])

    // This seems to be necessary when serializing a RangePartitioner, which writes out a ClassTag:
    //
    //  https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/Partitioner.scala#L220
    //
    // See also:
    //
    //   https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[Array[Byte]]])

    kryo.register(classOf[Array[AlignmentRecord]])
    kryo.register(classOf[ReferenceRegion])
    kryo.register(classOf[Array[ReferenceRegion]])

    kryo.register(classOf[Array[Object]])
  }
}
