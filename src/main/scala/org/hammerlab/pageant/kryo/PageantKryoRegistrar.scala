package org.hammerlab.pageant.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.kryo.AdamWorkAroundKryoRegistrar
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.pageant.fm.blocks.{BWTRun, BWTRunSerializer, FullBWTBlock, FullBWTBlockSerializer, RunLengthBWTBlock, RunLengthBWTBlockSerializer}
import org.hammerlab.pageant.fm.bwt.{NextStringPos, StringPos}
import org.hammerlab.pageant.fm.finder.{PosNeedle, TNeedle}
import org.hammerlab.pageant.fm.utils.{Bounds, BoundsMap, Counts, CountsMap, HiBound, LoBound, Pos}
import org.hammerlab.pageant.reads.{Bases, BasesSerializer}
import org.hammerlab.pageant.scratch.{BasesTuple, BasesTupleSerializer, CountsSerializer, KmerCount}
import org.hammerlab.pageant.suffixes.pdc3.{Joined, JoinedSerializer}

import scala.collection.mutable.ArrayBuffer

class PageantKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Bases], new BasesSerializer)
    kryo.register(classOf[KmerCount], new CountsSerializer)
    kryo.register(classOf[BasesTuple], new BasesTupleSerializer)
    kryo.register(classOf[BWTRun], new BWTRunSerializer)
    kryo.register(classOf[FullBWTBlock], new FullBWTBlockSerializer)
    kryo.register(classOf[RunLengthBWTBlock], new RunLengthBWTBlockSerializer)
    kryo.register(classOf[Array[BWTRun]])
    kryo.register(classOf[Counts])
    kryo.register(classOf[Array[Counts]])
    kryo.register(classOf[CountsMap])
    kryo.register(classOf[Pos])
    kryo.register(classOf[Array[Pos]])
    kryo.register(classOf[PosNeedle])
    kryo.register(classOf[TNeedle])
    kryo.register(classOf[LoBound])
    kryo.register(classOf[HiBound])
    kryo.register(classOf[Bounds])
    kryo.register(classOf[BoundsMap])
    kryo.register(classOf[StringPos])
    kryo.register(classOf[NextStringPos])
    kryo.register(classOf[Vector[_]])
    kryo.register(classOf[Array[Vector[_]]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])

    // Tuple2[Long, Any], afaict?
    // "J" == Long (obviously). https://github.com/twitter/chill/blob/6d03f6976f33f6e2e16b8e254fead1625720c281/chill-scala/src/main/scala/com/twitter/chill/TupleSerializers.scala#L861
    kryo.register(Class.forName("scala.Tuple2$mcJZ$sp"))
    kryo.register(Class.forName("scala.Tuple2$mcIZ$sp"))

    new ADAMKryoRegistrator().registerClasses(kryo)
    new AdamWorkAroundKryoRegistrar().registerClasses(kryo)

    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Int]])

    // https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(classOf[Joined], new JoinedSerializer)
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[Array[Byte]]])

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
