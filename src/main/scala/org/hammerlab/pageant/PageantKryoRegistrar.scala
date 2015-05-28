package org.hammerlab.pageant

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.avro.{
  PrincipalComponent,
  HistogramEntry,
  JointHistogramEntry,
  Histogram => H,
  PerContigJointHistogram => PCJH,
  JointHistogram => JH,
  LinearRegressionWeights => LRW
}

class PageantKryoRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[JointHistogramEntry]())
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[HistogramEntry]())
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[LRW]())
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[PrincipalComponent]())
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[H]())
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[PCJH]())
    kryo.register(classOf[AlignmentRecord], new AvroSerializer[JH]())
  }
}
