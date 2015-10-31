package org.hammerlab.pageant

import java.io.{ByteArrayInputStream, InputStream, OutputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.{Utils => SparkUtils}

import scala.reflect.ClassTag

object Serialization {

  def javaRead[T](bytes: Array[Byte])(implicit sc: SparkContext): T = {
    javaRead(new ByteArrayInputStream(bytes))
  }
  def javaRead[T](is: InputStream)(implicit sc: SparkContext): T = {
    val ois = new ObjectInputStream(is)
    val o = ois.readObject().asInstanceOf[T]
    ois.close()
    o
  }

  def javaRead[T](fn: String)(implicit sc: SparkContext): T = {
    javaRead(FileSystem.get(sc.hadoopConfiguration).open(new Path(fn)))
  }

  def javaWrite(o: Object, fn: String)(implicit sc: SparkContext): Unit = {
    javaWrite(o, FileSystem.get(sc.hadoopConfiguration).create(new Path(fn)))
  }

  def javaWrite(o: Object, os: OutputStream)(implicit sc: SparkContext): Unit = {
    val oos = new ObjectOutputStream(os)
    oos.writeObject(o)
    oos.close()
  }

  def javaBytes(o: Object)(implicit sc: SparkContext): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    javaWrite(o, baos)
    baos.toByteArray
  }

  def kryoRead[T](bytes: Array[Byte])(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](new Input(bytes))
  }
  def kryoRead[T](is: InputStream)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](new Input(is))
  }

  def kryoRead[T](fn: String)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](FileSystem.get(sc.hadoopConfiguration).open(new Path(fn)))
  }

//  def kryoRead[T](bytes: Array[Byte])(implicit ct: ClassTag[T], sc: SparkContext): T = {
//    kryoRead[T](new Input(bytes))
//  }

  def kryoRead[T](ip: Input)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    val ks = new KryoSerializer(sc.getConf)
    val kryo = ks.newKryo()

    val o = kryo.readObject(ip, ct.runtimeClass).asInstanceOf[T]
    ip.close()
    o
  }

  def kryoWrite(o: Object, os: OutputStream)(implicit sc: SparkContext): Unit = {
    val ks = new KryoSerializer(sc.getConf)
    val kryo = ks.newKryo()

    val op = new Output(os)
    kryo.writeObject(op, o)
    op.close()
  }

  def kryoWrite(o: Object, fn: String)(implicit sc: SparkContext): Unit = {
    kryoWrite(o, FileSystem.get(sc.hadoopConfiguration).create(new Path(fn)))
  }

  def kryoBytes(o: Object)(implicit sc: SparkContext): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    kryoWrite(o, baos)
    baos.toByteArray
  }

}

class CheckpointRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def serializeToFileDirectly(path: String): RDD[T] = {
    rdd.mapPartitionsWithIndex((idx, iter) => {
      val serializer = SparkEnv.get.serializer.newInstance()
      val filename = path + "/part-%05d".format(idx)

      val fs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf))
      val os = fs.create(new Path(filename))
      val ss = serializer.serializeStream(os)
      ss.writeAll(iter)
      ss.close()
      iter
    })
  }

  def serializeToFile(path: String): RDD[T] = {
    rdd.mapPartitions(iter => {
      val serializer = SparkEnv.get.serializer.newInstance()

      iter.map(x =>
        (
          NullWritable.get(),
          new BytesWritable(serializer.serialize(x).array())
        )
      )
    }).saveAsSequenceFile(path)

    rdd
  }
}

class SerdeSparkContext(val sc: SparkContext) {
  def fromFile[T](path: String)(implicit ct: ClassTag[T]): RDD[T] = {
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], 2)
      .mapPartitions[T](iter => {
      val serializer = SparkEnv.get.serializer.newInstance()
        iter.map(x => {
          serializer.deserialize(ByteBuffer.wrap(x._2.getBytes))
        })
      })
  }
}

object CheckpointRDD {
  implicit def toCheckpointRDD[T: ClassTag](rdd: RDD[T]): CheckpointRDD[T] = new CheckpointRDD(rdd)
  implicit def toSerdeSparkContext(sc: SparkContext): SerdeSparkContext = new SerdeSparkContext(sc)
}
