package org.hammerlab.pageant

import java.io.{ByteArrayOutputStream, OutputStream, InputStream}

import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

object KryoSerialization {
  def kryoRead[T](bytes: Array[Byte])(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](bytes, includeClass = false)
  }
  def kryoRead[T](bytes: Array[Byte], includeClass: Boolean)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    val ip = new Input(bytes)
    val o = kryoRead[T](ip, includeClass)
    ip.close()
    o
  }

  def kryoRead[T](is: InputStream)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](is, includeClass = false)
  }
  def kryoRead[T](is: InputStream, includeClass: Boolean)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    val ip = new Input(is)
    val o = kryoRead[T](ip, includeClass)
    ip.close()
    o
    //    kryoRead[T](new Input(is), includeClass)
  }

  def kryoRead[T](fn: String)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](FileSystem.get(sc.hadoopConfiguration).open(new Path(fn)), includeClass = false)
  }
  def kryoRead[T](fn: String, includeClass: Boolean)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](FileSystem.get(sc.hadoopConfiguration).open(new Path(fn)), includeClass)
  }

  //  def kryoRead[T](ip: Input)(implicit ct: ClassTag[T], sc: SparkContext): T = kryoRead(ip, includeClass = false)
  def kryoRead[T](ip: Input, includeClass: Boolean)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    val ks = new KryoSerializer(sc.getConf)
    val kryo = ks.newKryo()

    val o =
      if (includeClass) {
        kryo.readClassAndObject(ip).asInstanceOf[T]
      } else {
        kryo.readObject(ip, ct.runtimeClass).asInstanceOf[T]
      }

    if (includeClass) {
      println(s"readClassAndObject: $o")
    } else {
      println(s"readObject: $o")
    }

    //ip.close()
    o
  }

  def kryoWrite(o: Object, os: OutputStream, includeClass: Boolean)(implicit sc: SparkContext): Unit = {
    val ks = new KryoSerializer(sc.getConf)
    val kryo = ks.newKryo()

    val op = new Output(os)
    if (includeClass) {
      println(s"writeClassAndObject: $o")
      kryo.writeClassAndObject(op, o)
    } else {
      println(s"writeObject: $o")
      kryo.writeObject(op, o)
    }
    op.close()
  }

  def kryoWrite(o: Object, fn: String)(implicit sc: SparkContext): Unit = kryoWrite(o, fn, includeClass = false)
  def kryoWrite(o: Object, fn: String, includeClass: Boolean)(implicit sc: SparkContext): Unit = {
    kryoWrite(o, FileSystem.get(sc.hadoopConfiguration).create(new Path(fn)), includeClass)
  }

  def kryoBytes(o: Object)(implicit sc: SparkContext): Array[Byte] = kryoBytes(o, includeClass = false)
  def kryoBytes(o: Object, includeClass: Boolean)(implicit sc: SparkContext): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    kryoWrite(o, baos, includeClass)
    baos.toByteArray
  }
}
