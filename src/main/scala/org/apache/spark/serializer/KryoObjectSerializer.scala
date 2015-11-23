package org.apache.spark.serializer

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import org.apache.commons.io.IOUtils
import org.hammerlab.pageant.SerializedRDD.bytesToHex

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.{Kryo, KryoException}
import org.apache.spark.{SparkConf, SparkException}

import scala.reflect.ClassTag

class KryoObjectSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newInstance(): SerializerInstance = {
    new KryoObjectSerializerInstance(this)
  }
}

class KryoObjectSerializerInstance(ks: KryoObjectSerializer) extends KryoSerializerInstance(ks) {
  // Make these lazy vals to avoid creating a buffer unless we use them.
  private lazy val output = ks.newKryoOutput()
  private lazy val input = new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      //kryo.writeObject(output, t)
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkException(s"Kryo serialization failed: ${e.getMessage}. To avoid this, " +
          "increase spark.kryoserializer.buffer.max value.")
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  def read[T](kryo: Kryo)(implicit ct: ClassTag[T]): T = {
    kryo.readClassAndObject(input).asInstanceOf[T]
//    kryo.readObject(input, ct.runtimeClass).asInstanceOf[T]
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      input.setBuffer(bytes.array)
      read(kryo)
      //kryo.readObject(input).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      input.setBuffer(bytes.array)
      read(kryo)
      //kryo.readObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoObjectSerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoObjectDeserializationStream(this, s)
  }
}

class KryoObjectSerializationStream(serInstance: KryoObjectSerializerInstance,
                                    outStream: OutputStream)
  extends KryoSerializationStream(serInstance, outStream) {

  private[this] var output: KryoOutput = new KryoOutput(outStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    //kryo.writeObject(output, t)
    kryo.writeClassAndObject(output, t)
    println(s"KOSS.write: $t: ${bytesToHex(output.toBytes)}")
    this
  }
}

class KryoObjectDeserializationStream(serInstance: KryoObjectSerializerInstance,
                                      inStream: InputStream)
  extends DeserializationStream
  /*extends KryoDeserializationStream(serInstance, inStream)*/ {

//  val bytes = IOUtils.toByteArray(inStream)
//  println(s"KODS (${bytes.size}): ${bytesToHex(bytes)}")
//  private[this] var input: KryoInput = new KryoInput(bytes)
  private[this] var input: KryoInput = new KryoInput(inStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  def read[T]()(implicit ct: ClassTag[T]): T = {
    //println(s"KODS.reading: ${ct.runtimeClass.getCanonicalName}, ${bytesToHex(input.getBuffer.slice(input.position(), bytes.size))}")
    println(s"KODS.reading: ${ct.runtimeClass.getCanonicalName}")
    kryo.readClassAndObject(input).asInstanceOf[T]
//    kryo.readObject(input, ct.runtimeClass).asInstanceOf[T]
  }

  override def readObject[T: ClassTag](): T = {
//    try {
      read()
//    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
//      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
//        throw new EOFException
//    }
  }

  override def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
      }
    }
  }

}
