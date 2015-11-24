package org.hammerlab.pageant.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object JavaSerialization {
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

}
