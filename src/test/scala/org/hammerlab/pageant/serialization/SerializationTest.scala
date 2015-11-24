package org.hammerlab.pageant.serialization

import java.io.ByteArrayInputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.utils.misc.SparkFunSuite
import org.hammerlab.pageant.utils.Utils._
import org.scalatest.Matchers

import JavaSerialization._
import KryoSerialization._

import scala.collection.immutable.StringOps

class SerializationTest extends SparkFunSuite with Matchers {
  val l = List("aaaaaaaa", "bbbbbbbb", "cccccccc")

  sparkTest("java list") {
    implicit val isc = sc
    val bytes = javaBytes(l)
    bytes.size should be(263)
    javaRead[List[String]](bytes) should be(l)
  }

  sparkTest("kryo list") {
    implicit val isc = sc
    val bytes = kryoBytes(l)
    bytes.size should be(32)
    kryoRead[List[String]](bytes) should be(l)
  }

  sparkTest("java foo") {
    implicit val isc = sc
    val foo = Foo(187, "dddddddd")
    val bytes = javaBytes(foo)
    bytes.size should be(90)
    javaRead[Foo](bytes) should be(foo)

  }

  sparkTest("kryo foo") {
    implicit val isc = sc
    val foo = Foo(187, "dddddddd")
    val bytes = kryoBytes(foo)
    bytes.size should be(12)
    kryoRead[Foo](bytes) should be(foo)
  }

  sparkTest("kryo foo class") {
    implicit val isc = sc
//    val foo = Foo(187, "dddddddd")
//    val bytes = kryoBytes(foo, includeClass = true)
    val b1 = kryoBytes(Foo(127, "dddddddd"), includeClass = true)
    bs(b1)
//    b1.size should be(38)
    val b2 = kryoBytes(Foo(128, "dddddddd"), includeClass = true)
    bs(b2)
//    b2.size should be(39)
    val b3 = kryoBytes(Foo(129, "dddddddddddddddd"), includeClass = true)
    bs(b3)
//    b3.size should be(46)
    val b4 = kryoBytes(Foo(130, "dddddddddddddddd"), includeClass = true)
    bs(b4)
//    b4.size should be(47)
//    bytes.size should be(13)
//    kryoRead[Foo](bytes) should be(foo)
  }

  def bs(bytes: Array[Byte]): Unit = {
    println(bytes.map(byteToHex).mkString(" "))
  }

  sparkTest("kryo 1 string") {
    implicit val isc = sc
    val bytes = kryoBytes("aaaaaaaa")
    bytes.size should be(9)
    println(bytes.map(byteToHex).mkString(" "))
    val bais = new ByteArrayInputStream(bytes)
    kryoRead[String](bais) should be("aaaaaaaa")
  }

  sparkTest("kryo class and 1 string") {
    implicit val isc = sc
    val bytes = kryoBytes("aaaaaaaa", includeClass = true)
    bytes.size should be(10)
    println(bytes.map(byteToHex).mkString(" "))
    val bais = new ByteArrayInputStream(bytes)
    kryoRead[String](bais, includeClass = true) should be("aaaaaaaa")
  }

  sparkTest("kryo strings") {
    implicit val isc = sc
    val bytes = kryoBytes("aaaaaaaa") ++ kryoBytes("bbbbbbbb")
    bytes.size should be(18)
    println(bytes.map(byteToHex).mkString(" "))
    val ip = new Input(bytes)
    //val bais = new ByteArrayInputStream(bytes)
    kryoRead[String](ip, includeClass = false) should be("aaaaaaaa")
    kryoRead[String](ip, includeClass = false) should be("bbbbbbbb")
    ip.close()
  }

  sparkTest("kryo class and strings") {
    implicit val isc = sc
    val bytes = kryoBytes("aaaaaaaa", includeClass = true) ++ kryoBytes("bbbbbbbb", includeClass = true)
    bytes.size should be(20)
    println(bytes.map(byteToHex).mkString(" "))
//    val bais = new ByteArrayInputStream(bytes)
    val ip = new Input(bytes)
    kryoRead[String](ip, includeClass = true) should be("aaaaaaaa")
    kryoRead[String](ip, includeClass = true) should be("bbbbbbbb")
    ip.close()
  }
}

