package org.hammerlab.pageant

import java.io.{FilenameFilter, File}
import java.nio.file.Files

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.PrefixFileFilter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.SparkFunSuite
import Serialization._
import org.scalatest.Matchers
import CheckpointRDD._

import scala.collection.immutable.StringOps
import scala.reflect.ClassTag

case class Foo(n: Int, s: String)

object Foos {
  def apply(n: Int, k: Int = 8): Seq[Foo] = {
    (1 to n).map(i => {
      val ch = ((i%26) + 96).toChar
      Foo(i, new StringOps(ch.toString) * k)
    })
  }
}

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
}

trait KryoSerializerTest {
  self: SparkFunSuite =>

  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )
}

trait SerdeRDDTest extends SparkFunSuite with Matchers {

  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String)
  def deserializeRDD[T: ClassTag](path: String): RDD[T]

  def serializeListAsRDD[T](name: String,
                        l: Seq[T],
                        numPartitions: Int = 4,
                        files: Map[String, Int])(implicit ct: ClassTag[T]): Unit = {
    val tmpFile = Files.createTempDirectory("").toAbsolutePath.toString + "/" + name
    val rdd = sc.parallelize(l, numPartitions)
    serializeRDD(rdd, tmpFile)

    val filter: FilenameFilter = new PrefixFileFilter("part-")
    new File(tmpFile).listFiles(filter).map(f => {
      FilenameUtils.getBaseName(f.getAbsolutePath) -> f.length
    }).toMap should be(files)

    deserializeRDD(tmpFile).collect() should be(l.toArray)
  }

  def testInts(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd ints") {
      serializeListAsRDD(
        "ints",
        1 to 200,
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testFewFoos(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd foos") {
      serializeListAsRDD(
        "foos",
        List(
          Foo(111, "aaaaaaaa"),
          Foo(222, "bbbbbbbb"),
          Foo(333, "cccccccc"),
          Foo(444, "dddddddd"),
          Foo(555, "eeeeeeee")
        ),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testManyFoos(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("more foos") {
      serializeListAsRDD(
        "foos",
        Foos(10000, 20),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }
}

class CheckpointRDDTest extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToFile(path)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromFile[T](path)
}

class KryoCheckpointRDDTest extends CheckpointRDDTest with KryoSerializerTest {
  testInts(795, 832, 845, 845)
  testFewFoos(146, 146, 146, 197)
  testManyFoos(159092, 159155, 159155, 160964)
}

class JavaCheckpointRDDTest extends CheckpointRDDTest {
  testInts(4785, 4785, 4785, 4785)
  testFewFoos(197, 197, 197, 299)
  testManyFoos(287855, 287855, 287855, 287855)
}

class SerializedRDDTest extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToFileDirectly(path)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromDirectFile[T](path)
}

class JavaSerializedRDDTest extends SerializedRDDTest {
  testInts(571, 571, 571, 571)
  testFewFoos(90, 90, 90, 111)
  testManyFoos(84154, 84154, 84154, 84154)
}

class KryoSerializedRDDTest extends SerializedRDDTest with KryoSerializerTest {
  testInts(100, 137, 150, 150)
  testFewFoos(39, 39, 39, 78)
  testManyFoos(127437, 127500, 127500, 129309)
}

