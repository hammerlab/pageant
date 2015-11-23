package org.hammerlab.pageant

import java.io.{File, FilenameFilter}
import java.nio.file.Files

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.PrefixFileFilter
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers
import SerializableRDD._

import scala.reflect.ClassTag

trait KryoSerializerTest {
  self: SparkFunSuite =>

  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )
}

trait KryoObjectSerializerTest {
  self: SparkFunSuite =>

  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoObjectSerializer"
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

    //    deserializeRDD(tmpFile).collect() should be(l.toArray)
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

  def testShorts(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd shorts") {
      serializeListAsRDD(
        "shorts",
        (1 to 200).map(_ + 500),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testLongs(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd longs") {
      serializeListAsRDD(
        "longs",
        (1 to 200).map(_ + 12345678L),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testFewFoos(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("few foos") {
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
    sparkTest("many foos") {
      serializeListAsRDD(
        "foos",
        Foos(10000, 20),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }
}

class SequenceFileRDDTest extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToSequenceFile(path)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromSequenceFile[T](path)
}

class JavaSequenceFileRDDTest extends SequenceFileRDDTest {
  testInts(4785, 4785, 4785, 4785)
  testShorts(4785, 4785, 4785, 4785)
  testLongs(4835, 4835, 4835, 4835)
  testFewFoos(197, 197, 197, 299)
  testManyFoos(287855, 287855, 287855, 287855)
}

class KryoSequenceFileRDDTest extends SequenceFileRDDTest with KryoSerializerTest {
  testInts(795, 832, 845, 845)
  testShorts(845, 845, 845, 845)
  testLongs(945, 945, 945, 945)
  testFewFoos(146, 146, 146, 197)
  testManyFoos(159092, 159155, 159155, 160964)
}

class KryoObjectSequenceFileRDDTest extends SequenceFileRDDTest with KryoObjectSerializerTest {
  testInts(795, 832, 845, 845)
  testShorts(845, 845, 845, 845)
  testLongs(945, 945, 945, 945)
  testFewFoos(146, 146, 146, 197)
  testManyFoos(159092, 159155, 159155, 160964)
}

class DirectFileRDDTest extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToDirectFile(path)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromDirectFile[T](path)
}

class JavaDirectFileRDDTest extends DirectFileRDDTest {
  testInts(571, 571, 571, 571)
  testShorts(571, 571, 571, 571)
  testLongs(768, 768, 768, 768)
  testFewFoos(90, 90, 90, 111)
  testManyFoos(84154, 84154, 84154, 84154)
}

class KryoDirectFileRDDTest extends DirectFileRDDTest with KryoSerializerTest {
  testInts(100, 137, 150, 150)
  testShorts(150, 150, 150, 150)
  testLongs(250, 250, 250, 250)
  testFewFoos(39, 39, 39, 78)
  testManyFoos(127437, 127500, 127500, 129309)
}

class KryoObjectDirectFileRDDTest extends DirectFileRDDTest with KryoObjectSerializerTest {
  testInts(100, 137, 150, 150)
  testShorts(150, 150, 150, 150)
  testLongs(250, 250, 250, 250)
  testFewFoos(39, 39, 39, 78)
  testManyFoos(127437, 127500, 127500, 129309)
}

