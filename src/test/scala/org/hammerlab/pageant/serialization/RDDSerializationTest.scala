package org.hammerlab.pageant.serialization

import java.io.{File, FilenameFilter}
import java.nio.file.Files

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.PrefixFileFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DirectFileRDDSerializer
import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers

import SequenceFileSerializableRDD._
import DirectFileRDDSerializer._

import scala.reflect.ClassTag

trait KryoSerializerTest {
  self: SparkFunSuite =>
  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )
}

trait KryoFooRegistrarTest {
  self: SparkFunSuite =>
  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.hammerlab.pageant.serialization.FooKryoRegistrator"
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
    serializeRDD[T](rdd, tmpFile)

    val filter: FilenameFilter = new PrefixFileFilter("part-")
    new File(tmpFile).listFiles(filter).map(f => {
      FilenameUtils.getBaseName(f.getAbsolutePath) -> f.length
    }).toMap should be(files)

    deserializeRDD[T](tmpFile).collect() should be(l.toArray)
  }

  def testSmallInts(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd small ints") {
      serializeListAsRDD(
        "small-ints",
        1 to 400,
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testMediumInts(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd medium ints") {
      serializeListAsRDD(
        "medium-ints",
        (1 to 400).map(_ + 500),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testLongs(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("rdd longs") {
      serializeListAsRDD(
        "longs",
        (1 to 400).map(_ + 12345678L),
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

  def testSomeFoos(n: Int, p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest(s"some foos $n") {
      serializeListAsRDD(
        "foos",
        Foos(4*n, 20),
        4,
        Map("part-00000" -> p0, "part-00001" -> p1, "part-00002" -> p2, "part-00003" -> p3)
      )
    }
  }

  def testManyFoos(p0: Int, p1: Int, p2: Int, p3: Int): Unit = {
    sparkTest("many foos") {
      serializeListAsRDD(
        "foos",
        Foos(40000, 20),
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
    testSmallInts(9475, 9475, 9475, 9475)
  testMediumInts(9475, 9475, 9475, 9475)
  testLongs(9575, 9575, 9575, 9575)

  testSomeFoos(1, 223, 223, 223, 223)
  testSomeFoos(10, 1375, 1375, 1375, 1375)
  testSomeFoos(100, 13015, 13015, 13015, 13015)
}

class KryoSequenceFileRDDTest extends SequenceFileRDDTest with KryoSerializerTest {
  testSmallInts(1532, 1595, 1595, 1595)
  testMediumInts(1595, 1595, 1595, 1595)
  testLongs(1795, 1795, 1795, 1795)

  testSomeFoos(1, 171, 171, 171, 171)
  testSomeFoos(10, 855, 855, 855, 855)
  testSomeFoos(100, 7792, 7855, 7855, 7855)
}

class KryoSequenceFileFooRDDTest extends SequenceFileRDDTest with KryoFooRegistrarTest {
  testSmallInts(1532, 1595, 1595, 1595)
  testMediumInts(1595, 1595, 1595, 1595)
  testLongs(1795, 1795, 1795, 1795)

  testSomeFoos(1, 131, 131, 131, 131)
  testSomeFoos(10, 455, 455, 455, 455)
  testSomeFoos(100, 3752, 3815, 3815, 3815)
}

class DirectFileRDDTest(withClasses: Boolean = false) extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToDirectFile(path, withClasses)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromDirectFile[T](path, withClasses)
}

class JavaDirectFileRDDTest extends DirectFileRDDTest {
  testSmallInts(1072, 1072, 1072, 1072)
  testMediumInts(1072, 1072, 1072, 1072)
  testLongs(1469, 1469, 1469, 1469)

  testSomeFoos(1, 116, 116, 116, 116)
  testSomeFoos(10, 413, 413, 413, 413)
  testSomeFoos(100, 3384, 3384, 3384, 3384)
}

class KryoDirectFileRDDTest extends DirectFileRDDTest with KryoSerializerTest {
  testSmallInts(137, 200, 200, 200)
  testMediumInts(200, 200, 200, 200)
  testLongs(400, 400, 400, 400)

  testSomeFoos(1, 23, 23, 23, 23)
  testSomeFoos(10, 230, 230, 230, 230)
  testSomeFoos(100, 2337, 2400, 2400, 2400)
}

class KryoDirectFileWithClassesRDDTest extends DirectFileRDDTest(true) with KryoSerializerTest {
  testSmallInts(237, 300, 300, 300)
  testMediumInts(300, 300, 300, 300)
  testLongs(500, 500, 500, 500)

  testSomeFoos(1, 64, 64, 64, 64)
  testSomeFoos(10, 640, 640, 640, 640)
  testSomeFoos(100, 6437, 6500, 6500, 6500)
}

class KryoDirectFileWithClassesAndFooRDDTest extends DirectFileRDDTest(true) with KryoFooRegistrarTest {
  testSmallInts(237, 300, 300, 300)
  testMediumInts(300, 300, 300, 300)
  testLongs(500, 500, 500, 500)

  testSomeFoos(1, 24, 24, 24, 24)
  testSomeFoos(10, 240, 240, 240, 240)
  testSomeFoos(100, 2437, 2500, 2500, 2500)
}
