package org.hammerlab.pageant

import java.io.{FilenameFilter, File}
import java.nio.file.Files

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.PrefixFileFilter
import org.apache.spark.SparkContext
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

trait CheckpointRDDTest extends SparkFunSuite with Matchers {
  def serdeListAsRDD[T](name: String,
                        l: Seq[T],
                        numPartitions: Int = 4,
                        files: Map[String, Int])(implicit ct: ClassTag[T]): Unit = {
    val tmpFile = Files.createTempDirectory("").toAbsolutePath.toString + "/" + name
    val rdd = sc.parallelize(l, numPartitions)
    rdd.serializeToFile(tmpFile)

    val filter: FilenameFilter = new PrefixFileFilter("part-")
    new File(tmpFile).listFiles(filter).map(f => {
      FilenameUtils.getBaseName(f.getAbsolutePath) -> f.length
    }).toMap should be(files)

    sc.fromFile[T](tmpFile).collect() should be(l.toArray)
  }
}

class KryoCheckpointRDDTest extends CheckpointRDDTest {
  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )

  sparkTest("rdd ints") {
    serdeListAsRDD(
      "ints",
      1 to 200,
      4,
      Map(
        "part-00000" -> 795,
        "part-00001" -> 832,
        "part-00002" -> 845,
        "part-00003" -> 845)
    )
  }

  sparkTest("rdd foos") {
    serdeListAsRDD(
      "foos",
      List(
        Foo(111, "aaaaaaaa"),
        Foo(222, "bbbbbbbb"),
        Foo(333, "cccccccc"),
        Foo(444, "dddddddd"),
        Foo(555, "eeeeeeee")
      ),
      4,
      Map(
        "part-00000" -> 146,
        "part-00001" -> 146,
        "part-00002" -> 146,
        "part-00003" -> 197
      )
    )
  }

  sparkTest("more foos") {
    serdeListAsRDD(
      "foos",
      Foos(10000, 20),
      4,
      Map(
        "part-00000" -> 159092,
        "part-00001" -> 159155,
        "part-00002" -> 159155,
        "part-00003" -> 160964
      )
    )
  }
}

class JavaCheckpointRDDTest extends CheckpointRDDTest {
  sparkTest("rdd ints") {
    serdeListAsRDD(
      "ints",
      1 to 200,
      4,
      Map(
        "part-00000" -> 4785,
        "part-00001" -> 4785,
        "part-00002" -> 4785,
        "part-00003" -> 4785
      )
    )
  }

  sparkTest("rdd foos") {
    serdeListAsRDD(
      "foos",
      List(
        Foo(111, "aaaaaaaa"),
        Foo(222, "bbbbbbbb"),
        Foo(333, "cccccccc"),
        Foo(444, "dddddddd"),
        Foo(555, "eeeeeeee")
      ),
      4,
      Map(
        "part-00000" -> 197,
        "part-00001" -> 197,
        "part-00002" -> 197,
        "part-00003" -> 299
      )
    )
  }

  sparkTest("more foos") {
    serdeListAsRDD(
      "foos",
      Foos(10000, 20),
      4,
      Map(
        "part-00000" -> 287855,
        "part-00001" -> 287855,
        "part-00002" -> 287855,
        "part-00003" -> 287855
      )
    )
  }

}

trait SerializedRDDTest extends SparkFunSuite with Matchers {
  def serializeListAsRDD[T](name: String,
                            l: Seq[T],
                            numPartitions: Int = 4,
                            files: Map[String, Int])(implicit ct: ClassTag[T]): Unit = {
    val tmpFile = Files.createTempDirectory("").toAbsolutePath.toString + "/" + name
    val rdd = sc.parallelize(l, numPartitions)
    rdd.serializeToFileDirectly(tmpFile)

    val filter: FilenameFilter = new PrefixFileFilter("part-")
    new File(tmpFile).listFiles(filter).map(f => {
      FilenameUtils.getBaseName(f.getAbsolutePath) -> f.length
    }).toMap should be(files)

    sc.fromDirectFile[T](tmpFile).collect() should be(l.toArray)
  }
}

class JavaSerializedRDDTest extends SerializedRDDTest {
  sparkTest("rdd ints") {
    serializeListAsRDD(
      "ints",
      1 to 200,
      4,
      Map(
        "part-00000" -> 571,
        "part-00001" -> 571,
        "part-00002" -> 571,
        "part-00003" -> 571
      )
    )
  }

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
      Map(
        "part-00000" -> 90,
        "part-00001" -> 90,
        "part-00002" -> 90,
        "part-00003" -> 111
      )
    )
  }

  sparkTest("more foos") {
    serializeListAsRDD(
      "foos",
      Foos(10000, 20),
      4,
      Map(
        "part-00000" -> 84154,
        "part-00001" -> 84154,
        "part-00002" -> 84154,
        "part-00003" -> 84154
      )
    )
  }
}

class KryoSerializedRDDTest extends SerializedRDDTest {
  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )

  sparkTest("rdd ints") {
    serializeListAsRDD(
      "ints",
      1 to 200,
      4,
      Map(
        "part-00000" -> 100,
        "part-00001" -> 137,
        "part-00002" -> 150,
        "part-00003" -> 150
      )
    )
  }

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
      Map(
        "part-00000" -> 39,
        "part-00001" -> 39,
        "part-00002" -> 39,
        "part-00003" -> 78
      )
    )
  }

  sparkTest("more foos") {
    serializeListAsRDD(
      "foos",
      Foos(10000, 20),
      4,
      Map(
        "part-00000" -> 127437,
        "part-00001" -> 127500,
        "part-00002" -> 127500,
        "part-00003" -> 129309
      )
    )
  }

}

