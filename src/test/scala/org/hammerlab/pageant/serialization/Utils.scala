package org.hammerlab.pageant.serialization

import java.io.{File, FilenameFilter}
import java.nio.file.Files

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.PrefixFileFilter
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers

import SequenceFileSerializableRDD._
import org.apache.spark.serializer.DirectFileRDDSerializer._

import scala.reflect.ClassTag

trait Utils extends SparkFunSuite with Matchers {

  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String)
  def deserializeRDD[T: ClassTag](path: String): RDD[T]

  def verifyFileSizesAndSerde[T](name: String,
                                 l: Seq[T],
                                 fileSizes: Int*)(implicit ct: ClassTag[T]): Unit = {
    verifyFileSizeListAndSerde[T](name, l, fileSizes)(ct)
  }

  def verifyFileSizeListAndSerde[T](name: String,
                                    l: Seq[T],
                                    origFileSizes: Seq[Int])(implicit ct: ClassTag[T]): Unit = {
    val fileSizes: Seq[Int] =
      if (origFileSizes.size == 1)
        Array.fill(4)(origFileSizes(0))
      else
        origFileSizes

    val fileSizeMap = fileSizes.zipWithIndex.map(p => "part-000%02d".format(p._2) -> p._1).toMap

    val tmpFile = Files.createTempDirectory("").toAbsolutePath.toString + "/" + name
    val rdd = sc.parallelize(l, fileSizes.size)

    serializeRDD[T](rdd, tmpFile)

    val filter: FilenameFilter = new PrefixFileFilter("part-")
    new File(tmpFile).listFiles(filter).map(f => {
      FilenameUtils.getBaseName(f.getAbsolutePath) -> f.length
    }).toMap should be(fileSizeMap)

    deserializeRDD[T](tmpFile).collect() should be(l.toArray)
  }
}

class SequenceFileRDDTest extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToSequenceFile(path)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromSequenceFile[T](path)
}

class DirectFileRDDTest(withClasses: Boolean = false) extends SerdeRDDTest {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String) = rdd.serializeToDirectFile(path, withClasses)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromDirectFile[T](path, withClasses)
}

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
