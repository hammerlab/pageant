package org.hammerlab.pageant.serialization

import java.io.{File, FilenameFilter}

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.PrefixFileFilter
import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodec, GzipCodec, SnappyCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.serialization.SequenceFileSerializableRDD._
import org.hammerlab.pageant.utils.{KryoSuite, SparkSuite}
import org.scalatest.Matchers

import scala.reflect.ClassTag

trait Utils extends SparkSuite with Matchers {

  def serializeRDD[T: ClassTag](rdd: RDD[T], file: File): RDD[T] = serializeRDD(rdd, file.toString)
  def deserializeRDD[T: ClassTag](file: File): RDD[T] = deserializeRDD(file.toString)

  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String): RDD[T]
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
        Array.fill(4)(origFileSizes.head)
      else if (origFileSizes.size == 2)
        origFileSizes ++ Array(origFileSizes(1), origFileSizes(1))
      else
        origFileSizes

    val fileSizeMap = fileSizes.zipWithIndex.map(p => "part-%05d".format(p._2) -> p._1).toMap

    val tmp = new File(tmpDir(), name)

    val rdd = sc.parallelize(l, fileSizes.size)

    serializeRDD[T](rdd, tmp)

    val filter: FilenameFilter = new PrefixFileFilter("part-")
    tmp.listFiles(filter).map(f => {
      FilenameUtils.getBaseName(f.getAbsolutePath) -> f.length
    }).toMap should be(fileSizeMap)

    deserializeRDD[T](tmp).collect() should be(l.toArray)
  }
}

class SequenceFileRDDTest(codec: Option[Class[_ <: CompressionCodec]] = None) extends Utils {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String): RDD[T] = rdd.serializeToSequenceFile(path, codec)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.fromSequenceFile[T](path)
}

object SequenceFileRDDTest {
  def apply(codec: Class[_ <: CompressionCodec]): SequenceFileRDDTest = new SequenceFileRDDTest(Some(codec))
}

class SnappySequenceFileRDDTest extends SequenceFileRDDTest(Some(classOf[SnappyCodec]))

class BZippedSequenceFileRDDTest extends SequenceFileRDDTest(Some(classOf[BZip2Codec]))

class DirectFileRDDTest(withClasses: Boolean = false, gzip: Boolean = false) extends Utils {
  def serializeRDD[T: ClassTag](rdd: RDD[T], path: String): RDD[T] = rdd.saveAsDirectFile(path, withClasses, gzip = gzip)
  def deserializeRDD[T: ClassTag](path: String): RDD[T] = sc.directFile[T](path, withClasses, gzip = gzip)
}

class GzippedDirectFileRDDTest(withClasses: Boolean = false) extends DirectFileRDDTest(withClasses, gzip = true)

trait FooRegistrarTest extends KryoSuite {
  self: SparkSuite =>
  props ++= Map(
    "spark.kryo.referenceTracking" -> "true",
    "spark.kryo.registrator" -> "org.hammerlab.pageant.serialization.FooKryoRegistrator"
  )
}
