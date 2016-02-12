package org.hammerlab.pageant.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.collection.mutable.ArrayBuffer

trait SparkSuite extends FunSuite with Matchers with BeforeAndAfter {

  var inits: ArrayBuffer[SparkContext => Unit] = ArrayBuffer()

  var sc: SparkContext = _
  val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.hammerlab.pageant.kryo.PageantKryoRegistrar",
    "spark.master" -> "local[%d]".format(Runtime.getRuntime.availableProcessors()),
    "spark.app.name" -> this.getClass.getName
  )

  before {
    val conf: SparkConf = new SparkConf()
    properties.foreach(kv => conf.set(kv._1, kv._2))
    sc = new SparkContext(conf)
    sc.setCheckpointDir("tmp")
    inits.foreach(_(sc))
  }

  after {
    sc.stop()
  }
}