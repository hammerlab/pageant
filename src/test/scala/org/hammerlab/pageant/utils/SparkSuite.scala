package org.hammerlab.pageant.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FunSuite}

import scala.collection.mutable.ArrayBuffer

trait SparkSuite extends FunSuite with Matchers with TmpFilesTest {

  var inits: ArrayBuffer[SparkContext => Unit] = ArrayBuffer()

  var sc: SparkContext = _
  var properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.hammerlab.pageant.kryo.PageantKryoRegistrar",
    "spark.master" -> "local[%d]".format(Runtime.getRuntime.availableProcessors()),
    "spark.app.name" -> this.getClass.getName
  )

  tmpdirBefores.append((dir) => {
    val conf: SparkConf = new SparkConf()
    properties.foreach(kv => conf.set(kv._1, kv._2))
    sc = new SparkContext(conf)
    val checkpointsDir = dir
    sc.setCheckpointDir(checkpointsDir.toString)
    println(s"checkpointing to $checkpointsDir")
    inits.foreach(_(sc))
  })

  afters.append(() => {
    sc.stop()
  })
}
