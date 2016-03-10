package org.hammerlab.pageant.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FunSuite}

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

trait SparkSuite extends FunSuite with Matchers with TmpFilesTest {

  var inits: ArrayBuffer[SparkContext => Unit] = ArrayBuffer()

  var props: ArrayBuffer[(String, String)] = ArrayBuffer()

  var sc: SparkContext = _
  var properties = MMap(
    "spark.master" -> "local[%d]".format(Runtime.getRuntime.availableProcessors()),
    "spark.app.name" -> this.getClass.getName,
    "spark.driver.allowMultipleContexts" -> "true",
    "spark.driver.host" → "localhost"
  )

  befores.append(() => {
    val dir = tmpDir()
    val conf: SparkConf = new SparkConf()
    properties.foreach(kv => conf.set(kv._1, kv._2))
    props.foreach(kv => conf.set(kv._1, kv._2))

    sc = new SparkContext(conf)
    val checkpointsDir = dir
    sc.setCheckpointDir(checkpointsDir.toString)
    inits.foreach(_(sc))
  })

  afters.append(() => {
    sc.stop()
  })
}
