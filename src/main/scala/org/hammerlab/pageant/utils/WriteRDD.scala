package org.hammerlab.pageant.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.hammerlab.csv._

import scala.reflect.runtime.universe.TypeTag

object WriteRDD {
  def apply[T <: Product : TypeTag](dir: String, fn: String, rdd: RDD[T], force: Boolean, conf: Configuration): Unit = {
    val path = new Path(dir, fn)
    val fs = path.getFileSystem(conf)
    val csvLines = rdd.mapPartitions(_.toCSV(includeHeaderLine = false))
    (fs.exists(path), force) match {
      case (true, true) ⇒
        println(s"Removing $path")
        fs.delete(path, true)
        csvLines.saveAsTextFile(path.toString)
      case (true, false) ⇒
        println(s"Skipping $path, already exists")
      case _ ⇒
        csvLines.saveAsTextFile(path.toString)
    }
  }
}
