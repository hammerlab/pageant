package org.hammerlab.pageant.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.hammerlab.csv.CSVRowI

object WriteRDD {
  def apply(dir: String, fn: String, rdd: RDD[CSVRowI], force: Boolean, conf: Configuration): Unit = {
    val path = new Path(dir, fn)
    val fs = path.getFileSystem(conf)
    (fs.exists(path), force) match {
      case (true, true) ⇒
        println(s"Removing $path")
        fs.delete(path, true)
        rdd.saveAsTextFile(path.toString)
      case (true, false) ⇒
        println(s"Skipping $path, already exists")
      case _ ⇒
        rdd.saveAsTextFile(path.toString)
    }
  }
}
