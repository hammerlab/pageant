package org.hammerlab

import org.apache.spark.SparkContext

package object pageant {
  var sc: SparkContext = null

  import org.hammerlab.pageant._
  import org.apache.spark.rdd.RDD
  import org.apache.hadoop.fs.{Path, FileSystem}

  val dir = "/datasets/illumina_platinum/50x"
  val filePrefix = s"${dir}/ERR194146"
}
