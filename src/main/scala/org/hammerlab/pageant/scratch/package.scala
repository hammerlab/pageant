package org.hammerlab.pageant

import org.apache.spark.SparkContext

package object scratch {
  var sc: SparkContext = null

  var dir = "/datasets/illumina_platinum/50x/ERR194146"
  var filePrefix = s"${dir}"
}
