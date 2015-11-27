package org.hammerlab.pageant

import org.apache.spark.SparkContext

package object scratch {
  var _c: SparkContext = _

  def c: SparkContext = {
    if (_c == null) throw new Exception("c_ is null")
    _c
  }
  var dir = "/datasets/illumina_platinum/50x/ERR194146"
  var filePrefix = s"${dir}"
}
