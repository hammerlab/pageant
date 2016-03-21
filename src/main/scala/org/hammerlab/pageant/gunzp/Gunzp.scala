package org.hammerlab.pageant.gunzp

import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

object Gunzp {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath =
      if (args.length > 1)
        args(1)
      else if (inputPath.endsWith(".gz"))
        inputPath.dropRight(3)
      else
        throw new IllegalArgumentException(s"Suspicious input path, not sure what to name output: $inputPath")

    val conf = new SparkConf()
    conf.setAppName(s"Gunzp:$outputPath")
    val sc = new SparkContext(conf)

    gunzip(sc, new Path(inputPath), new Path(outputPath))
  }

  def gunzip(sc: SparkContext, inputPath: Path, outputPath: Path): Long = {
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)

    if (fs.exists(outputPath)) {
      println(s"$outputPath already exists")
      return 0L
    }

    val instream = new GZIPInputStream(fs.open(inputPath, 65536))
    val outstream = fs.create(outputPath)
    val n = IOUtils.copyLarge(instream, outstream)
    instream.close()
    outstream.close()
    n
  }
}
