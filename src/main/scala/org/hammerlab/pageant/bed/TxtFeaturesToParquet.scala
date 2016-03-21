package org.hammerlab.pageant.bed

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Contig, Feature}

object TxtFeaturesToParquet {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("TxtFeaturesToParquet")
    val sc = new SparkContext(conf)

    val filename = args(0)
    val outputFilename = args(1)
    val partitions =
      if (args.length > 2) {
        args(2).toInt
      } else {
        val fs = FileSystem.get(sc.hadoopConfiguration)
        (fs.getFileStatus(new Path(filename)).getLen / (1 << 16)).toInt
      }

    sc
      .textFile(filename, partitions)
      .flatMap(line ⇒ {
        val cols = line.split("\t")
        try {
          val (contig, start, end) = (cols(1), cols(2).toLong, cols(3).toLong)
          Some(
            Feature
              .newBuilder()
              .setContig(
                Contig
                  .newBuilder()
                  .setContigName(contig)
                  .build()
              )
              .setStart(start)
              .setEnd(end)
              .build()
          )
        } catch {
          case e: NumberFormatException ⇒ None
          case e: ArrayIndexOutOfBoundsException ⇒ None
        }
      })
      .adamParquetSave(outputFilename)
  }
}
