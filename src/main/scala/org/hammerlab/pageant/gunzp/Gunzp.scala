package org.hammerlab.pageant.gunzp

import java.io.{FileOutputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkEnv

abstract class Gunzp {
  def gunzip(fn: String): Long = {
    if (!fn.endsWith(".gz")) {
      throw new Exception(s"Bad path: $fn")
    }

    val infile = new Path(fn)
    val env = SparkEnv.get
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
    val fs = infile.getFileSystem(hadoopConf)
    val outfile = fn.dropRight(3)
    val outpath = new Path(outfile)

    if (fs.exists(outpath)) {
      return 0L
    }

    val instream = new GZIPInputStream(fs.open(infile, 65536))
    val outstream = fs.create(outpath)
    val n = IOUtils.copyLarge(instream, outstream)
    instream.close()
    outstream.close()
    n
  }

//  val in = new GZIPInputStream(new FileInputStream("normal.bam.gz"))
//  val out = new FileOutputStream("normal.bam")
//  var buffer: Array[Byte] = Array.fill(1024)(0)
//  var len = in.read(buffer)
//  while (len >= 0) {
//    out.write(buffer, 0, len)
//    len = in.read(buffer)
//  }
//  in.close()
//  out.close()
}
