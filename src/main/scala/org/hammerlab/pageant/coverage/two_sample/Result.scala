package org.hammerlab.pageant.coverage.two_sample

import java.io.PrintWriter

import org.apache.hadoop.fs.Path
import org.hammerlab.csv._
import org.hammerlab.pageant.coverage.CoverageDepth.getJointHistogramPath
import org.hammerlab.pageant.histogram.JointHistogram
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import org.hammerlab.pageant.utils.{ WriteLines, WriteRDD }
import spire.algebra.Monoid

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

abstract class Result[C: Monoid, CSVRow <: Product : TypeTag : ClassTag]
  extends Serializable {

  def jh: JointHistogram
  def pdf: PDF[C]
  def cdf: CDF[C]

  def toCSVRow(d2c: ((Depth, Depth), C)): CSVRow
  def writeMisc(pw: PrintWriter): Unit

  def save(dir: String,
           force: Boolean = false,
           writeFullDistributions: Boolean = false,
           writeJointHistogram: Boolean = false): this.type = {

    val fs = new Path(dir).getFileSystem(jh)

    if (writeFullDistributions) {
      WriteRDD(dir, s"pdf", pdf.rdd.map(toCSVRow), force, jh)
      WriteRDD(dir, s"cdf", cdf.rdd.map(toCSVRow), force, jh)
    }

    if (writeJointHistogram) {
      val jhPath = getJointHistogramPath(dir)

      if (fs.exists(jhPath)) {
        fs.delete(jhPath, true)
      }

      jh.write(jhPath)
    }

    WriteLines(dir, s"pdf.csv", pdf.filtered.map(toCSVRow).toCSV(), force, jh)
    WriteLines(dir, s"cdf.csv", cdf.filtered.map(toCSVRow).toCSV(), force, jh)

    val miscPath = new Path(dir, "misc")
    if (force || !fs.exists(miscPath)) {
      val pw = new PrintWriter(fs.create(miscPath))
      writeMisc(pw)
      pw.close()
    }

    this
  }
}
