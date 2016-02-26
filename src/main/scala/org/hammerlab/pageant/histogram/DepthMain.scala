package org.hammerlab.pageant.histogram

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.hammerlab.pageant.histogram.JointHistogram.{Depths, JointHist, L, OS}

case class FK(c: String, d1: Long, d2: Long, n: Long)
object FK {
  def make(t: ((OS, Depths), L)): FK = new FK(t._1._1.get, t._1._2(0).get, t._1._2(1).get, t._2)
}

object DepthMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("pageant")
    val sc = new SparkContext(conf)

    val normalPath = args(0)
    val tumorPath = args(1)
    val intervalPath = args(2)
    val outPath = args(3)

    println(s"Analyzing ($normalPath, $tumorPath) against $intervalPath and writing to $outPath")

    val normalReads = sc.loadAlignments(normalPath)
    val tumorReads = sc.loadAlignments(tumorPath)
    val intervals = sc.loadFeatures(intervalPath)

    val jh = JointHistogram.fromFiles(sc, Seq(normalPath, tumorPath), Seq(intervalPath))
    val j = jh.jh

    def getHist(j: JointHist): Vector[(Long, Long)] = {
      val k = j.map(t => new FK(t._1._1.get, t._1._2(0).get, t._1._2(1).get, t._2))

      val bmdc = k.map(fk => math.min(fk.d1, fk.d2) -> fk.n).reduceByKey(_ + _).sortByKey().collect

      bmdc.reverse.foldLeft((Vector[(Long,Long)](), 0L))((s, t) => {
        val n = s._2 + t._2
        (s._1 :+ (t._1, n), n)
      })._1.reverse.drop(1)
    }

    val on = j.filter(_._1._2(2).exists(_ == 1))
    val off = j.filter(_._1._2(2).exists(_ == 0))

    println(s"${on.count} (${on.map(_._2).sum}bp) loci on-target, ${off.count} (${off.map(_._2).sum}bp) off")

    val total = getHist(j)
    val onTarget = getHist(on)
    val offTarget = getHist(off)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    def writeCSV(fn: String, v: Vector[(Long, Long)]): Unit = {
      val path = new Path(outPath, fn)
      if (fs.exists(path)) {
        println(s"Skipping $path, already exists")
      } else {
        val os = fs.create(path)
        os.writeBytes(v.map(t => s"${t._1},${t._2}").mkString("", "\n", "\n"))
        os.close()
      }
    }

    writeCSV("all.csv", total)
    writeCSV("on.csv", onTarget)
    writeCSV("off.csv", offTarget)
  }
}
