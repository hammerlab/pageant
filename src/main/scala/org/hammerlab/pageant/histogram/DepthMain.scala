package org.hammerlab.pageant.histogram

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.hammerlab.pageant.histogram.DepthMain.{Depth, NumBP, NumLoci}
import org.hammerlab.pageant.histogram.JointHistogram.{Depths, JointHist, L, OS}

import collection.mutable.{ArrayBuffer, Map ⇒ MMap}
import scala.collection.Map

case class FK(c: String, d1: Depth, d2: Depth, n: Long)
object FK {
  def make(t: ((OS, Depths), L)): FK = new FK(t._1._1.get, t._1._2(0).get, t._1._2(1).get, t._2)
}

case class Result(jh: JointHistogram,
                  dir: String,
//                  minCDF: Vector[(Depth, NumLoci)],
                  pdf: Seq[((Depth, Depth), Count)],//RDD[((Depth, Depth), Count)],
                  cdf: Seq[((Depth, Depth), Count)]) {

  @transient lazy val sc = jh.sc
  @transient lazy val fs = FileSystem.get(sc.hadoopConfiguration)

  def writeCSV(fn: String, strs: Iterable[String]): Unit = {
    val path = new Path(dir, fn)
    if (fs.exists(path)) {
      println(s"Skipping $path, already exists")
    } else {
      val os = fs.create(path)
      os.writeBytes(strs.mkString("", "\n", "\n"))
      os.close()
    }
  }

  def writeCSV(fn: String, v: Vector[(Depth, NumLoci)]): Unit = {
    writeCSV(fn, v.map(t => s"${t._1},${t._2}"))
  }

  def writeRDD(fn: String, rdd: RDD[((Depth, Depth), Count)]): Unit = {
    val path = new Path(dir, fn)
    if (fs.exists(path)) {
      println(s"Skipping $path, already exists")
    } else {
      (for {
        ((d1, d2), count) <- rdd
      } yield {
        List(d1, d2, count.bp1, count.bp2, count.n).mkString(",")
      })
      .saveAsTextFile(path.toString)
    }
  }

  def save(name: String): Unit = {
//    writeCSV(s"$name-mins.csv", minCDF)
    writeCSV(s"$name-pdf.csv", pdf.map(t ⇒ List(t._1._1, t._1._2, t._2).mkString(",")))
    writeCSV(s"$name-cdf.csv", cdf.map(t ⇒ List(t._1._1, t._1._2, t._2).mkString(",")))
    val jhPath = new Path(dir, "jh")
    if (!fs.exists(jhPath)) {
      jh.write(jhPath)
    }
//    writeRDD(new Path(dir, s"$name-pdf"), pdf)
//    writeRDD(new Path(dir, s"$name-cdf"), cdf)
    //writeCSV(new Path(dir, s"$name-cdf.csv"), cdf.map(t ⇒ List(t._1._1, t._1._2, t._2).mkString(",")))
  }
}

object Result {
  def apply(jh: JointHistogram, dir: String): Result = {
    val k = jh.jh.map(t => new FK(t._1._1.get, t._1._2(0).get, t._1._2(1).get, t._2))

    val pdf = k.map(fk ⇒ (fk.d1, fk.d2) → Count(fk.d1 * fk.n, fk.d2 * fk.n, fk.n)).reduceByKey(_ + _).collectAsMap()
    val maxD1 = pdf.map(_._1._1).max
    val maxD2 = pdf.map(_._1._2).max
    val cdf: MMap[(Int, Int), Count] = MMap()
    println(s"Building cdf: $maxD1 x $maxD2 == ${maxD1.toLong * maxD2}")
    for {
      r ← (maxD1 - 1) to 0 by -1
    } {
      var rSum = Count.empty
      for {
        c ← (maxD2 - 1) to 0 by -1
        cur = pdf.getOrElse((r, c), Count.empty)
        above = cdf.getOrElse((r - 1, c), Count.empty)
      } {
        rSum += cur
        cdf((r, c)) = rSum + above
      }
    }

    Result(jh, dir, pdf.toList.sortBy(_._1), cdf.toList.sortBy(_._1))
  }
}

case class Count(bp1: NumBP, bp2: NumBP, n: NumLoci) {
  def +(o: Count): Count = Count(bp1 + o.bp1, bp2 + o.bp2, n + o.n)
  def -(o: Count): Count = Count(bp1 - o.bp1, bp2 - o.bp2, n - o.n)
}

object Count {
  def apply(fk: FK): Count = Count(fk.n * fk.d1, fk.n * fk.d2, fk.n)
  val empty = Count(0, 0, 0)
}

object DepthMain {

  type Depth = Int
  type NumLoci = Long
  type NumBP = Long

  def parseArgs(args: Array[String],
                defaults: Map[String, Any] = Map.empty,
                aliases: Map[Char, String] = Map.empty): (Map[String, String], Map[String, Boolean], Seq[String]) = {
    val it = args.toIterator
    val strings: MMap[String, String] = MMap()
    val bools: MMap[String, Boolean] = MMap()
    defaults.foreach(t ⇒ t._2 match {
      case b: Boolean ⇒ bools(t._1) = b
      case s: String ⇒ strings(t._1) = s
    })
    val unparsed: ArrayBuffer[String] = ArrayBuffer()

    def processKey(key: String): Unit = {
      defaults.get(key) match {
        case Some(b: Boolean) ⇒ bools(key) = b
        case _ ⇒ strings(key) = it.next()
      }
    }

    while (it.hasNext) {
      val arg = it.next()
      if (arg.startsWith("--")) {
        val key = arg.drop(2)
        processKey(key)
      } else if (arg.startsWith("-")) {
        val key = aliases.get(arg(1)) match {
          case Some(k) ⇒ k
          case _ ⇒ throw new Exception("Bad arg: $arg")
        }
        processKey(key)
      } else {
        unparsed.append(arg)
      }
    }
    (strings, bools, unparsed)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("pageant")
    val sc = new SparkContext(conf)

    val (strings, bools, arguments) =
      parseArgs(
        args,
        Map.empty,
        Map(
          'n' → "normal",
          't' → "tumor",
          'i' → "intervals",
          'j' → "joint-hist"
        )
      )

    val outPath = arguments(0)

    val jh = strings.get("joint-hist") match {
      case Some(jhPath) ⇒
        println("Loading JointHistogram: $jhPath")
        JointHistogram.load(sc, jhPath)
      case _ ⇒
        val normalPath = strings("normal")
        val tumorPath = strings("tumor")
        val intervalPathOpt = strings.get("intervals")

        println(s"Analyzing ($normalPath, $tumorPath) against ${intervalPathOpt.getOrElse("-")} and writing to $outPath")

        JointHistogram.fromFiles(sc, Seq(normalPath, tumorPath), intervalPathOpt.toSeq)
    }

//    val on = j.filter(_._1._2(2).exists(_ == 1))
//    val off = j.filter(_._1._2(2).exists(_ == 0))

    //println(s"${on.count} (${on.map(_._2).sum}bp) loci on-target, ${off.count} (${off.map(_._2).sum}bp) off")

    val all = Result(jh, outPath)
//    val onTarget = Result(on, outPath)
//    val offTarget = Result(off, outPath)

    all.save("all")
//    onTarget.save("on")
//    offTarget.save("off")
  }
}
