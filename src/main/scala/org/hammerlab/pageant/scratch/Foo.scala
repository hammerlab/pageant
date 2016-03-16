package org.hammerlab.pageant.scratch

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord

class Foo(@transient val sc: SparkContext) extends Serializable {

  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.bdgenomics.adam.rdd.ADAMContext

  //  import org.apache.spark.SparkContext
//  import org.apache.spark.broadcast.Broadcast
//  import org.apache.spark.rdd.RDD
//  import org.bdgenomics.formats.avro.AlignmentRecord

  def readFile(filename: String): List[String] = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val is = fs.open(new Path(filename))
    val lines = scala.io.Source.fromInputStream(is).getLines.map(_.trim).toList
    is.close()
    lines
  }

  val fqs = readFile("fqs")

  import htsjdk.samtools.ValidationStringency

  val ac = new ADAMContext(sc)
  val prs = fqs.map(bn =>
    bn.substring(bn.lastIndexOf("/") + 1)
    .replaceFirst("_R1_", "_R_")
    .replaceFirst("_R2_", "_R_") -> bn)
            .groupBy(_._1)
            .map(p => (p._2(0)._2, p._2(1)._2))
            .toList


  val rdds = for {
    (f1, f2) <- prs
    rgn = f1.substring(f1.lastIndexOf("/") + 1).drop("PT189_".length).replaceAll("_L[0-9]{3}_R[1-2]_[0-9]+.*", "")
  } yield {
    ac.loadPairedFastq(f1, f2, Some(rgn), ValidationStringency.SILENT).rdd
  }

  val urdd = sc.union(rdds)

  //  val rgnh = urdd.flatMap(r => Option(r.getRecordGroupName)).map(_ -> 1L).reduceByKey(_+_).setName("rgn hist").collect.sortBy(-_._2)
  //  val rghm = rgnh.toMap
  val rghm = Map(
    "9_12_ATTGAGGA" -> 254079356,
    "Right_Ovary_L1_AGCCATGC" -> 1270378500,
    "Right_Ovary_L2_CCGACAAC" -> 1560351472,
    "6_14_Exome_CACCGG" -> 466287376,
    "6_13_GTCGTAGA" -> 221204852,
    "Normal_Exome_TATAAT" -> 123395822,
    "Colon_AGTACAAG" -> 218742694,
    "PBMC_GATGAATC" -> 250945282,
    "Left_Ovary_Exome_GTGGCC" -> 524441622,
    "Uterus_GTACGCAA" -> 233734846,
    "OMentum_ACATTGGC" -> 310792680,
    "11_13_AGAGTCAA" -> 219759098
  )
  val rghb = sc.broadcast(rghm)

  val complement = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')

  def rc(s: String) = s.reverse.map(c => complement.getOrElse(c, c))

  val ks = readFile("kmers").map(_.trim)
  val cks = ks.map(_.filterNot(_ == '*'))
  val sks = ks.zipWithIndex.flatMap(p => {
    val ss = p._1.split("\\*")
    (ss(0), ss(1), p._2, false) ::(rc(ss(1)), rc(ss(0)), p._2, true) :: Nil
  })

  //  object Bar extends Serializable {
  def checkN(n: Int) = {

    val needles = (for {
      (pre, suf, idx, rcd) <- sks
      if pre.size >= n && suf.size >= n
    } yield {
      (pre.substring(pre.size - n) + suf.substring(0, n), (idx, rcd))
    }).toMap

    (for {
      r <- urdd
      s = r.getSequence
      ss <- s.sliding(2 * n)
      (idx, rcd) <- needles.get(ss)
      rg <- Option(r.getRecordGroupName)
      rgn <- rghb.value.get(rg)
    } yield {
      (rg, rgn, idx, rcd) -> 1L
    }).reduceByKey(_ + _).setName(s"overlaps-$n").collect
  }

  //  }
//}

//object Baz {
//  val foo = new Foo(sc)
//  import foo._

  //import Bar.checkN
  val c30 = checkN(15)
//  val c30 = r30.collect

  val tf30 = (for {
    ((rg, rgn, idx, rcd), num) <- c30
  } yield {
    rcd -> num
  }).groupBy(_._1).mapValues(_.map(_._2).sum)

  val i30 = (for {
    ((rg, rgn, idx, rcd), num) <- c30
  } yield {
    idx -> num
  }).groupBy(_._1).mapValues(_.map(_._2).sum)

  val s30 = (for {
    ((rg, rgn, idx, rcd), num) <- c30
  } yield {
    (rg, idx) -> num
  }).groupBy(_._1).mapValues(_.map(_._2).sum)

  val l30 = s30.toList.groupBy(_._1._1).toList.map(p => {
    val (rg, vs) = p
    val l = vs.map(t => (t._1._2, t._2)).sortBy(-_._2)
    val n = l.map(_._2).sum
    val rgn = rghm(rg)
    (rg, n, math.log(n * 1.0 / rgn), l)
  }).sortBy(-_._3)

  println(
    l30.map(p =>
      "%s: %d, %f ->\n\t%s".format(
        p._1,
        p._2,
        p._3,
        p._4.map(t =>
          "%d:\t%d".format(t._1, t._2)
        ).mkString("\n\t")
      )
    ).mkString("\n")
  )

  def foo(urdd: RDD[AlignmentRecord], cklb: Broadcast[List[(String, Boolean, Int)]]) = {
    for {
      ar <- urdd
      s = ar.getSequence
      (k, rcd, idx) <- cklb.value
      i = s.indexOf(k)
      if i >= 0
    } yield {
      (ar, k, rcd, idx)
    }
  }
}
