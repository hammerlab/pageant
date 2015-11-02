package org.hammerlab.pageant

import java.io.{InputStreamReader, BufferedReader, ObjectOutputStream, ByteArrayOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord

//import java.nio.file.FileSystem

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, SeqLike}

case class Bases(bytes: Array[Byte], length: Int) extends SeqLike[Char, Bases] {

  override def apply(idx: Int): Char = {
    val byteIdx = idx / 4
    val offset = (idx%4)*2
    Bases.alphabet((bytes(byteIdx) & ((1 << offset) | (1 << (offset+1)))) >> offset)
  }

  override def iterator: Iterator[Char] = {
    val self = this
    new Iterator[Char] {
      var idx = 0
      override def hasNext: Boolean = idx < self.length

      override def next(): Char = {
        val r = self(idx)
        idx += 1
        r
      }
    }
  }

  override def seq: Seq[Char] = {
    iterator.toSeq
  }

  override protected[this] def newBuilder(): mutable.Builder[Char, Bases] = Bases.builder

  override def toString(): String = {
    val sb = new StringBuilder()
    iterator.foreach(sb += _)
    sb.result()
  }

  def rc: Bases = {
    Bases(seq.reverse.map(Bases.complement.apply))
  }

  override def equals(o: Any): Boolean = {
    o match {
      case b: Bases =>
        b.length == length && bytes.indices.forall(i => bytes(i) == b.bytes(i))
      case _ => false
    }
  }
}

object Bases {
  val alphabet = "ACGT"
  val cToI = Map('A' -> 0, 'C' -> 1, 'G' -> 2, 'T' -> 3)
  val complement = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A')

  def builder: mutable.Builder[Char, Bases] = {
    new mutable.Builder[Char, Bases] {
      val bytes = ArrayBuffer[Byte]()
      var size = 0
      override def +=(elem: Char): this.type = {
        if (size % 4 == 0) {
          bytes += 0.toByte
        }
        val offset = (size % 4) * 2
        size += 1
        val i = Bases.cToI(elem)
        val i2 = (i & 2)
        val i1 = (i & 1)
        bytes(bytes.size - 1) = (bytes(bytes.size - 1) | (i1 << offset) | (i2 << offset)).toByte
        this
      }

      override def result(): Bases = {
        Bases(bytes.toArray, size)
      }

      override def clear(): Unit = {
        bytes.clear()
        size = 0
      }
    }
  }

  def apply(chars: Iterable[Char]): Bases = {
    val bldr = builder
    chars.foreach(bldr += _)
    bldr.result()
  }

  def apply(s: String): Bases = {
    val bldr = builder
    s.foreach(bldr += _)
    bldr.result()
  }
}

class BasesSerializer extends Serializer[Bases] {
  override def write(kryo: Kryo, output: Output, o: Bases): Unit = {
    output.writeInt(o.length)
    output.write(o.bytes)
  }

  override def read(kryo: Kryo, input: Input, tpe: Class[Bases]): Bases = {
    val length = input.readInt()
    Bases(input.readBytes(length), length)
  }
}

trait Misc {

  def bytes(o: Object): Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(os)
    oos.writeObject(o)
    oos.close()
    os.toByteArray
  }

  def bases(s: String): ByteArrayOutputStream = {
    val os = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(os)
    val b = Bases(s)
    oos.writeObject(b)
    oos.close()
    os
  }

  import TuplesRDD.loadTuples

  type Hist = Map[Long, Long]
  type Hists = Map[Int, Hist]

  def histFn(k: Int) = s"$filePrefix.${k}mers.hist"

  def hist(k: Int, numPartitions: Int = 5000) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(histFn(k))
    val h =
      (for {
        (_, (first, other)) <- loadTuples(k)
        num = first + other
      } yield {
        num -> 1L
      }).reduceByKey(_ + _, numPartitions).sortByKey().collect()

    val os = fs.create(path)
    h.foreach(p => os.writeBytes("%d,%d\n".format(p._1, p._2)))
    os.close()
  }

  def loadHist(k: Int): Hist = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(histFn(k))
    val is = fs.open(path)
    (for {
      line <- scala.io.Source.fromInputStream(is).getLines()
      nums = line.split(",")
      if nums.size == 2
      key = nums(0).toLong
      value = nums(1).toLong
    } yield {
      key -> value
    }).toMap
  }

  def loadHists(): Hists = {
    (for {
      k <- 1 to 101
    } yield
      k -> loadHist(k)
      ).toMap
  }

  def histsKeys(hists: Hists): List[Long] = {
    (for {
      (_, hist) <- hists
      (k, _) <- hist
    } yield
      k
      ).toSet.toList.sorted
  }

  def joinHists(hists: Hists): Hists = {
    val keys = histsKeys(hists)
    (for {
      (k, hist) <- hists
    } yield {
      k -> (
             for {
               key <- keys
             } yield {
               key -> hist.getOrElse(key, 0L)
             }
             ).toMap
    })
  }

  def histsCsvFn = s"$filePrefix.hists.csv"

  def writeHists() = {
    val hists = loadHists
    val keys = histsKeys(hists)
    val headerLine = ("" :: keys.map(_.toString())).mkString(",")
    val bodyLines = (for {
      (k, hist) <- hists.toList.sortBy(_._1)
    } yield {
      val joinedHist =
        (for {
          key <- keys
        } yield {
          key -> hist.getOrElse(key, 0L)
        }).sortBy(_._1).map(_._2)

      (k :: joinedHist).mkString(",")
    })

    val lines = headerLine :: bodyLines

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(histsCsvFn)
    val os = fs.create(path)
    lines.foreach(line => os.writeBytes(line + '\n'))
    os.close()
  }

}


import org.apache.spark.SparkContext
class Foo(@transient val sc: SparkContext) extends Serializable {

  import org.bdgenomics.adam.rdd.ADAMContext
  import org.apache.hadoop.fs.{FileSystem, Path}

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
    ac.loadPairedFastq(f1, f2, Some(rgn), ValidationStringency.SILENT)
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

//urdd.flatMap(ar => {
//val s = ar.getSequence
//for {
//(k, rcd, idx) <- cklb.value
//i = s.indexOf(k)
//if i >= 0
//} yield {
//(ar, k, rcd, idx)
//}
//})
