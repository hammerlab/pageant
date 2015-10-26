package org.hammerlab.pageant

import java.io.{InputStreamReader, BufferedReader, ObjectOutputStream, ByteArrayOutputStream}
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
