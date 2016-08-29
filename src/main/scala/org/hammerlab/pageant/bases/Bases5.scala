package org.hammerlab.pageant.bases

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SeqLike, mutable}

case class Bases5(bytes: Array[Byte], length: Int) extends SeqLike[Char, Bases] {

  override def apply(idx: Int): Char = {
    val byteIdx = idx / 3
    val byte = bytes(byteIdx)
    val chIdx =
      idx % 3 match {
        case 0 ⇒ byte % 5
        case 1 ⇒ byte % 25 / 5
        case 2 ⇒ byte / 25
      }
    Bases5.alphabet(chIdx)
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
    Bases(Bases.rc(seq.mkString("")))
  }

  override def equals(o: Any): Boolean = {
    o match {
      case b: Bases5 =>
        b.length == length && bytes.indices.forall(i => bytes(i) == b.bytes(i))
      case _ => false
    }
  }
}

object Bases5 {
  val alphabet = "ACGTN"
  val cToT: Map[Char, Byte] = Map('A' -> 0, 'C' -> 1, 'G' -> 2, 'T' -> 3, 'N' → 4)
  val tToC: Map[Byte, Char] = cToT.map(_.swap)
  val complement = Map('A' -> 'T', 'C' -> 'G', 'G' -> 'C', 'T' -> 'A', 'N' → 'N')

  def builder: mutable.Builder[Char, Bases5] = {
    new mutable.Builder[Char, Bases5] {
      val bytes = ArrayBuffer[Byte]()
      var size = 0
      override def +=(elem: Char): this.type = {
        if (size % 3 == 0) {
          bytes += 0.toByte
        }
        val i = Bases5.cToT.getOrElse(elem, throw new Exception(s"Invalid char: ${elem.toByte}"))
        val fivePow = size % 3 match {
          case 0 ⇒ 1
          case 1 ⇒ 5
          case 2 ⇒ 25
        }
        size += 1
        bytes(bytes.size - 1) = (bytes(bytes.size - 1) + fivePow * i).toByte
        this
      }

      override def result(): Bases5 = {
        Bases5(bytes.toArray, size)
      }

      override def clear(): Unit = {
        bytes.clear()
        size = 0
      }
    }
  }

  def apply(chars: Iterable[Char]): Bases5 = {
    val bldr = builder
    chars.foreach(bldr += _)
    bldr.result()
  }

  def apply(s: String): Bases5 = {
    val bldr = builder
    s.foreach(bldr += _)
    bldr.result()
  }

  def apply(ar: AlignmentRecord): List[Bases] = {
    List(ar.getSequence).flatMap(x => List(x, rc(x))).map(Bases.apply)
  }

  def rc(seq: String): String = seq.reverse.map(complement.apply)
}

class Bases5Serializer extends Serializer[Bases5] {
  override def write(kryo: Kryo, output: Output, o: Bases5): Unit = {
    output.writeByte(o.length)
    output.write(o.bytes)
  }

  override def read(kryo: Kryo, input: Input, tpe: Class[Bases5]): Bases5 = {
    val length: Int = input.readByte() & 0xFF
    Bases5(input.readBytes((length + 2)/3), length)
  }
}
