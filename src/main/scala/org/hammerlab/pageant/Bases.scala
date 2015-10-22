package org.hammerlab.pageant

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
