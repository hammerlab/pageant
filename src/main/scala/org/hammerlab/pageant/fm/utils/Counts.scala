package org.hammerlab.pageant.fm.utils

import Utils.{N, T}
import org.hammerlab.pageant.fm.blocks.BWTRun

import scala.collection.mutable.ArrayBuffer

case class Counts(c: Array[Long]) {
  def +(other: Counts): Counts = {
    val newC = c.clone()
    (0 until N).foreach(i ⇒ newC(i) += other(i))
    Counts(newC)
  }

  def +(t: T): Counts = {
    val newC = c.clone()
    newC(t) += 1
    Counts(newC)
  }

  def +=(t: T): Unit = {
    c(t) += 1
  }

  def apply(t: T): Long = c(t)
  def apply(i: Int): Long = c(i)

  def +=(other: Counts): Unit = {
    (0 until N).foreach(i => c(i) += other(i))
  }
  def sum: Long = c.sum
  def sameElements(other: Counts): Boolean = c.sameElements(other.c)
  def mkString(sep: String): String = c.mkString(sep)
  def update(t: T, n: Long) = {
    c(t) = n
  }
  def update(i: Int, n: Long) = {
    c(i) = n
  }
  def partialSum(): Counts = {
    val sum = Counts()
    for {i <- 1 until N} {
      sum(i) = sum(i - 1) + c(i - 1)
    }
    sum
  }
  override def toString: String = c.mkString("(", ",", ")")
  override def equals(o: Any): Boolean = o match {
    case other: Counts ⇒ c.sameElements(other.c)
    case _ ⇒ false
  }
  def copy(): Counts = Counts(c.clone())
}

object Counts {
  def apply(): Counts = Counts(Array.fill(N)(0L))
  def apply(it: Iterator[T]): Counts = {
    val counts = Array.fill(N)(0L)
    it.foreach(t ⇒ counts(t) += 1)
    Counts(counts)
  }

  def partialSums(css: Seq[Counts]): (Seq[Counts], Counts) = {
    val sums: ArrayBuffer[Counts] = ArrayBuffer()
    var cur = Counts()
    css.foreach(counts ⇒ {
      sums += cur
      cur = cur + counts
    })
    (sums, cur)
  }
}

case class Pos(var idx: Long, counts: Counts) {
  def +(other: Pos): Pos = Pos(idx + other.idx, counts + other.counts)
  def +=(other: Pos): Unit = {
    idx = idx + other.idx
    counts += other.counts
  }
  def +(run: BWTRun): Pos = {
    val newCounts = counts.copy()
    newCounts(run.t) += run.n
    Pos(idx + run.n, newCounts)
  }
  def +=(run: BWTRun): Unit = {
    idx = idx + run.n
    counts(run.t) += run.n
  }
  def +(runs: Seq[BWTRun]): Pos = {
    var i = idx
    val newCounts = counts.copy()
    runs.foreach(run ⇒ {
      i += run.n
      newCounts(run.t) += run.n
    })
    Pos(i, newCounts)
  }
  def +=(runs: Seq[BWTRun]): Unit = {
    runs.foreach(run ⇒ this += run)
  }
  def apply(t: T): Long = counts(t)
  override def toString: String = s"($idx:${counts.c.mkString(",")})"
  def copy(): Pos = {
    Pos(idx, counts.copy())
  }
}

object Pos {
  def apply(): Pos = Pos(0, Counts())
  def apply(counts: Counts): Pos = Pos(counts.sum, counts)
  def apply(counts: Array[Long]): Pos = Pos(Counts(counts))
  def apply(idx: Long, counts: Array[Long]): Pos = Pos(idx, Counts(counts))
  def partialSums(pss: Seq[Pos]): Seq[Pos] = {
    val sums: ArrayBuffer[Pos] = ArrayBuffer()
    var cur = Pos()
    pss.foreach(pos ⇒ {
      sums += cur
      cur = cur + pos
    })
    sums
  }
}
