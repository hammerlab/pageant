package org.hammerlab.pageant.fm.utils

import org.hammerlab.pageant.fm.blocks.BWTRun
import org.hammerlab.pageant.fm.utils.Utils.T

import scala.collection.mutable.ArrayBuffer

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
    var cur = Counts()
    pss.foreach(pos ⇒ {
      sums += Pos(pos.idx, cur)
      cur = cur + pos.counts
    })
    sums
  }
}
