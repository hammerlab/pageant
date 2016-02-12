package org.hammerlab.pageant.fm.finder

import org.hammerlab.pageant.fm.utils.{Bound}
import org.hammerlab.pageant.fm.utils.Utils.{AT, Idx, TPos, toC}

trait Needle {
  def idx: Idx
  def start: TPos
  def end: TPos
  def bound: Bound
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty
  def keyByPos: ((Idx, TPos, TPos), Bound) = (idx, start, end) -> bound
}

case class TNeedle(idx: Idx, end: TPos, remainingTs: AT, bound: Bound) extends Needle {
  def start = remainingTs.length
  def isEmpty: Boolean = remainingTs.isEmpty
  override def toString: String = {
    s"Needle($idx($end), ${remainingTs.map(toC).mkString("")}, $bound)"
  }
}

case class PosNeedle(idx: Idx, start: TPos, end: TPos, bound: Bound) extends Needle {
  def isEmpty: Boolean = start == 0
  override def toString: String = {
    s"Needle($idx[$start,$end): $bound)"
  }
}

