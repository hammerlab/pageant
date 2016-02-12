package org.hammerlab.pageant.fm.utils

trait Bound {
  def v: Long
  def blockIdx(blockSize: Long): Long
  def move(n: Long): Bound
}

case class LoBound(v: Long) extends Bound {
  def blockIdx(blockSize: Long) = v / blockSize
  def move(n: Long) = LoBound(n)
  override def toString: String = s"$v↓"
}
case class HiBound(v: Long) extends Bound {
  def blockIdx(blockSize: Long) = (v - 1) / blockSize
  def move(n: Long) = HiBound(n)
  override def toString: String = s"$v↑"
}

