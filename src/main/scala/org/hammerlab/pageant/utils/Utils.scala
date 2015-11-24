package org.hammerlab.pageant.utils

object Utils {
  def byteToHex(b: Byte) = {
    val s = b.toInt.toHexString.takeRight(2)
    if (s.size == 1) "0" + s else s
  }

  def bytesToHex(a: Array[Byte]) = a.map(byteToHex).mkString(",")
}
