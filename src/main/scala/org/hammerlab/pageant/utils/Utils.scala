package org.hammerlab.pageant.utils

object Utils {
  def byteToHex(b: Byte) = {
    val s = b.toInt.toHexString.takeRight(2)
    if (s.length == 1) "0" + s else s
  }

  def bytesToHex(a: Array[Byte]) = a.map(byteToHex).mkString(",")
  def rev[T, U](t: (T, U)): (U, T) = (t._2, t._1)

  def resourcePath(fn: String): String = ClassLoader.getSystemClassLoader.getResource(fn).getFile
}
