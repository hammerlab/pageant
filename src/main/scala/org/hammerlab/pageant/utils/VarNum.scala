package org.hammerlab.pageant.utils

import java.io.{OutputStream, InputStream}

object VarNum {
  def write(output: OutputStream, l: Long): Unit = {
    var n = l
    var more = true
    var total = 0
    while (more) {
      if (total == 56) {
        output.write(n.toByte)
        more = false
      } else {
        val b = (n & 0x7F).toByte
        n = n >> 7
        total += 7
        more = n > 0
        output.write(b | (if (more) 0x80 else 0).toByte)
      }
    }
  }

  def read(input: InputStream): Long = {
    var l = 0L
    var bits = 0
    var readBytes = Array[Byte](0)
    while (bits < 64) {
      input.read(readBytes)
      val b = readBytes(0)
      if (bits == 56) {
        l += (b.toLong << bits)
        bits += 8
      } else {
        l += (b & 0x7F).toLong << bits
        if ((b & 0x80) == 0) {
          bits = 64
        } else {
          bits += 7
        }
      }
    }
    l
  }

}
