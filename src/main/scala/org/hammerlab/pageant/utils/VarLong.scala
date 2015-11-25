package org.hammerlab.pageant.utils

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

object VarLong {
  def write(kryo: Kryo, output: Output, l: Long): Unit = {
    var n = l
    var more = true
    var total = 0
    while (more) {
      if (total == 56) {
        output.writeByte(n.toByte)
        more = false
      } else {
        val b = (n & 0x7F).toByte
        n = n >> 7
        more = (n > 0)
        output.writeByte(b | (if (more) 0x80 else 0).toByte)
      }
    }
  }

  def read(kryo: Kryo, input: Input): Long = {
    var l = 0L
    var bytes = 0
    while (bytes < 64) {
      val b = input.readByte()
      if (bytes == 56) {
        l = (l << bytes) + b
        bytes += 8
      } else {
        l = (l << bytes) + (b & 0x7F)
        if ((b & 0x80) == 0) {
          bytes = 64
        } else {
          bytes += 7
        }
      }
    }
    l
  }

}
