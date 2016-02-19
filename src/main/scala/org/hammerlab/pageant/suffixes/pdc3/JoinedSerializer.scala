package org.hammerlab.pageant.suffixes.pdc3

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.hammerlab.pageant.suffixes.pdc3.PDC3.OL

import scala.collection.mutable.ArrayBuffer

class JoinedSerializer extends Serializer[Joined] {
  override def write(kryo: Kryo, output: Output, o: Joined): Unit = {
    val longs: ArrayBuffer[Long] = ArrayBuffer()

    var l = 0L
    if (o.t0O.isDefined) {
      l |= 0x8000000000000000L
      longs.append(o.t0O.get)
    }
    if (o.t1O.isDefined) {
      l |= 0x4000000000000000L
      longs.append(o.t1O.get)
    }
    if (o.n0O.isDefined) {
      l |= 0x2000000000000000L
      longs.append(o.n0O.get)
    }
    if (o.n1O.isDefined) {
      l |= 0x1000000000000000L
      longs.append(o.n1O.get)
    }

    if (longs.isEmpty)
      longs.append(l)
    else
      longs(0) = longs(0) | l

    longs.foreach(lng => output.writeLong(lng))
  }

  override def read(kryo: Kryo, input: Input, tpe: Class[Joined]): Joined = {
    var t0O: OL = None
    var t1O: OL = None
    var n0O: OL = None
    var n1O: OL = None

    var firstLong = true
    val first = input.readLong()
    var curLong = first & 0x0fffffffffffffffL
    if ((first & 0x8000000000000000L) != 0L) {
      t0O = Some(curLong)
      firstLong = false
    }
    if ((first & 0x4000000000000000L) != 0L) {
      if (firstLong)
        firstLong = false
      else
        curLong = input.readLong()
      t1O = Some(curLong)
    }
    if ((first & 0x2000000000000000L) != 0L) {
      if (firstLong)
        firstLong = false
      else
        curLong = input.readLong()
      n0O = Some(curLong)
    }
    if ((first & 0x1000000000000000L) != 0L) {
      if (firstLong)
        firstLong = false
      else
        curLong = input.readLong()
      n1O = Some(curLong)
    }
    Joined(t0O, t1O, n0O, n1O)
  }
}
