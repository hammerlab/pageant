package org.hammerlab.pageant.fm.blocks

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection.mutable.ArrayBuffer

class RunLengthBWTBlockSerializer extends Serializer[RunLengthBWTBlock] with BWTBlockSerializer {
  override def write(kryo: Kryo, output: Output, o: RunLengthBWTBlock): Unit = {
    write(output, o, o.pieces.length)
    o.pieces.foreach(kryo.writeObject(output, _))
  }

  override def read(kryo: Kryo, input: Input, tpe: Class[RunLengthBWTBlock]): RunLengthBWTBlock = {
    val (startIdx, startCounts, length) = read(input)
    val pieces = ArrayBuffer[BWTRun]()
    for { i <- 0 until length } {
      pieces.append(kryo.readObject(input, classOf[BWTRun]))
    }
    RunLengthBWTBlock(startIdx, startCounts, pieces)
  }
}
