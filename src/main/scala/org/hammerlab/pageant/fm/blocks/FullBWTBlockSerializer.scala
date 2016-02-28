package org.hammerlab.pageant.fm.blocks

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

class FullBWTBlockSerializer extends Serializer[FullBWTBlock] with BWTBlockSerializer {
  override def write(kryo: Kryo, output: Output, o: FullBWTBlock): Unit = {
    write(output, o, o.data.length)
    o.data.foreach(b ⇒ output.writeByte(b))
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[FullBWTBlock]): FullBWTBlock = {
    val (startIdx, startCounts, length) = read(input)
    val data = input.readBytes(length)
    FullBWTBlock(startIdx, startCounts, data)
  }
}
