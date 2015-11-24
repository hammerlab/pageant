package org.hammerlab.pageant.serialization

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.immutable.StringOps

case class Foo(n: Int, s: String)

object Foos {
  def apply(n: Int, k: Int = 8): Seq[Foo] = {
    (1 to n).map(i => {
      val ch = ((i%26) + 96).toChar
      Foo(i, new StringOps(ch.toString) * k)
    })
  }
}

class FooKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Foo])
  }
}

