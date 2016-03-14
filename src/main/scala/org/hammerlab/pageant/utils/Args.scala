package org.hammerlab.pageant.utils

import scala.collection.Map
import collection.mutable.{ArrayBuffer, Map ⇒ MMap}

case class Args(strings: Map[String, String],
                longs: Map[String, Long],
                bools: Map[String, Boolean],
                args: Seq[String])

object Args {
  def apply(args: Array[String],
            defaults: Map[String, Any] = Map.empty,
            aliases: Map[Char, String] = Map.empty,
            types: Map[String, Char] = Map.empty): Args = {
    val it = args.toIterator
    val strings: MMap[String, String] = MMap()
    val bools: MMap[String, Boolean] = MMap()
    val longs: MMap[String, Long] = MMap()
    defaults.foreach(t ⇒ t._2 match {
      case b: Boolean ⇒ bools(t._1) = b
      case s: String ⇒ strings(t._1) = s
      case i: Int ⇒ longs(t._1) = i
      case l: Long ⇒ longs(t._1) = l
    })
    val unparsed: ArrayBuffer[String] = ArrayBuffer()

    def processKey(key: String): Unit = {
      (defaults.get(key), types.get(key)) match {
        case (Some(b: Boolean), _) ⇒ bools(key) = true
        case (Some(i: Int), _) ⇒ longs(key) = i
        case (Some(l: Long), _) ⇒ longs(key) = l
        case (Some(x), _) ⇒ throw new Exception("Invalid default for arg $key: $x")
        case (_, Some('b')) ⇒ bools(key) = true
        case (_, Some('i')) ⇒ longs(key) = it.next().toInt
        case (_, Some('l')) ⇒ longs(key) = it.next().toLong
        case _ ⇒ strings(key) = it.next()
      }
    }

    while (it.hasNext) {
      val arg = it.next()
      if (arg.startsWith("--")) {
        val key = arg.drop(2)
        processKey(key)
      } else if (arg.startsWith("-")) {
        val key = aliases.get(arg(1)) match {
          case Some(k) ⇒ k
          case _ ⇒ throw new Exception("Bad arg: $arg")
        }
        processKey(key)
      } else {
        unparsed.append(arg)
      }
    }
    Args(strings, longs, bools, unparsed)
  }
}

