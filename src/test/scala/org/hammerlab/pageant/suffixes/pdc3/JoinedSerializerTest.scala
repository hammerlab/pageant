package org.hammerlab.pageant.suffixes.pdc3

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import org.hammerlab.pageant.suffixes.pdc3.PDC3.T
import org.hammerlab.pageant.utils.{KryoNoReferenceTracking, PageantSuite}

class JoinedSerializerTest extends PageantSuite with KryoNoReferenceTracking {

  def makeJ(t0: T = -1, t1: T = -1, n0: T = -1, n1: T = -1): Joined = {
    def opt(l: Long) = if (l < 0) None else Some(l)
    Joined(opt(t0), opt(t1), opt(n0), opt(n1))
  }

  def testFn(name: String, size: Int, t0: Int = -1, t1: Int = -1, n0: Int = -1, n1: Int = -1): Unit = {
    test(name) {
      val joined = makeJ(t0, t1, n0, n1)
      val baos = new ByteArrayOutputStream()
      val output = new Output(baos)
      kryo.writeObject(output, joined)
      output.close()
      val bytes = baos.toByteArray
      bytes.length should be(size)

      val bais = new ByteArrayInputStream(bytes)
      val input = new Input(bais)
      val j = kryo.readObject(input, classOf[Joined])

      j should be(joined)
    }
  }

  testFn("t0-t1-n0-n1", 32,  1,  2,  3,  4)

  testFn("t0-t1-n0",    24,  1,  2,  3, -1)
  testFn("t0-t1-n1",    24,  1,  2, -1,  4)
  testFn("t0-n0-n1",    24,  1, -1,  3,  4)
  testFn("t1-n0-n1",    24, -1,  2,  3,  4)

  testFn("t0-t1",       16,  1,  2, -1, -1)
  testFn("t0-n0",       16,  1, -1,  3, -1)
  testFn("t0-n1",       16,  1, -1, -1,  4)
  testFn("t1-n0",       16, -1,  2,  3, -1)
  testFn("t1-n1",       16, -1,  2, -1,  4)
  testFn("n0-n1",       16, -1, -1,  3,  4)

  testFn("t0",           8,  1, -1, -1, -1)
  testFn("t1",           8, -1,  2, -1, -1)
  testFn("n0",           8, -1, -1,  3, -1)
  testFn("n1",           8, -1, -1, -1,  4)

  testFn("none",         8, -1, -1, -1, -1)
}
