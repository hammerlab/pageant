package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.RunLengthIterator
import org.hammerlab.pageant.fm.utils.Counts
import org.hammerlab.pageant.fm.utils.Utils.toI
import org.hammerlab.pageant.serialization.KryoSerialization._
import org.hammerlab.pageant.utils.{KryoNoReferenceTracking, KryoRegistrationRequired, KryoSuite, PageantSuite}

class BWTRunSerializerTest
  extends PageantSuite
    with KryoSuite
    with KryoNoReferenceTracking
    with KryoRegistrationRequired {

  test("simple") {
    val t = 0.toByte
    for {
      n <- 1 to 15
    } {
      val bytes = kryoBytes(BWTRun(t, n))
      withClue(s"$n:") {
        bytes.length should be(1)
      }
      val BWTRun(t2, n2) = kryoRead[BWTRun](bytes)
      t2 should be(t)
      n2 should be(n)
    }

    for {
      n <- 16 to 100
    } {
      val bytes = kryoBytes(BWTRun(t, n))
      withClue(s"$n:") {
        bytes.length should be(2)

        val BWTRun(t2, n2) = kryoRead[BWTRun](bytes)
        t2 should be(t)
        n2 should be(n)
      }
    }
  }
}
