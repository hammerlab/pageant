package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.RunLengthIterator
import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.utils.Utils.toI
import org.hammerlab.pageant.serialization.KryoSerialization._
import org.hammerlab.pageant.utils.Utils.pt
import org.hammerlab.pageant.utils.{KryoRegistrationRequired, KryoNoReferenceTracking, KryoSuite, PageantSuite}

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

  def testBlockSizes(name: String,
                     seq: String,
                     expectedPieces: String,
                     fullBlockSize: Int,
                     rlBlockSize: Int,
                     start: Int = 0,
                     end: Int = 0,
                     counts: Counts = Array.fill(6)(0)): Unit = {
    test(name) {
      val ts = seq.trim().stripMargin.split("\n").mkString("").map(toI)
      val rl = RunLengthIterator(ts).toArray
      rl.mkString(" ") should be(expectedPieces.trim().stripMargin.split("\n").mkString(" "))

      val fullBlock = FullBWTBlock(start, counts, ts.toArray)
      val fullBytes = kryoBytes(fullBlock)
      fullBytes.length should be(fullBlockSize)

      println("")

      val rlBlock = RunLengthBWTBlock(start, counts, rl)
      val rlBytes = kryoBytes(rlBlock)
      rlBytes.length should be(rlBlockSize)
    }
  }

  testBlockSizes(
    "complex block",
    "ATTTTTAAGAGAAAAAACTGAAAGTTAATAGAGAGGTGACTCAGATCCAGAGGTGGAAGAGGAAGGAAGCTTGGAACCCTATAGAGTTGCTGAGTGCCAGG",
    """
      |1A 5T 2A 1G 1A 1G 6A 1C 1T 1G
      |3A 1G 2T 2A 1T 1A 1G 1A 1G 1A
      |2G 1T 1G 1A 1C 1T 1C 1A 1G 1A
      |1T 2C 1A 1G 1A 2G 1T 2G 2A 1G
      |1A 2G 2A 2G 2A 1G 1C 2T 2G 2A
      |3C 1T 1A 1T 1A 1G 1A 1G 2T 1G
      |1C 1T 1G 1A 1G 1T 1G 2C 1A 2G
      |""",
    110,
    78
  )

  testBlockSizes(
    "simple block",
    """
      |AAAAAAAAAA
      |CCCCCCCCCC
      |GGGGGGGGGG
      |TTTTTTTTTT
      |""",
    "10A 10C 10G 10T",
    49,
    12
  )
}
