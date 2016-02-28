package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.RunLengthIterator
import org.hammerlab.pageant.fm.utils.Counts
import org.hammerlab.pageant.fm.utils.Utils.toI
import org.hammerlab.pageant.serialization.KryoSerialization.{kryoBytes, kryoRead}
import org.hammerlab.pageant.utils.{KryoNoReferenceTracking, KryoRegistrationRequired, KryoSuite, PageantSuite}

class BWTBlockSerializerTest
  extends PageantSuite
    with KryoSuite
    with KryoNoReferenceTracking
    with KryoRegistrationRequired{

  def testBlockSizes(name: String,
                     seq: String,
                     expectedPieces: String,
                     fullBlockSize: Int,
                     rlBlockSize: Int,
                     start: Int = 0,
                     end: Int = 0,
                     counts: Counts = Counts()): Unit = {
    test(name) {
      val ts = seq.trim().stripMargin.split("\n").mkString("").map(toI)
      val rl = RunLengthIterator(ts).toArray
      rl.mkString(" ") should be(expectedPieces.trim().stripMargin.split("\n").mkString(" "))

      val fullBlock = FullBWTBlock(start, counts, ts.toArray)
      val fullBytes = kryoBytes(fullBlock)
      fullBytes.length should be(fullBlockSize)

      val deserFullBlock = kryoRead[FullBWTBlock](fullBytes)
      deserFullBlock should be(fullBlock)

      val rlBlock = RunLengthBWTBlock(start, counts, rl)
      val rlBytes = kryoBytes(rlBlock)
      rlBytes.length should be(rlBlockSize)

      val deserRLBlock = kryoRead[RunLengthBWTBlock](rlBytes)
      deserRLBlock should be(rlBlock)
    }
  }

  testBlockSizes(
    "simple block",
    """
      |AAAAAAAAAA
      |CCCCCCCCCC
      |GGGGGGGGGG
      |TTTTTTTTTT
      |""",
    "10A 10C 10G 10T",
    48,
    12
  )

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
    109,
    78
  )

  testBlockSizes(
    "repeat block",
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCGGGGGGGGGGGGGGG",
    "32A 48C 15G",
    103,
    13
  )
}
