package org.hammerlab.pageant.fm.index

import org.apache.spark.SparkContext
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.fm.blocks.RunLengthBWTBlock
import org.hammerlab.pageant.fm.utils.FMSuite
import org.hammerlab.pageant.utils.Utils.resourcePath

class SparkFMBamTest extends FMSuite {
  def initFM(sc: SparkContext) = {
    // Written by PDC3Test
    val sa = sc.directFile[Long](resourcePath("normal.bam.sa"), gzip = true)
    val ts = sc.directFile[Byte](resourcePath("normal.bam.ts"), gzip = true)

    val count = 102000
    sa.count should be(count)

    ts.getNumPartitions should be(4)

    val fm = SparkFM(sa.zipWithIndex(), ts.zipWithIndex().map(_.swap), count, N = 6)
    fm.save("src/test/resources/normal.bam.fm", gzip = true)

    fm
  }

  test("count") {
    fm.count should be(102000)
  }

  test("blocks") {
    fm.bwtBlocks.count should be(1020)
    val rlBlocks = fm.bwtBlocks.map(_._2.asInstanceOf[RunLengthBWTBlock])

    rlBlocks.map(_.pieces.map(_.n).sum -> 1).reduceByKey(_ + _).collectAsMap should be(Map(100 -> 1020))

    val pieceLengths = rlBlocks.flatMap(_.pieces).map(_.n)
    pieceLengths.sum should be(102000)

    val pieceLengthHist = pieceLengths.map(_ -> 1).reduceByKey(_ + _).sortByKey().collect

    pieceLengthHist should be(
      List(
         1 -> 17552,
         2 -> 4247,
         3 -> 1571,
         4 -> 698,
         5 -> 469,
         6 -> 330,
         7 -> 347,
         8 -> 316,
         9 -> 287,
        10 -> 270,
        11 -> 258,
        12 -> 255,
        13 -> 251,
        14 -> 247,
        15 -> 225,
        16 -> 216,
        17 -> 202,
        18 -> 170,
        19 -> 145,
        20 -> 148,
        21 -> 118,
        22 -> 106,
        23 -> 102,
        24 -> 59,
        25 -> 67,
        26 -> 52,
        27 -> 43,
        28 -> 41,
        29 -> 24,
        30 -> 31,
        31 -> 19,
        32 -> 22,
        33 -> 21,
        34 -> 17,
        35 -> 19,
        36 -> 15,
        37 -> 10,
        38 -> 11,
        39 -> 5,
        40 -> 5,
        41 -> 5,
        42 -> 1,
        43 -> 1,
        44 -> 3,
        45 -> 2,
        46 -> 2,
        47 -> 6,
        48 -> 2,
        49 -> 4,
        52 -> 1,
        53 -> 1,
        55 -> 1,
        57 -> 1,
        59 -> 1,
        62 -> 1,
        65 -> 2,
        93 -> 1
      )
    )
  }

  test("totals") {
    fm.totalSums should be(Array(0, 1000, 32291, 56475, 80113, 101897))
  }
}
