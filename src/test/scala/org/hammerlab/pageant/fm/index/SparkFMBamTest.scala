package org.hammerlab.pageant.fm.index

import org.apache.spark.SparkContext
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.fm.blocks.RunLengthBWTBlock
import org.hammerlab.pageant.fm.utils.FMSuite
import org.hammerlab.pageant.utils.Utils.resourcePath

abstract class SparkFMBamTest extends FMSuite {
  test("count") {
    fm.count should be(102000)
  }

  test("partitions") {
    fm.bwtBlocks.getNumPartitions should be(12)
  }

  test("block idxs") {
    fm.bwtBlocks.sortByKey().keys.collect should be(0 until 1020 toArray)
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
        1 -> 7527,
        2 -> 1958,
        3 -> 781,
        4 -> 365,
        5 -> 231,
        6 -> 157,
        7 -> 140,
        8 -> 101,
        9 -> 105,
        10 -> 94,
        11 -> 108,
        12 -> 111,
        13 -> 106,
        14 -> 91,
        15 -> 106,
        16 -> 93,
        17 -> 100,
        18 -> 77,
        19 -> 89,
        20 -> 102,
        21 -> 79,
        22 -> 84,
        23 -> 65,
        24 -> 74,
        25 -> 79,
        26 -> 85,
        27 -> 66,
        28 -> 67,
        29 -> 78,
        30 -> 73,
        31 -> 87,
        32 -> 91,
        33 -> 72,
        34 -> 68,
        35 -> 51,
        36 -> 59,
        37 -> 59,
        38 -> 66,
        39 -> 63,
        40 -> 64,
        41 -> 48,
        42 -> 43,
        43 -> 45,
        44 -> 35,
        45 -> 33,
        46 -> 27,
        47 -> 20,
        48 -> 19,
        49 -> 19,
        50 -> 12,
        51 -> 17,
        52 -> 14,
        53 -> 17,
        54 -> 19,
        55 -> 11,
        56 -> 12,
        57 -> 9,
        58 -> 10,
        59 -> 6,
        60 -> 7,
        61 -> 4,
        62 -> 5,
        63 -> 3,
        64 -> 1,
        65 -> 2,
        66 -> 2,
        68 -> 1,
        70 -> 1,
        71 -> 5,
        72 -> 2,
        73 -> 2,
        75 -> 1,
        76 -> 1,
        77 -> 4,
        78 -> 1,
        79 -> 1,
        80 -> 1,
        83 -> 1,
        87 -> 1,
        89 -> 1,
        98 -> 1
      )
    )
  }

  test("totals") {
    fm.totalSums should be(Array(0, 1000, 32291, 56475, 80113, 101897))
  }
}

class GenerateFMBamTest extends SparkFMBamTest {
  def initFM(sc: SparkContext) = {
    val count = 102000

    // Written by PDC3Test
    val ts = sc.directFile[Byte](resourcePath("normal.bam.ts"), gzip = true)
    ts.getNumPartitions should be(4)
    ts.count should be(count)

    val sa = sc.directFile[Long](resourcePath("normal.bam.sa"), gzip = true)
    sa.getNumPartitions should be(12)
    sa.count should be(count)

    val fm = SparkFM(sa.zipWithIndex(), ts.zipWithIndex().map(_.swap), count, N = 6)
    fm.save("src/test/resources/normal.bam.fm", gzip = true)

    fm
  }
}

class LoadFMBamTest extends SparkFMBamTest {
  def initFM(sc: SparkContext) = {
    SparkFM.load(sc, "src/test/resources/normal.bam.fm", gzip = true)
  }
}
