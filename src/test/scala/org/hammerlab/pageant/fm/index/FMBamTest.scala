package org.hammerlab.pageant.fm.index

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.hammerlab.pageant.fm.blocks.RunLengthBWTBlock
import org.hammerlab.pageant.fm.index.FMIndex.FMI
import org.hammerlab.pageant.fm.utils.{FMSuite, Utils}

abstract class FMBamTest extends FMSuite {

  def name: String
  def num: Int
  def numPartitions: Int
  def blockSize = 100
  def total = 102 * num
  def numBlocks = (total + blockSize - 1) / blockSize

  def blockLengthMap = Map(blockSize → numBlocks)
  def pieceLengthHist: Map[Int, Int]
  def totalSums: Array[Int]

  var loadedFM: FMI = _

  // For debugging
  def writeBWT(fm: FMI): Unit = {
    //val bwtString = fm.bwtBlocks.collect.map(t ⇒ s"${t._1}:\t${t._2.data.map(Utils.toC).mkString("")}").mkString("\n")
    val bwtString = fm.bwtBlocks.collect.mkString("\n")

    val fos = new PrintWriter(s"src/test/resources/$num.$name.bwt")
    fos.println(bwtString)
    fos.close()
  }

  def writeMode = false
  val fmPath = s"src/test/resources/$num.fm"
  def initFM(sc: SparkContext): FMI = {
    loadedFM = FMIndex.load(sc, fmPath, gzip = false)
    val fm = generateFM(sc)
//    writeBWT(fm)
    if (writeMode) {
      fm.save(fmPath, gzip = false)
    }
    fm
  }

  def generateFM(sc: SparkContext): FMI

  test("count") {
    fm.count should be(total)
  }

  test("partitions") {
    fm.bwtBlocks.getNumPartitions should be(numPartitions)
  }

  test("block idxs") {
    fm.bwtBlocks.sortByKey().keys.collect should be(0 until numBlocks toArray)
  }

  test("blocks") {

    loadedFM.bwtBlocks.collect should be(fm.bwtBlocks.collect)

    fm.bwtBlocks.count should be(numBlocks)
    val rlBlocks = fm.bwtBlocks.map(_._2.asInstanceOf[RunLengthBWTBlock])

    rlBlocks.map(_.pieces.map(_.n).sum → 1).reduceByKey(_ + _).collectAsMap should be(blockLengthMap)

    val pieceLengths = rlBlocks.flatMap(_.pieces).map(_.n)
    pieceLengths.sum should be(total)

    val actualPieceLengthHist = pieceLengths.map(_ → 1).reduceByKey(_ + _).sortByKey().collectAsMap

    actualPieceLengthHist should be(pieceLengthHist)
  }

  test("totalSums") {
    fm.totalSums.c should be(totalSums)
    fm.totalSums.c should be(loadedFM.totalSums.c)
  }
}

trait TenReadTest extends FMBamTest {
  override def num = 10
  override def numPartitions = 4
  override def blockLengthMap = Map(100 → 10, 20 → 1)
  override def totalSums = Array(0, 10, 360, 514, 827, 1020)
  override def pieceLengthHist = Map(
     1 → 70,
     2 → 16,
     3 → 12,
     4 → 9,
     5 → 2,
     6 → 9,
     7 → 1,
     8 → 6,
     9 → 24,
    10 → 14,
    11 → 4,
    12 → 1,
    13 → 1,
    14 → 2,
    16 → 1,
    17 → 1,
    18 → 3,
    19 → 1,
    20 → 1,
    25 → 1,
    26 → 2,
    32 → 1,
    39 → 1
  )
}

trait HundredReadTest extends FMBamTest {
  override def num = 100
  override def numPartitions = 10
  override def totalSums = Array(0, 100, 3302, 5438, 8238, 10099)
  override def pieceLengthHist = Map(
     1 → 464,
     2 → 131,
     3 → 45,
     4 → 38,
     5 → 24,
     6 → 23,
     7 → 12,
     8 → 28,
     9 → 23,
    10 → 17,
    11 → 15,
    12 → 18,
    13 → 14,
    14 → 12,
    15 → 20,
    16 → 18,
    17 → 18,
    18 → 10,
    19 → 15,
    20 → 8,
    21 → 10,
    22 → 5,
    23 → 7,
    24 → 6,
    25 → 11,
    26 → 8,
    27 → 9,
    28 → 5,
    29 → 7,
    30 → 9,
    31 → 5,
    32 → 12,
    33 → 5,
    34 → 9,
    35 → 3,
    36 → 3,
    37 → 8,
    38 → 10,
    39 → 8,
    40 → 2,
    41 → 6,
    42 → 5,
    43 → 2,
    44 → 2,
    48 → 1,
    50 → 1,
    51 → 1,
    52 → 1,
    53 → 1,
    54 → 1,
    56 → 1,
    57 → 1,
    58 → 1,
    66 → 1,
    68 → 1,
    69 → 1,
    71 → 1,
    78 → 1,
    80 → 1,
    98 → 1,
    100 → 1
  )
}

trait ThousandReadTest extends FMBamTest {
  override def num = 1000
  override def numPartitions = 10
  override def totalSums = Array(0, 1000, 32291, 56475, 80113, 101897)
  override def pieceLengthHist = Map(
     1 → 7527,
     2 → 1958,
     3 →  781,
     4 →  365,
     5 →  231,
     6 →  157,
     7 →  140,
     8 →  101,
     9 →  105,
    10 →   94,
    11 →  108,
    12 →  111,
    13 →  106,
    14 →   91,
    15 →  106,
    16 →   93,
    17 →  100,
    18 →   77,
    19 →   89,
    20 →  102,
    21 →   79,
    22 →   84,
    23 →   65,
    24 →   74,
    25 →   79,
    26 →   85,
    27 →   66,
    28 →   67,
    29 →   78,
    30 →   73,
    31 →   87,
    32 →   91,
    33 →   72,
    34 →   68,
    35 →   51,
    36 →   59,
    37 →   59,
    38 →   66,
    39 →   63,
    40 →   64,
    41 →   48,
    42 →   43,
    43 →   45,
    44 →   35,
    45 →   33,
    46 →   27,
    47 →   20,
    48 →   19,
    49 →   19,
    50 →   12,
    51 →   17,
    52 →   14,
    53 →   17,
    54 →   19,
    55 →   11,
    56 →   12,
    57 →    9,
    58 →   10,
    59 →    6,
    60 →    7,
    61 →    4,
    62 →    5,
    63 →    3,
    64 →    1,
    65 →    2,
    66 →    2,
    68 →    1,
    70 →    1,
    71 →    5,
    72 →    2,
    73 →    2,
    75 →    1,
    76 →    1,
    77 →    4,
    78 →    1,
    79 →    1,
    80 →    1,
    83 →    1,
    87 →    1,
    89 →    1,
    98 →    1
  )
}

