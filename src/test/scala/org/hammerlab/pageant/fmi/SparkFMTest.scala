package org.hammerlab.pageant.fmi

import scala.collection.mutable.ArrayBuffer
import Utils._
import org.hammerlab.pageant.utils.Utils.rev

case class SparkFMTest(saPartitions: Int,
                       tsPartitions: Int,
                       blockSize: Int) extends SmallFMSuite {

  val ts = "ACGTTGCA$"

  test(s"bwt-blocks-$saPartitions-$tsPartitions-$blockSize") {
    fm.tShifted.getNumPartitions should be(tsPartitions)
    val tShifted = fm.tShifted.collect
    tShifted should be(
      Array(
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 4),
        (6, 3),
        (7, 2),
        (8, 1),
        (0, 0)
      )
    )

    val maxPartitions = math.max(saPartitions, tsPartitions)

    fm.indexedBwtt.getNumPartitions should be(maxPartitions)
    val ibwt = fm.indexedBwtt.collect
    ibwt should be(
      Array(
        (0, 1),
        (1, 2),
        (2, 0),
        (3, 3),
        (4, 1),
        (5, 4),
        (6, 2),
        (7, 4),
        (8, 3)
      )
    )

    fm.bwtt.getNumPartitions should be(maxPartitions)
    val bwtt = fm.bwtt.collect
    bwtt should be(bwt)

    bwt.map(toC).mkString("") should be("AC$GATCTG")

    fm.partitionCounts.getNumPartitions should be(maxPartitions)

    fm.totalSums should be(Array(0L, 1L, 3L, 5L, 7L))

    val curCounts = Array.fill(5)(0)
    val counts = ArrayBuffer[Array[Int]]()
    for {i <- bwt} {
      counts.append(curCounts.clone())
      curCounts(i) += 1
    }

    val blocks =
      (for {
        start <- bwt.indices by blockSize
      } yield {
        val end = math.min(start + blockSize, bwt.length)
        BWTBlock(start, end, counts(start).map(_.toLong), bwt.slice(start, end))
      }).toArray.zipWithIndex.map(rev)

    fm.bwtBlocks.collect.sortBy(_._1) should be(blocks)
  }

}

object SparkFMTest {
  for {
    saPartitions <- List(1, 3, 6)
    tsPartitions <- List(1, 3, 6)
    blockSize <- 1 to 6
  } {
    SparkFMTest(saPartitions, tsPartitions, blockSize)
  }
}

