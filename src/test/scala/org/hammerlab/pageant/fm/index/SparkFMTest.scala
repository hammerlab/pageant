package org.hammerlab.pageant.fm.index

import java.io.File

import org.hammerlab.pageant.fm.utils.SmallFMSuite
import org.hammerlab.pageant.utils.{TmpFilesTest, SparkSuite}
import org.hammerlab.pageant.utils.Utils.rev

import scala.collection.mutable.ArrayBuffer

class SparkFMTest extends SparkSuite with TmpFilesTest {

  def testCase(saPartitions: Int, tsPartitions: Int, blockSize: Int): Unit = {
    test(s"spark-fm-$saPartitions-$tsPartitions-$blockSize") {
      val (_, bwt, fm) = SmallFMSuite.initFM(sc, saPartitions, "ACGTTGCA$", tsPartitions, blockSize, N = 6)

      val curCounts = Array.fill(6)(0)
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

      def testFM(fm: SparkFM) {
        fm.totalSums should be(Array(0, 1, 3, 5, 7, 9).map(_.toLong))
        fm.bwtBlocks.collect.sortBy(_._1) should be(blocks)
      }

      testFM(fm)
      val fn = tmpPath("small")
      fm.save(fn)

      testFM(SparkFM.load(sc, fn))
    }
  }

  for {
    saPartitions <- List(1, 3, 6)
    tsPartitions <- List(1, 3, 6)
    blockSize <- 1 to 6
  } {
    testCase(saPartitions, tsPartitions, blockSize)
  }
}

