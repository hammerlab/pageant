package org.hammerlab.pageant.fm.bwt

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.Utils.{counts, makeRLBlocks, order, sToTs}
import org.hammerlab.pageant.fm.utils.Utils.VT
import org.hammerlab.pageant.utils.PageantSuite
import org.scalatest.Matchers

trait BWTTest extends PageantSuite with Matchers {

  def strings: List[String]
  def blocksMap: Map[Int, List[List[(String, String)]]]
  def boundsMap: Map[Int, List[List[Int]]]
  def stepCounts: List[String]

  def nspsMap: Map[Int, List[NextStringPos]] = Map.empty
  def spsMap: Map[Int, List[StringPos]] = Map.empty

  def testFn(blockSize: Int, blocksPerPartition: Int = 1): Unit =
    testFn(
      strings,
      blockSize,
      blocksPerPartition,
      blocksMap(blockSize).toIterator,
      boundsMap(blockSize * blocksPerPartition).map(_.zipWithIndex.map(_.swap)).toIterator,
      stepCounts.toIterator
    )

  def testFn(strings: List[String],
             blockSize: Int,
             blocksPerPartition: Int,
             blocks: Iterator[List[(String, String)]],
             partitionBounds: Iterator[List[(Int, Int)]],
             countsIter: Iterator[String]): Unit = {

    val name = s"simple-$blockSize${if (blocksPerPartition != 1) s"-$blocksPerPartition" else ""}"

    test(name) {
      val tss: RDD[VT] = sc.parallelize(strings.map(sToTs))

      var nsi = NextStepInfo(tss, blockSize, blocksPerPartition)
      var si: StepInfo = null
      val n = strings.size
      var step = 1

      while (step <= n + 1) {
        withClue(s"step $step:") {
          if (si != null) {
            nsi = BWT.primeNextStep(si)
          }
          val nextCounts = counts(countsIter.next())

          nsi.counts should be(nextCounts)
          nsi.curSize should be((step - 1) * n)

          nspsMap.get(step).foreach(nsps ⇒
            order(nsi.nextStringPoss) should be(nsps)
          )

          si = BWT.toNextStep(nsi)
          si.counts should be(nextCounts)
          si.curSize should be(step * n)
          si.n should be(n)
          si.partitionBounds should be(partitionBounds.next().map(pb ⇒ (pb._1, pb._2.toLong)).toArray)

          spsMap.get(step).foreach(sps ⇒
            order(si.stringPoss) should be(sps)
          )

          order(si.bwt) should be(makeRLBlocks(blockSize, blocks.next()))

          step += 1
        }
      }
    }
  }

  def opt(l: Long): Option[Long] = if (l > 0) Some(l) else None

  def sp(t: (String, Int, Int)): StringPos = sp(t._1, t._2, t._3)
  def sp(s: String, curPos: Long, nextInsertPos: Long = -1): StringPos = {
    StringPos(sToTs(s), curPos, opt(nextInsertPos))
  }

  def nsp(t: (String, Int, Int, Int)): NextStringPos = nsp(t._1, t._2, t._3, t._4)
  def nsp(s: String, nextInsertPos: Long, nextPos: Long, next2InsertPos: Long = -1): NextStringPos = {
    NextStringPos(sToTs(s), nextInsertPos, nextPos, opt(next2InsertPos))
  }
}
