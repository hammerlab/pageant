package org.hammerlab.pageant.fm.bwt

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTRun, BlockIterator, MergeInsertsIterator, RunLengthBWTBlock}
import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.index.{BoundPartitioner, DualBoundPartitioner}
import org.hammerlab.pageant.fm.utils.Utils
import org.hammerlab.pageant.fm.utils.Utils.{BlockIdx, Idx, N, T, VT}

import scala.collection.mutable.ArrayBuffer

object BWT {

  case class StepInfo(stringPoss: RDD[(Idx, StringPos)],
                      counts: Counts,
                      bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                      partitionBounds: Seq[(Int, Long)],
                      blockSize: Int,
                      n: Long)

  case class NextStepInfo(nextStringPoss: RDD[(Idx, NextStringPos)],
                          counts: Counts,
                          bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                          partitionBounds: Seq[(Int, Long)],
                          blockSize: Int,
                          n: Long)

  case class StringPos(ts: VT, curPos: Long, nextInsertPos: Option[Long]) {
    override def toString: String = {
      s"SP(${ts.map(Utils.toC).mkString("")} $curPos ${nextInsertPos.map(n ⇒ s" $n").getOrElse("")})"
    }
  }

  case class NextStringPos(ts: VT, nextInsertPos: Long, nextPos: Long, next2InsertPos: Option[Long]) {
    override def toString: String = {
      s"NSP(${ts.map(Utils.toC).mkString("")} $nextInsertPos $nextPos${next2InsertPos.map(n ⇒ s" $n").getOrElse("")})"
    }
  }

  def primeNextStep(info: StepInfo): NextStepInfo = {
    val (nextStringPoss, counts) = primeNextStep(info.bwt, info.stringPoss, info.counts, info.blockSize)
    NextStepInfo(nextStringPoss, counts, info.bwt, info.partitionBounds, info.blockSize, info.n)
  }

  def primeNextStep(bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                    stringPoss: RDD[(Idx, StringPos)],
                    counts: Counts,
                    blockSize: Int): (RDD[(Idx, NextStringPos)], Counts) = {
    val newCounts = updateCounts(stringPoss, counts)

    val targets: RDD[(BlockIdx, (Idx, T, Long, Boolean))] =
      for {
        (tIdx, StringPos(ts, curPos, nextInsertPosOpt)) ← stringPoss
        last = ts.last
        nextToLastOpt =
        if (ts.length > 1)
          nextInsertPosOpt
          .map(
            nextInsertPos ⇒ (tIdx, ts(ts.length - 2), nextInsertPos, true)
          )
          .orElse(
            throw new Exception(s"Missing nextInsertPos for $tIdx: $ts $curPos")
          )
        else
          None
        target ← (tIdx, last, curPos, false) :: nextToLastOpt.toList
        blockIdx = target._3 / blockSize
      } yield {
        blockIdx → target
      }

    val sc = bwt.context
    val newCountsBC = sc.broadcast(newCounts)

    val occs: RDD[(Idx, (Long, Boolean))] =
      for {
        (blockIdx, (block, (tIdx, t, pos, isNextToLast))) ← bwt.join(targets)
        count = newCountsBC.value(t)
        occ = block.occ(t, pos)
      } yield {
        tIdx → (count + occ, isNextToLast)
      }

    val nextStringPoss =
      stringPoss.cogroup(occs).map {
        case (tIdx, (stringPosIter, occsIter)) ⇒
          if (stringPosIter.size != 1) {
            throw new Exception(s"Got ${stringPosIter.size} string-pos's in join at $tIdx: ${stringPosIter.mkString(",")}")
          }

          val StringPos(ts, curPos, nextInsertPosOpt) = stringPosIter.head

          val occsArr = occsIter.toArray.sortBy(_._2)
          if (occsArr.length < 1 || occsArr.length > 2) {
            throw new Exception(s"Got ${occsArr.length} occs hits in join at $tIdx: ${occsArr.mkString(",")}")
          }

          val firstOcc = occsArr.head
          val secondOccOpt =
            if (occsArr.length == 2)
              Some(occsArr(1))
            else
              None

          if (secondOccOpt.exists(_._2 == firstOcc._2)) {
            throw new Exception(s"Got two ${firstOcc._2} occs hits at $tIdx: ${firstOcc._1} ${secondOccOpt.get._1}")
          }

          val nextInsertPos =
            nextInsertPosOpt.getOrElse(
              throw new Exception(s"Missing nextInsertPos for $tIdx: $ts $curPos")
            )

          val nextStringPos = NextStringPos(ts.dropRight(1), nextInsertPos, firstOcc._1, secondOccOpt.map(_._1))

          tIdx → nextStringPos
      }

    (nextStringPoss, newCounts)
  }

  def toNextStep(info: NextStepInfo): StepInfo = {
    val (bwt, stringPoss, partitionBounds) = toNextStep(info.bwt, info.nextStringPoss, info.blockSize, info.partitionBounds, info.n)
    StepInfo(stringPoss, info.counts, bwt, partitionBounds, info.blockSize, info.n)
  }

  def toNextStep(bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                 nextPoss: RDD[(Idx, NextStringPos)],
                 blockSize: Int,
                 partitionBounds: Seq[(Int, Long)],
                 n: Long): (RDD[(BlockIdx, RunLengthBWTBlock)], RDD[(Idx, StringPos)], Seq[(Int, Long)]) = {

    val partitioner = new BoundPartitioner(partitionBounds)
    val dualPartitioner = new DualBoundPartitioner(partitionBounds)

    val insertionSumsAndCounts: Array[(Long, Counts)] =
      (for {
        (tIdx, NextStringPos(ts, nextPos, nextInsertPos, _)) <- nextPoss
        t = if (ts.isEmpty) 0.toByte else ts.last
      } yield {
        nextInsertPos → t
      }).partitionBy(partitioner).values.mapPartitions(it ⇒ {
        val counts = Array.fill(N)(0L)
        it.foreach(t ⇒ {
          counts(t) += 1
        })
        Iterator((counts.sum, counts))
      }).collect

    var totalInsertions = 0L
    val insertionCounts = Array.fill(N)(0L)
    val partialSums: ArrayBuffer[(Long, Counts)] =
      if (insertionSumsAndCounts.length > 1) {
        var ps: ArrayBuffer[(Long, Counts)] = ArrayBuffer()
        for {
          (partitionTotal, partitionCounts) ← insertionSumsAndCounts.dropRight(1)
        } {
          ps.append((totalInsertions, insertionCounts.clone()))
          totalInsertions += partitionTotal
          (0 until N).foreach(i => insertionCounts(i) += partitionCounts(i))
        }
        ps
      } else
        ArrayBuffer((totalInsertions, insertionCounts))

    val sc = bwt.context
    val partialSumsRDD = sc.parallelize(partialSums, partitioner.numPartitions)

    val newChars: RDD[(Long, T)] =
      (for {
        (tIdx, NextStringPos(ts, nextInsertPos, nextPos, _)) ← nextPoss
        last = if (ts.isEmpty) 0.toByte else ts.last
      } yield {
        (nextPos, nextInsertPos) → last
      }).repartitionAndSortWithinPartitions(dualPartitioner).map(t => (t._1._2, t._2))

    val newBwt: RDD[(BlockIdx, RunLengthBWTBlock)] =
      bwt.values.zipPartitions(partialSumsRDD, newChars)(
        (blocksIter, partialSumIter, newCharsIter) => {
          val (startTotal, startCounts) = partialSumIter.next()
          if (partialSumIter.hasNext) {
            throw new Exception(s"partialSumIter with ${partialSumIter.size + 1} elements, starting with $startTotal")
          }

//          val blocksArr = blocksIter.toArray
//          val newCharsArr = newCharsIter.toArray
//
//          val mergedIter = new MergeInsertsIterator(blocksArr.toIterator, newCharsArr.toIterator)
//          val mergedArr = mergedIter.toArray
//
//          new BlockIterator(startTotal, startCounts, blockSize, mergedArr.toIterator)

          val mergedIter = new MergeInsertsIterator(blocksIter, newCharsIter)
          new BlockIterator(startTotal, startCounts, blockSize, mergedIter)
        }
      ).groupByKey.mapValues(blocksIter ⇒ {
        val blocks = blocksIter.toArray.sortBy(_.startIdx)
        val first = blocks.head
        RunLengthBWTBlock(first.startIdx, first.startCounts, blocks.flatMap(_.pieces))
      }).sortByKey()

    val newBounds: Array[(Int, Long)] = newBwt.keys.mapPartitionsWithIndex((idx, it) ⇒ {
      Iterator((idx, (it.toArray.last + 1) * blockSize))
    }).collect

    newBounds(newBounds.length - 1) = (newBounds(newBounds.length - 1)._1, newBounds(newBounds.length - 1)._2 + n)

    val stringPoss =
      for {
        (tIdx, NextStringPos(ts, _, nextPos, next2InsertPos)) ← nextPoss
      } yield {
        tIdx → StringPos(ts, nextPos, next2InsertPos)
      }

    (newBwt, stringPoss, newBounds)
  }

  def updateCounts(stringPoss: RDD[(Long, StringPos)], curCounts: Counts): Counts = {
    val newCharsMap = stringPoss.map(_._2.ts.last -> 1L).reduceByKey(_ + _).collectAsMap()
    val newCounts = Array.fill(N)(0L)
    for {
      (t, c) <- newCharsMap
    } {
      newCounts(t) += c
    }

    var next = 0L
    (0 until N).foreach(i ⇒ {
      val cur = next
      next += newCounts(i)
      newCounts(i) = curCounts(i) + cur
    })

    newCounts
  }

  object NextStepInfo {
    def apply(tss: RDD[VT], blockSize: Int = 100): NextStepInfo = {
      val n = tss.count()
      val nextStringPoss = tss.zipWithIndex().map(t ⇒ t._2 → NextStringPos(t._1.dropRight(1), 0L, t._2, Some(n)))
      val counts = Array.fill(N)(n)
      counts(0) = 0L
      val sc = tss.context
      val bwt = sc.parallelize[(BlockIdx, RunLengthBWTBlock)](
        List(
          (
            0L,
            RunLengthBWTBlock(0L, counts, Array[BWTRun]())
          )
        ),
        1
      )

      val bounds = Array((0, n))

      NextStepInfo(nextStringPoss, counts, bwt, bounds, blockSize, n)
    }
  }

  object StepInfo {
    def apply(tss: RDD[VT], blockSize: Int = 100): StepInfo = {
      val nextStepInfo = NextStepInfo(tss, blockSize)
      toNextStep(nextStepInfo)
    }
  }

  def apply(tss: RDD[VT], blockSize: Int, steps: Int): RDD[(BlockIdx, RunLengthBWTBlock)] = {
    var curStep = StepInfo(tss, blockSize)
    (1 until steps).foreach(i ⇒ {
      curStep = step(curStep)
    })
    curStep.bwt
  }

  def step(info: StepInfo): StepInfo = {
    toNextStep(primeNextStep(info))
//    val (tssi, counts, bwt, partitionBounds) = step(info.tssi, info.counts, info.bwt, info.partitionBounds, info.blockSize)
//    StepInfo(tssi, counts, bwt, partitionBounds, info.blockSize)
  }

//  def step(tssi: RDD[(Idx, (VT, Long))],
//           counts: Counts,
//           bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
//           partitionBounds: Seq[(Int, Long)],
//           blockSize: Int): (RDD[(Idx, (VT, Long))], Counts, RDD[(BlockIdx, RunLengthBWTBlock)], Seq[(Int, Long)]) = {
//
//    val newCharsMap = tssi.map(_._2._1.last -> 1L).reduceByKey(_ + _).collectAsMap()
//    val newCounts = Array.fill(N)(0L)
//    for {
//      (t, c) <- newCharsMap
//    } {
//      newCounts(t) += c
//    }
//
//    var next = 0L
//    (0 until N).foreach(i ⇒ {
//      val cur = next
//      next += newCounts(i)
//      newCounts(i) = counts(i) + cur
//    })
//
//    val sc = tssi.context
//    val newCountsBC = sc.broadcast(newCounts)
//
//    val positionsToBlocks: RDD[(BlockIdx, (Idx, T, Long))] =
//      for {
//        (tIdx, (ts, p)) <- tssi
//        t = ts.last
//        blockIdx = p / blockSize
//      } yield {
//        blockIdx -> (tIdx, t, p)
//      }
//
//    val joined = bwt.join(positionsToBlocks)
//
//    val occsToTs: RDD[(Idx, Long)] =
//      for {
//        (blockIdx, (block, (tIdx, t, pos))) <- joined
//        occ = block.occ(t, pos)
//      } yield {
//        tIdx -> occ
//      }
//
//    val newTs: RDD[(Idx, (VT, Long, Long))] =
//      for {
//        (tIdx, ((ts, p), occ)) <- tssi.join(occsToTs)
//        t = ts.last
//        newCount = newCountsBC.value(t)
//        newPos = newCount + occ
//      } yield {
//        (tIdx, (ts.dropRight(1), newPos, p))
//      }
//
//    val partitioner = new BoundPartitioner(partitionBounds)
//    val dualPartitioner = new DualBoundPartitioner(partitionBounds)
//
//    val newChars: RDD[(Long, T)] =
//      (for {
//        (tIdx, (ts, pos, prevPos)) <- newTs
//      } yield {
//        (pos, prevPos) -> ts.last
//      }).repartitionAndSortWithinPartitions(dualPartitioner).map(t => (t._1._2, t._2))
//
//    val insertionSumsAndCounts: Array[(Long, Counts)] =
//      (for {
//        (tIdx, (ts, pos, prevPos)) <- newTs
//        t = ts.last
//      } yield {
//        prevPos → t
//      }).partitionBy(partitioner).values.mapPartitions(it ⇒ {
//        val counts = Array.fill(N)(0L)
//        it.foreach(t ⇒ {
//          counts(t) += 1
//        })
//        Iterator((counts.sum, counts))
//      }).collect
//
//    var totalInsertions = 0L
//    val insertionCounts = Array.fill(N)(0L)
//    val partialSums: ArrayBuffer[(Long, Counts)] =
//      if (insertionSumsAndCounts.length > 1) {
//        var ps: ArrayBuffer[(Long, Counts)] = ArrayBuffer()
//        for {
//          (partitionTotal, partitionCounts) ← insertionSumsAndCounts.dropRight(1)
//        } {
//          ps.append((totalInsertions, insertionCounts.clone()))
//          totalInsertions += partitionTotal
//          (0 until N).foreach(i => insertionCounts(i) += partitionCounts(i))
//        }
//        ps
//      } else
//        ArrayBuffer((totalInsertions, insertionCounts))
//
//    val partialSumsRDD = sc.parallelize(partialSums, partitioner.numPartitions)
//
//    val newBwt: RDD[(BlockIdx, RunLengthBWTBlock)] =
//      bwt.values.zipPartitions(partialSumsRDD, newChars)(
//        (blocksIter, partialSumIter, newCharsIter) => {
//          val (startTotal, startCounts) = partialSumIter.next()
//          if (partialSumIter.hasNext) {
//            throw new Exception(s"partialSumIter with ${partialSumIter.size + 1} elements, starting with $startTotal")
//          }
//
//          val blocksArr = blocksIter.toArray
//          val newCharsArr = newCharsIter.toArray
//
//          val mergedIter = new MergeInsertsIterator(blocksArr.toIterator, newCharsArr.toIterator)
//          val mergedArr = mergedIter.toArray
//
//          new BlockIterator(startTotal, startCounts, blockSize, mergedArr.toIterator)
//        }
//      ).groupByKey.mapValues(blocksIter ⇒ {
//        val blocks = blocksIter.toArray.sortBy(_.startIdx)
//        val first = blocks.head
//        RunLengthBWTBlock(first.startIdx, first.startCounts, blocks.flatMap(_.pieces))
//      }).sortByKey()
//
//    val newBounds = newBwt.keys.mapPartitionsWithIndex((idx, it) ⇒ {
//      Iterator((idx, (it.toArray.last + 1) * blockSize))
//    }).collect
//
//    (newTs.map(t => (t._1, (t._2._1, t._2._2))), newCounts, newBwt, newBounds)
//  }



//  def runLengthEncodeBWT(sc: SparkContext, bwt: RDD[T]): RDD[BWTRun] = {

//    val firstPass = bwt.mapPartitions(iter => new RunLengthIterator(iter)).setName("RLE BWT first pass")
//    firstPass.cache()
//
//    var lastPieces =
//      firstPass
//      .mapPartitions(iter => {
//        val arr = iter.toArray
//        Array ((arr.headOption, arr.lastOption, arr.length == 1)).toIterator
//      })
//      .collect
//      .sliding(2)
//      .map(a => (a(0), a(1)))
//
//    val dropLasts = Array.fill(lastPieces.length)(false)
//    val incFirsts = Array.fill(lastPieces.length)(0)
//    var i = 0
//    for {
//      ((_, prevLastOption, prevIsSolo), (nextFirstOption, _, _)) <- lastPieces
//    } {
//      (prevLastOption, nextFirstOption) match {
//        case (Some(prevLast), Some(nextFirst)) if prevLast.t == nextFirst.t =>
//          dropLasts(i) = true
//          incFirsts(i + 1) += prevLast.n
//          if (prevIsSolo) incFirsts(i + 1) += incFirsts(i)
//      }
//      i += 1
//    }
//
//    firstPass
//    .zipPartitions(
//      sc.parallelize(
//        dropLasts.zip(incFirsts),
//        lastPieces.length
//      )
//    )(
//      (runIter, modsIter) => {
//        val modsArr = modsIter.toArray
//        assert(modsArr.length == 1, s"Bad mods arr: $modsArr")
//        val (dropFirst, incLast) = modsArr.head
//        new DropFirstIncLastIterator(dropFirst, incLast, runIter)
//      }
//    )
//  }
}
