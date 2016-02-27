package org.hammerlab.pageant.fm.bwt

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTRun, BlockIterator, MergeInsertsIterator, RunLengthBWTBlock}
import org.hammerlab.pageant.fm.utils.Counts
import org.hammerlab.pageant.fm.index.{BoundPartitioner, DualBoundPartitioner, RunLengthIterator}
import org.hammerlab.pageant.fm.utils.{Pos, Utils}
import org.hammerlab.pageant.fm.utils.Utils.{BlockIdx, Idx, N, T, VT}


object BWT {

  case class StepInfo(stringPoss: RDD[(Idx, StringPos)],
                      counts: Counts,
                      bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                      partitionBounds: Seq[(Int, Long)],
                      blockSize: Int,
                      blocksPerPartition: Long,
                      curSize: Long,
                      n: Long)

  case class NextStepInfo(nextStringPoss: RDD[(Idx, NextStringPos)],
                          counts: Counts,
                          bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                          partitionBounds: Seq[(Int, Long)],
                          blockSize: Int,
                          blocksPerPartition: Long,
                          curSize: Long,
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

  object NextStepInfo {
    def apply(tss: RDD[VT], blockSize: Int = 100, blocksPerPartition: Long = 10000): NextStepInfo = {
      val n = tss.count()
      val nextStringPoss = tss.zipWithIndex().map(t ⇒ t._2 → NextStringPos(t._1.dropRight(1), 0L, t._2, Some(n)))
      val countsArr = Array.fill(N)(n)
      countsArr(0) = 0L
      val counts = Counts(countsArr)
      val sc = tss.context

      val bounds = getBounds(blockSize, blocksPerPartition, 0, n)

      val bwt = sc.parallelize[(BlockIdx, RunLengthBWTBlock)](
        Array.fill(bounds.length)(
          (
            0L,
            RunLengthBWTBlock(Pos(), Array[BWTRun]())
          )
        ),
        numSlices = bounds.length
      )

      NextStepInfo(nextStringPoss, counts, bwt, bounds, blockSize, blocksPerPartition, 0, n)
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
  }

  // StepInfo => NextStepInfo
  def primeNextStep(info: StepInfo): NextStepInfo = {
    val (nextStringPoss, counts) =
      primeNextStep(
        info.bwt,
        info.stringPoss,
        info.counts,
        info.blockSize
      )
    NextStepInfo(
      nextStringPoss,
      counts,
      info.bwt,
      info.partitionBounds,
      info.blockSize,
      info.blocksPerPartition,
      info.curSize,
      info.n
    )
  }

  // NextStepInfo => StepInfo
  def toNextStep(info: NextStepInfo): StepInfo = {
    val (newBwt, stringPoss, newBounds, newSize) =
      toNextStep(
        info.bwt,
        info.nextStringPoss,
        info.blockSize,
        info.blocksPerPartition,
        info.partitionBounds,
        info.curSize,
        info.n
      )
    StepInfo(
      stringPoss,
      info.counts,
      newBwt,
      newBounds,
      info.blockSize,
      info.blocksPerPartition,
      newSize,
      info.n
    )
  }

  // StepInfo => NextStepInfo
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

  // NextStepInfo => StepInfo
  def toNextStep(bwt: RDD[(BlockIdx, RunLengthBWTBlock)],
                 nextPoss: RDD[(Idx, NextStringPos)],
                 blockSize: Int,
                 blocksPerPartition: Long,
                 partitionBounds: Seq[(Int, Long)],
                 curSize: Long,
                 n: Long): (RDD[(BlockIdx, RunLengthBWTBlock)], RDD[(Idx, StringPos)], Seq[(Int, Long)], Long) = {

    val partitioner = new BoundPartitioner(partitionBounds)
    val dualPartitioner = new DualBoundPartitioner(partitionBounds)

    val insertionSumsAndCounts: Array[Counts] =
      (for {
        (tIdx, NextStringPos(ts, nextInsertPos, nextPos, _)) <- nextPoss
        t = if (ts.isEmpty) 0.toByte else ts.last
      } yield {
        nextInsertPos → t
      })
      .partitionBy(partitioner)
      .values
      .mapPartitions(it ⇒ Iterator(Counts(it)))
      .collect

    val insertionPartialSums = Counts.partialSums(insertionSumsAndCounts)._1.map(c ⇒ Pos(c))

    val sc = bwt.context
    val insertionPartialSumRDD = sc.parallelize(insertionPartialSums, partitioner.numPartitions)

    val newChars: RDD[(Long, T)] =
      (for {
        (tIdx, NextStringPos(ts, nextInsertPos, nextPos, _)) ← nextPoss
        last = if (ts.isEmpty) 0.toByte else ts.last
      } yield {
        (nextPos, nextInsertPos) → last
      }).repartitionAndSortWithinPartitions(dualPartitioner).map(t => (t._1._2, t._2))

    val mergedBwt: RDD[(BlockIdx, RunLengthBWTBlock)] =
      bwt
        .values
        .zipPartitions(insertionPartialSumRDD, newChars)(
          (blocksIter, insertionPartialSumIter, newCharsIter) => {
            val insertionsPos = insertionPartialSumIter.next()
            if (insertionPartialSumIter.hasNext) {
              throw new Exception(s"insertionPartialSumIter with ${insertionPartialSumIter.size + 1} elements, starting with $insertionsPos")
            }

//            val ba = blocksIter.toArray
//            val nca = newCharsIter.toArray
//
//            val mergedIter = new MergeInsertsIterator(ba.toIterator, nca.toIterator)
//            val ma = mergedIter.toArray
//
//            val firstBlock = ba.head
//            val firstBlockPos = firstBlock.pos
//            val startPos = firstBlockPos + insertionsPos
//
//            val bla = new BlockIterator(startPos, blockSize, ma.toIterator).toArray
//
//            bla.toIterator

            val bufferedBlocksIter = blocksIter.buffered
            val firstBlock = bufferedBlocksIter.head
            val firstBlockPos = firstBlock.pos
            val startPos = firstBlockPos + insertionsPos

            val mergedIter = new MergeInsertsIterator(bufferedBlocksIter, newCharsIter)
            new BlockIterator(startPos, blockSize, mergedIter)
          }
        )
        .groupByKey
        .mapValues(blocksIter ⇒ {
          val blocks = blocksIter.toArray.sortBy(_.startIdx)
          val first = blocks.head
          RunLengthBWTBlock(first.startIdx, first.startCounts, blocks.flatMap(_.pieces))
        })
        .sortByKey()

    val stringPoss =
      for {
        (tIdx, NextStringPos(ts, _, nextPos, next2InsertPos)) ← nextPoss
      } yield {
        tIdx → StringPos(ts, nextPos, next2InsertPos)
      }

    val newSize = curSize + n
    val newBounds = getBounds(blockSize, blocksPerPartition, curSize, n)

    val newPartitioner = new BoundPartitioner(newBounds)
    val newBwt =
      (for { (blockIdx, block) <- mergedBwt } yield {
        block.startIdx → (blockIdx, block)
      })
      .repartitionAndSortWithinPartitions(newPartitioner)
      .values

    (newBwt, stringPoss, newBounds, newSize)
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

    Counts(newCounts)
  }

  def getBounds(blockSize: Int, blocksPerPartition: Long, curSize: Long, n: Long): Seq[(Int, Long)] = {
    val newSize = curSize + n

    val numBlocks = (newSize + blockSize - 1) / blockSize
    val newNumPartitions = ((numBlocks + blocksPerPartition - 1) / blocksPerPartition).toInt

    (0 until newNumPartitions).map(i ⇒ (i, blocksPerPartition * blockSize * (i + 1)))
  }

  def runLengthEncodeBWT(sc: SparkContext, bwt: RDD[T]): RDD[BWTRun] = {

    val firstPass = bwt.mapPartitions(iter => new RunLengthIterator(iter)).setName("RLE BWT first pass")
    firstPass.cache()

    val lastPieces =
      firstPass
      .mapPartitions(iter => {
        val arr = iter.toArray
        Array ((arr.headOption, arr.lastOption, arr.length == 1)).toIterator
      })
      .collect
      .sliding(2)
      .map(a => (a(0), a(1)))

    val dropLasts = Array.fill(lastPieces.length)(false)
    val incFirsts = Array.fill(lastPieces.length)(0)
    var i = 0
    for {
      ((_, prevLastOption, prevIsSolo), (nextFirstOption, _, _)) <- lastPieces
    } {
      (prevLastOption, nextFirstOption) match {
        case (Some(prevLast), Some(nextFirst)) if prevLast.t == nextFirst.t =>
          dropLasts(i) = true
          incFirsts(i + 1) += prevLast.n
          if (prevIsSolo) incFirsts(i + 1) += incFirsts(i)
      }
      i += 1
    }

    firstPass
    .zipPartitions(
      sc.parallelize(
        dropLasts.zip(incFirsts),
        lastPieces.length
      )
    )(
      (runIter, modsIter) => {
        val modsArr = modsIter.toArray
        assert(modsArr.length == 1, s"Bad mods arr: $modsArr")
        val (dropFirst, incLast) = modsArr.head
        new DropFirstIncLastIterator(dropFirst, incLast, runIter)
      }
    )
  }
}
