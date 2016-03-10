package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.Utils.{T, VT}

class MergeInsertsIterator(blocksIter: Iterator[RunLengthBWTBlock],
													 insertsIter: Iterator[(Long, T)]) extends Iterator[BWTRun] {

	val bufferedBlocksIter = blocksIter.buffered

  var nextRunStartIdx =
    if (bufferedBlocksIter.hasNext)
      bufferedBlocksIter.head.startIdx
    else
      -1L
	var nextRun: BWTRun = _
	var nextRunEndIdx = -1L

  val runsIter = BWTRunsIterator(bufferedBlocksIter)

	var nextInsert: (Long, T) = null
	var nextInsertPos: Long = -1
	var nextInsertT: T = _
	var insertsDone: Boolean = !insertsIter.hasNext

	var insertsConsumed = 0

	var finalInsertsPos: Long = -1L

	def advanceInsert(): Unit = {
		if (!insertsDone) {
			nextInsert = insertsIter.next()
			nextInsertPos = nextInsert._1
			nextInsertT = nextInsert._2
			insertsDone = !insertsIter.hasNext
		} else {
			nextInsert = null
		}
	}

	def advanceRun(): Unit = {
		if (runsIter.hasNext) {
      if (nextRun != null) {
        nextRunStartIdx += nextRun.n
      }
			//nextRunStartIdx = runsIter.idx
			nextRun = runsIter.next()
			nextRunEndIdx = nextRunStartIdx + nextRun.n
		} else {
			nextRun = null
		}
	}

	advanceRun()
	advanceInsert()

	override def hasNext: Boolean = {
		nextRun != null || nextInsert != null
	}

	var runInProgress: BWTRun = _
	def nInProgress(): Int = Option(runInProgress).map(_.n).getOrElse(0)

	def emitRun(): BWTRun = {
		val run = runInProgress
		runInProgress = null
		run
	}

	override def next(): BWTRun = {
		(runInProgress == null, nextInsert == null, nextRun == null) match {
			case (true, true, true) ⇒
				throw new NoSuchElementException(
					List(
						"Past end of inserts and runs; remaining runs/inserts:",
						s"${runsIter.mkString(",")}",
						s"${insertsIter.mkString(",")}"
					).mkString("\n")
				)
			case (false, true, true) =>
				emitRun()
			case (true, true, false) =>
				runInProgress = nextRun
				advanceRun()
				next()
			case (false, true, false) =>
				if (runInProgress.t == nextRun.t) {
					runInProgress = BWTRun(runInProgress.t, runInProgress.n + nextRun.n)
					advanceRun()
					next()
				} else {
					emitRun()
				}
			case (true, false, true) =>
				runInProgress = BWTRun(nextInsertT, 1)
				advanceInsert()
				next()
			case (false, false, true) =>
				if (nextInsertT == runInProgress.t) {
					runInProgress = BWTRun(runInProgress.t, runInProgress.n + 1)
					advanceInsert()
					next()
				} else {
					emitRun()
				}
			case (_, false, false) ⇒
				if (nextInsertPos == nextRunStartIdx) {
					if (runInProgress == null || nextInsertT == runInProgress.t) {
						runInProgress = BWTRun(nextInsertT, nInProgress() + 1)
						advanceInsert()
						next()
					} else {
						emitRun()
					}
				} else {
					if (runInProgress == null || nextRun.t == runInProgress.t) {
						val take =
							if (nextInsertPos < nextRunEndIdx) {
								(nextInsertPos - nextRunStartIdx).toInt
							} else {
								nextRun.n
							}
						val leave = nextRun.n - take
						runInProgress = BWTRun(nextRun.t, nInProgress() + take)
						if (leave > 0) {
							nextRunStartIdx += take
							nextRun = BWTRun(nextRun.t, leave)
						} else {
							advanceRun()
						}
						next()
					} else {
						emitRun()
					}
				}
		}
	}
}
