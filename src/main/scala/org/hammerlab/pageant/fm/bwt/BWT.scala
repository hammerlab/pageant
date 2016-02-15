package org.hammerlab.pageant.fm.bwt

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.BWTRun
import org.hammerlab.pageant.fm.index.RunLengthIterator
import org.hammerlab.pageant.fm.utils.Utils.T

object BWT {
  def runLengthEncodeBWT(sc: SparkContext, bwt: RDD[T]): RDD[BWTRun] = {

    val firstPass = bwt.mapPartitions(iter => new RunLengthIterator(iter)).setName("RLE BWT first pass")
    firstPass.cache()

    var lastPieces =
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
