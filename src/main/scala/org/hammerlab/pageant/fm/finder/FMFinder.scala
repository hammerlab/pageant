package org.hammerlab.pageant.fm.finder

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.hammerlab.pageant.fm.index._
import org.hammerlab.pageant.fm.utils.Utils.{BlockIdx, Idx, TPos, AT}
import org.hammerlab.pageant.fm.utils.{BoundsMap, CountsMap, Bounds}

import scala.reflect.ClassTag

abstract class FMFinder[NT <: Needle](fm: SparkFM) extends Serializable {

  def occAll(tss: RDD[AT]): RDD[(AT, BoundsMap)]
  def occ(tss: RDD[AT]): RDD[(AT, Bounds)]
  def occBidi(tss: RDD[(AT, TPos, TPos)]): RDD[(AT, BoundsMap)]

  def countAll(tss: RDD[AT]): RDD[(AT, CountsMap)] = occAll(tss).mapValues(CountsMap.apply)
  def count(tss: RDD[AT]): RDD[(AT, Long)] = occ(tss).mapValues(_.count)
  def countBidi(tss: RDD[(AT, TPos, TPos)]): RDD[(AT, CountsMap)] = occBidi(tss).mapValues(CountsMap.apply)

  @transient protected val sc = fm.sc
  @transient protected val bwtBlocks = fm.bwtBlocks

  protected val count = fm.count
  protected val blockSize = fm.blockSize
  protected val totalSumsBC = fm.totalSumsBC

  def occsToBoundsMap(occs: RDD[Needle]): RDD[(Idx, BoundsMap)] = {
    occs
      .map(_.keyByPos)
      .groupByKey()
      .mapValues(Bounds.merge)
      .map({
        case ((tIdx, start, end), bounds) => ((tIdx, start), (end, bounds))
      })
      .groupByKey()
      .mapValues(_.toMap)
      .map({
        case ((tIdx, start), endMap) => (tIdx, (start, endMap))
      })
      .groupByKey()
      .mapValues(m => BoundsMap(m.toMap))
  }

  def occsToBounds(occs: RDD[Needle]): RDD[(Idx, Bounds)] = {
    occs
      .map(n => (n.idx, n.bound))
      .groupByKey()
      .mapValues(Bounds.merge)
  }

  def findFinished(next: RDD[(BlockIdx, NT)],
                   emitIntermediateRanges: Boolean): (RDD[Needle], RDD[(BlockIdx, NT)], Long) = {
    next.checkpoint()
    val newFinished: RDD[Needle] =
      if (emitIntermediateRanges)
        next.map(_._2)
      else
        next
          .map(_._2: Needle)
          .filter(_.isEmpty)
    newFinished.cache()

    val notFinished = next.filter(_._2.nonEmpty).setName("leftover")
    notFinished.cache()
    val numLeft = notFinished.count()
    (newFinished, notFinished, numLeft)
  }

  def joinBounds[T, U: ClassTag](tssi: RDD[(Idx, U)], finished: RDD[(Idx, T)]): RDD[(U, T)] = {
    (for {
      (idx, (tsIter, boundsIter)) <- tssi.cogroup(finished)
    } yield {

      assert(tsIter.size == 1, s"Found ${tsIter.size} ts with idx $idx")
      val ts = tsIter.head

      idx -> (ts -> boundsIter.head)
    }).sortByKey().map(_._2)
  }
}
