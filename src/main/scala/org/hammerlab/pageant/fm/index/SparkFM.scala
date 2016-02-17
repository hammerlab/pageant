package org.hammerlab.pageant.fm.index

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.fm.blocks.{FullBWTBlock, RunLengthBWTBlock, BWTBlock}
import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.utils.Utils.{BlockIdx, Idx, T, V}
import org.hammerlab.pageant.suffixes.pdc3.PDC3


import scala.collection.mutable.ArrayBuffer

case class SparkFM(bwtBlocks: RDD[(BlockIdx, BWTBlock)],
                   totalSums: Counts,
                   count: Long,
                   blockSize: Int,
                   runLengthEncoded: Boolean) extends Serializable {

  @transient val sc = bwtBlocks.sparkContext

  val totalSumsBC = sc.broadcast(totalSums)

  def save(fn: String, gzip: Boolean = false): SparkFM = {
    val lastDot = fn.lastIndexOf('.')
    val dir = if (lastDot < 0 || fn.substring(lastDot, lastDot + 3) != ".fm") fn + ".fmi" else fn
    val blocksPath = new Path(dir, "blocks")
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    bwtBlocks.saveAsDirectFile(blocksPath.toString, gzip = gzip)

    val configPath = new Path(dir, "counts")
    val os = fs.create(configPath)
    val oos = new ObjectOutputStream(os)
    oos.writeObject(totalSums)
    oos.writeLong(count)
    oos.writeInt(blockSize)
    oos.writeBoolean(runLengthEncoded)
    oos.close()
    this
  }
}

object SparkFM {

  type Counts = Array[Long]

  def load(sc: SparkContext, fn: String, gzip: Boolean = false): SparkFM = {
    val dir = if (fn.endsWith(".fm")) fn else fn + ".fmi"
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)

    val configPath = new Path(dir, "counts")
    val is = fs.open(configPath)
    val ios = new ObjectInputStream(is)
    val totalSums = ios.readObject().asInstanceOf[Counts]
    val count = ios.readLong()
    val blockSize = ios.readInt()
    val runLengthEncoded = ios.readBoolean()

    val blocksPath = new Path(dir, "blocks")
    val bwtBlocks =
      if (runLengthEncoded)
        sc.directFile[(BlockIdx, RunLengthBWTBlock)](blocksPath.toString, gzip = gzip).mapValues(b => b: BWTBlock)
      else
        sc.directFile[(BlockIdx, FullBWTBlock)](blocksPath.toString, gzip = gzip).mapValues(b => b: BWTBlock)

    SparkFM(bwtBlocks, totalSums, count, blockSize, runLengthEncoded)
  }

  def apply[U](us: RDD[U],
               N: Int,
               toT: (U) => T,
               blockSize: Int = 100): SparkFM = {
    @transient val sc = us.context
    us.cache()
    val count = us.count
    @transient val t: RDD[T] = us.map(toT)
    t.cache()
    @transient val tZipped: RDD[(Idx, T)] = t.zipWithIndex().map(_.swap)
    @transient val sa = PDC3(t.map(_.toLong), count)
    @transient val saZipped: RDD[(V, Idx)] = sa.zipWithIndex()

    SparkFM(saZipped, tZipped, count, N, blockSize)
  }

  def makeBwtBlocks(indexedBwtt: RDD[(Idx, T)],
                    startCountsRDD: RDD[Counts],
                    blockSize: Int,
                    runLengthEncode: Boolean = true): RDD[(BlockIdx, BWTBlock)] = {
    indexedBwtt.zipPartitions(startCountsRDD)((bwtIter, startCountIter) => {
      var startCounts = startCountIter.next()
      assert(
        startCountIter.isEmpty,
        s"Got more than one summed-count in partition starting from $startCounts"
      )

      var data: ArrayBuffer[T] = ArrayBuffer()
      var rets: ArrayBuffer[Array[Int]] = ArrayBuffer()
      var blocks: ArrayBuffer[(BlockIdx, BWTBlock)] = ArrayBuffer()
      var blockIdx = -1L
      var startIdx = -1L
      var idx = -1L
      var counts: Counts = null

      for {
        (idx, t) <- bwtIter
      } {
        if (blockIdx == -1L) {
          blockIdx = idx / blockSize
          startIdx = idx
          counts = startCounts.clone()
        } else if (idx % blockSize == 0) {
          val block = FullBWTBlock(startIdx, startCounts, data.toArray)
          blocks.append((blockIdx, block))
          blockIdx = idx / blockSize
          startIdx = idx
          data.clear()
          startCounts = counts.clone()
        }
        counts(t) += 1
        data.append(t)
      }

      if (data.nonEmpty) {
        blocks.append((blockIdx, FullBWTBlock(startIdx, startCounts, data.toArray)))
      }
      blocks.toIterator
    }).groupByKey.mapValues(iter => {
      val data: ArrayBuffer[T] = ArrayBuffer()
      val blocks = iter.toArray.sortBy(_.startIdx)
      val first = blocks.head
      val last = blocks.last
      for {block <- blocks} {
        data ++= block.data
      }
      (
        if (runLengthEncode)
          RunLengthBWTBlock.fromTs(first.startIdx, first.startCounts, data)
        else
          FullBWTBlock(first.startIdx, first.startCounts, data)
      ): BWTBlock
    }).setName("BWTBlocks")
  }

  def getStartCountsRDD(sc: SparkContext,
                        bwtt: RDD[T],
                        N: Int): (RDD[Counts], Counts) = {
    @transient val partitionCounts: RDD[Array[Int]] =
      bwtt.mapPartitions(
        iter => {
          var counts: Array[Int] = Array.fill(N)(0)
          iter.foreach(t => {
            counts(t) += 1
          })
          Array(counts).iterator
        },
        preservesPartitioning = false
      ).setName("partitionCounts")

    @transient val lastCounts: Array[Array[Int]] = partitionCounts.collect

    @transient val startCountsBuf: ArrayBuffer[Counts] = ArrayBuffer()
    @transient var curStartCounts = Array.fill(N)(0L)
    lastCounts.foreach(lastCount => {
      startCountsBuf.append(curStartCounts.clone())
      var i = 0
      lastCount.foreach(c => {
        curStartCounts(i) += c
        i += 1
      })
    })

    val startCounts: Array[Counts] = startCountsBuf.toArray
    var totalSums: Counts = Array.fill(N)(0L)
    for {i <- 1 until N} {
      totalSums(i) = totalSums(i - 1) + curStartCounts(i - 1)
    }

    (
      sc.parallelize(startCounts, startCounts.length),
      totalSums
    )
  }

  def apply(saZipped: RDD[(V, Idx)],
            tZipped: RDD[(Idx, T)],
            count: Long,
            N: Int,
            blockSize: Int = 100,
            runLengthEncode: Boolean = true): SparkFM = {
    @transient val sc = saZipped.sparkContext

    @transient val tShifted: RDD[(Idx, T)] =
      tZipped
      .map(p =>
        (
          if (p._1 + 1 == count)
            0L
          else
            p._1 + 1,
          p._2
        )
      )

    @transient val indexedBwtt: RDD[(Idx, T)] =
      saZipped
        .join(tShifted)
        .map(p => {
          val (sufPos, (idx, t)) = p
          (idx, t)
        })
        .sortByKey().setName("indexedBwtt")

    indexedBwtt.cache()

    @transient val bwtt: RDD[T] = indexedBwtt.map(_._2).setName("bwtt")

    val (startCountsRDD, totalSums) = getStartCountsRDD(sc, bwtt, N)
    val bwtBlocks: RDD[(BlockIdx, BWTBlock)] =
      makeBwtBlocks(
        indexedBwtt,
        startCountsRDD,
        blockSize,
        runLengthEncode
      )

    bwtBlocks.cache()

    SparkFM(bwtBlocks, totalSums, count, blockSize, runLengthEncode)
  }
}
