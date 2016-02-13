package org.hammerlab.pageant.fm.index

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.fm.utils.Utils.{BlockIdx, Idx, V, AT, T}
import org.hammerlab.pageant.suffixes.PDC3
import org.hammerlab.pageant.utils.Utils.rev

import scala.collection.mutable.ArrayBuffer

case class SparkFM(bwtBlocks: RDD[(BlockIdx, BWTBlock)],
                   totalSums: Array[Long],
                   count: Long,
                   blockSize: Int) extends Serializable {

  @transient val sc = bwtBlocks.sparkContext

  val totalSumsBC = sc.broadcast(totalSums)

  def save(fn: String): SparkFM = {
    val dir = if (fn.endsWith(".fm")) fn else fn + ".fmi"
    val blocksPath = new Path(dir, "blocks")
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    bwtBlocks.saveAsDirectFile(blocksPath.toString)

    val configPath = new Path(dir, "counts")
    val os = fs.create(configPath)
    val oos = new ObjectOutputStream(os)
    oos.writeObject(totalSums)
    oos.writeLong(count)
    oos.writeInt(blockSize)
    oos.close()
    this
  }
}

object SparkFM {

  def load(sc: SparkContext, fn: String): SparkFM = {
    val dir = if (fn.endsWith(".fm")) fn else fn + ".fmi"
    val blocksPath = new Path(dir, "blocks")
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val bwtBlocks = sc.directFile[(BlockIdx, BWTBlock)](blocksPath.toString)

    val configPath = new Path(dir, "counts")
    val is = fs.open(configPath)
    val ios = new ObjectInputStream(is)
    val totalSums = ios.readObject().asInstanceOf[Array[Long]]
    val count = ios.readLong()
    val blockSize = ios.readInt()

    SparkFM(bwtBlocks, totalSums, count, blockSize)
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
    @transient val tZipped: RDD[(Idx, T)] = t.zipWithIndex().map(rev)
    @transient val sa = PDC3(t.map(_.toLong), count)
    @transient val saZipped: RDD[(V, Idx)] = sa.zipWithIndex()

    SparkFM(saZipped, tZipped, count, N, blockSize)
  }

  def apply(saZipped: RDD[(V, Idx)],
            tZipped: RDD[(Idx, T)],
            count: Long,
            N: Int,
            blockSize: Int = 100): SparkFM = {
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
      saZipped.join(tShifted).map(p => {
        val (sufPos, (idx, t)) = p
        (idx, t)
      })
      .sortByKey().setName("indexedBwtt")

    indexedBwtt.cache()

    @transient val bwtt: RDD[T] = indexedBwtt.map(_._2).setName("bwtt")

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

    @transient val summedCountsBuf: ArrayBuffer[(Array[Long], Long)] = ArrayBuffer()
    @transient var curSummedCounts = Array.fill(N)(0L)
    @transient var total = 0L
    lastCounts.foreach(lastCount => {
      summedCountsBuf.append((curSummedCounts.clone(), total))
      var i = 0
      lastCount.foreach(c => {
        curSummedCounts(i) += c
        i += 1
        total += c
      })
    })

    var totalSums = Array.fill(N)(0L)
    for {i <- 1 until N} {
      totalSums(i) = totalSums(i - 1) + curSummedCounts(i - 1)
    }

    val summedCounts: Array[(Array[Long], Long)] = summedCountsBuf.toArray
    val summedCountsRDD = sc.parallelize(summedCounts, summedCounts.length)

    val bwtBlocks: RDD[(BlockIdx, BWTBlock)] =
      indexedBwtt.zipPartitions(summedCountsRDD)((bwtIter, summedCountIter) => {
        var (startCounts, total) = summedCountIter.next()
        assert(
          summedCountIter.isEmpty,
          s"Got more than one summed-count in partition starting from $startCounts $total"
        )

        var data: ArrayBuffer[T] = ArrayBuffer()
        var rets: ArrayBuffer[Array[Int]] = ArrayBuffer()
        var blocks: ArrayBuffer[(BlockIdx, BWTBlock)] = ArrayBuffer()
        var blockIdx = -1L
        var startIdx = -1L
        var idx = -1L
        var counts: Array[Long] = null

        for {
          (i, t) <- bwtIter
        } {
          if (blockIdx == -1L) {
            idx = i
            blockIdx = idx / blockSize
            startIdx = idx
            counts = startCounts.clone()
          } else if (idx % blockSize == 0) {
            val block = BWTBlock(startIdx, idx, startCounts.clone(), data.toArray)
            blocks.append((blockIdx, block))
            blockIdx = idx / blockSize
            startIdx = idx
            data.clear()
            startCounts = counts.clone()
          }
          counts(t) += 1
          data.append(t)
          idx += 1
        }

        if (data.nonEmpty) {
          blocks.append((blockIdx, BWTBlock(startIdx, idx, startCounts.clone(), data.toArray)))
        }
        blocks.toIterator
      }).groupByKey.mapValues(iter => {
        val data: ArrayBuffer[T] = ArrayBuffer()
        val blocks = iter.toArray.sortBy(_.endIdx)
        val first = blocks.head
        val last = blocks.last
        for {block <- blocks} {
          data ++= block.data
        }
        BWTBlock(first.startIdx, last.endIdx, first.startCounts, data.toArray)
      }).setName("BWTBlocks")

    bwtBlocks.cache()

    SparkFM(bwtBlocks, totalSums, count, blockSize)
  }
}
