package org.hammerlab.pageant.fm.index

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.blocks.{BWTBlock, FullBWTBlock, RunLengthBWTBlock}
import org.hammerlab.pageant.fm.utils.Counts
import org.hammerlab.pageant.fm.utils.Utils.BlockIdx

case class FMIndex(bwtBlocks: RDD[(BlockIdx, BWTBlock)],
                   totalSums: Counts,
                   count: Long,
                   blockSize: Int,
                   runLengthEncoded: Boolean) extends Serializable {

  @transient val sc = bwtBlocks.sparkContext

  val totalSumsBC = sc.broadcast(totalSums)

  def save(fn: String, gzip: Boolean = false): this.type = {
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

object FMIndex {
  type FMI = FMIndex
  type RunLengthFMIndex = FMIndex

  def load(sc: SparkContext, fn: String, gzip: Boolean = false): FMIndex = {
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
    val bwtBlocks: RDD[(BlockIdx, BWTBlock)] =
      if (runLengthEncoded)
        sc.directFile[(BlockIdx, RunLengthBWTBlock)](blocksPath.toString, gzip = gzip).mapValues(b => b: BWTBlock)
      else
        sc.directFile[(BlockIdx, FullBWTBlock)](blocksPath.toString, gzip = gzip).mapValues(b => b: BWTBlock)

    FMIndex(bwtBlocks, totalSums, count, blockSize, runLengthEncoded)
  }

}
