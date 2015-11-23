package org.hammerlab.pageant

import java.nio.ByteBuffer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkEnv, TaskContext, SparkException, SparkContext, Partition}

import scala.reflect.ClassTag

class SerializedRDDPartition(val index: Int) extends Partition

class SerializedRDD[T: ClassTag](@transient val sc: SparkContext, filename: String) extends RDD[T](sc, Nil) {

  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val path = new Path(filename)
  @transient private val fs = path.getFileSystem(hadoopConf)

  override protected def getPartitions: Array[Partition] = {
    // listStatus can throw exception if path does not exist.
    val inputFiles = fs.listStatus(path)
                     .map(_.getPath)
                     .filter(_.getName.startsWith("part-"))
                     .sortBy(_.toString)
    // Fail fast if input files are invalid
    inputFiles.zipWithIndex.foreach { case (partFile, i) =>
      if (!partFile.toString.endsWith(SerializedRDD.partitionFileName(i))) {
        throw new SparkException(s"Invalid checkpoint file: $filename")
      }
    }
    Array.tabulate(inputFiles.length)(i => new SerializedRDDPartition(i))

  }

  //  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(filename, SerializedRDD.partitionFileName(split.index))

    val env = SparkEnv.get
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
    //    val hadoopConf = broadcastedConf.value.value
    val fs = file.getFileSystem(hadoopConf)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val fileInputStream = fs.open(file, bufferSize)
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

}

object SerializedRDD {

  def byteToHex(b: Byte) = {
    val s = b.toInt.toHexString.takeRight(2)
    if (s.size == 1) "0" + s else s
  }

  def bytesToHex(a: Array[Byte]) = a.map(byteToHex).mkString(",")

  /**
    * Return the file name for the given partition.
    */
  private def partitionFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }
}

class SerializableRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def serializeToDirectFile(path: String): Unit = {
    def writePartition(ctx: TaskContext, iter: Iterator[T]): Unit = {
      val idx = ctx.partitionId()
      val serializer = SparkEnv.get.serializer.newInstance()

      val fs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf))
      val os = fs.create(new Path(path, "part-%05d".format(idx)))
      val ss = serializer.serializeStream(os)
      ss.writeAll(iter)
      ss.close()
    }
    rdd.context.runJob(rdd, writePartition _)
  }

  def serializeToSequenceFile(path: String): RDD[T] = {
    rdd.mapPartitions(iter => {
      val serializer = SparkEnv.get.serializer.newInstance()
      iter.map(x =>
        (
          NullWritable.get(),
          new BytesWritable(serializer.serialize(x).array())
          )
      )
    }).saveAsSequenceFile(path)

    rdd
  }
}

class SerdeSparkContext(val sc: SparkContext) {
  def fromSequenceFile[T](path: String)(implicit ct: ClassTag[T]): RDD[T] = {
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], 2)
    .mapPartitions[T](iter => {
      val serializer = SparkEnv.get.serializer.newInstance()
      iter.map(x => {
        serializer.deserialize(ByteBuffer.wrap(x._2.getBytes))
      })
    })
  }

  def fromDirectFile[T: ClassTag](path: String): RDD[T] = {
    new SerializedRDD[T](sc, path)
  }
}

object SerializableRDD {
  implicit def toSerializableRDD[T: ClassTag](rdd: RDD[T]): SerializableRDD[T] = new SerializableRDD(rdd)
  implicit def toSerdeSparkContext(sc: SparkContext): SerdeSparkContext = new SerdeSparkContext(sc)
}
