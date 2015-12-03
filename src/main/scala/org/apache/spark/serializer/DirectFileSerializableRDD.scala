package org.apache.spark.serializer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, SparkEnv, SparkException, TaskContext}

import scala.reflect.ClassTag

class DirectFileSerializableRDDPartition(val index: Int) extends Partition

class DirectFileSerializableRDD[T: ClassTag](@transient val sc: SparkContext,
                                             filename: String,
                                             readClass: Boolean = false)
  extends RDD[T](sc, Nil) {

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
      if (!partFile.toString.endsWith(DirectFileSerializableRDD.partitionFileName(i))) {
        throw new SparkException(s"Invalid checkpoint file: $filename")
      }
    }
    Array.tabulate(inputFiles.length)(i => new DirectFileSerializableRDDPartition(i))

  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(filename, DirectFileSerializableRDD.partitionFileName(split.index))

    val env = SparkEnv.get
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
    val fs = file.getFileSystem(hadoopConf)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val fileInputStream = fs.open(file, bufferSize)
    val serializer = env.serializer.newInstance()

    serializer match {
      case ksi: KryoSerializerInstance =>
        val deserializeStream = new KryoObjectDeserializationStream(ksi, fileInputStream, readClass)
        // Register an on-task-completion callback to close the input stream.
        context.addTaskCompletionListener(context => deserializeStream.close())
        deserializeStream.asIterator[T]
      case _ =>
        val deserializeStream = serializer.deserializeStream(fileInputStream)
        // Register an on-task-completion callback to close the input stream.
        context.addTaskCompletionListener(context => deserializeStream.close())
        deserializeStream.asIterator.asInstanceOf[Iterator[T]]
    }
  }

}

object DirectFileSerializableRDD {
  /**
    * Return the file name for the given partition.
    */
  private def partitionFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }
}

class DirectFileRDDSerializer[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def directFile = saveAsDirectFile _
  def saveAsDirectFile(path: String, writeClass: Boolean = false): RDD[T] = {
    def writePartition(ctx: TaskContext, iter: Iterator[T]): Unit = {
      val idx = ctx.partitionId()
      val serializer = SparkEnv.get.serializer.newInstance()

      val fs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf))
      val os = fs.create(new Path(path, "part-%05d".format(idx)))

      val ss = serializer match {
        case ksi: KryoSerializerInstance => new KryoObjectSerializationStream(ksi, os, writeClass)
        case _ => serializer.serializeStream(os)
      }
      ss.writeAll(iter)
      ss.close()
    }
    rdd.context.runJob(rdd, writePartition _)
    rdd
  }
}

object DirectFileRDDSerializer {
  implicit def toDirectFileRDD[T: ClassTag](rdd: RDD[T]): DirectFileRDDSerializer[T] = new DirectFileRDDSerializer(rdd)
  implicit def toDirectFileSparkContext(sc: SparkContext): DirectFileRDDDeserializer = new DirectFileRDDDeserializer(sc)
}

class DirectFileRDDDeserializer(val sc: SparkContext) {
  def directFile[T: ClassTag](path: String, readClass: Boolean = false): RDD[T] = {
    new DirectFileSerializableRDD[T](sc, path, readClass)
  }
}

