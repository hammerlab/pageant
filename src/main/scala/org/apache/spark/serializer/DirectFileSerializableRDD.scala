package org.apache.spark.serializer

import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, SparkEnv, SparkException, TaskContext}

import scala.reflect.ClassTag

class DirectFileSerializableRDDPartition(val index: Int) extends Partition

class DirectFileSerializableRDD[T: ClassTag](@transient val sc: SparkContext,
                                             filename: String,
                                             readClass: Boolean = false,
                                             gzip: Boolean = true)
  extends RDD[T](sc, Nil) {

  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val path = new Path(filename)
  @transient private val fs = path.getFileSystem(hadoopConf)

  override protected def getPartitions: Array[Partition] = {
    // listStatus can throw exception if path does not exist.
    val inputFiles = fs.listStatus(path)
                     .map(_.getPath)
                     .filter(path => path.getName.startsWith("part-") && (gzip == path.getName.endsWith(".gz")))
                     .sortBy(_.toString)
    // Fail fast if input files are invalid
    inputFiles.zipWithIndex.foreach { case (partFile, i) =>
      if (!partFile.toString.endsWith(DirectFileSerializableRDD.partitionFileName(i, gzip))) {
        throw new SparkException(
          s"Invalid checkpoint file $i: $partFile ${DirectFileSerializableRDD.partitionFileName(i, gzip)}"
        )
      }
    }
    Array.tabulate(inputFiles.length)(i => new DirectFileSerializableRDDPartition(i))

  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(filename, DirectFileSerializableRDD.partitionFileName(split.index, gzip))

    val env = SparkEnv.get
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
    val fs = file.getFileSystem(hadoopConf)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val hadoopInputStream = fs.open(file, bufferSize)
    val fileInputStream = if (gzip) new GZIPInputStream(hadoopInputStream) else hadoopInputStream
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
  def partitionFileName(partitionIndex: Int, gzip: Boolean = true): String = {
    s"part-%05d${if (gzip) ".gz" else ""}".format(partitionIndex)
  }
}

class DirectFileRDDSerializer[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def directFile = saveAsDirectFile _
  def saveAsDirectFile(path: String,
                       writeClass: Boolean = false,
                       gzip: Boolean = true,
                       returnOriginal: Boolean = false): RDD[T] = {
    println(s"saving ${rdd.name} with ${rdd.getNumPartitions} partitions")
    def writePartition(ctx: TaskContext, iter: Iterator[T]): Unit = {
      val idx = ctx.partitionId()
      val serializer = SparkEnv.get.serializer.newInstance()

      val fs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf))
      val hos = fs.create(new Path(path, DirectFileSerializableRDD.partitionFileName(idx, gzip)))
      val os = if (gzip) new GZIPOutputStream(hos) else hos

      val ss = serializer match {
        case ksi: KryoSerializerInstance => new KryoObjectSerializationStream(ksi, os, writeClass)
        case _ => serializer.serializeStream(os)
      }
      ss.writeAll(iter)
      ss.close()
    }
    rdd.context.runJob(rdd, writePartition _)
    if (returnOriginal)
      rdd
    else
      new DirectFileSerializableRDD[T](rdd.context, path, readClass = writeClass, gzip = gzip)
  }

  def direct[U: ClassTag](path: String,
                          fn: RDD[T] => RDD[U],
                          writeClass: Boolean = false,
                          gzip: Boolean = true,
                          returnOriginal: Boolean = false): RDD[U] = {
    val fs = FileSystem.get(rdd.context.hadoopConfiguration)
    import DirectFileRDDSerializer._
    if (fs.exists(new Path(path))) {
      rdd.context.directFile[U](path, writeClass, gzip)
    } else {
      fn(rdd).saveAsDirectFile(path, writeClass, gzip, returnOriginal)
    }
  }
}

object DirectFileRDDSerializer {
  implicit def toDirectFileRDD[T: ClassTag](rdd: RDD[T]): DirectFileRDDSerializer[T] = new DirectFileRDDSerializer(rdd)
  implicit def toDirectFileSparkContext(sc: SparkContext): DirectFileRDDDeserializer = new DirectFileRDDDeserializer(sc)
}

class DirectFileRDDDeserializer(val sc: SparkContext) {
  def directFile[T: ClassTag](path: String, readClass: Boolean = false, gzip: Boolean = true): RDD[T] = {
    new DirectFileSerializableRDD[T](sc, path, readClass, gzip)
  }
}

