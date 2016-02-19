package org.apache.spark.serializer

import java.io.EOFException
import java.text.SimpleDateFormat
import java.util.{NoSuchElementException, Date}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapred.{FileSplit, JobID}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat, RecordWriter => NewRecordWriter}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataReadMethod, DataWriteMethod, OutputMetrics}
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.{HadoopPartition, RDD}
import org.apache.spark.util.{NextIterator, SerializableConfiguration, Utils}
import org.apache.spark.{Partition, SerializableWritable, SparkContext, SparkEnv, SparkException, TaskContext}

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
    new NextIterator[T] {
      val file = new Path(filename, DirectFileSerializableRDD.partitionFileName(split.index, gzip))

      val env = SparkEnv.get
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)
      val fs = file.getFileSystem(hadoopConf)
      val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
      val hadoopInputStream = fs.open(file, bufferSize)
      val fileInputStream = if (gzip) new GZIPInputStream(hadoopInputStream) else hadoopInputStream
      val serializer = env.serializer.newInstance()

      val inputMetrics = context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback =
        inputMetrics.bytesReadCallback.orElse(
          SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
        )
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      val (stream, itt) =
        serializer match {
          case ksi: KryoSerializerInstance =>
            val deserializeStream = new KryoObjectDeserializationStream(ksi, fileInputStream, readClass)
            // Register an on-task-completion callback to close the input stream.
            context.addTaskCompletionListener(context => deserializeStream.close())
            (deserializeStream, deserializeStream.asIterator[T])
          case _ =>
            val deserializeStream = serializer.deserializeStream(fileInputStream)
            // Register an on-task-completion callback to close the input stream.
            context.addTaskCompletionListener(context => deserializeStream.close())
            (deserializeStream, deserializeStream.asIterator.asInstanceOf[Iterator[T]])
        }

      val arr = itt.toArray
      val it = arr.toIterator
      context.addTaskCompletionListener{ context => closeIfNeeded() }

      var t: T = _
      override protected def getNext(): T = {
        try {
          t = it.next()
          //finished = !it.hasNext
        } catch {
          case e: EOFException =>
            finished = true
          case e: NoSuchElementException =>
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        t
      }

      override protected def close(): Unit = {
        stream match {
          case k: KryoObjectDeserializationStream => k.close()
          case d: DeserializationStream => d.close()
        }
      }
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

class DirectFileRDDSerializer[T: ClassTag](@transient val rdd: RDD[T])
    extends Serializable
    with SparkHadoopMapReduceUtil {

  def directFile = saveAsDirectFile _
  def saveAsDirectFile(path: String,
                       writeClass: Boolean = false,
                       gzip: Boolean = true,
                       returnOriginal: Boolean = false): RDD[T] = {

    val hadoopConf = rdd.context.hadoopConfiguration
    val job = new NewAPIHadoopJob(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = rdd.id
    val jobConfiguration = SparkHadoopUtil.get.getConfigurationFromJobContext(job)
    val wrappedConf = new SerializableConfiguration(jobConfiguration)

    def writePartition(ctx: TaskContext, iter: Iterator[T]): Unit = {
      val config = wrappedConf.value
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId =
        newTaskAttemptID(
          jobtrackerID,
          stageId,
          isMap = false,
          ctx.partitionId,
          ctx.attemptNumber
        )
      val taskAttemptContext = newTaskAttemptContext(config, attemptId)

      val dir = new Path(path)

      val taskCommitter = new FileOutputCommitter(dir, taskAttemptContext)
      taskCommitter.setupTask(taskAttemptContext)

      val idx = ctx.partitionId()
      val serializer = SparkEnv.get.serializer.newInstance()

      val metrics = ctx.taskMetrics()
      val fs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf))
      val hos = fs.create(new Path(path, DirectFileSerializableRDD.partitionFileName(idx, gzip)))
      val os = if (gzip) new GZIPOutputStream(hos) else hos

      val ss = serializer match {
        case ksi: KryoSerializerInstance => new KryoObjectSerializationStream(ksi, os, writeClass)
        case _ => serializer.serializeStream(os)
      }

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(ctx)
      var recordsWritten = 0L

      Utils.tryWithSafeFinally {
        while (iter.hasNext) {
          val record = iter.next()
          ss.writeObject(record)

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } {
        ss.close()
      }

      SparkHadoopMapRedUtil.commitTask(taskCommitter, taskAttemptContext, ctx.stageId(), ctx.partitionId())
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }

    val jobID = new JobID(jobtrackerID, rdd.id)
    val jobContext = newJobContext(jobConfiguration, jobID)
    val jobCommitter = new FileOutputCommitter(new Path(path), jobContext)

    jobCommitter.setupJob(jobContext)
    rdd.context.runJob(rdd, writePartition _)
    jobCommitter.commitJob(jobContext)

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

  private def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, Option[() => Long]) = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    if (bytesWrittenCallback.isDefined) {
      context.taskMetrics.outputMetrics = Some(outputMetrics)
    }
    (outputMetrics, bytesWrittenCallback)
  }

  val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256
  private def maybeUpdateOutputMetrics(bytesWrittenCallback: Option[() => Long],
                                       outputMetrics: OutputMetrics, recordsWritten: Long): Unit = {
    if (recordsWritten % RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
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

