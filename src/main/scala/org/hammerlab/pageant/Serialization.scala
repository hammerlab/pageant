package org.hammerlab.pageant

import java.io.{ByteArrayInputStream, InputStream, OutputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkException, Partition, TaskContext, SparkEnv, SparkContext}
import org.apache.spark.rdd.{CheckpointRDDPartition, RDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.{Utils => SparkUtils}

import scala.reflect.ClassTag

object Serialization {

  def javaRead[T](bytes: Array[Byte])(implicit sc: SparkContext): T = {
    javaRead(new ByteArrayInputStream(bytes))
  }
  def javaRead[T](is: InputStream)(implicit sc: SparkContext): T = {
    val ois = new ObjectInputStream(is)
    val o = ois.readObject().asInstanceOf[T]
    ois.close()
    o
  }

  def javaRead[T](fn: String)(implicit sc: SparkContext): T = {
    javaRead(FileSystem.get(sc.hadoopConfiguration).open(new Path(fn)))
  }

  def javaWrite(o: Object, fn: String)(implicit sc: SparkContext): Unit = {
    javaWrite(o, FileSystem.get(sc.hadoopConfiguration).create(new Path(fn)))
  }

  def javaWrite(o: Object, os: OutputStream)(implicit sc: SparkContext): Unit = {
    val oos = new ObjectOutputStream(os)
    oos.writeObject(o)
    oos.close()
  }

  def javaBytes(o: Object)(implicit sc: SparkContext): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    javaWrite(o, baos)
    baos.toByteArray
  }

  def kryoRead[T](bytes: Array[Byte])(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](new Input(bytes))
  }
  def kryoRead[T](is: InputStream)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](new Input(is))
  }

  def kryoRead[T](fn: String)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    kryoRead[T](FileSystem.get(sc.hadoopConfiguration).open(new Path(fn)))
  }

//  def kryoRead[T](bytes: Array[Byte])(implicit ct: ClassTag[T], sc: SparkContext): T = {
//    kryoRead[T](new Input(bytes))
//  }

  def kryoRead[T](ip: Input)(implicit ct: ClassTag[T], sc: SparkContext): T = {
    val ks = new KryoSerializer(sc.getConf)
    val kryo = ks.newKryo()

    val o = kryo.readObject(ip, ct.runtimeClass).asInstanceOf[T]
    ip.close()
    o
  }

  def kryoWrite(o: Object, os: OutputStream)(implicit sc: SparkContext): Unit = {
    val ks = new KryoSerializer(sc.getConf)
    val kryo = ks.newKryo()

    val op = new Output(os)
    kryo.writeObject(op, o)
    op.close()
  }

  def kryoWrite(o: Object, fn: String)(implicit sc: SparkContext): Unit = {
    kryoWrite(o, FileSystem.get(sc.hadoopConfiguration).create(new Path(fn)))
  }

  def kryoBytes(o: Object)(implicit sc: SparkContext): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    kryoWrite(o, baos)
    baos.toByteArray
  }

}

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

  @DeveloperApi
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
  /**
    * Return the file name for the given partition.
    */
  private def partitionFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }
}

class CheckpointRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {
  def serializeToFileDirectly(path: String): Unit = {
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

  def serializeToFile(path: String): RDD[T] = {
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
  def fromFile[T](path: String)(implicit ct: ClassTag[T]): RDD[T] = {
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

object CheckpointRDD {
  implicit def toCheckpointRDD[T: ClassTag](rdd: RDD[T]): CheckpointRDD[T] = new CheckpointRDD(rdd)
  implicit def toSerdeSparkContext(sc: SparkContext): SerdeSparkContext = new SerdeSparkContext(sc)
}