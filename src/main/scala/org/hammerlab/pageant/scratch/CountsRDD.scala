package org.hammerlab.pageant.scratch

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.reads.Bases
import org.hammerlab.pageant.utils.VarLong

import TuplesRDD._

case class Counts(bases: Bases, num: Long) {
  def b = bases
  def n = num
}

class CountsSerializer extends Serializer[Counts] {
  override def write(kryo: Kryo, output: Output, o: Counts): Unit = {
    kryo.writeObject(output, o.bases)
    VarLong.write(output, o.num)
  }

  override def read(kryo: Kryo, input: Input, clz: Class[Counts]): Counts = {
    Counts(
      kryo.readObject(input, classOf[Bases]),
      VarLong.read(input)
    )
  }
}

object Counts {
  def apply(t: (Bases, Long)): Counts = Counts(t._1, t._2)
}


object CountsRDD {

  import org.apache.spark.rdd.RDD

  type CountsRDD = RDD[Counts]

  def countsFn(k: Int) = s"$dir/${k}mers.counts"
  def kountsFn(k: Int) = s"$dir/${k}mers.kounts"

  def loadCountsObjFile(k: Int): CountsRDD = sc.objectFile(countsFn(k)).setName(s"${k}counts")
  def loadCountsDirectFile(k: Int): CountsRDD = sc.directFile(kountsFn(k)).setName(s"${k}kounts")

  def kmers(rdd: RDD[Bases],
            k: Int,
            numPartitions: Int = 5000,
            cache: Boolean = true): CountsRDD = {
    val ks = (for {
      bases <- rdd
      i <- 0 to bases.length - k
      sub = bases.slice(i, i + k)
    } yield {
      sub -> 1L
    }).reduceByKey(_ + _, numPartitions).map(Counts.apply).setName(s"${k}mers")
    if (cache) ks.cache
    else ks
  }

  def apply(rdd: TuplesRDD): CountsRDD = {
    for {
      BasesTuple(b, f, o) <- rdd
    } yield {
      Counts(b, f+o)
    }
  }
}
