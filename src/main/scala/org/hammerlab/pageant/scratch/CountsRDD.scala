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

  def loadCounts(k: Int): CountsRDD = c.directFile(countsFn(k)).setName(s"${k}counts")

  def kmers(rdd: RDD[Bases],
            k: Int,
            numPartitions: Int = 10000,
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

  var _c101: CountsRDD = null
  def c101: CountsRDD = {
    if (_c101 == null) {
      _c101 = loadCounts(101).cache()
    }
    _c101
  }

  def saveFrom(from: Int,
               to: Int,
               by: Int = -1,
               numPartitions: Int = 10000): Unit = {
    (from to to by by).foreach(k => save(k, numPartitions))
  }

  def save(k: Int,
           numPartitions: Int = 10000): CountsRDD = {
    compute(k, numPartitions, cache = false).saveAsDirectFile(countsFn(k))
  }

  def compute(k: Int,
              numPartitions: Int = 10000,
              cache: Boolean = true): CountsRDD = {
    step(c101, k, numPartitions, cache)
  }

  def step(c101: CountsRDD,
           k: Int,
           numPartitions: Int = 10000,
           cache: Boolean = true): CountsRDD = {
    val kmers =
      (for {
        Counts(bases, count) <- c101
        i <- 0 to bases.length - k
        kmer = bases.slice(i, i + k)
      } yield {
        kmer -> count
      }).reduceByKey(_ + _, numPartitions).map(Counts.apply).setName(s"${k}mers")

    if (cache)
      kmers.cache
    else
      kmers
  }

  def apply(rdd: RDD[Bases]): CountsRDD = {
    (for {
      bases <- rdd
    } yield {
      bases -> 1L
    }).reduceByKey(_ + _).map(Counts.apply)
  }

  def fromTuples(rdd: TuplesRDD): CountsRDD = {
    for {
      BasesTuple(b, f, o) <- rdd
    } yield {
      Counts(b, f+o)
    }
  }
}
