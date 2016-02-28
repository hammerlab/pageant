package org.hammerlab.pageant.scratch

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.hammerlab.pageant.reads.Bases
import org.hammerlab.pageant.utils.VarNum

import TuplesRDD._

case class KmerCount(bases: Bases, num: Long) {
  def b = bases
  def n = num
}

class CountsSerializer extends Serializer[KmerCount] {
  override def write(kryo: Kryo, output: Output, o: KmerCount): Unit = {
    kryo.writeObject(output, o.bases)
    VarNum.write(output, o.num)
  }

  override def read(kryo: Kryo, input: Input, clz: Class[KmerCount]): KmerCount = {
    KmerCount(
      kryo.readObject(input, classOf[Bases]),
      VarNum.read(input)
    )
  }
}

object KmerCount {
  def apply(t: (Bases, Long)): KmerCount = KmerCount(t._1, t._2)
}


object KmerCountRDD {

  import org.apache.spark.rdd.RDD

  type KmerCountRDD = RDD[KmerCount]

  def countsFn(k: Int) = s"$dir/${k}mers.counts"

  def loadCounts(k: Int): KmerCountRDD = c.directFile(countsFn(k)).setName(s"${k}counts")

  def kmers(rdd: RDD[Bases],
            k: Int,
            numPartitions: Int = 10000,
            cache: Boolean = true): KmerCountRDD = {
    val ks = (for {
      bases <- rdd
      i <- 0 to bases.length - k
      sub = bases.slice(i, i + k)
    } yield {
      sub -> 1L
    }).reduceByKey(_ + _, numPartitions).map(KmerCount.apply).setName(s"${k}mers")
    if (cache) ks.cache
    else ks
  }

  var _c101: KmerCountRDD = null
  def c101: KmerCountRDD = {
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
           numPartitions: Int = 10000): KmerCountRDD = {
    compute(k, numPartitions, cache = false).saveAsDirectFile(countsFn(k))
  }

  def compute(k: Int,
              numPartitions: Int = 10000,
              cache: Boolean = true): KmerCountRDD = {
    step(c101, k, numPartitions, cache)
  }

  def step(c101: KmerCountRDD,
           k: Int,
           numPartitions: Int = 10000,
           cache: Boolean = true): KmerCountRDD = {
    val kmers =
      (for {
        KmerCount(bases, count) <- c101
        i <- 0 to bases.length - k
        kmer = bases.slice(i, i + k)
      } yield {
        kmer -> count
      }).reduceByKey(_ + _, numPartitions).map(KmerCount.apply).setName(s"${k}mers")

    if (cache)
      kmers.cache
    else
      kmers
  }

  def apply(rdd: RDD[Bases]): KmerCountRDD = {
    (for {
      bases <- rdd
    } yield {
      bases -> 1L
    }).reduceByKey(_ + _).map(KmerCount.apply)
  }

  def fromTuples(rdd: TuplesRDD): KmerCountRDD = {
    for {
      BasesTuple(b, f, o) <- rdd
    } yield {
      KmerCount(b, f + o)
    }
  }
}
