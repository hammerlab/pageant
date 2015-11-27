package org.hammerlab.pageant.scratch

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.reads.Bases
import org.hammerlab.pageant.utils.VarLong
import org.apache.spark.serializer.DirectFileRDDSerializer._

case class BasesTuple(bases: Bases, numFirst: Long, numOther: Long) {
  def b = bases
  def f = numFirst
  def o = numOther
  def num: Long = numFirst + numOther
}

class BasesTupleSerializer extends Serializer[BasesTuple] {
  override def write(kryo: Kryo, output: Output, o: BasesTuple): Unit = {
    kryo.writeObject(output, o.bases)
    VarLong.write(output, o.numFirst)
    VarLong.write(output, o.numOther)
  }

  override def read(kryo: Kryo, input: Input, clz: Class[BasesTuple]): BasesTuple = {
    BasesTuple(
      kryo.readObject(input, classOf[Bases]),
      VarLong.read(input),
      VarLong.read(input)
    )
  }
}

object BasesTuple {
  def apply(t: (Bases, (Long, Long))): BasesTuple = BasesTuple(t._1, t._2._1, t._2._2)
}

object TuplesRDD {

  type TuplesRDD = RDD[BasesTuple]
  type TRDD = TuplesRDD

  def tuplesFn(k: Int) = s"$dir/${k}mers.tuples"

  def loadTuples(k: Int): TuplesRDD = c.directFile[BasesTuple](tuplesFn(k)).setName(s"${k}tuples")

  def sumTuples(ta: (Long, Long), tb: (Long, Long)): (Long, Long) = (ta._1 + tb._1, ta._2 + tb._2)

  def apply(rdd: RDD[Bases], numPartitions: Int = 10000): TuplesRDD = {
    (for {
      bases <- rdd
    } yield {
      (bases, (1L, 0L))
    }).reduceByKey(sumTuples _, numPartitions).map(BasesTuple.apply)
  }

  var _t101: TuplesRDD = null
  def t101: TuplesRDD = {
    if (_t101 == null) {
      _t101 = loadTuples(101).cache
    }
    _t101
  }

  def from101(k: Int,
              numPartitions: Int = 10000,
              cache: Boolean = true): TuplesRDD = {
    stepTupleRdd(t101, k, numPartitions, cache)
  }

  def stepFrom101(from: Int,
                  to: Int,
                  by: Int = -1,
                  numPartitions: Int = 10000,
                  cache: Boolean = false,
                  printCounts: Boolean = false): TuplesRDD = {
    stepFrom(from, to, by, numPartitions, cache, t101, printCounts)
  }

  def stepTupleRdd(rdd: TuplesRDD,
                   k: Int,
                   numPartitions: Int = 10000,
                   cache: Boolean = true): TuplesRDD = {
    val ks = (for {
      BasesTuple(bases, numFirsts, num) <- rdd
      i <- 0 to bases.length - k
      if numFirsts > 0 || (i == bases.length - k && num > 0)
      sub = bases.slice(i, i + k)
    } yield {
      (
        sub,
        (
          if (i == 0) numFirsts else 0L,
          (if (i > 0) numFirsts else 0L) + (if (i == bases.length - k) num else 0L)
        )
      )
    }).reduceByKey(sumTuples _, numPartitions).map(BasesTuple.apply).setName(s"${k}tuples")

    if (cache)
      ks.cache
    else
      ks
  }

  def stepFrom(from: Int,
               to: Int,
               by: Int = -1,
               numPartitions: Int = 10000,
               cache: Boolean = true,
               fromTuples: TuplesRDD = null,
               printCounts: Boolean = true): TuplesRDD = {
    stepSeq(from to to by by, numPartitions, cache, fromTuples, printCounts)
  }

  def stepSeq(idxs: Seq[Int],
              numPartitions: Int = 10000,
              cache: Boolean = true,
              fromTuples: TuplesRDD = null,
              printCounts: Boolean = true,
              saveFiles: Boolean = true,
              alwaysUseFirstAsBase: Boolean = false): TuplesRDD = {
    val sortedIdxs = idxs.sorted.reverse
    val (firstRdd, idxsTodo) = Option(fromTuples) match {
      case Some(rdd) => (rdd, sortedIdxs)
      case _ => (loadTuples(sortedIdxs(0)), sortedIdxs.tail)
    }
    idxsTodo.foldLeft(firstRdd)((tuplesRdd, k) => {
      val r =
        stepTupleRdd(
          if (alwaysUseFirstAsBase)
            firstRdd
          else
            tuplesRdd,
          k,
          numPartitions,
          cache
        )

      if (saveFiles)
        r.saveAsDirectFile(tuplesFn(k))

      if (cache) {
        tuplesRdd.unpersist()
      }
      if (printCounts) {
        val count = r.count
        println(s"${k}tuples: $count")
      }
      r
    })
  }
}
