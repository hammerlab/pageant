package org.hammerlab.pageant.scratch

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.reads.Bases
import org.hammerlab.pageant.utils.VarLong

case class BasesTuple(bases: Bases, numFirst: Long, numOther: Long) {
  def b = bases
  def f = numFirst
  def o = numOther
  def num: Long = numFirst + numOther

  //def +(o: BasesTuple): BasesTuple = B
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

  def tuplesFn(k: Int) = s"$dir/${k}mers.tuples"

  def loadTuples(k: Int): TuplesRDD = sc.objectFile(tuplesFn(k)).setName(s"${k}tuples")

  def sumTuples(ta: (Long, Long), tb: (Long, Long)): (Long, Long) = (ta._1 + tb._1, ta._2 + tb._2)

  def apply(rdd: RDD[Bases], numPartitions: Int = 10000): TuplesRDD = {
    (for {
      bases <- rdd
    } yield {
      (bases, (1L, 0L))
    }).reduceByKey(sumTuples(_, _), numPartitions).map(BasesTuple.apply)
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
    }).reduceByKey(sumTuples(_, _), numPartitions).map(BasesTuple.apply).setName(s"${k}tuples")
    if (cache) ks.cache
    else ks
  }
}
