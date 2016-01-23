package org.hammerlab.pageant.suffixes

import org.apache.spark.rdd.RDD
import org.apache.spark.sortwith.SortWithRDD._
import org.joda.time.Duration
import org.joda.time.format.PeriodFormatterBuilder

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// done:
// - fix 0-padding subtleties

// probably done:
// - less caching
// - profile GCs

// TODO:
// - PDC7

// not possible?
// - ints vs. longs
// - return indexed SA, save ZWI() job

object PDC3 {

//  type T = Int
//  val zero = 0
//  def ItoT(i: Int): T = i
//  def TtoI(t: T): Int = t
//  def LtoT(l: Long): T = l.toInt

  type T = Long
  val zero = 0L
  def ItoT(i: Int): T = i
  def TtoI(t: T): Int = t.toInt
  def LtoT(l: Long): T = l

  type PartitionIdx = Int
  type Name = T
  type T3 = (T, T, T)
  type T3I = (T3, T)
  type NameTuple = (PartitionIdx, Name, T, T3, T3)

  val debug = false

  def print(s: String) = {
    if (debug) {
      println(s)
    }
  }

  def pm[U:ClassTag](name: String, r: RDD[U]): Unit = {
    if (debug) {
      val partitioned =
        r
          .mapPartitionsWithIndex((idx, iter) => iter.map((idx, _)))
          .groupByKey
          .collect
          .sortBy(_._1)
          .map(p => s"${p._1} -> [ ${p._2.mkString(", ")} ]")

      print(s"$name:\n\t${partitioned.mkString("\n\t")}\n")
    }
  }

  def name(s: RDD[T3I], N: String): (Boolean, RDD[(T, Name)], RDD[(Name, T3, T)]) = {

    //print(s"s: ${s.collect.mkString(",")}")

    val namedTupleRDD: RDD[(Name, T3, T)] =
      s.mapPartitions(iter => {
        var prevTuples = ArrayBuffer[(Name, T3, T)]()
        var prevTuple: Option[(Name, T3, T)] = None
        iter.foreach(cur => {
          val (curTuple, curIdx) = cur
          val curName =
            prevTuple match {
              case Some((prevName, prevLastTuple, prevLastIdx)) =>
                if (prevLastTuple == curTuple)
                  prevName
                else
                  prevName + 1
              case None => zero
            }

          prevTuple = Some((curName, curTuple, curIdx))
          prevTuples += ((curName, curTuple, curIdx))
        })
        prevTuples.toIterator
      }).setName(s"$N-intra-partition-named-tuples").cache()

    val lastTuplesRDD: RDD[NameTuple] =
      namedTupleRDD.mapPartitionsWithIndex((partitionIdx, iter) => {
        val elems = iter.toArray
        elems.headOption.map(_._2).map(firstTuple => {
          val (lastName, lastTuple, _) = elems.last
          (partitionIdx, lastName, ItoT(elems.length), firstTuple, lastTuple)
        }).toIterator
      }).setName(s"$N-name-bound-info")

    val lastTuples = lastTuplesRDD.collect.sortBy(_._1)

    //print(s"lastTuples: ${lastTuples.mkString(", ")}")

    var partitionStartIdxs = ArrayBuffer[(PartitionIdx, Name)]()
    var foundDupes = false
    var prevEndCount = zero + 1
    var prevLastTupleOpt: Option[T3] = None

    lastTuples.foreach(cur => {
      val (partitionIdx, curCount, partitionCount, curFirstTuple, curLastTuple) = cur

      val curStartCount =
        if (prevLastTupleOpt.exists(_ != curFirstTuple))
          prevEndCount + 1
        else
          prevEndCount

      prevEndCount = curStartCount + curCount
      if (!foundDupes &&
        ((curCount + 1 != partitionCount) ||
          prevLastTupleOpt.exists(_ == curFirstTuple))) {
        foundDupes = true
      }
      prevLastTupleOpt = Some(curLastTuple)
      partitionStartIdxs += ((partitionIdx, curStartCount))
    })

    val partitionStartIdxsBroadcast = s.sparkContext.broadcast(partitionStartIdxs.toMap)

    //print(s"partitionStartIdxs: $partitionStartIdxs")

    (
      foundDupes,
      namedTupleRDD.mapPartitionsWithIndex((partitionIdx, iter) => {
        partitionStartIdxsBroadcast.value.get(partitionIdx) match {
          case Some(partitionStartName) =>
            for {
              (name, _, idx) <- iter
            } yield {
              (idx, partitionStartName + name)
            }
          case _ =>
            if (iter.nonEmpty)
              throw new Exception(
                s"No partition start idxs found for $partitionIdx: ${iter.mkString(",")}"
              )
            else
              Nil.toIterator
        }
      }).setName(s"$N-named"),
      namedTupleRDD
    )
  }

//  def toTuple2(l: List[Long]): (Long, Long) = {
//    l match {
//      case e1 :: Nil => (e1, 0L)
//      case es => (es(0), es(1))
//    }
//  }

//  def toTuple3(l: List[T]): (T, T, T) = {
//    l match {
//      case e1 :: Nil => (e1, 0L, 0L)
//      case e1 :: e2 :: Nil => (e1, e2, 0L)
//      case es => (es(0), es(1), es(2))
//    }
//  }

  type OT = Option[T]
  case class Joined(t0O: OT = None, t1O: OT = None, n0O: OT = None, n1O: OT = None) {
    override def toString: String = {
      val s =
        List(
          t0O.getOrElse(" "),
          t1O.getOrElse(" "),
          n0O.getOrElse(" "),
          n1O.getOrElse(" ")
        ).mkString(",")
        s"J($s)"
    }
  }

  object Joined {
    def merge(j1: Joined, j2: Joined): Joined = {
      def get(fn: Joined => OT): OT = {
        (fn(j1), fn(j2)) match {
          case (Some(f1), Some(f2)) => throw new Exception(s"Merge error: $j1 $j2")
          case (f1O, f2O) => f1O.orElse(f2O)
        }
      }
      Joined(
        get(_.t0O),
        get(_.t1O),
        get(_.n0O),
        get(_.n1O)
      )
    }
    def merge(t: (Joined, Joined)): Joined = merge(t._1, t._2)
  }

  val orderingL = implicitly[Ordering[T]]
  val orderingL2 = implicitly[Ordering[(T, T)]]
  val orderingL3 = implicitly[Ordering[(T, T, T)]]

  def cmpFn(o1: (Name, Joined), o2: (Name, Joined)): Int = {
    val (i1, j1) = o1
    val (i2, j2) = o2
    (i1 % 3, i2 % 3) match {
      case (0, 0) =>
        orderingL2.compare(
          (j1.t0O.get, j1.n0O.getOrElse(zero)),
          (j2.t0O.get, j2.n0O.getOrElse(zero))
        )
      case (0, 1) =>
        orderingL2.compare(
          (j1.t0O.get, j1.n0O.getOrElse(zero)),
          (j2.t0O.get, j2.n1O.getOrElse(zero))
        )
      case (1, 0) =>
        orderingL2.compare(
          (j1.t0O.get, j1.n1O.getOrElse(zero)),
          (j2.t0O.get, j2.n0O.getOrElse(zero))
        )
      case (0, 2) | (2, 0) =>
        orderingL3.compare(
          (j1.t0O.get, j1.t1O.getOrElse(zero), j1.n1O.getOrElse(zero)),
          (j2.t0O.get, j2.t1O.getOrElse(zero), j2.n1O.getOrElse(zero))
        )
      case _ =>
        orderingL.compare(
          j1.n0O.get,
          j2.n0O.get
        )
    }
  }

  val formatter = new PeriodFormatterBuilder()
                  .appendDays()
                  .appendSuffix("d")
                  .appendHours()
                  .appendSuffix("h")
                  .appendMinutes()
                  .appendSuffix("m")
                  .appendSeconds()
                  .appendSuffix("s")
                  .toFormatter

  def since(start: Long, now: Long = 0L): String = {
    val n = if (now > 0L) now else System.currentTimeMillis()
    val d = (n - start) / 1000
    if (d >= 60)
      s"${d/60}m${d%60}s"
    else
      s"${d}s"
    //formatter.print(new Duration( - start).toPeriod())
  }

  var lastPrintedTime: Long = 0L

  def apply(t: RDD[T]): RDD[T] = {
    val count = LtoT(t.count)
    val startTime = System.currentTimeMillis()
    lastPrintedTime = startTime
    apply(t.setName("t").cache(), count, count / t.partitions.length, startTime)
  }

  def apply(t: RDD[T], n: T, target: T, startTime: Long): RDD[T] = {
    var phaseStart = System.currentTimeMillis()
//    var lastPrintedTime = lastTime

    val numDigits = n.toString.length
    val N = s"e${numDigits - 1}*${n.toString.head}"

    def pl(s: String): Unit = {
      println(s"${List(since(startTime), since(lastPrintedTime), since(phaseStart), N).mkString("\t")} $s")
      lastPrintedTime = System.currentTimeMillis()
    }

    pl(s"PDC3: $target")

    if (n <= target) {
      val r = t.map(_.toInt).collect()
      return t.context.parallelize(
        KarkainnenSuffixArray.make(r, r.length).map(ItoT)
      )
    }

    //print(s"n: $n ($n0, $n1, $n2), n02: $n02")

    pm("SA", t)

    val ti = t.zipWithIndex().map(p => (p._1, LtoT(p._2))).setName(s"$N-zipped-t")
    ti.checkpoint()

    pm("ti", ti)

    val tuples: RDD[T3I] =
      (for {
        (e, i) <- ti
        j <- i-2 to i
        if j >= 0 && j % 3 != 0
      } yield {
        (j, (e, i))
      }).setName(s"$N-flatmapped")
        .groupByKey().setName(s"$N-tuples-grouped")
        .mapValues(
          ts => {
            ts.toList.sortBy(_._2).map(_._1) match {
              case e1 :: Nil => (e1, zero, zero)
              case e1 :: e2 :: Nil => (e1, e2, zero)
              case es => (es(0), es(1), es(2))
            }
          }).setName(s"$N-list->tupled;zero-padded")
        .map(_.swap).setName(s"$N-tuples")

    val S: RDD[T3I] =
      (
        if (n % 3 == 1)
          (tuples ++ t.context.parallelize(((zero, zero, zero), n) :: Nil)).setName(s"$N-zero3-appended")
        else if (n % 3 == 0)
          (tuples ++ t.context.parallelize(((zero, zero, zero), n+1) :: Nil)).setName(s"$N-zero3-appended")
        else
          tuples
      ).map(_ -> null).setName(s"$N-null-paired")
       .sortByKey().setName(s"$N-post-sort")
       .keys.setName(s"$N-S")

    pm("S", S)

    val (foundDupes, named, intraPartitionsRDD) = name(S, N)

    //print(s"foundDupes: $foundDupes")
    pm("named", named)

    val (n0, n2) = ((n + 2) / 3, n / 3)
    val n1 = (n + 1)/3 +
      (n % 3 match {
        case 0|1 => 1
        case 2 => 0
      })

    val P: RDD[(T, Name)] =
      if (foundDupes) {
        val onesThenTwos =
          named
            .map(p => ((p._1 % 3, p._1 / 3), p._2)).setName(s"$N-mod-div-keyed")
            .sortByKey().setName(s"$N-mod-div-sorted")
            .values.setName(s"$N-onesThenTwos")
        pm("onesThenTwos", onesThenTwos)

        intraPartitionsRDD.unpersist()

        val SA12: RDD[T] = this.apply(onesThenTwos, n1 + n2, target, startTime).setName(s"$N-SA12")
        //pl("Done recursing")
        pm("SA12", SA12)

        SA12
          .zipWithIndex().setName(s"$N-SA12-zipped")
          .map(p => {
            (
              if (p._1 < n1)
                3*p._1 + 1
              else
                3*(p._1 - n1) + 2,
              LtoT(p._2) + 1
            )
          }).setName(s"$N-indices-remapped")
          .sortByKey().setName(s"$N-index-name-sorted")

      } else {
        intraPartitionsRDD.unpersist()
        named.sortByKey().setName(s"$N-index-name-sorted-no-dupes")
      }

    pm("P", P)

    val keyedP: RDD[(T, Joined)] =
      (for {
        (idx, name) <- P
        //if idx < n
        i <- idx-2 to idx
        if i >= 0 && i < n
      } yield {
        val joined =
          (i % 3, idx - i) match {
            case (0, 1) => Joined(n0O = Some(name))
            case (0, 2) => Joined(n1O = Some(name))
            case (1, 0) => Joined(n0O = Some(name))
            case (1, 1) => Joined(n1O = Some(name))
            case (2, 0) => Joined(n0O = Some(name))
            case (2, 2) => Joined(n1O = Some(name))
            case _ => throw new Exception(s"Invalid (idx,i): ($idx,$i); $name")
          }
        (i, joined)
      }).setName(s"$N-keyedP")
        .reduceByKey(Joined.merge).setName(s"$N-reducedP")
    keyedP.checkpoint()

    pm("keyedP", keyedP)

    val keyedT: RDD[(T, Joined)] =
      (for {
        (e, i) <- ti
        start = if (i % 3 == 2) i else i-1
        j <- start to i
        if j >= 0
      } yield {
        val joined =
          (j % 3, i - j) match {
            case (0, 0) => Joined(t0O = Some(e))
            case (0, 1) => Joined(t1O = Some(e))
            case (1, 0) => Joined(t0O = Some(e))
            case (2, 0) => Joined(t0O = Some(e))
            case (2, 1) => Joined(t1O = Some(e))
            case _ => throw new Exception(s"Invalid (i,j): ($i,$j); $e")
          }
        (j, joined)
      }).setName(s"$N-keyedT")
        .reduceByKey(Joined.merge).setName(s"$N-reducedT")

    pm("keyedT", keyedT)

    val joined = keyedT.join(keyedP).mapValues(Joined.merge).setName(s"$N-joined")
    //val joined = (keyedT ++ keyedP).setName(s"$N-keyed-T+P").reduceByKey(Joined.merge).setName(s"$N-joined")

    pm("joined", joined)

    val sorted = joined.sortWith(cmpFn).setName(s"$N-sorted")
    pm("sorted", sorted)

    pl("Returning")
    sorted.map(_._1).setName(s"$N-ret")
  }
}
