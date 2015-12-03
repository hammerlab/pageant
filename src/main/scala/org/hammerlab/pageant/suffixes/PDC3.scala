package org.hammerlab.pageant.suffixes

import java.util.Comparator

import org.apache.spark.rdd.RDD
import org.apache.spark.sortwith.SortWithRDD._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object PDC3 {

  type PartitionIdx = Int
  type Name = Long
  type L3 = (Long, Long, Long)
  type L3I = (L3, Long)
  type NameTuple = (PartitionIdx, Name, Long, L3, L3)

//  def pm[T:ClassTag](name: String, r: RDD[T]): Unit = {
//    val partitioned =
//      r
//        .mapPartitionsWithIndex((idx, iter) => iter.map((idx, _)))
//        .groupByKey
//        .collect
//        .sortBy(_._1)
//        .map(p => s"${p._1} -> [ ${p._2.mkString(", ")} ]")
//
//    println(s"$name:\n\t${partitioned.mkString("\n\t")}\n")
//  }

  def name(s: RDD[L3I], N: String): (Boolean, RDD[(Name, Long)]) = {

//    println(s"s: ${s.collect.mkString(",")}")

    val namedTupleRDD: RDD[(Name, L3, Long)] =
      s.mapPartitions(iter => {
        var prevTuples = ArrayBuffer[(Name, L3, Long)]()
        var prevTuple: Option[(Name, L3, Long)] = None
        iter.foreach(cur => {
          val (curTuple, curIdx) = cur
          val curName =
            prevTuple match {
              case Some((prevName, prevLastTuple, prevLastIdx)) =>
                if (prevLastTuple == curTuple)
                  prevName
                else
                  prevName + 1
              case None => 0L
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
          (partitionIdx, lastName, elems.length.toLong, firstTuple, lastTuple)
        }).toIterator
      }).setName(s"$N-name-bound-info")

    val lastTuples = lastTuplesRDD.collect.sortBy(_._1)

//    println(s"lastTuples: ${lastTuples.mkString(", ")}")

    var partitionStartIdxs = ArrayBuffer[(PartitionIdx, Name)]()
    var foundDupes = false
    var prevEndCount = 1L
    var prevLastTupleOpt: Option[L3] = None

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

//    println(s"partitionStartIdxs: $partitionStartIdxs")

    (
      foundDupes,
      namedTupleRDD.mapPartitionsWithIndex((partitionIdx, iter) => {
        partitionStartIdxsBroadcast.value.get(partitionIdx) match {
          case Some(partitionStartName) =>
            for {
              (name, _, idx) <- iter
            } yield {
              (partitionStartName + name, idx)
            }
          case _ =>
            if (iter.nonEmpty)
              throw new Exception(
                s"No partition start idxs found for $partitionIdx: ${iter.mkString(",")}"
              )
            else
              Nil.toIterator
        }
      }).setName(s"$N-named")
    )
  }

  def reverseTuple[T, U](t: (T, U)): (U, T) = (t._2, t._1)

  def toTuple2(l: List[Long]): (Long, Long) = {
    l match {
      case e1 :: Nil => (e1, 0L)
      case es => (es(0), es(1))
    }
  }

//  def toTuple3(l: List[T]): (T, T, T) = {
//    l match {
//      case e1 :: Nil => (e1, 0L, 0L)
//      case e1 :: e2 :: Nil => (e1, e2, 0L)
//      case es => (es(0), es(1), es(2))
//    }
//  }

  type OL = Option[Long]
  case class Joined(t0O: OL = None, t1O: OL = None, n0O: OL = None, n1O: OL = None) {
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
      def get(fn: Joined => OL): OL = {
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
  }

  val orderingL = implicitly[Ordering[Long]]
  val orderingL2 = implicitly[Ordering[(Long, Long)]]
  val orderingL3 = implicitly[Ordering[(Long, Long, Long)]]

  def cmpFn(o1: (Name, Joined), o2: (Name, Joined)): Int = {
    val (i1, j1) = o1
    val (i2, j2) = o2
    (i1 % 3, i2 % 3) match {
      case (0, 0) => orderingL2.compare((j1.t0O.get, j1.n0O.getOrElse(0L)), (j2.t0O.get, j2.n0O.getOrElse(0L)))
      case (0, 1) => orderingL2.compare((j1.t0O.get, j1.n0O.getOrElse(0L)), (j2.t0O.get, j2.n1O.getOrElse(0L)))
      case (1, 0) => orderingL2.compare((j1.t0O.get, j1.n1O.getOrElse(0L)), (j2.t0O.get, j2.n0O.getOrElse(0L)))
      case (0, 2) | (2, 0) => orderingL3.compare((j1.t0O.get, j1.t1O.getOrElse(0L), j1.n1O.getOrElse(0L)), (j2.t0O.get, j2.t1O.getOrElse(0L), j2.n1O.getOrElse(0L)))
      case _ => orderingL.compare(j1.n0O.get, j2.n0O.get)
    }
  }

  def apply(t: RDD[Long]): RDD[Long] = {
    val count = t.count
    apply(t.cache(), count, count / t.partitions.length)
  }

  def apply(t: RDD[Long], n: Long, target: Long): RDD[Long] = {
    println(s"PDC3: $n, $target")
    if (n <= target) {
      val r = t.map(_.toInt).collect()
      return t.context.parallelize(
        KarkainnenSuffixArray.make(r, r.length)
      ).map(_.toLong)
    }

    val (n0, n1, n2) = ((n + 2) / 3, (n + 1) / 3, n / 3)
    val n02 = n0 + n2

    val numDigits = n.toString.length
    val N = s"${n.toString.head}e${numDigits - 1}"

//    println(s"n: $n ($n0, $n1, $n2), n02: $n02")

//    pm("SA", t)

    val ti = t.zipWithIndex().setName(s"$N-zipped-t").cache()

//    pm("ti", ti)

    val tuples: RDD[L3I] =
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
              case e1 :: Nil => (e1, 0L, 0L)
              case e1 :: e2 :: Nil => (e1, e2, 0L)
              case es => (es(0), es(1), es(2))
            }
          }).setName(s"$N-list->tupled;zero-padded")
        .map(reverseTuple).setName(s"$N-tuples")

    val S: RDD[(L3, Long)] =
      (
        if (n % 3 == 1)
          (tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)).setName(s"$N-zero3-appended")
        else
          tuples
      ).map(p => p -> p._2).setName(s"$N-keyed")
       .sortByKey().setName(s"$N-post-sort")
       .map(_._1).setName(s"$N-S")

//    pm("S", S)

    val (foundDupes, named) = name(S, N)

//    println(s"foundDupes: $foundDupes")
//    pm("named", named)

    val P: RDD[(Name, Long)] =
      if (foundDupes) {
        val onesThenTwos = named.sortBy(p => (p._2 % 3, p._2 / 3)).setName(s"$N-onesThenTwos")
//        pm("onesThenTwos", onesThenTwos)

        val SA12: RDD[Long] = this.apply(onesThenTwos.map(_._1), n02, target).setName(s"$N-SA12")

//        pm("SA12", SA12)

        SA12
          .zipWithIndex().setName(s"$N-SA12-zipped")
          .map(p => {
            (
              if (p._1 < n0)
                3*p._1 + 1
              else
                3*(p._1 - n0) + 2,
              p._2 + 1
            )
          }).setName(s"$N-indices-remapped")
          .map(reverseTuple).setName(s"$N-name-index")
          .sortBy(_._2).setName(s"$N-name-index-sorted")

      } else
        named.sortBy(_._2).setName(s"$N-name-index-sorted-no-dupes")

//    pm("P", P)

    val keyedP: RDD[(Long, Joined)] =
      (for {
        (name, idx) <- P
        if idx < n
        i <- idx-2 to idx
        if i >= 0
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

//    pm("keyedP", keyedP)

    val keyedT: RDD[(Long, Joined)] =
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

//    pm("keyedT", keyedT)

    val joined = (keyedT ++ keyedP).setName(s"$N-keyed-T+P").reduceByKey(Joined.merge).setName(s"$N-joined")

//    pm("joined", joined)

    val sorted = joined.sortWith(cmpFn).setName(s"$N-sorted")
//    pm("sorted", sorted)

    sorted.map(_._1).setName(s"$N-ret")
  }
}
