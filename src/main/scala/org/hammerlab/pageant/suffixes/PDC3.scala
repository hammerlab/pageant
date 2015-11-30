package org.hammerlab.pageant.suffixes

import java.util.Comparator

import org.apache.spark.rdd.RDD
import org.apache.spark.sortwith.SortWithRDD._

object PDC3 {

  type PartitionIdx = Int
  type Name = Long
  type L3 = (Long, Long, Long)
  type L3I = (L3, Long)
  type NameTuple = (PartitionIdx, Name, Long, L3, L3)

  def name(s: RDD[L3I]): (Boolean, RDD[(Name, Long)]) = {

    val namedTupleRDD: RDD[(Name, L3, Long)] = {
      s.mapPartitions(iter => {
        iter.foldLeft[Vector[(Name, L3, Long)]](Vector())((prevTuples, cur) => {
          val (curTuple, curIdx) = cur
          val curName =
            prevTuples.lastOption match {
              case Some((prevName, prevLastTuple, prevLastIdx)) =>
                if (prevLastTuple == curTuple)
                  prevName
                else
                  prevName + 1
              case None => 0L
            }

          prevTuples :+ (curName, curTuple, curIdx)
        }).toIterator
      })
    }

    val lastTuplesRDD: RDD[NameTuple] =
      namedTupleRDD.mapPartitionsWithIndex((partitionIdx, iter) => {
        val elems = iter.toArray
        elems.headOption.map(_._2).map(firstTuple => {
          val (lastName, lastTuple, _) = elems.last
          (partitionIdx, lastName, elems.length.toLong, firstTuple, lastTuple)
        }).toIterator
      })

    val lastTuples = lastTuplesRDD.collect.sortBy(_._1)

    val (_, _, foundDupes, partitionStartIdxs) =
      lastTuples.foldLeft[(Name, Option[L3], Boolean, Vector[(PartitionIdx, Name)])]((1L, None, false, Vector()))((prev, cur) => {
        val (prevEndCount, prevLastTupleOpt, prevFoundDupes, prevTuples) = prev

        val (partitionIdx, curCount, partitionCount, curFirstTuple, curLastTuple) = cur

        val curStartCount =
          if (prevLastTupleOpt.exists(_ != curFirstTuple))
            prevEndCount + 1
          else
            prevEndCount

        val curEndCount = curStartCount + curCount

        val foundDupes =
          prevFoundDupes ||
            (curCount != partitionCount) ||
            prevLastTupleOpt.exists(_ == curFirstTuple)

        (
          curEndCount,
          Some(curLastTuple),
          foundDupes,
          prevTuples :+ (partitionIdx, curStartCount)
        )
      })

    val partitionStartIdxsBroadcast = s.sparkContext.broadcast(partitionStartIdxs.toMap)

    (
      foundDupes,
      namedTupleRDD.mapPartitionsWithIndex((partitionIdx, iter) => {
        val partitionStartName = partitionStartIdxsBroadcast.value(partitionIdx)
        for {
          (name, _, idx) <- iter
        } yield {
          (partitionStartName + name, idx)
        }
      })
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
  case class Joined(t0O: OL = None, t1O: OL = None, n0O: OL = None, n1O: OL = None)

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

  //type Joined = (Long, )

  def apply(t: RDD[Long]): RDD[Long] = {
    val ti = t.zipWithIndex()
    val S = (for {
      (e, i) <- ti
      if i % 3 != 0
      j <- i-2 to i
      if j >= 0
    } yield {
      (j, (e, i))
    }).groupByKey().mapValues(ts => {
      ts.toList.sortBy(_._2).map(_._1) match {
        case e1 :: Nil => (e1, 0L, 0L)
        case e1 :: e2 :: Nil => (e1, e2, 0L)
        case es => (es(0), es(1), es(2))
      }
    }).map(reverseTuple).sortByKey()

    val (foundDupes, named) = name(S)

    val P =
      if (foundDupes) {
        val onesThenTwos = named.sortBy(p => (p._2 % 3, p._2 / 3))
        //val numOnes = named.filter(_._2 % 3 == 1).count
        val SA12: RDD[Long] = this.apply(onesThenTwos.map(_._1))
        val SA12I: RDD[Name] = SA12.zipWithIndex().sortBy(_._1).map(_._2)

        SA12I.zip(onesThenTwos.map(_._2)).sortBy(_._2)
      } else
        named.sortBy(_._2)

    val keyedP: RDD[(Long, Joined)] =
      for {
        (name, idx) <- P
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
      }

    val keyedT: RDD[(Long, Joined)] =
      for {
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
      }

    val joined = (keyedT ++ keyedP).reduceByKey(Joined.merge)

    def cmpFn(o1: (Name, Joined), o2: (Name, Joined)): Int = {
      val (i1, j1) = o1
      val (i2, j2) = o2
      val ordering2 = implicitly[Ordering[(Long, Long)]]
      val ordering3 = implicitly[Ordering[(Long, Long, Long)]]
      (i1 % 3, i2 % 3) match {
        case (0, 0) => ordering2.compare((j1.t0O.get, j1.n0O.get), (j2.t0O.get, j2.n0O.get))
        case (0, 1) => ordering2.compare((j1.t0O.get, j1.n0O.get), (j2.t0O.get, j2.n0O.get))
        case (0, 2) => ordering3.compare((j1.t0O.get, j1.t1O.get, j1.n1O.get), (j2.t0O.get, j2.t1O.get, j2.n1O.get))
        case (1, 0) => ordering2.compare((j2.t0O.get, j2.n0O.get), (j1.t0O.get, j1.n0O.get))
        case (2, 0) => ordering3.compare((j2.t0O.get, j2.t1O.get, j2.n1O.get), (j1.t0O.get, j1.t1O.get, j1.n1O.get))
        case _ => implicitly[Ordering[Long]].compare(j1.t0O.get, j2.t0O.get)
      }
    }

    joined.sortWith(cmpFn).map(_._1)
  }
}
