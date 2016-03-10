package org.hammerlab.pageant.suffixes.pdc3

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DirectFileRDDSerializer._
import org.apache.spark.sortwith.SortWithRDD._
import org.hammerlab.pageant.rdd.SlidingRDD._
import org.hammerlab.pageant.suffixes.dc3.DC3
import org.hammerlab.pageant.utils.Utils.longToCmpFnReturn
import org.joda.time.format.PeriodFormatterBuilder

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// done:
// - fix 0-padding subtleties
// - less caching

// probably done:
// - profile GCs

// TODO:
// - PDC7

// not possible?
// - return indexed SA, save ZWI() job

//case class NameTuple

case class CheckpointConfig(dir: Option[String],
                            whitelist: Set[String],
                            blacklist: Set[String],
                            unzip: Set[String],
                            writeClasses: Set[String])

object PDC3 {

  type L = Long

  type PartitionIdx = Int
  type Name = L
  type L3 = (L, L, L)
  type L3I = (L3, L)
  type NameTuple = (PartitionIdx, Name, L, L3, L3)
  type OL = Option[L]

  def apply(t: RDD[L]): RDD[L] = {
    apply(t, t.count)
  }

  def apply(t: RDD[L],
            count: Long,
            backupPath: String = null,
            backupWhitelist: Set[String] = Set(),
            backupBlacklist: Set[String] = Set(),
            unzip: Set[String] = Set(),
            includeClasses: Set[String] = Set()): RDD[L] = {
    val startTime = System.currentTimeMillis()
    withCount(
      t.setName("t").cache(),
      count,
      count / t.getNumPartitions,
      startTime,
      backupPath,
      backupWhitelist,
      backupBlacklist,
      unzip,
      includeClasses
    )
  }

  def withIndices(ti: RDD[(L, Long)],
                  count: Long,
                  backupPath: String = null,
                  backupWhitelist: Set[String] = Set(),
                  backupBlacklist: Set[String] = Set(),
                  unzip: Set[String] = Set(),
                  includeClasses: Set[String] = Set()): RDD[L] = {
    val startTime = System.currentTimeMillis()
    lastPrintedTime = startTime
    withIndices(ti, count, count / ti.getNumPartitions, startTime, backupPath, backupWhitelist, backupBlacklist, unzip, includeClasses)
  }

  def withIndices(ti: RDD[(L, Long)],
                  n: L,
                  target: L,
                  startTime: Long,
                  backupPath: String,
                  backupWhitelist: Set[String],
                  backupBlacklist: Set[String],
                  unzip: Set[String],
                  includeClasses: Set[String]): RDD[L] = {
    saImpl(None, Some(ti), n, target, startTime, backupPath, backupWhitelist, backupBlacklist, unzip, includeClasses)
  }

  def withCount(t: RDD[L],
                n: L,
                target: L,
                startTime: Long,
                backupPath: String,
                backupWhitelist: Set[String] = Set(),
                backupBlacklist: Set[String] = Set(),
                unzip: Set[String] = Set(),
                includeClasses: Set[String] = Set()): RDD[L] = {
    saImpl(Some(t), None, n, target, startTime, backupPath, backupWhitelist, backupBlacklist, unzip, includeClasses)
  }

  val debug = false

  def print(s: String) = {
    if (debug) {
      println(s)
    }
  }

  def pm[U:ClassTag](name: String, r: () => RDD[U]): Unit = {
    if (debug) {
      val partitioned =
        r()
        .mapPartitionsWithIndex((idx, iter) => iter.map((idx, _)))
        .groupByKey
        .collect
        .sortBy(_._1)
        .map(p => s"${p._1} -> [ ${p._2.mkString(", ")} ]")

      print(s"$name:\n\t${partitioned.mkString("\n\t")}\n")
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
  }

  var lastPrintedTime: Long = 0L

  def equivalentTuples(t1: L3, t2: L3): Boolean = {
    t1 == t2 &&
      t1._1 != 0L && t1._2 != 0L && t1._3 != 0L &&
      t2._1 != 0L && t2._2 != 0L && t2._3 != 0L
  }

  def cmp2(t1: (L, L, L), t2: (L, L, L)): Int = {
    longToCmpFnReturn(
      if (t1._1 == t2._1)
        if (t1._1 == 0L)
          t1._3 - t2._3
        else
          t1._2 - t2._2
      else
        t1._1 - t2._1
    )
  }

  def cmp3(t1: L3I, t2: L3I): Int = cmp3((t1._1._1, t1._1._2, t1._1._3, t1._2), (t2._1._1, t2._1._2, t2._1._3, t2._2))
  def cmp3(t1: (L, L, L, L), t2: (L, L, L, L)): Int = {
    longToCmpFnReturn(
      if (t1._1 == t2._1)
        if (t1._1 == 0L)
          t1._4 - t2._4
        else if (t1._2 == t2._2)
          if (t1._2 == 0L)
            t1._4 - t2._4
          else if (t1._3 == t2._3)
            t1._4 - t2._4
          else
            t1._3 - t2._3
        else
          t1._2 - t2._2
      else
        t1._1 - t2._1
    )
  }

  def cmpFn(o1: (Name, Joined), o2: (Name, Joined)): Int = {
    val (i1, j1) = o1
    val (i2, j2) = o2
    (i1 % 3, i2 % 3) match {
      case (0, 0) =>
        cmp2(
          (j1.t0O.get, j1.n0O.getOrElse(0L), i1),
          (j2.t0O.get, j2.n0O.getOrElse(0L), i2)
        )
      case (0, 1) =>
        cmp2(
          (j1.t0O.get, j1.n0O.getOrElse(0L), i1),
          (j2.t0O.get, j2.n1O.getOrElse(0L), i2)
        )
      case (1, 0) =>
        cmp2(
          (j1.t0O.get, j1.n1O.getOrElse(0L), i1),
          (j2.t0O.get, j2.n0O.getOrElse(0L), i2)
        )
      case (0, 2) | (2, 0) =>
        cmp3(
          (j1.t0O.get, j1.t1O.getOrElse(0L), j1.n1O.getOrElse(0L), i1),
          (j2.t0O.get, j2.t1O.getOrElse(0L), j2.n1O.getOrElse(0L), i2)
        )
      case _ =>
        longToCmpFnReturn(j1.n0O.get - j2.n0O.get)
    }
  }

  def saImpl(tOpt: Option[RDD[L]],
             tiOpt: Option[RDD[(L, Long)]],
             n: L,
             target: L,
             startTime: Long,
             backupPath: String,
             backupWhitelist: Set[String],
             backupBlacklist: Set[String],
             unzip: Set[String],
             includeClasses: Set[String]): RDD[L] = {

    val backupPathOpt = Option(backupPath)
    val sc = tOpt.orElse(tiOpt).get.context
    val fs = FileSystem.get(sc.hadoopConfiguration)

    var phaseStart = System.currentTimeMillis()

    val numDigits = n.toString.length
    val N = s"e${numDigits - 1}·${n.toString.head}"

    def backupAny[U: ClassTag](name: String, fn: () => U): U = {
      backupPathOpt match {
        case Some(bp) if !backupBlacklist(name) && (backupWhitelist.isEmpty || backupWhitelist(name)) =>
          val pathStr = s"$bp/$n-$name"
          val path = new Path(pathStr)
          if (fs.exists(path)) {
            val ois = new ObjectInputStream(fs.open(path))
            val u = ois.readObject().asInstanceOf[U]
            ois.close()
            u
          } else {
            val u = fn()
            val oos = new ObjectOutputStream(fs.create(path))
            oos.writeObject(u)
            oos.close()
            u
          }
        case _ => fn()
      }
    }

    def backup[U: ClassTag](name: String, fn: () => RDD[U]): RDD[U] = {
      (backupPathOpt match {
        case Some(bp) if !backupBlacklist(name) || (backupBlacklist.isEmpty && backupWhitelist(name)) =>
          val path = s"$bp/$n/$name"
          val donePath = new Path(s"$path.done")
          if (fs.exists(new Path(path)) && fs.exists(donePath)) {
            sc.directFile[U](path, includeClasses(name), !unzip(name))
          } else {
            val rdd = fn().setName(s"$n-$name").saveAsDirectFile(path, includeClasses(name), !unzip(name))
            fs.create(donePath).writeLong(1L)
            rdd
          }
        case _ => fn()
      }).setName(s"$N-$name")
    }

    def pl(s: String): Unit = {
      println(s"${List(since(startTime), since(lastPrintedTime), since(phaseStart), s"$N ($n)").mkString("\t")} $s")
      lastPrintedTime = System.currentTimeMillis()
    }

    val numPartitions = tOpt.orElse(tiOpt).get.getNumPartitions

    pl(s"PDC3: $target ($n/$numPartitions)")

    lazy val t = tOpt.getOrElse(tiOpt.get.keys)

    if (n <= target || numPartitions == 1) {
      val r = t.map(_.toInt).collect()
      return t.context.parallelize(
        DC3.make(r, r.length).map(_.toLong)
      )
    }

    pm("SA", () => t)

    val ti = backup("ti", () => tiOpt.getOrElse(t.zipWithIndex()))

    pm("ti", () => ti)

    val tuples: RDD[L3I] =
      if (n / t.getNumPartitions < 2)
        backup("tuples", () =>
          (for {
            (e, i) <- ti
            j <- i-2 to i
            if j >= 0 && j % 3 != 0
          } yield {
            (j, (e, i))
          })
          .setName(s"$N-flatmapped")
          .groupByKey()
          .setName(s"$N-tuples-grouped")
          .mapValues(
            ts => {
              ts.toList.sortBy(_._2).map(_._1) match {
                case e1 :: Nil => (e1, 0L, 0L)
                case e1 :: e2 :: Nil => (e1, e2, 0L)
                case es => (es(0), es(1), es(2))
              }
            })
          .setName(s"$N-list->tupled;zero-padded")
          .map(_.swap)
        )
      else {
        backup("sliding", () => t.sliding3(0).zipWithIndex().filter(_._2 % 3 != 0))
      }

    //println(s"${tuples.collect.map { case ((t1,t2,t3),i) => s"$i -> ($t1,$t2,$t3)"} mkString("\n")}")

    val padded =
      if (n % 3 == 0)
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n+1) :: Nil)
      else if (n % 3 == 1)
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)
      else
//        tuples
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)

//    val padded =
//      if (n % 3 == 0)
//        tuples ++ t.context.parallelize(((0L, 0L, 0L), n+1) :: Nil)
//      else
//        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)

    // All [12]%3 triplets and indexes, sorted.
    val S: RDD[L3I] = backup("S", () => padded.sortWith(cmp3))

    pm("S", () => S)

    def name(s: RDD[L3I], N: String): (Boolean, RDD[(L, Name)]) = {

      val namedTupleRDD: RDD[(Name, L3, L)] =
        s.mapPartitions(iter => {
          var prevTuples = ArrayBuffer[(Name, L3, L)]()
          var prevTuple: Option[(Name, L3, L)] = None
          iter.foreach(cur => {
            val (curTuple, curIdx) = cur
            val curName =
              prevTuple match {
                case Some((prevName, prevLastTuple, prevLastIdx)) =>
                  if (equivalentTuples(prevLastTuple, curTuple))
                    prevName
                  else
                    prevName + 1
                case None => 0L
              }

            prevTuple = Some((curName, curTuple, curIdx))
            prevTuples += ((curName, curTuple, curIdx))
          })
          prevTuples.toIterator
        })

      var foundDupes = false

      val named =
        backup[(L, Long)](
          "named",
          () => {

            val lastTuplesRDD: RDD[NameTuple] =
              namedTupleRDD.mapPartitionsWithIndex((partitionIdx, iter) => {
                if (iter.hasNext) {
                  var last: (Name, L3, L) = iter.next
                  val firstTuple = last._2
                  var len = 1
                  while (iter.hasNext) {
                    last = iter.next()
                    len += 1
                  }
                  val (lastName, lastTuple, _) = last
                  Array((partitionIdx, lastName, len.toLong, firstTuple, lastTuple)).toIterator
                } else {
                  Iterator()
                }
              })

            val lastTuples = lastTuplesRDD.collect.sortBy(_._1)

            var partitionStartIdxs = ArrayBuffer[(PartitionIdx, Name)]()
            var prevEndCount = 1L
            var prevLastTupleOpt: Option[L3] = None

            lastTuples.foreach(cur => {
              val (partitionIdx, curCount, partitionCount, curFirstTuple, curLastTuple) = cur

              val curStartCount =
                if (prevLastTupleOpt.exists(!equivalentTuples(_, curFirstTuple)))
                  prevEndCount + 1
                else
                  prevEndCount

              prevEndCount = curStartCount + curCount
              if (!foundDupes &&
                ((curCount + 1 != partitionCount) ||
                  prevLastTupleOpt.exists(equivalentTuples(_, curFirstTuple)))) {
                foundDupes = true
              }
              prevLastTupleOpt = Some(curLastTuple)
              partitionStartIdxs += ((partitionIdx, curStartCount))
            })

            foundDupes = backupAny[Boolean]("foundDupes", () => foundDupes)

            val partitionStartIdxsBroadcast = s.sparkContext.broadcast(partitionStartIdxs.toMap)

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
                      List(
                        s"No partition start idxs found for partition $partitionIdx:",
                        s"\t${partitionStartIdxsBroadcast.value.toList.map(p => s"${p._1} -> ${p._2}").mkString("\n\t")}",
                        s"${iter.take(100).mkString(",")}"
                      ).mkString("\n\n")
                    )
                  else
                    Nil.toIterator
              }
            })
          }
        )

      (foundDupes, named)
    }

    val (n0, n2) = ((n + 2) / 3, (n + 1) / 3)
    val n1 = (n + 1)/3 +
      (n % 3 match {
        case 0|1 => 1
        case 2 => 0
      })

    val P: RDD[(L, Name)] =
      backup(
        "P",
        () => {
          val (foundDupes, named) = name(S, N)
          pm("named", () => named)

          if (foundDupes) {
            val onesThenTwos =
              backup(
                "ones-then-twos",
                () => named
                        .map(p => ((p._1 % 3, p._1 / 3), p._2)).setName(s"$N-mod-div-keyed")
                        .sortByKey(numPartitions = ((n1 + n2) / target).toInt).setName(s"$N-mod-div-sorted")
                        .values
              )
            pm("onesThenTwos", () => onesThenTwos)

            val SA12: RDD[L] = saImpl(
              Some(onesThenTwos),
              None,
              n1 + n2,
              target,
              startTime,
              backupPath,
              backupWhitelist,
              backupBlacklist,
              unzip,
              includeClasses
            ).setName(s"$N-SA12")

            //pl("Done recursing")
            pm("SA12", () => SA12)

            SA12
            .zipWithIndex().setName(s"$N-SA12-zipped")
            .map(p => {
              (
                if (p._1 < n1)
                  3 * p._1 + 1
                else
                  3 * (p._1 - n1) + 2,
                p._2 + 1
              )
            }).setName(s"$N-indices-remapped")
            .sortByKey()
          } else {
            named.sortByKey()
          }
        }
      )

    pm("P", () => P)

    val keyedP: RDD[(L, Joined)] =
      backup("keyedP", () =>
        (for {
          (idx, name) <- P
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
        .reduceByKey(Joined.merge _, numPartitions = t.getNumPartitions)
      )

    pm("keyedP", () => keyedP)

    val keyedT: RDD[(L, Joined)] =
      backup("keyedT", () =>
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
        .reduceByKey(Joined.merge)
      )

    pm("keyedT", () => keyedT)

    val joined = backup("joined", () => keyedT.join(keyedP).mapValues(Joined.mergeT))
    //val joined = (keyedT ++ keyedP).setName(s"$N-keyed-T+P").reduceByKey(Joined.merge).setName(s"$N-joined")

    pm("joined", () => joined)

    val sorted = backup("sorted", () => joined.sortWith(cmpFn).map(_._1))
    pm("sorted", () => sorted)

    val num = sorted
      .map(_ → null)
      .sortByKey()
      .keys
      .zipWithIndex
      .map(t ⇒
        if (t._1 != t._2)
          throw new Exception(s"idx ${t._2}: ${t._1}")
      ).count()

    pl("Returning")
    sorted
  }

}
