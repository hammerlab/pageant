package org.hammerlab.pageant.scratch

import org.apache.hadoop.fs.{FileSystem, Path}
//import BasesRDD._
import TuplesRDD._
import KmerCountRDD._

object Count {

  def countFn(k: Int) = s"$dir/${k}mers.count"
  def countPath(k: Int) = new Path(countFn(k))

  def writeCount(k: Int, rdd: TuplesRDD): Long = {
    val fs = FileSystem.get(c.hadoopConfiguration)
    if (!fs.exists(countPath(k))) {
      writeCount(k, rdd.count)
    } else {
      val is = fs.open(countPath(k))
      val count = is.readLong()
      is.close()
      count
    }
  }
  def writeCount(k: Int, count: Long): Long = {
    val fs = FileSystem.get(c.hadoopConfiguration)
    val os = fs.create(countPath(k))
    os.writeLong(count)
    os.close()
    count
  }

  def stepTuples(fromK: Int,
                 toK: Int,
                 byK: Int = -1,
                 startRddOpt: Option[TuplesRDD] = None,
                 numPartitions: Int = 10000): TuplesRDD =
    (fromK to toK by byK).foldLeft(startRddOpt.getOrElse(loadTuples(fromK - byK)))((rdd, k) => {
      val next = stepTupleRdd(rdd, k, numPartitions)
      next.saveAsObjectFile(tuplesFn(k))
      writeCount(k, next)
      rdd.unpersist()
      next
    })

  def checkTuples(k: Int): (Long, Long) = {
    val rdd = loadTuples(k)
    val count = rdd.count
    val total = rdd.map(_.num).reduce(_ + _)
    println(s"*** Count: $count, Total: $total ***")
    (count, total)
  }

  def getCount(k: Int): Long = {
    val fs = FileSystem.get(c.hadoopConfiguration)
    val path = countPath(k)
    if (fs.exists(path)) {
      val is = fs.open(path)
      val count = is.readLong()
      is.close()
      count
    } else {
      val count =
        if (fs.exists(new Path(tuplesFn(k))))
          loadTuples(k).count
        else if (fs.exists(new Path(countsFn(k)))) {
          loadCounts(k).count
//        else if (fs.exists(new Path(stepsFn(k)))) {
//          val tuples = basesRddToTuplesRdd(loadSteps(k))
//          tuples.saveAsObjectFile(tuplesFn(k))
//          tuples.count
        } else
          throw new Exception(s"Can't find ${tuplesFn(k)} or ${countsFn(k)}")

      writeCount(k, count)
    }
  }

}
