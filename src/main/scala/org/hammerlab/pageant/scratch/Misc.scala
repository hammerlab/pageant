package org.hammerlab.pageant.scratch

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.hammerlab.pageant.reads.Bases

/**
  * Created by ryan on 11/24/15.
  */
trait Misc {

  def bytes(o: Object): Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(os)
    oos.writeObject(o)
    oos.close()
    os.toByteArray
  }

  def bases(s: String): ByteArrayOutputStream = {
    val os = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(os)
    val b = Bases(s)
    oos.writeObject(b)
    oos.close()
    os
  }

  import TuplesRDD.loadTuples

  type Hist = Map[Long, Long]
  type Hists = Map[Int, Hist]

  def histFn(k: Int) = s"$filePrefix.${k}mers.hist"

  def hist(k: Int, numPartitions: Int = 5000) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(histFn(k))
    val h =
      (for {
        (_, (first, other)) <- loadTuples(k)
        num = first + other
      } yield {
        num -> 1L
      }).reduceByKey(_ + _, numPartitions).sortByKey().collect()

    val os = fs.create(path)
    h.foreach(p => os.writeBytes("%d,%d\n".format(p._1, p._2)))
    os.close()
  }

  def loadHist(k: Int): Hist = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(histFn(k))
    val is = fs.open(path)
    (for {
      line <- scala.io.Source.fromInputStream(is).getLines()
      nums = line.split(",")
      if nums.size == 2
      key = nums(0).toLong
      value = nums(1).toLong
    } yield {
      key -> value
    }).toMap
  }

  def loadHists(): Hists = {
    (for {
      k <- 1 to 101
    } yield
      k -> loadHist(k)
      ).toMap
  }

  def histsKeys(hists: Hists): List[Long] = {
    (for {
      (_, hist) <- hists
      (k, _) <- hist
    } yield
      k
      ).toSet.toList.sorted
  }

  def joinHists(hists: Hists): Hists = {
    val keys = histsKeys(hists)
    (for {
      (k, hist) <- hists
    } yield {
      k -> (
             for {
               key <- keys
             } yield {
               key -> hist.getOrElse(key, 0L)
             }
             ).toMap
    })
  }

  def histsCsvFn = s"$filePrefix.hists.csv"

  def writeHists() = {
    val hists = loadHists
    val keys = histsKeys(hists)
    val headerLine = ("" :: keys.map(_.toString())).mkString(",")
    val bodyLines = (for {
      (k, hist) <- hists.toList.sortBy(_._1)
    } yield {
      val joinedHist =
        (for {
          key <- keys
        } yield {
          key -> hist.getOrElse(key, 0L)
        }).sortBy(_._1).map(_._2)

      (k :: joinedHist).mkString(",")
    })

    val lines = headerLine :: bodyLines

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(histsCsvFn)
    val os = fs.create(path)
    lines.foreach(line => os.writeBytes(line + '\n'))
    os.close()
  }

}
