package org.hammerlab.pageant

import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.{Feature, AlignmentRecord}
import org.hammerlab.pageant.JointHistogram.{Depths, OB, D, JointHistKey, OS, OL, S, L, JointHist}
import org.hammerlab.pageant.avro.{Depth, JointHistogramRecord}

import scala.collection.mutable.{Map => MMap}

case class RegressionWeights(slope: Double, intercept: Double, mse: Double, rSquared: Double) {
  override def toString: String = {
    "(%.3f %.3f, %.3f, %.3f)".format(slope, intercept, mse, rSquared)
  }
}

case class Stats(xx: Double, yy: Double, xy: Double, sx: Double, sy: Double, n: Double)

case class Covariance(vx: Double, vy: Double, vxy: Double)

case class Eigen(value: Double, vector: (Double, Double), varianceExplained: Double) {
  override def toString: String = {
    "%.3f (%.3f, %.3f) (%.3f)".format(value, vector._1, vector._2, varianceExplained)
  }
}

case class JointHistogram(jh: JointHist) {

  @transient val sc = jh.context

  val _hists: MMap[(Boolean, Set[Int]), JointHist] = MMap()

  def select(depths: Depths, keep: Boolean, idxs: Set[Int]): Depths = {
    for {
      (depth, idx) <- depths.zipWithIndex
    } yield {
      (if (idxs(idx) == keep) depth else None)
    }
  }

  def drop(depths: Depths, idxs: Int*): Depths = drop(depths, idxs.toSet)
  def drop(depths: Depths, idxs: Set[Int]): Depths = select(depths, keep = false, idxs)

  def keep(depths: Depths, idxs: Int*): Depths = keep(depths, idxs.toSet)
  def keep(depths: Depths, idxs: Set[Int]): Depths =  select(depths, keep = true, idxs)

  def hist(keepIdxs: Set[Int], sumContigs: Boolean = false): JointHist = {
    _hists.getOrElseUpdate((sumContigs, keepIdxs), {

      val ib = sc.broadcast(keepIdxs)

      def keep(r: JointHistKey): JointHistKey = {
        (
          if (sumContigs) None else r._1,
          r._2.zipWithIndex.map(p =>
            if (ib.value(p._2))
              p._1
            else
              None
          )
        )
      }

      (for {
        (k, nl) <- jh
      } yield
        keep(k) -> nl
      ).reduceByKey(_ + _).setName((if (sumContigs) "t" else "") + ib.value.toList.sortBy(x => x).mkString(",")).cache()

    })
  }

  def printPerContigs(pc: Map[(OB, OS), L], includesTotals: Boolean = false) = {
    lazy val tl: Map[(OB, OS), L] =
      (for {
        ((gO, cO), nl) <- pc
        c <- cO
      } yield
        (gO, None: OS) -> nl
      ).groupBy(_._1).mapValues(_.map(_._2).sum)

    val pct =
      if (includesTotals)
        pc
      else
        pc ++ tl.map(p => (p._1, None) -> p._2)

    val cs = pct.map(_._1._2).toList.distinct
    val ts = cs.map(c => {
      val (all, on, off) = (pct.get(None, c), pct.get(Some(true), c), pct.get(Some(false), c))
      val ratio = on.getOrElse(0L) * 100.0 / all.get
      (c, all, on, off, ratio)
    }).sortBy(-_._5)

    val fmt = "%30s:\t%12s\t%12s\t%12s\t%12s"
    println(fmt.format("contig name", "total bp", "exon bp", "non-exon bp", "% exon bp"))
    println(
      ts
        .map(t =>
          fmt.format(
            t._1.getOrElse("all"),
            t._2.map(_.toString).getOrElse("-"),
            t._3.map(_.toString).getOrElse("-"),
            t._4.map(_.toString).getOrElse("-"),
            "%.3f".format(t._5)
          )
        )
        .mkString("\n")
    )

    (pct, cs, ts)
  }

  def ppc = printPerContigs _

  lazy val totalLoci = for {
    ((contig, _), nl) <- (hist(Set()) ++ hist(Set(), true)).collect().toMap
  } yield {
    contig -> nl
  }

  case class Comp2(name: String, fn: (Int, Int, Long) => Double) {
    val _cache: MMap[(Int, Int), RDD[(JointHistKey, Double)]] = MMap()
    def get(i1: Int, i2: Int): RDD[(JointHistKey, Double)] =
      _cache.getOrElseUpdate((i1, i2),
        (for {
          ((contig, depths), nl) <- jh
          d1 <- depths(i1)
          d2 <- depths(i2)
        } yield
          ((contig, drop(depths, i1, i2)) -> fn(d1, d2, nl))
        ).reduceByKey(_ + _)
      )

    def getMap(i1: Int, i2: Int): RDD[(JointHistKey, Map[String, Double])] =
      for {
        (key, v) <- get(i1, i2)
      } yield {
        key -> Map(s"$name-$i1-$i2" -> v)
      }
  }

  val sums = Comp2("sum", (d1, d2, nl) => d1 * nl)
  val sqsums = Comp2("sqsum", (d1, d2, nl) => d1 * d1 * nl)
  val dots = Comp2("dot", (d1, d2, nl) => d1 * d2 * nl)
  val ns = Comp2("n", (_, _, nl) => nl)

  val _stats: MMap[(Int, Int), RDD[(JointHistKey, Stats)]] = MMap()
  def stats(i1: Int, i2: Int): RDD[(JointHistKey, Stats)] = {
    _stats.getOrElseUpdate((i1, i2), {
      val merged = (sums.getMap(i1, i2) ++ sums.getMap(i2, i1) ++ dots.getMap(i1, i2) ++ sqsums.getMap(i1, i2) ++ sqsums.getMap(i2, i1) ++ ns.getMap(i1, i2)).reduceByKey(_ ++ _)
      for {
        ((contig, depths), m) <- merged
        foo = println(s"merged: $m")
        xx = m(s"sqsum-$i1-$i2")
        yy = m(s"sqsum-$i2-$i1")
        xy = m(s"dot-$i1-$i2")
        sx = m(s"sum-$i1-$i2")
        sy = m(s"sum-$i2-$i1")
        n = m(s"n-$i1-$i2")
      } yield {
        (contig, depths) -> Stats(xx, yy, xy, sx, sy, n)
      }
    })
  }


  val _weights: MMap[(Int, Int), RDD[(JointHistKey, (RegressionWeights, RegressionWeights))]] = MMap()
  def weights(i1: Int, i2: Int): RDD[(JointHistKey, (RegressionWeights, RegressionWeights))] = {
    _weights.getOrElseUpdate((i1, i2), {
      for {
        ((contig, depths), Stats(xx, yy, xy, sx, sy, n)) <- stats(i1, i2)
      } yield {
        def weights(xx: D, yy: D, sx: D, sy: D): RegressionWeights = {
          val den = n * xx - sx * sx
          val m = (n * xy - sx * sy) * 1.0 / den
          val b = (sy * xx - sx * xy) * 1.0 / den
          val err = yy + m * m * xx + b * b * n - 2 * m * xy - 2 * b * sy + 2 * b * m * sx
          val num = sx * sy - n * xy
          val rSquared = num * 1.0 / (sx * sx - n * xx) * num / (sy * sy - n * yy)
          RegressionWeights(m, b, err, rSquared)
        }

        (contig, drop(depths, i1, i2)) -> (weights(xx, yy, sx, sy), weights(yy, xx, sy, sx))
      }
    })
  }

  val _cov: MMap[(Int, Int), RDD[(JointHistKey, Covariance)]] = MMap()
  def cov(i1: Int, i2: Int): RDD[(JointHistKey, Covariance)] = {
    _cov.getOrElseUpdate((i1, i2), {
      for {
        ((contig, depths), Stats(xx, yy, xy, sx, sy, n)) <- stats(i1, i2)
      } yield {
        (contig, drop(depths, i1, i2)) ->
          Covariance(
            (xx - sx*sx/n) / (n-1),
            (yy - sy*sy/n) / (n-1),
            (xy - sx*sy/n) / (n-1)
          )
      }
    })
  }

  val _eigens: MMap[(Int, Int), RDD[(JointHistKey, (Eigen, Eigen))]] = MMap()
  def eigens(i1: Int, i2: Int): RDD[(JointHistKey, (Eigen, Eigen))] = {
    _eigens.getOrElseUpdate((i1, i2), {
      for {
        ((contig, depths), Covariance(vx, vy, vxy)) <- cov(i1, i2)
      } yield {
        val T = (vx + vy) / 2
        val D = vx*vy - vxy*vxy

        val e1 = T + math.sqrt(T*T - D)
        val e2 = T - math.sqrt(T*T - D)

        val d1 = math.sqrt((e1-vy)*(e1-vy) + vxy*vxy)
        val v1 = ((e1 - vy) / d1, vxy / d1)

        val d2 = math.sqrt((e2-vy)*(e2-vy) + vxy*vxy)
        val v2 = ((e2 - vy) / d2, vxy / d2)
        (contig, drop(depths, i1, i2)) -> (Eigen(e1, v1, e1 / (e1 + e2)), Eigen(e2, v2, e2 / (e1 + e2)))
      }
    })
  }

  def write(filename: String): Unit = {
    JointHistogram.write(jh, filename)
  }
}

object JointHistogram {

  type B = Boolean
  type D = Double
  type L = Long
  type I = Int
  type S = String
  type OB = Option[B]
  type OL = Option[L]
  type OS = Option[S]
  type OI = Option[I]

  type Contig = S
  type Locus = L
  type Pos = (Contig, Locus)
  type Depth = I
  type DepthMap = RDD[(Pos, Depth)]
  type Depths = Seq[OI]

  type JointHistKey = (OS, Depths)
  type JointHistElem = (JointHistKey, L)
  type JointHist = RDD[JointHistElem]

  def write(l: JointHist, filename: String): Unit = {
    val entries =
      for {
        ((onGene, depths), numLoci) <- l
      } yield {
        JointHistogramRecord.newBuilder()
          .setNumLoci(numLoci)
          .setDepths(
            (for {
              depth <- depths
            } yield {
              val b = Depth.newBuilder()
              depth.foreach(d => b.setDepth(d))
              b.build()
            }).toList
          )
          .build()
      }

    entries.adamParquetSave(filename)
  }

  def load(sc: SparkContext, fn: String): JointHistogram = {
    val rdd: RDD[JointHistogramRecord] = sc.loadParquet(fn)
    val jointHist: JointHist =
      rdd.map(e => {
        val depths: Seq[OI] =
          for {
            depth <- e.getDepths
            dO = Option(depth).map(_.getDepth.toInt)
          } yield {
            dO
          }
        val c: OS = Option(e.getContig)
        val nl: Long = e.getNumLoci
        val key: JointHistKey = (c, depths)
        key -> nl
      })

    JointHistogram(jointHist)
  }

  def j2s(m: java.util.Map[String, java.lang.Long]): Map[String, Long] = m.toMap.mapValues(Long2long)
  def s2j(m: Map[String, Long]): java.util.Map[String, java.lang.Long] = mapToJavaMap(m.mapValues(long2Long))

  def fromFiles(sc: SparkContext,
                readFiles: Seq[String] = Nil,
                featureFiles: Seq[String] = Nil): JointHistogram = {
    val projectionOpt =
      Some(
        Projection(
          AlignmentRecordField.readMapped,
          AlignmentRecordField.sequence,
          AlignmentRecordField.contig,
          AlignmentRecordField.start,
          AlignmentRecordField.cigar
        )
      )

    val reads = readFiles.map(file => sc.loadAlignments(file, projectionOpt))
    val features = featureFiles.map(file => sc.loadFeatures(file))
    JointHistogram.fromReadsAndFeatures(reads, features)
  }

  def readsToDepthMap(reads: RDD[AlignmentRecord]): DepthMap = {
    (for {
      read <- reads if read.getReadMapped
      contig <- Option(read.getContig).toList
      name <- Option(contig.getContigName).toList
      start <- Option(read.getStart).toList
      refLen = RichAlignmentRecord(read).referenceLength
      i <- (0 until refLen)
    } yield {
        ((name, start + i), 1)
      }).reduceByKey(_ + _)
  }

  def featuresToDepthMap(features: RDD[Feature]): DepthMap = {
    (for {
      feature <- features
      contig <- Option(feature.getContig).toList
      name <- Option(contig.getContigName).toList
      start <- Option(feature.getStart).toList
      end <- Option(feature.getEnd).toList
      refLen = end - start
      i <- (0 until refLen.toInt)
    } yield {
        ((name, start + i), 1)
      }).reduceByKey(_ + _)
  }

  def fromReadsAndFeatures(reads: Seq[RDD[AlignmentRecord]] = Nil, features: Seq[RDD[Feature]] = Nil): JointHistogram = {
    fromDepthMaps(reads.map(readsToDepthMap) ++ features.map(featuresToDepthMap))
  }

  def sumSeqs(a: Depths, b: Depths): Depths = a.zip(b).map {
    case (Some(e1), e2O) => Some(e1 + e2O.getOrElse(0))
    case (e1O, Some(e2)) => Some(e2 + e1O.getOrElse(0))
    case _ => None
  }

  def fromDepthMaps(rdds: Seq[DepthMap]): JointHistogram = {
    val union: RDD[(Pos, Depths)] =
      rdds(0).context.union(for {
        (rdd, idx) <- rdds.zipWithIndex
      } yield {
          for {
            (pos, depth) <- rdd
            seq = ((0 until idx).toList.map(i => Some(0)) :+ Some(depth)) ++ ((idx + 1) until rdds.length).map(i => Some(0)): Depths
          } yield {
            pos -> seq
          }
        }).reduceByKey(sumSeqs)

    JointHistogram(
      (for {
        ((contig, _), depths) <- union
        key = (Some(contig), depths): JointHistKey
      } yield {
        key -> 1L
      }).reduceByKey(_ + _)
    )
  }
}

