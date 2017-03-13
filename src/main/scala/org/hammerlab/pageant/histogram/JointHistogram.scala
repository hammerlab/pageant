package org.hammerlab.pageant.histogram

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{ AlignmentRecordField, FeatureField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.genomics.reference
import org.hammerlab.genomics.reference.{ ContigName, Locus, NumLoci, Position ⇒ Pos }
import org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD._
import org.hammerlab.pageant.histogram.JointHistogram._

import scala.collection.mutable
import scala.collection.mutable.{ Map ⇒ MMap }

case class Record(contigOpt: Option[ContigName], depths: Seq[Option[Int]], numLoci: NumLoci)

case class RegressionWeights(slope: Double, intercept: Double, mse: Double, rSquared: Double) {
  override def toString: String =
    "(%.3f %.3f, %.3f, %.3f)".format(slope, intercept, mse, rSquared)
}

case class Stats(xx: Double, yy: Double, xy: Double, sx: Double, sy: Double, n: Double)

case class Covariance(vx: Double, vy: Double, vxy: Double)

case class Eigen(value: Double, vector: (Double, Double), varianceExplained: Double) {
  override def toString: String =
    "%.3f (%.3f, %.3f) (%.3f)".format(value, vector._1, vector._2, varianceExplained)
}

case class JointHistogram(jh: JointHist) {

  @transient val sc = jh.context

  def select(depths: Depths, keep: Boolean, idxs: Set[Int]): Depths =
    for {
      (depth, idx) <- depths.zipWithIndex
    } yield
      if (idxs(idx) == keep)
        depth
      else
        None

  import JointHistogram.depthsOrd

  def drop(depths: Depths, idxs: Int*): Depths = drop(depths, idxs.toSet)
  def drop(depths: Depths, idxs: Set[Int]): Depths = select(depths, keep = false, idxs)

  def keep(depths: Depths, idxs: Int*): Depths = keep(depths, idxs.toSet)
  def keep(depths: Depths, idxs: Set[Int]): Depths =  select(depths, keep = true, idxs)

  val _hists: MMap[(Boolean, Set[Int]), JointHist] = MMap()
  def hist(keepIdxs: Set[Int], sumContigs: Boolean = false): JointHist =
    _hists.getOrElseUpdate(
      (sumContigs, keepIdxs),
      {

        val ib = sc.broadcast(keepIdxs)

        def dropUnkeptDepths(cO: OCN, depths: Depths): JointHistKey =
          (
            if (sumContigs)
              None
            else
              cO,
            for {
              (dO, idx) <- depths.zipWithIndex
              if ib.value(idx)
            } yield
              dO
          )

        (for {
          ((cO, depths), nl) <- jh
        } yield
          dropUnkeptDepths(cO, depths) → nl
        )
        .reduceByKey(_ + _)
        .setName(
          (
            if (sumContigs)
              "total:"
            else
              ""
          ) +
            keepIdxs
              .toVector
              .sorted
              .mkString(",")
        )
        .cache()
      }
    )

  /*
   * For a sample representing features, return the number of loci with coverage represented in this joint-distribution
   * that are "on" and "off" of that feature-set.
   */
  def coveredLociCounts(idx: Int): (NumLoci, NumLoci) = {
    val totalLociMap =
    (for {
        ((_, depths), numLoci) ← jh
        depth = depths(idx).get
      } yield
        depth → numLoci
    )
    .reduceByKey(_ + _)
    .collectAsMap()

    val totalOnLoci: NumLoci = totalLociMap.getOrElse(1, NumLoci(0))
    val totalOffLoci: NumLoci = totalLociMap.getOrElse(0, NumLoci(0))

    (totalOnLoci, totalOffLoci)
  }

  def printPerContigs(pc: Map[(OB, OCN), L], includesTotals: Boolean = false) = {
    lazy val tl: Map[(OB, OCN), L] =
      (for {
        ((gO, cO), nl) <- pc
        _ <- cO
      } yield
        (gO, None: OCN) → nl
      )
      .groupBy(_._1)
      .mapValues(_.values.sum)

    val pct =
      if (includesTotals)
        pc
      else
        pc ++ tl.map(p => (p._1, None) → p._2)

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

  @transient lazy val totalLoci: Map[OCN, NumLoci] =
    for {
      ((contig, _), nl) <- (hist(Set()) ++ hist(Set(), sumContigs = true)).collect().toMap
    } yield
      contig → nl

  case class Comp2(name: String, fn: (Int, Int, Long) => Double) {
    val _cache: MMap[(Int, Int), RDD[(JointHistKey, Double)]] = MMap()
    def get(i1: Int, i2: Int): RDD[(JointHistKey, Double)] =
      _cache.getOrElseUpdate((i1, i2),
        (for {
          ((contig, depths), nl) <- jh
          d1 <- depths(i1)
          d2 <- depths(i2)
        } yield
          contig → drop(depths, i1, i2) → fn(d1, d2, nl)
        ).reduceByKey(_ + _)
      )

    def getMap(i1: Int, i2: Int): RDD[(JointHistKey, Map[String, Double])] =
      for {
        (key, v) <- get(i1, i2)
      } yield
        key → Map(s"$name-$i1-$i2" → v)

  }

  val sums = Comp2("sum", (d1, _, nl) => d1 * nl)
  val sqsums = Comp2("sqsum", (d1, _, nl) => d1 * d1 * nl)
  val dots = Comp2("dot", (d1, d2, nl) => d1 * d2 * nl)
  val ns = Comp2("n", (_, _, nl) => nl)

  val _stats: MMap[(Int, Int), RDD[(JointHistKey, Stats)]] = MMap()
  def stats(i1: Int, i2: Int): RDD[(JointHistKey, Stats)] =
    _stats.getOrElseUpdate(
      (i1, i2),
      {

        val merged =
          (
            sums.getMap(i1, i2) ++
              sums.getMap(i2, i1) ++
              dots.getMap(i1, i2) ++
              sqsums.getMap(i1, i2) ++
              sqsums.getMap(i2, i1) ++
              ns.getMap(i1, i2)
            ).reduceByKey(_ ++ _)

        for {
          ((contig, depths), m) <- merged
          xx = m(s"sqsum-$i1-$i2")
          yy = m(s"sqsum-$i2-$i1")
          xy = m(s"dot-$i1-$i2")
          sx = m(s"sum-$i1-$i2")
          sy = m(s"sum-$i2-$i1")
          n = m(s"n-$i1-$i2")
        } yield
          contig → depths → Stats(xx, yy, xy, sx, sy, n)
      }
    )

  val _weights: MMap[(Int, Int), RDD[(JointHistKey, (RegressionWeights, RegressionWeights))]] = MMap()
  def weights(i1: Int, i2: Int): RDD[(JointHistKey, (RegressionWeights, RegressionWeights))] =
    _weights.getOrElseUpdate(
      (i1, i2),
      {
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

          contig →
            depths →
              (weights(xx, yy, sx, sy), weights(yy, xx, sy, sx))
        }
      }
    )

  val _cov: MMap[(Int, Int), RDD[(JointHistKey, Covariance)]] = MMap()
  def cov(i1: Int, i2: Int): RDD[(JointHistKey, Covariance)] =
    _cov.getOrElseUpdate(
      (i1, i2),
      {
        for {
          ((contig, depths), Stats(xx, yy, xy, sx, sy, n)) <- stats(i1, i2)
        } yield
          contig →
            depths →
              Covariance(
                (xx - sx*sx/n) / (n - 1),
                (yy - sy*sy/n) / (n - 1),
                (xy - sx*sy/n) / (n - 1)
              )
      }
    )

  val _eigens: MMap[(Int, Int), RDD[(JointHistKey, (Eigen, Eigen))]] = MMap()
  def eigens(i1: Int, i2: Int): RDD[(JointHistKey, (Eigen, Eigen))] =
    _eigens.getOrElseUpdate(
      (i1, i2),
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

        contig →
          depths →
            (Eigen(e1, v1, e1 / (e1 + e2)), Eigen(e2, v2, e2 / (e1 + e2)))
      }
    )

  def write(path: Path): Unit = write(path.toString)
  def write(filename: String): Unit = JointHistogram.write(jh, filename)
}

object JointHistogram {

  type B = Boolean
  type D = Double
  type L = Long
  type I = Int
  type S = String
  type OB = Option[B]
  type OL = Option[L]
  type OCN = Option[ContigName]
  type OI = Option[I]

  type Depth = I
  type DepthMap = RDD[(Pos, Depth)]
  type Depths = Seq[Option[Depth]]

  type JointHistKey = (OCN, Depths)
  type JointHistElem = (JointHistKey, NumLoci)
  type JointHist = RDD[JointHistElem]

  implicit val depthsOrd: Ordering[Depths] =
    Ordering.by[Depths, Iterable[Option[Depth]]](_.toIterable)(Ordering.Iterable[Option[Depth]])

  implicit val jointHistKeyOrd = Ordering.Tuple2[OCN, Depths]

  def write(l: JointHist, filename: String): Unit = {
    val entries =
      for {
        ((contigOpt, depths), numLoci) <- l
      } yield
        Record(contigOpt, depths, numLoci)

    entries.saveCompressed(filename)
  }

  def load(sc: SparkContext, fn: String): JointHistogram =
    load(sc, new Path(fn))

  def load(sc: SparkContext, path: Path): JointHistogram = {
    val rdd: RDD[Record] = sc.fromSequenceFile(path)
    val jointHist: JointHist =
      for {
        Record(contigOpt, depthOpts, numLoci) <- rdd
      } yield
        contigOpt → depthOpts → numLoci

    JointHistogram(jointHist)
  }

  def fromFiles(sc: SparkContext,
                readFiles: Seq[String] = Nil,
                featureFiles: Seq[String] = Nil,
                dedupeFeatureLoci: Boolean = true,
                bytesPerIntervalPartition: Int = 1 << 16): JointHistogram = {

    val projection =
      Projection(
        AlignmentRecordField.readMapped,
        AlignmentRecordField.sequence,
        AlignmentRecordField.contigName,
        AlignmentRecordField.start,
        AlignmentRecordField.cigar
      )

    val featuresProjection =
      Projection(
        FeatureField.contigName,
        FeatureField.start,
        FeatureField.end
      )

    val reads = readFiles.map(file => sc.loadAlignments(file, Some(projection)).rdd)

    val features =
      for {
        file ← featureFiles
        path = new Path(file)
        fs = path.getFileSystem(sc.hadoopConfiguration)
        fileLength = fs.getFileStatus(path).getLen
        numPartitions = (fileLength / bytesPerIntervalPartition).toInt
      } yield {
        println(s"Loading interval file $file of size $fileLength using $numPartitions")
        sc.loadFeatures(file, Some(featuresProjection), Some(numPartitions))
      }

    JointHistogram.fromReadsAndFeatures(reads, features, dedupeFeatureLoci)
  }

  def readsToDepthMap(reads: RDD[AlignmentRecord]): DepthMap = {
    val rdd = (for {
      read ← reads if read.getReadMapped
      contigName ← Option(read.getContigName).toList
      start ← Option(Locus(read.getStart)).toList
      end ← Option(Locus(read.getEnd)).toList
      refLen = (end - start).toInt
      i ← 0 until refLen
    } yield
      Pos(contigName, start + i) → 1
    )

    rdd.reduceByKey(_ + _)
  }

  def featuresToDepthMap(features: FeatureRDD, dedupeLoci: Boolean = true): DepthMap = {
    val lociCounts: RDD[Pos] =
      for {
        feature <- features.rdd
        contigName ← Option(feature.getContigName).toList
        start ← Option(Locus(feature.getStart)).toList
        end ← Option(Locus(feature.getEnd)).toList
        refLen = (end - start).toInt
        i ← 0 until refLen
      } yield
        Pos(contigName, start + i)

    if (dedupeLoci)
      lociCounts
        .distinct
        .map(_ → 1)
    else
      lociCounts
        .map(_ → 1)
        .reduceByKey(_ + _)
  }

  def fromReadsAndFeatures(reads: Seq[RDD[AlignmentRecord]] = Nil,
                           features: Seq[FeatureRDD] = Nil,
                           dedupeFeatureLoci: Boolean = true): JointHistogram = {
    fromDepthMaps(
      reads.map(readsToDepthMap) ++
        features.map(featuresToDepthMap(_, dedupeFeatureLoci))
    )
  }

  def sumSeqs(a: Depths, b: Depths): Depths =
    a
      .zip(b)
      .map {
        case (Some(e1), e2O) => Some(e1 + e2O.getOrElse(0))
        case (e1O, Some(e2)) => Some(e2 + e1O.getOrElse(0))
        case _ => None
      }

  def oneHotOpts(num: Int, idx: Int, value: Int): Depths =
    (
      Array.fill(idx)(Some(0)) :+
      Some(value)
    ) ++
      Array.fill(num - idx - 1)(Some(0))

  def fromDepthMaps(rdds: Seq[DepthMap]): JointHistogram = {
    val sc = rdds.head.context

    val union: RDD[(Pos, Depths)] =
      sc.union(
        for { (rdd, idx) ← rdds.zipWithIndex } yield
          for {
            (Pos(contig, locus), depth) ← rdd
            seq = oneHotOpts(rdds.length, idx, depth)
          } yield
            Pos(contig, locus) → seq
      )
      .reduceByKey(sumSeqs)

    val rawCounts =
      for {
        (Pos(contig, _), depths) ← union
        key = (Some(contig), depths): JointHistKey
      } yield
        key → NumLoci(1)

    val counts = rawCounts.reduceByKey(_ + _)

    JointHistogram(counts)
  }

  def register(kryo: Kryo): Unit = {
    // JointHistogram.hist can broadcast an empty set.
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))

    // Not necessarily serialized in the normal course, but reasonable to `collect`.
    kryo.register(classOf[RegressionWeights])
    kryo.register(classOf[Eigen])
    kryo.register(classOf[mutable.ArraySeq[_]])

    new reference.Registrar().registerClasses(kryo)
  }

  implicit def toSparkContext(jh: JointHistogram): SparkContext = jh.sc
  implicit def toHadoopConfiguration(jh: JointHistogram): Configuration = jh.sc.hadoopConfiguration
}

