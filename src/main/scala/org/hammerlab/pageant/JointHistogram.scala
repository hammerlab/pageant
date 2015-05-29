package org.hammerlab.pageant

import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.JointHistogram.{D, SampleContigsTotals, SampleTotals, ContigTotals, ReadDepthHist, JointHistKey, OS, OL, S, L, JointHist}
import org.hammerlab.pageant.avro.{JointHistogram => JH, PrincipalComponent, JointHistogramRecord}
import org.hammerlab.pageant.avro.{
  PerContigJointHistogram => PCJH,
  JointHistogramEntry,
  PrincipalComponent,
  LinearRegressionWeights => LRW
}


case class JointHistogram(readDepths: ReadDepthHist,
                          contigTotals: ContigTotals,
                          sample1Totals: SampleTotals,
                          sample2Totals: SampleTotals,
                          sample1ContigTotals: SampleContigsTotals,
                          sample2ContigTotals: SampleContigsTotals,
                          perContigTotals: Map[String, Long],
                          totalLoci: Long
) {

  @transient val sc = readDepths.context

  lazy val l = readDepths
  lazy val n = totalLoci

  lazy val readDepthsHist: JointHist =
    (for {
      ((c, d1, d2), nl) <- readDepths
    } yield ((Some(c), Some(d1), Some(d2)): JointHistKey, nl)).setName("readDepthsHist").cache()

  lazy val contigHist: JointHist =
    (for {
      ((d1, d2), nl) <- contigTotals
    } yield ((None, Some(d1), Some(d2)): JointHistKey, nl)).setName("contigHist").cache()

  lazy val sample1Hist: JointHist =
    (for {
      ((c, d1), nl) <- sample1Totals
    } yield ((Some(c), Some(d1), None): JointHistKey, nl)).setName("sample1Hist").cache()

  lazy val sample2Hist: JointHist =
    (for {
      ((c, d2), nl) <- sample2Totals
    } yield ((Some(c), None, Some(d2)): JointHistKey, nl)).setName("sample2Hist").cache()

  lazy val sample1ContigHist: JointHist =
    (for {
      (d1, nl) <- sample1ContigTotals
    } yield ((None, Some(d1), None): JointHistKey, nl)).setName("sample1ContigHist").cache()

  lazy val sample2ContigHist: JointHist =
    (for {
      (d2, nl) <- sample2ContigTotals
    } yield ((None, None, Some(d2)): JointHistKey, nl)).setName("sample2ContigHist").cache()

  lazy val perContigHist: JointHist = sc.parallelize(
    (for {
      (c, nl) <- perContigTotals.toList
    } yield ((Some(c), None, None): JointHistKey, nl))
  )

  lazy val totalHist: JointHist = sc.parallelize(List(((None, None, None), totalLoci)))

  lazy val hist: JointHist =
    readDepthsHist ++
      contigHist ++
      sample1Hist ++
      sample2Hist ++
      sample1ContigHist ++
      sample2ContigHist ++
      perContigHist ++
      totalHist

  lazy val contigsHist = (readDepthsHist ++ contigHist).setName("contigsHist").cache()
  lazy val contigs = contigsHist.map(_._1._1).distinct.collect().toSet
  lazy val sample1All = (sample1Hist ++ sample1ContigHist).setName("sample1All").cache()
  lazy val sample2All = (sample2Hist ++ sample2ContigHist).setName("sample2All").cache()

  lazy val m1: Map[(OS, L), L] =
    for {
      ((cO, d1O, _), nl) <- sample1All.collectAsMap().toMap
      d1 <- d1O
    } yield (cO, d1) -> nl

  lazy val m2: Map[(OS, L), L] =
    for {
      ((cO, _, d2O), nl) <- sample2All.collectAsMap().toMap
      d2 <- d2O
    } yield (cO, d2) -> nl

  def contigTotal(cO: OS): L = cO.map(perContigTotals.apply).getOrElse(totalLoci)

  lazy val readsDot: Map[OS, D] =
    (for {
      ((cO, d1O, d2O), numLoci) <- contigsHist
      d1 <- d1O
      d2 <- d2O
    } yield {
        cO -> numLoci * d1 * d2
      }).reduceByKey(_ + _).mapValues(_.toDouble).collectAsMap().toMap

  lazy val xyc = readsDot

  def addTuples(a: (L, L), b: (L, L)) = (a._1 + b._1, a._2 + b._2)

  lazy val (sxc, xxc) = {
    val m =
      (for {
        ((cO, d1O, _), numLoci) <- sample1All
        d1 <- d1O
      } yield {
          val xi = numLoci * d1
          cO -> (xi, xi * d1)
        }).reduceByKey(addTuples)

    (
      m.mapValues(_._1.toDouble).collectAsMap().toMap,
      m.mapValues(_._2.toDouble).collectAsMap().toMap
    )
  }

  lazy val (syc, yyc) = {
    val m =
      (for {
        ((cO, _, d2O), numLoci) <- sample2All
        d2 <- d2O
      } yield {
          val xi = numLoci * d2
          cO -> (xi, xi * d2)
        }).reduceByKey(addTuples)

    (
      m.mapValues(_._1.toDouble).collectAsMap().toMap,
      m.mapValues(_._2.toDouble).collectAsMap().toMap
    )
  }

  lazy val stats =
    (for {
      c <- contigs
      xx = xxc(c)
      yy = yyc(c)
      xy = xyc(c)
      sx = sxc(c)
      sy = syc(c)
    } yield
      c -> ((xx, yy, xy), (sx, sy))
    ).toMap

  // h.weights.toList.sortBy(-_._2._1.getRSquared).map(p => "%30s:\t\t%f\t\t%s".format(p._1.getOrElse("all"), p._2._1.getRSquared, List(p._2._1,p._2._2).map(x => "(%f, %f, %f)".format(x.getSlope, x.getIntercept, x.getMse)).mkString(" "))).toList.mkString("\n")
  lazy val weights = {
    for {
      (c, ((xx, yy, xy), (sx, sy))) <- stats
      n = contigTotal(c)
    } yield {
      def weights(xx: D, yy: D, sx: D, sy: D): LRW = {
        val den = n*xx - sx*sx
        val m = (n*xy - sx*sy) * 1.0 / den
        val b = (sy*xx - sx*xy) * 1.0 / den
        val err = yy + m*m*xx + b*b*n - 2*m*xy - 2*b*sy + 2*b*m*sx
        val num = sx*sy - n*xy
        val rSquared = num * 1.0 / (sx*sx - n*xx) * num / (sy*sy - n*yy)
        LRW.newBuilder
        .setSlope(m)
        .setIntercept(b)
        .setMse(err)
        .setRSquared(rSquared)
        .build()
      }

      c -> (weights(xx, yy, sx, sy), weights(yy, xx, sy, sx))
    }
  }

  lazy val cov =
    (for {
      (c, ((xx, yy, xy), (sx, sy))) <- stats
      n = contigTotal(c)
    } yield
      c -> (
        (xx - sx*sx*1.0/n) / (n-1),
        (yy - sy*sy*1.0/n) / (n-1),
        (xy - sx*sy*1.0/n) / (n-1)
      )
    ).toMap

  lazy val eigens = {
    (for {
      c <- contigs
      (vx, vy, vxy) = cov(c)
    } yield {
        val T = (vx + vy) / 2
        val D = vx*vy - vxy*vxy

        val e1 = T + math.sqrt(T*T - D)
        val e2 = T - math.sqrt(T*T - D)

        val d1 = math.sqrt((e1-vy)*(e1-vy) + vxy*vxy)
        val v1 = ((e1 - vy) / d1, vxy / d1)

        val d2 = math.sqrt((e2-vy)*(e2-vy) + vxy*vxy)
        val v2 = ((e2 - vy) / d2, vxy / d2)

        c -> List((e1,v1), (e2,v2))
      }).toMap
  }

  lazy val sum_esc = for {
    (c,e) <- eigens
  } yield c -> e.map(_._1).sum

  lazy val pcsc = (for {
    (c, es) <- eigens
    (e,v) <- es
    sum = sum_esc(c)
  } yield {
    c ->
      PrincipalComponent.newBuilder()
        .setValue(e)
        .setVector(List(v._1, v._2).map(double2Double))
        .setVarianceFraction(e / sum)
        .build()
  }).groupBy(_._1).mapValues(_.map(_._2).toList)

  lazy val mutualInformation: Map[OS, Double] = {
    val b1 = sc.broadcast(m1)
    val b2 = sc.broadcast(m2)
    (for {
      ((cO, d1O, d2O), numLoci) <- contigsHist
      d1 <- d1O
      d2 <- d2O
    } yield {

        val n = contigTotal(cO)
        val vx = b1.value.getOrElse(
          (cO, d1),
          throw new Exception(s"Depth $d1 not found in RDH1 map for contig ${cO.getOrElse("all")}")
        )

        val vy = b2.value.getOrElse(
          (cO, d2),
          throw new Exception(s"Depth $d2 not found in RDH2 map for contig ${cO.getOrElse("all")}")
        )

        (cO, numLoci * (math.log(numLoci) + math.log(n) - math.log(vx) - math.log(vy)))

      }).reduceByKey(_ + _)
        .map(p => (
          p._1,
          p._2 / math.log(2) / contigTotal(p._1)
        )
      ).collectAsMap().toMap
  }

  def write(filename: String): Unit = {
    JointHistogram.write(hist, filename)
  }
}

object JointHistogram {

  type D = Double
  type L = Long
  type S = String
  type OL = Option[L]
  type OS = Option[S]

  type ReadDepthKey = (S, L, L)
  type ReadDepthElem = (ReadDepthKey, L)
  type ReadDepthHist = RDD[ReadDepthElem]

  type ContigsKey = (L, L)
  type ContigsElem = (ContigsKey, L)
  type ContigTotals = RDD[ContigsElem]

  type SampleKey = (S, L)
  type SampleElem = (SampleKey, L)
  type SampleTotals = RDD[SampleElem]

  type SampleContigsKey = L
  type SampleContigsElem = (SampleContigsKey, L)
  type SampleContigsTotals = RDD[SampleContigsElem]

  type JointHistKey = (OS, OL, OL)
  type JointHistElem = (JointHistKey, Long)
  type JointHist = RDD[JointHistElem]

  type AllContigsHistKey = (OL, OL)
  type AllContigsHistElem = (AllContigsHistKey, Long)
  type AllContigsHist = RDD[AllContigsHistElem]

  type SampleHistKey = (OS, OL)
  type SampleHistElem = (SampleHistKey, Long)
  type SampleHist = RDD[SampleHistElem]

  type SampleContigHistKey = OL
  type SampleContigHistElem = (SampleContigHistKey, Long)
  type SampleContigHist = RDD[SampleContigHistElem]

  def write(l: JointHist, filename: String): Unit = {
    val entries =
      for {
        ((contig, depth1, depth2), numLoci) <- l
      } yield {
        val builder = JointHistogramRecord.newBuilder().setNumLoci(numLoci)
        depth1.foreach(l => builder.setDepth1(l))
        depth2.foreach(l => builder.setDepth2(l))
        contig.foreach(c => builder.setContig(c))
        builder.build()
      }

    entries.adamParquetSave(filename)
  }

  def load(sc: SparkContext, fn: String): JointHistogram = {
    val rdd: RDD[JointHistogramRecord] = sc.adamLoad(fn)
    JointHistogram(
      rdd.map(e => {
        val d1: OL = Option(e.getDepth1).map(Long2long)
        val d2: OL = Option(e.getDepth2).map(Long2long)
        val c: OS = Option(e.getContig)
        val nl: Long = e.getNumLoci
        //val m: Map[String, Long] = e.getLociPerContig.toMap.mapValues(Long2long)
        ((c, d1, d2), nl)
      })
    )
  }

  def j2s(m: java.util.Map[String, java.lang.Long]): Map[String, Long] = m.toMap.mapValues(Long2long)
  def s2j(m: Map[String, Long]): java.util.Map[String, java.lang.Long] = mapToJavaMap(m.mapValues(long2Long))

  def fromAlignmentFiles(sc: SparkContext,
                         file1: String,
                         file2: String): JointHistogram = {
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
    val reads = sc.loadAlignments(file1, None, projectionOpt).setName("reads1")
    val reads2 = sc.loadAlignments(file2, None, projectionOpt).setName("reads2")
    JointHistogram.fromAlignments(reads, reads2)
//    JointHistogram(InterleavedJointHistogram.fromAlignmentFiles(sc, file1, file2))
  }

//  def apply(joinedReadDepthPerLocus: RDD[((String, Long), (Long, Long))]): JointHistogram = {
//
//  }

  def apply(jointHist: JointHist): JointHistogram = {
    jointHist.setName("jointHist").cache()

    val readDepths: RDD[((String, Long, Long), Long)] =
      (for {
        ((cO, d1O, d2O), nl) <- jointHist
        c <- cO
        d1 <- d1O
        d2 <- d2O
      } yield (c, d1, d2) -> nl).reduceByKey(_ + _).setName("readDepths").cache()

    val contigTotals: RDD[((Long, Long), Long)] =
      (for {
        ((_, d1, d2), nl) <- readDepths
      } yield
        (d1, d2) -> nl
      ).reduceByKey(_ + _).setName("contigTotals").cache()

    val sample1Totals: RDD[((String, Long), Long)] =
      (for {
        ((c, d1, _), nl) <- readDepths
      } yield
        (c, d1) -> nl
        ).reduceByKey(_ + _).setName("sample1Totals").cache()

    val sample2Totals: RDD[((String, Long), Long)] =
      (for {
        ((c, _, d2), nl) <- readDepths
      } yield
        (c, d2) -> nl
      ).reduceByKey(_ + _).setName("sample2Totals").cache()

    val sample1ContigTotals: RDD[(Long, Long)] =
      (for {
        ((_, d1), nl) <- sample1Totals
      } yield
        d1 -> nl
      ).reduceByKey(_ + _).setName("sample1ContigTotals").cache()

    val sample2ContigTotals: RDD[(Long, Long)] =
      (for {
        ((_, d2), nl) <- sample2Totals
      } yield
        d2 -> nl
      ).reduceByKey(_ + _).setName("sample2ContigTotals").cache()

    val perContigTotals: Map[String, Long] =
      (for {
        ((c, _), nl) <- sample1Totals
      } yield
        c -> nl
      ).reduceByKey(_ + _).collectAsMap().toMap

    val totalLoci = perContigTotals.values.sum

    JointHistogram(
      readDepths,
      contigTotals,
      sample1Totals,
      sample2Totals,
      sample1ContigTotals,
      sample2ContigTotals,
      perContigTotals,
      totalLoci
    )

  }

  def fromAlignments(reads: RDD[AlignmentRecord],
                     reads2: RDD[AlignmentRecord]): JointHistogram = {
    val joinedReadDepthPerLocus: RDD[((String, Long), (Long, Long))] = Alignments.joinedReadDepths(reads, reads2)
    val jointHist: JointHist =
      (for {
        ((contig, _), (d1, d2)) <- joinedReadDepthPerLocus
        key = (Some(contig), Some(d1), Some(d2)): JointHistKey
      } yield key -> 1L).reduceByKey(_ + _)

    apply(jointHist)
  }

  def loadFromAvro(sc: SparkContext, fn: String): RDD[JH] = {
    sc.adamLoad(fn)
  }

}

