package org.hammerlab.pageant

import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{Feature, AlignmentRecord}
import org.hammerlab.pageant.JointHistogram.{OB, D, JointHistKey, OS, OL, S, L, JointHist}
import org.hammerlab.pageant.avro.{
  PrincipalComponent,
  JointHistogramRecord,
  LinearRegressionWeights => LRW
}

case class JointHistWithSums(name: String,
                             readDepthsHist: JointHist,
                             contigHist: JointHist,
                             sample1Hist: JointHist,
                             sample2Hist: JointHist,
                             sample1ContigHist: JointHist,
                             sample2ContigHist: JointHist,
                             perContigHist: JointHist,
                             totalLociHist: JointHist) {

  @transient val sc = readDepthsHist.context

  lazy val hists = List(
    readDepthsHist,
    contigHist,
    sample1Hist,
    sample2Hist,
    sample1ContigHist,
    sample2ContigHist,
    perContigHist,
    totalLociHist
  )

  lazy val hist: JointHist = sc.union(hists)

  def cache(): Unit = {
    readDepthsHist.setName(s"$name-readDepthsHist").cache()
    contigHist.setName(s"$name-contigHist").cache()
    sample1Hist.setName(s"$name-sample1Hist").cache()
    sample2Hist.setName(s"$name-sample2Hist").cache()
    sample1ContigHist.setName(s"$name-sample1ContigHist").cache()
    sample2ContigHist.setName(s"$name-sample2ContigHist").cache()
    perContigHist.setName(s"$name-perContigHist").cache()
    totalLociHist.setName(s"$name-totalLociHist").cache()
  }
}

object JointHistWithSums {

  def fromPrecomputed(name: String, jointHist: JointHist): JointHistWithSums = {
    val readDepthsHist = jointHist.filter {
      case ((_, Some(c), Some(d1), Some(d2)), _) => true
      case _ => false
    }

    val contigsHist = jointHist.filter {
      case ((_, None, Some(d1), Some(d2)), _) => true
      case _ => false
    }

    val sample1Hist = jointHist.filter {
      case ((_, Some(c), Some(d1), None), _) => true
      case _ => false
    }

    val sample2Hist = jointHist.filter {
      case ((_, Some(c), None, Some(d2)), _) => true
      case _ => false
    }

    val sample1ContigHist = jointHist.filter {
      case ((_, None, Some(d1), None), _) => true
      case _ => false
    }

    val sample2ContigHist = jointHist.filter {
      case ((_, None, None, Some(d2)), _) => true
      case _ => false
    }

    val perContigHist = jointHist.filter {
      case ((_, Some(c), None, None), _) => true
      case _ => false
    }

    val totalLociHist = jointHist.filter {
      case ((_, None, None, None), _) => true
      case _ => false
    }


    JointHistWithSums(
      name,
      readDepthsHist,
      contigsHist,
      sample1Hist,
      sample2Hist,
      sample1ContigHist,
      sample2ContigHist,
      perContigHist,
      totalLociHist
    )
  }

  def computeSums(name: String, jointHist: JointHist): JointHistWithSums = {
    jointHist.setName(s"$name-jointHist").cache()

    val readDepths: JointHist =
      (for {
        ((gO, cO, d1O, d2O), nl) <- jointHist
        c <- cO
        d1 <- d1O
        d2 <- d2O
        key = (gO, Some(c), Some(d1), Some(d2)): JointHistKey
      } yield
        key -> nl
      ).reduceByKey(_ + _).setName(s"$name-readDepths").cache()

    val contigTotals: JointHist =
      (for {
        ((gO, _, d1, d2), nl) <- readDepths
        key = (gO, None, d1, d2): JointHistKey
      } yield
        key -> nl
      ).reduceByKey(_ + _).setName(s"$name-contigTotals").cache()

    val sample1Totals: JointHist =
      (for {
        ((gO, c, d1, _), nl) <- readDepths
        key = (gO, c, d1, None): JointHistKey
      } yield
        key -> nl
        ).reduceByKey(_ + _).setName(s"$name-sample1Totals").cache()

    val sample2Totals: JointHist =
      (for {
        ((gO, c, _, d2), nl) <- readDepths
        key = (gO, c, None, d2): JointHistKey
      } yield
        key -> nl
        ).reduceByKey(_ + _).setName(s"$name-sample2Totals").cache()

    val sample1ContigTotals: JointHist =
      (for {
        ((gO, _, d1, _), nl) <- sample1Totals
        key = (gO, None, d1, None): JointHistKey
      } yield
        key -> nl
        ).reduceByKey(_ + _).setName(s"$name-sample1ContigTotals").cache()

    val sample2ContigTotals: JointHist =
      (for {
        ((gO, _, _, d2), nl) <- sample2Totals
        key = (gO, None, None, d2): JointHistKey
      } yield
        key -> nl
        ).reduceByKey(_ + _).setName(s"$name-sample2ContigTotals").cache()

    val perContigTotals: JointHist =
      (for {
        ((gO, c, _, _), nl) <- sample1Totals
        key = (gO, c, None, None): JointHistKey
      } yield
        key -> nl
        ).reduceByKey(_ + _).setName(s"$name-perContigTotals").cache()

    val totalLoci: JointHist =
      (for {
        ((gO, _, _, _), nl) <- perContigTotals
        key = (gO, None, None, None): JointHistKey
      } yield
        key -> nl
      ).reduceByKey(_ + _).setName(s"$name-totalLoci").cache()

    JointHistWithSums(
      name,
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

  def union(jhwss: Seq[JointHistWithSums]): JointHistWithSums = {
    val sc = jhwss.head.sc
    JointHistWithSums(
      "union",
      sc.union(jhwss.map(_.readDepthsHist)),
      sc.union(jhwss.map(_.contigHist)),
      sc.union(jhwss.map(_.sample1Hist)),
      sc.union(jhwss.map(_.sample2Hist)),
      sc.union(jhwss.map(_.sample1ContigHist)),
      sc.union(jhwss.map(_.sample2ContigHist)),
      sc.union(jhwss.map(_.perContigHist)),
      sc.union(jhwss.map(_.totalLociHist))
    )
  }
}

case class JointHistogram(all: JointHistWithSums,
                          onGene: JointHistWithSums,
                          offGene: JointHistWithSums) {

  @transient val sc = all.readDepthsHist.context

  lazy val hists = List(all, onGene, offGene)
  lazy val hist: JointHist = sc.union(hists.map(_.hist))

  lazy val allLengths = hists.map(_.hists.map(_.count))

  def cache(): Unit = {
    hists.foreach(_.cache())
  }

  lazy val l = JointHistWithSums.union(hists)

  lazy val contigsHist = (l.readDepthsHist ++ l.contigHist).setName("contigsHist").cache()
  lazy val contigs = contigsHist.map(t => (t._1._1, t._1._2)).distinct.collect().toSet
  lazy val sample1All = (l.sample1Hist ++ l.sample1ContigHist).setName("sample1All").cache()
  lazy val sample2All = (l.sample2Hist ++ l.sample2ContigHist).setName("sample2All").cache()

  lazy val m1: Map[(OB, OS, L), L] =
    (for {
      ((gO, cO, d1O, _), nl) <- sample1All
      d1 <- d1O
    } yield
      (gO, cO, d1) -> nl
    ).collectAsMap().toMap

  lazy val m2: Map[(OB, OS, L), L] =
    (for {
      ((gO, cO, _, d2O), nl) <- sample2All
      d2 <- d2O
    } yield
      (gO, cO, d2) -> nl
    ).collectAsMap().toMap

  lazy val perContigTotals: Map[(OB, OS), L] =
    (for {
      ((gO, cO, _, _), nl) <- l.perContigHist
    } yield
      (gO, cO) -> nl
    ).collectAsMap().toMap

  lazy val sample1Bases: Map[(OB, OS), L] =
    (for {
      ((gO, cO, d1O, _), nl) <- sample1All
      d1 <- d1O
    } yield
      (gO, cO) -> d1*nl
    ).reduceByKey(_ + _).collectAsMap().toMap

  lazy val sample2Bases: Map[(OB, OS), L] =
    (for {
      ((gO, cO, _, d2O), nl) <- sample2All
      d2 <- d2O
    } yield
      (gO, cO) -> d2*nl
      ).reduceByKey(_ + _).collectAsMap().toMap

  lazy val pc = perContigTotals

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

  lazy val totalLoci: Map[OB, L] =
    (for {
      ((gO, _, _, _), nl) <- l.totalLociHist
    } yield
      gO -> nl
    ).reduceByKey(_ + _).collectAsMap().toMap

  lazy val tl = totalLoci

  def contigTotal(t: (OB, OS)): L = perContigTotals.get(t).getOrElse(totalLoci(t._1))

  lazy val readsDot: Map[(OB, OS), D] =
    (for {
      ((gO, cO, d1O, d2O), numLoci) <- contigsHist
      d1 <- d1O
      d2 <- d2O
    } yield {
      (gO, cO) -> numLoci * d1 * d2
    }).reduceByKey(_ + _).mapValues(_.toDouble).collectAsMap().toMap

  lazy val xyc = readsDot

  def addTuples(a: (L, L), b: (L, L)) = (a._1 + b._1, a._2 + b._2)

  lazy val (sxc, xxc) = {
    val m =
      (for {
        ((gO, cO, d1O, _), numLoci) <- sample1All
        d1 <- d1O
      } yield {
        val xi = numLoci * d1
        (gO, cO) -> (xi, xi * d1)
      }).reduceByKey(addTuples)

    (
      m.mapValues(_._1.toDouble).collectAsMap().toMap,
      m.mapValues(_._2.toDouble).collectAsMap().toMap
    )
  }

  lazy val (syc, yyc) = {
    val m =
      (for {
        ((gO, cO, _, d2O), numLoci) <- sample2All
        d2 <- d2O
      } yield {
        val xi = numLoci * d2
        (gO, cO) -> (xi, xi * d2)
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
    )

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

  lazy val mutualInformation: Map[(OB, OS), Double] = {
    val b1 = sc.broadcast(m1)
    val b2 = sc.broadcast(m2)
    (for {
      ((gO, cO, d1O, d2O), numLoci) <- contigsHist
      d1 <- d1O
      d2 <- d2O
    } yield {

        val n = contigTotal((gO, cO))
        val vx = b1.value.getOrElse(
          (gO, cO, d1),
          throw new Exception(s"Depth $d1 not found in RDH1 map for contig ${cO.getOrElse("all")}")
        )

        val vy = b2.value.getOrElse(
          (gO, cO, d2),
          throw new Exception(s"Depth $d2 not found in RDH2 map for contig ${cO.getOrElse("all")}")
        )

        (gO, cO) -> (numLoci * (math.log(numLoci) + math.log(n) - math.log(vx) - math.log(vy)))

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

  type B = Boolean
  type D = Double
  type L = Long
  type S = String
  type OB = Option[B]
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

  type JointHistKey = (OB, OS, OL, OL)
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
        ((onGene, contig, depth1, depth2), numLoci) <- l
      } yield {
        val builder = JointHistogramRecord.newBuilder().setNumLoci(numLoci)
        onGene.foreach(l => builder.setOnGene(l))
        depth1.foreach(l => builder.setDepth1(l))
        depth2.foreach(l => builder.setDepth2(l))
        contig.foreach(c => builder.setContig(c))
        builder.build()
      }

    entries.adamParquetSave(filename)
  }

  def load(sc: SparkContext, fn: String): JointHistogram = {
    val rdd: RDD[JointHistogramRecord] = sc.adamLoad(fn)
    val jointHist =
      rdd.map(e => {
        val onGene: OB = Option(e.getOnGene).map(Boolean2boolean)
        val d1: OL = Option(e.getDepth1).map(Long2long)
        val d2: OL = Option(e.getDepth2).map(Long2long)
        val c: OS = Option(e.getContig)
        val nl: Long = e.getNumLoci
        ((onGene, c, d1, d2), nl)
      })

    JointHistogram(
      JointHistWithSums.fromPrecomputed("all", jointHist.filter(_._1._1 == None)),
      JointHistWithSums.fromPrecomputed("on", jointHist.filter(_._1._1 == Some(true))),
      JointHistWithSums.fromPrecomputed("off", jointHist.filter(_._1._1 == Some(false)))
    )
  }

  def j2s(m: java.util.Map[String, java.lang.Long]): Map[String, Long] = m.toMap.mapValues(Long2long)
  def s2j(m: Map[String, Long]): java.util.Map[String, java.lang.Long] = mapToJavaMap(m.mapValues(long2Long))

  def fromAlignmentFiles(sc: SparkContext,
                         readsFile1: String,
                         readsFile2: String,
                         intervalsFileOpt: Option[String] = None,
                         partitionSize: Option[Long] = None): JointHistogram = {
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
    val reads = sc.loadAlignments(readsFile1, None, projectionOpt).setName("reads1")
    val reads2 = sc.loadAlignments(readsFile2, None, projectionOpt).setName("reads2")
    val featuresOpt = intervalsFileOpt.map(f => sc.loadFeatures(f))
    JointHistogram.fromAlignments(
      reads,
      reads2,
      featuresOpt.map(
        (_, partitionSize.getOrElse(10000L))
      )
    )
  }

  def computeSums(jointHist: JointHist): JointHistogram = {
    jointHist.setName("jointHist").cache()

    val onGene = jointHist.filter(_._1._1.exists(_ == true))
    val offGene = jointHist.filter(_._1._1.exists(_ == false))
    val combined: JointHist =
      (for {
        ((_, cO, d1O, d2O), nl) <- jointHist
        key = (None, cO, d1O, d2O): JointHistKey
      } yield
        key -> nl
      ).reduceByKey(_ + _)

    JointHistogram(
      JointHistWithSums.computeSums("combined", combined),
      JointHistWithSums.computeSums("onGene", onGene),
      JointHistWithSums.computeSums("offGene", offGene)
    )

  }

  def fromAlignments(reads: RDD[AlignmentRecord],
                     reads2: RDD[AlignmentRecord],
                     featuresOpt: Option[(RDD[Feature], Long)] = None): JointHistogram = {
    val joinedReadDepthPerLocus: RDD[((String, Long, Option[Boolean]), (Long, Long))] =
      Alignments.joinedReadDepths(reads, reads2, featuresOpt)

    val jointHist: JointHist =
      (for {
        ((contig, _, onGene), (d1, d2)) <- joinedReadDepthPerLocus
        key = (onGene, Some(contig), Some(d1), Some(d2)): JointHistKey
      } yield key -> 1L).reduceByKey(_ + _)

    computeSums(jointHist)
  }

}

