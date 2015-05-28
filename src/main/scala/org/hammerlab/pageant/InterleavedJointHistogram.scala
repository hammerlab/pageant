package org.hammerlab.pageant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.pageant.PerContigJointHistogram.PerContigJointHist
import org.hammerlab.pageant.avro.{
  JointHistogram => JH,
  PerContigJointHistogram => PCJH,
  JointHistogramRecord
}

object InterleavedJointHistogram {
  type InterleavedJointHist = RDD[((Long,Long), (Long, Map[String, Long]))]

  def write(l: InterleavedJointHist, filename: String): Unit = {
    val entries =
      for {
        ((depth1, depth2), (numLoci, perContig)) <- l
      } yield {
        JointHistogramRecord
          .newBuilder()
          .setDepth1(depth1)
          .setDepth2(depth2)
          .setNumLoci(numLoci)
          .setLociPerContig(JointHistogram.s2j(perContig))
          .build()
      }

    entries.adamParquetSave(filename)
  }
  def splitContigs(l: InterleavedJointHist): (PerContigJointHistogram, Map[String, PerContigJointHistogram]) = {
    val total = PerContigJointHistogram(l.map(p => (p._1, p._2._1)))

    val keys = l.flatMap(_._2._2.keys).distinct().collect()

    val perContig: Map[String, PerContigJointHistogram] =
      (for {
        key <- keys
      } yield {
          key -> PerContigJointHistogram(
            for {
              ((d1,d2), (_,m)) <- l
              nl <- m.get(key)
            } yield {
              ((d1, d2), nl)
            }
          )
        }).toMap

    (total, perContig)
  }

  def joinContigs(l: PerContigJointHist, perContigs: Map[String, PerContigJointHistogram]): InterleavedJointHist = {
    val fromL: InterleavedJointHist =
      for {
        ((d1,d2), nl) <- l
      } yield {
        ((d1,d2), (nl, Map[String, Long]()))
      }

    val rdds: InterleavedJointHist =
      l.context.union(
        (for {
          (key, h) <- perContigs
        } yield {
            for {
              ((d1,d2), nl) <- h.l
            } yield {
              ((d1, d2), (0L, Map(key -> nl)))
            }
          }).toSeq :+ fromL
      )

    rdds.reduceByKey(mergeCounts)
  }

  def fromAlignmentFiles(sc: SparkContext,
                         file1: String,
                         file2: String): InterleavedJointHist = {
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
    InterleavedJointHistogram.fromAlignments(sc, reads, reads2)
  }

  def fromAlignments(sc: SparkContext,
                     reads: RDD[AlignmentRecord],
                     reads2: RDD[AlignmentRecord]): InterleavedJointHist = {

    val joinedReadDepthPerLocus: RDD[((String, Long), (Long, Long))] = Alignments.joinedReadDepths(reads, reads2)

    joinedReadDepthPerLocus.map({
      case ((contig, locus), count) => (count, (1L, Map(contig -> 1L)))
    }).reduceByKey(mergeCounts)
      .sortBy(_._1)
      .setName("joined hist")
      .persist()
  }

  def mergeCounts(a: (Long, Map[String, Long]),
                  b: (Long, Map[String, Long])): (Long, Map[String, Long]) =
    (a._1 + b._1, mergeMaps(a._2, b._2))

  def mergeMaps(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] = {
    (a.keySet ++ b.keySet).map(i => (i,a.getOrElse(i, 0L) + b.getOrElse(i, 0L))).toMap
  }

  def load(sc: SparkContext, fn: String): InterleavedJointHist = {
    val rdd: RDD[JointHistogramRecord] = sc.adamLoad(fn)
    rdd.map(e => {
      val d1: Long = e.getDepth1
      val d2: Long = e.getDepth2
      val nl: Long = e.getNumLoci
      val m: Map[String, Long] = e.getLociPerContig.toMap.mapValues(Long2long)
      ((d1, d2), (nl, m))
    })
  }

  def collect(l: InterleavedJointHist): JH = {
    val pcjh = PerContigJointHistogram.collect((l.map(p => (p._1, p._2._1))))

    val contigs = l.flatMap(_._2._2.keys).distinct().collect()

    lazy val lociDepthsPerContig: Map[String, PCJH] =
      (for {
        contig <- contigs
      } yield {

          val b = for {
            (depths, (_, numsPerContig)) <- l
            num <- numsPerContig.get(contig)
          } yield {
              (depths, num)
            }

          contig -> PerContigJointHistogram.collect(b)
        }).toMap

    JH.newBuilder()
    .setTotal(pcjh)
    .setContigHistograms(lociDepthsPerContig)
    .build()
  }

}

