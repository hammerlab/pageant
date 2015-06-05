package org.hammerlab.pageant

import org.bdgenomics.adam.rich.RichAlignmentRecord

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition}
import org.bdgenomics.adam.rdd.{BroadcastRegionJoin, ShuffleRegionJoin}
import org.bdgenomics.formats.avro.{Feature, AlignmentRecord}
import org.bdgenomics.adam.rdd.ADAMContext._

object Alignments {
  def getReadDepthPerLocus(reads: RDD[AlignmentRecord]): RDD[((String, Long), Long)] = {
    (for {
      read <- reads if read.getReadMapped
      contig <- Option(read.getContig).toList
      name <- Option(contig.getContigName).toList
      start <- Option(read.getStart).toList
      refLen = RichAlignmentRecord(read).referenceLength
      i <- (0 until refLen)
    } yield {
      ((name, start + i), 1L)
    }).reduceByKey(_ + _)
  }

  def joinedReadDepths(reads: RDD[AlignmentRecord],
                       reads2: RDD[AlignmentRecord],
                       featuresOpt: Option[(RDD[Feature], Long)] = None,
                       shuffle: Boolean = false): RDD[((String, Long, Option[Boolean]), (Long, Long))] = {

    val readDepthPerLocus: RDD[((String, Long), Long)] = Alignments.getReadDepthPerLocus(reads)
    val readDepthPerLocus2: RDD[((String, Long), Long)] = Alignments.getReadDepthPerLocus(reads2)

    val lociToDepths =
      readDepthPerLocus.fullOuterJoin(readDepthPerLocus2).map {
        case (locus, (count1Opt, count2Opt)) => (locus, (count1Opt.getOrElse(0L), count2Opt.getOrElse(0L)))
      }

    featuresOpt match {
      case Some((features, partitionSize)) =>
        val lociMap = lociToDepths.keys.distinct().map {
          case (chr, locus) => (ReferenceRegion(ReferencePosition(chr, locus)) -> (chr, locus))
        }

        val featureMap = features.map(f => ReferenceRegion(f) -> f)

        val sd1 = reads.adamGetSequenceDictionary()
        val sd2 = reads2.adamGetSequenceDictionary()
        val sd = sd1 ++ sd2

        val joined =
          if (shuffle)
            ShuffleRegionJoin(sd, partitionSize).partitionAndJoin(
              featureMap,
              lociMap
            ).collect()
          else
            BroadcastRegionJoin.partitionAndJoin(
              featureMap,
              lociMap
            ).collect()

        val overlappingLoci = joined.map(_._2 -> true).distinct

        lociToDepths.leftOuterJoin(lociToDepths.context.parallelize(overlappingLoci)).map {
          case ((chr, locus), ((d1, d2), Some(true))) => (chr, locus, Some(true)) -> (d1, d2)
          case ((chr, locus), ((d1, d2), None)) => (chr, locus, Some(false)) -> (d1, d2)
        }
      case None =>
        for {
          ((chr, locus), (d1, d2)) <- lociToDepths
        } yield
          (chr, locus, None) -> (d1, d2)
    }
  }
}
