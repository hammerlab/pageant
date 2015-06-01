package org.hammerlab.pageant

import reflect.{ClassTag, classTag}
import htsjdk.samtools.TextCigarCodec
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition}
import org.bdgenomics.adam.rdd.ShuffleRegionJoin
import org.bdgenomics.adam.rich.ReferenceMappingContext.{ReferenceRegionReferenceMapping, FeatureReferenceMapping}
import org.bdgenomics.formats.avro.{Feature, AlignmentRecord}
import org.bdgenomics.adam.rdd.ADAMContext._

object Alignments {
  def getReadDepthPerLocus(reads: RDD[AlignmentRecord]): RDD[((String, Long), Long)] = {
    (for {
      read <- reads if read.getReadMapped
      contig <- Option(read.getContig)
      name <- Option(contig.getContigName)
      start <- Option(read.getStart)
      cigar = TextCigarCodec.getSingleton.decode(read.getCigar)
    } yield {
        cigar.getCigarElements.foldLeft((0, List[((String,Long), Long)]()))((p, elem) => {
          if (elem.getOperator.consumesReferenceBases()) {
            val (offset, tuples) = p
            val l =
              (0 until elem.getLength).map(i => ((name, start + offset + i), 1L)).toList

            (offset + elem.getLength, tuples ++ l)
          } else {
            p
          }
        })._2
      }).flatMap(x => x).reduceByKey(_ + _)
  }

  def joinedReadDepths(reads: RDD[AlignmentRecord],
                       reads2: RDD[AlignmentRecord],
                       featuresOpt: Option[(RDD[Feature], Long)] = None): RDD[((String, Long, Option[Boolean]), (Long, Long))] = {

    val readDepthPerLocus: RDD[((String, Long), Long)] = Alignments.getReadDepthPerLocus(reads)
    val readDepthPerLocus2: RDD[((String, Long), Long)] = Alignments.getReadDepthPerLocus(reads2)

    val joined =
      readDepthPerLocus.fullOuterJoin(readDepthPerLocus2).map {
        case (locus, (count1Opt, count2Opt)) => (locus, (count1Opt.getOrElse(0L), count2Opt.getOrElse(0L)))
      }

    featuresOpt match {
      case Some((features, partitionSize)) =>
        val loci = joined.keys.distinct().map {
          case (chr, locus) => ReferenceRegion(ReferencePosition(chr, locus))
        }

        val sd1 = reads.adamGetSequenceDictionary()
        val sd2 = reads2.adamGetSequenceDictionary()
        val sd = sd1 ++ sd2

        val overlappingLoci =
          ShuffleRegionJoin.partitionAndJoin(
            features.context,
            loci,
            features,
            sd,
            partitionSize
          )(
              ReferenceRegionReferenceMapping,
              FeatureReferenceMapping,
              classTag[ReferenceRegion],
              classTag[Feature]
          )
            .map(t => (t._1.referenceName, t._1.start) -> true)
            .distinct()

        joined.leftOuterJoin(overlappingLoci).map {
          case ((chr, locus), ((d1, d2), Some(true))) => (chr, locus, Some(true)) -> (d1, d2)
          case ((chr, locus), ((d1, d2), None)) => (chr, locus, Some(false)) -> (d1, d2)
        }
      case None =>
        for {
          ((chr, locus), (d1, d2)) <- joined
        } yield
          (chr, locus, None) -> (d1, d2)
    }
  }
}
