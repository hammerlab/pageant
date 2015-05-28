package org.hammerlab.pageant

import htsjdk.samtools.TextCigarCodec
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
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
                       reads2: RDD[AlignmentRecord]): RDD[((String, Long), (Long, Long))] = {

    val readDepthPerLocus: RDD[((String, Long), Long)] = Alignments.getReadDepthPerLocus(reads)
    val readDepthPerLocus2: RDD[((String, Long), Long)] = Alignments.getReadDepthPerLocus(reads2)

    readDepthPerLocus.fullOuterJoin(readDepthPerLocus2).map {
      case (locus, (count1Opt, count2Opt)) => (locus, (count1Opt.getOrElse(0L), count2Opt.getOrElse(0L)))
    }
  }
}
