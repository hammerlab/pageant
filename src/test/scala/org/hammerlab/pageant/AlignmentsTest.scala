
package org.hammerlab.pageant

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{Contig, AlignmentRecord}
import org.scalatest.Matchers

class AlignmentsTest extends ADAMFunSuite with Matchers {

  def makeRead(start: Long, sequence: String, cigar: String): AlignmentRecord = {
    AlignmentRecord.newBuilder()
      .setContig(Contig.newBuilder().setContigName("chr1").build())
      .setSequence(sequence)
      .setStart(start)
      .setCigar(cigar)
      .setReadMapped(true)
      .build()
  }

  sparkTest("skipInsertionCoverage") {

    /*

       idx:  123456789012
        r1:  ACGTAACCGGTT
        r2:      AA----TT
        r3:   CGTAACCGG
                 ^
                AAA
     depth:  122233333322
     */

    val reads = sc.parallelize(
      List(
        makeRead(1, "ACGTAACCGGTT", "12M"),
        makeRead(5, "AATT", "2M4D2M"),
        makeRead(2, "CGTAAAAACCGG", "3M3I6M")
      )
    )

    val l: Array[(Long, Long)] = Alignments.getReadDepthPerLocus(reads).collect().map(p => (p._1._2, p._2)).sortBy(_._1)

    l should be(
      Array(
        (1,1),
        (2,2),
        (3,2),
        (4,2),
        (5,3),
        (6,3),
        (7,3),
        (8,3),
        (9,3),
        (10,3),
        (11,2),
        (12,2)
      )
    )
  }
}
