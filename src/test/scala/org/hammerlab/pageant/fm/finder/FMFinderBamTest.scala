package org.hammerlab.pageant.fm.finder

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.index.SparkFM
import org.hammerlab.pageant.fm.utils.Utils._
import org.hammerlab.pageant.fm.utils.{Bounds, FMSuite}
import org.hammerlab.pageant.utils.Utils.resourcePath

abstract class FMFinderBamTest extends FMSuite with FMFinderTest {

  def initFM(sc: SparkContext): SparkFM = {
    SparkFM.load(sc, "src/test/resources/normal.bam.fm", gzip = false)
  }

  def formatActual(result: RDD[(AT, Bounds)]) = {
    result.collect.map(p => (p._1.map(toC).mkString(""), p._2.toTuple))
  }

  def testAllLF(name: String, tuples: (String, Int)*) =
    testLF(
      name,
      (for {
        ((s1, n1), (s2, n2)) <- (("", 0) :: tuples.toList).sliding(2).map(a => (a(0), a(1))).toArray
      } yield {
        (s2, (n1, n2))
      }): _*
    )

  def testLF(name: String, expected: (String, (Int, Int))*): Unit = {
    val strs = expected.map(_._1)
    val needles: Seq[Array[T]] = strs.map(_.toArray.map(toI))

    test(name) {
      val needlesRdd = sc.parallelize(needles, 2)
      formatActual(fmf.occ(needlesRdd)) should be(expected)
    }
  }

  testAllLF(
    "normal-1",
    ("$", 1000),
    ("A", 32291),
    ("C", 56475),
    ("G", 80113),
    ("T", 101897),
    ("N", 102000)
  )

  testAllLF(
    "normal-2",
    "$$" → 0,
    "$A" → 313,
    "$C" → 564,
    "$G" → 768,
    "$T" → 1000,
    "$N" → 1000,
    "A$" → 1323,
    "AA" → 11566,
    "AC" → 18265,
    "AG" → 26614,
    "AT" → 32290,
    "AN" → 32291,
    "C$" → 32535,
    "CA" → 42004,
    "CC" → 48825,
    "CG" → 49576,
    "CT" → 56474,
    "CN" → 56475,
    "G$" → 56699,
    "GA" → 63816,
    "GC" → 69233,
    "GG" → 75882,
    "GT" → 80111,
    "GN" → 80113,
    "T$" → 80319,
    "TA" → 84467,
    "TC" → 89463,
    "TG" → 97148,
    "TT" → 101897,
    "TN" → 101897,
    "N$" → 101900,
    "NA" → 101901,
    "NC" → 101901,
    "NG" → 101901,
    "NT" → 101901,
    "NN" → 102000
  )

  testAllLF(
    "normal-3",
    "$$$" → 0,
    "$$A" → 0,
    "$$C" → 0,
    "$$G" → 0,
    "$$T" → 0,
    "$$N" → 0,
    "$A$" → 0,
    "$AA" → 100,
    "$AC" → 160,
    "$AG" → 245,
    "$AT" → 312,
    "$AN" → 313,
    "$C$" → 313,
    "$CA" → 426,
    "$CC" → 491,
    "$CG" → 502,
    "$CT" → 564,
    "$CN" → 564,
    "$G$" → 564,
    "$GA" → 626,
    "$GC" → 676,
    "$GG" → 729,
    "$GT" → 768,
    "$GN" → 768,
    "$T$" → 768,
    "$TA" → 810,
    "$TC" → 857,
    "$TG" → 948,
    "$TT" → 1000,
    "$TN" → 1000,
    "$N$" → 1000,
    "$NA" → 1000,
    "$NC" → 1000,
    "$NG" → 1000,
    "$NT" → 1000,
    "$NN" → 1000,
    "A$$" → 1000,
    "A$A" → 1082,
    "A$C" → 1140,
    "A$G" → 1220,
    "A$T" → 1323,
    "A$N" → 1323,
    "AA$" → 1429,
    "AAA" → 5221,
    "AAC" → 7502,
    "AAG" → 9871,
    "AAT" → 11566,
    "AAN" → 11566,
    "AC$" → 11634,
    "ACA" → 14821,
    "ACC" → 16390,
    "ACG" → 16602,
    "ACT" → 18264,
    "ACN" → 18265,
    "AG$" → 18346,
    "AGA" → 21214,
    "AGC" → 23138,
    "AGG" → 25271,
    "AGT" → 26614,
    "AGN" → 26614,
    "AT$" → 26669,
    "ATA" → 28098,
    "ATC" → 29617,
    "ATG" → 31187,
    "ATT" → 32290,
    "ATN" → 32290,
    "AN$" → 32290,
    "ANA" → 32290,
    "ANC" → 32290,
    "ANG" → 32290,
    "ANT" → 32290,
    "ANN" → 32291,
    "C$$" → 32291,
    "C$A" → 32385,
    "C$C" → 32439,
    "C$G" → 32487,
    "C$T" → 32535,
    "C$N" → 32535,
    "CA$" → 32632,
    "CAA" → 34987,
    "CAC" → 37310,
    "CAG" → 39975,
    "CAT" → 42004,
    "CAN" → 42004,
    "CC$" → 42062,
    "CCA" → 44683,
    "CCC" → 46753,
    "CCG" → 46972,
    "CCT" → 48825,
    "CCN" → 48825,
    "CG$" → 48835,
    "CGA" → 49050,
    "CGC" → 49196,
    "CGG" → 49345,
    "CGT" → 49576,
    "CGN" → 49576,
    "CT$" → 49640,
    "CTA" → 50884,
    "CTC" → 52535,
    "CTG" → 55188,
    "CTT" → 56474,
    "CTN" → 56474,
    "CN$" → 56475,
    "CNA" → 56475,
    "CNC" → 56475,
    "CNG" → 56475,
    "CNT" → 56475,
    "CNN" → 56475,
    "G$$" → 56475,
    "G$A" → 56551,
    "G$C" → 56630,
    "G$G" → 56665,
    "G$T" → 56699,
    "G$N" → 56699,
    "GA$" → 56771,
    "GAA" → 59366,
    "GAC" → 60512,
    "GAG" → 62813,
    "GAT" → 63816,
    "GAN" → 63816,
    "GC$" → 63871,
    "GCA" → 65586,
    "GCC" → 67279,
    "GCG" → 67399,
    "GCT" → 69233,
    "GCN" → 69233,
    "GG$" → 69312,
    "GGA" → 71333,
    "GGC" → 72745,
    "GGG" → 74671,
    "GGT" → 75880,
    "GGN" → 75882,
    "GT$" → 75930,
    "GTA" → 76586,
    "GTC" → 77504,
    "GTG" → 79286,
    "GTT" → 80111,
    "GTN" → 80111,
    "GN$" → 80112,
    "GNA" → 80113,
    "GNC" → 80113,
    "GNG" → 80113,
    "GNT" → 80113,
    "GNN" → 80113,
    "T$$" → 80113,
    "T$A" → 80172,
    "T$C" → 80232,
    "T$G" → 80273,
    "T$T" → 80319,
    "T$N" → 80319,
    "TA$" → 80367,
    "TAA" → 81768,
    "TAC" → 82656,
    "TAG" → 83585,
    "TAT" → 84467,
    "TAN" → 84467,
    "TC$" → 84530,
    "TCA" → 86363,
    "TCC" → 87787,
    "TCG" → 87976,
    "TCT" → 89463,
    "TCN" → 89463,
    "TG$" → 89517,
    "TGA" → 91468,
    "TGC" → 93353,
    "TGG" → 95741,
    "TGT" → 97148,
    "TGN" → 97148,
    "TT$" → 97187,
    "TTA" → 97964,
    "TTC" → 98825,
    "TTG" → 100414,
    "TTT" → 101897,
    "TTN" → 101897,
    "TN$" → 101897,
    "TNA" → 101897,
    "TNC" → 101897,
    "TNG" → 101897,
    "TNT" → 101897,
    "TNN" → 101897,
    "N$$" → 101897,
    "N$A" → 101899,
    "N$C" → 101899,
    "N$G" → 101899,
    "N$T" → 101900,
    "N$N" → 101900,
    "NA$" → 101900,
    "NAA" → 101900,
    "NAC" → 101901,
    "NAG" → 101901,
    "NAT" → 101901,
    "NAN" → 101901,
    "NC$" → 101901,
    "NCA" → 101901,
    "NCC" → 101901,
    "NCG" → 101901,
    "NCT" → 101901,
    "NCN" → 101901,
    "NG$" → 101901,
    "NGA" → 101901,
    "NGC" → 101901,
    "NGG" → 101901,
    "NGT" → 101901,
    "NGN" → 101901,
    "NT$" → 101901,
    "NTA" → 101901,
    "NTC" → 101901,
    "NTG" → 101901,
    "NTT" → 101901,
    "NTN" → 101901,
    "NN$" → 101902,
    "NNA" → 101902,
    "NNC" → 101902,
    "NNG" → 101902,
    "NNT" → 101902,
    "NNN" → 102000
  )

  testLF(
    "tens",
    "GGCCCTAAACA" → (72020, 72054),
    "GCCCTAAACAG" → (66538, 66572),
    "CCCTAAACAGG" → (46240, 46274),
    "CCTAAACAGGT" → (46999, 47031),
    "CTAAACAGGTG" → (49671, 49703),
    "TAAACAGGTGG" → (80557, 80591),
    "AAACAGGTGGT" → ( 3214,  3249),
    "AACAGGTGGTA" → ( 5939,  5974),
    "ACAGGTGGTAA" → (13883, 13916),
    "CAGGTGGTAAG" → (39538, 39569)
  )

  def testCountBidi(strTuples: ((String, String, String), List[Int])*): Unit = {
    val tuples = for {
      ((left, middle, right), counts) <- strTuples.toList
      str = List(left, middle, right).mkString("")
      start = left.length
      end = start + middle.length
    } yield {
      ((str, start, end), counts)
    }
    val strs = tuples.map(_._1._1)
    val expectedMap =
      for {
        ((str, start, end), counts) <- tuples.toMap
      } yield {
        counts.length should be((start + 1) * (str.length - end + 1))
        str -> (for {
          (idx, count) <- counts.zipWithIndex.map(_.swap).toMap
          r = idx / (start + 1)
          c = end + (idx % (start + 1))
        } yield {
          (r, c) -> count
        })
      }

    test(s"countBidi-${strs.mkString(",")}") {
      val needles: Seq[(Array[T], TPos, TPos)] =
        for {
          ((str, start, end), _) <- tuples
        } yield {
          (str.toArray.map(toI), start, end)
        }

      val needlesRdd = sc.parallelize(needles, 2)

      val actualMap = for {
        (ts, map) <- fmf.countBidi(needlesRdd).collectAsMap()
        str = ts.map(toC).mkString("")
      } yield {
        str -> (for {
          (r, rm) <- map.m.toList
          (c, count) <- rm.toList
        } yield {
          (r, c) -> count
        }).sortBy(_._1).toMap
      }

      for {
        (str, expected) <- expectedMap
        counts = actualMap(str).toList.sortBy(_._1)
      } {
        counts should be(expected.toList.sortBy(_._1))
      }
    }
  }

  // Count occurrences in the corpus of every substring starting in the left
  // string, continuing through the middle string, and ending in the right string.
  testCountBidi(
    ("GGCCC", "TAAACAGGTG", "GTAAG") -> List(

               /*    15  16  17  18  19  20   */
               /*     G   G   T   A   A   G   */

      /*  0: G  */   31, 31, 30, 30, 28, 26,
      /*  1: G  */   31, 31, 30, 30, 28, 26,
      /*  2: C  */   32, 32, 31, 31, 29, 27,
      /*  3: C  */   32, 32, 31, 31, 29, 27,
      /*  4: C  */   32, 32, 31, 31, 29, 27,
      /*  5: T  */   34, 34, 33, 33, 31, 29
    )
  )
}

class BroadcastFinderBamTest extends FMFinderBamTest with BroadcastFinderTest
class AllTFinderBamTest extends FMFinderBamTest with AllTFinderTest
