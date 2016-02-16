package org.hammerlab.pageant.fm.finder

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.fm.index.SparkFM
import org.hammerlab.pageant.fm.utils.Utils._
import org.hammerlab.pageant.fm.utils.{Bounds, FMSuite}
import org.hammerlab.pageant.utils.Utils.resourcePath

abstract class FMFinderBamTest extends FMSuite with FMFinderTest {

  def initFM(sc: SparkContext): SparkFM = {
    SparkFM.load(sc, resourcePath("normal.bam.fm"), gzip = true)
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
    ("$$", 0),
    ("$A", 307),
    ("$C", 534),
    ("$G", 778),
    ("$T", 999),
    ("$N", 1000),
    ("A$", 1311),
    ("AA", 11922),
    ("AC", 18955),
    ("AG", 25370),
    ("AT", 32286),
    ("AN", 32291),
    ("C$", 32534),
    ("CA", 39170),
    ("CC", 45149),
    ("CG", 51386),
    ("CT", 56475),
    ("CN", 56475),
    ("G$", 56710),
    ("GA", 63810),
    ("GC", 69361),
    ("GG", 75511),
    ("GT", 80112),
    ("GN", 80113),
    ("T$", 80323),
    ("TA", 86956),
    ("TC", 92350),
    ("TG", 96942),
    ("TT", 101897),
    ("TN", 101897),
    ("N$", 101898),
    ("NA", 101902),
    ("NC", 101902),
    ("NG", 101902),
    ("NT", 101904),
    ("NN", 102000)
  )

  testAllLF(
    "normal-3",
    ("$$$", 0),
    ("$$A", 0),
    ("$$C", 0),
    ("$$G", 0),
    ("$$T", 0),
    ("$$N", 0),
    ("$A$", 0),
    ("$AA", 102),
    ("$AC", 181),
    ("$AG", 241),
    ("$AT", 307),
    ("$AN", 307),
    ("$C$", 307),
    ("$CA", 364),
    ("$CC", 425),
    ("$CG", 485),
    ("$CT", 534),
    ("$CN", 534),
    ("$G$", 534),
    ("$GA", 599),
    ("$GC", 669),
    ("$GG", 731),
    ("$GT", 778),
    ("$GN", 778),
    ("$T$", 778),
    ("$TA", 854),
    ("$TC", 903),
    ("$TG", 943),
    ("$TT", 999),
    ("$TN", 999),
    ("$N$", 999),
    ("$NA", 999),
    ("$NC", 999),
    ("$NG", 999),
    ("$NT", 999),
    ("$NN", 1000),
    ("A$$", 1000),
    ("A$A", 1109),
    ("A$C", 1179),
    ("A$G", 1243),
    ("A$T", 1311),
    ("A$N", 1311),
    ("AA$", 1426),
    ("AAA", 5387),
    ("AAC", 7579),
    ("AAG", 9800),
    ("AAT", 11921),
    ("AAN", 11922),
    ("AC$", 11992),
    ("ACA", 14117),
    ("ACC", 15781),
    ("ACG", 17667),
    ("ACT", 18955),
    ("ACN", 18955),
    ("AG$", 19021),
    ("AGA", 20954),
    ("AGC", 22338),
    ("AGG", 24381),
    ("AGT", 25369),
    ("AGN", 25370),
    ("AT$", 25432),
    ("ATA", 27876),
    ("ATC", 29375),
    ("ATG", 30863),
    ("ATT", 32286),
    ("ATN", 32286),
    ("AN$", 32286),
    ("ANA", 32287),
    ("ANC", 32287),
    ("ANG", 32287),
    ("ANT", 32288),
    ("ANN", 32291),
    ("C$$", 32291),
    ("C$A", 32355),
    ("C$C", 32422),
    ("C$G", 32489),
    ("C$T", 32534),
    ("C$N", 32534),
    ("CA$", 32602),
    ("CAA", 34531),
    ("CAC", 36107),
    ("CAG", 37394),
    ("CAT", 39169),
    ("CAN", 39170),
    ("CC$", 39234),
    ("CCA", 40726),
    ("CCC", 42785),
    ("CCG", 43997),
    ("CCT", 45149),
    ("CCN", 45149),
    ("CG$", 45193),
    ("CGA", 47095),
    ("CGC", 48791),
    ("CGG", 50164),
    ("CGT", 51386),
    ("CGN", 51386),
    ("CT$", 51443),
    ("CTA", 52842),
    ("CTC", 54175),
    ("CTG", 55443),
    ("CTT", 56475),
    ("CTN", 56475),
    ("CN$", 56475),
    ("CNA", 56475),
    ("CNC", 56475),
    ("CNG", 56475),
    ("CNT", 56475),
    ("CNN", 56475),
    ("G$$", 56475),
    ("G$A", 56538),
    ("G$C", 56584),
    ("G$G", 56652),
    ("G$T", 56709),
    ("G$N", 56710),
    ("GA$", 56779),
    ("GAA", 59158),
    ("GAC", 60958),
    ("GAG", 62357),
    ("GAT", 63809),
    ("GAN", 63810),
    ("GC$", 63865),
    ("GCA", 65515),
    ("GCC", 66524),
    ("GCG", 68047),
    ("GCT", 69361),
    ("GCN", 69361),
    ("GG$", 69415),
    ("GGA", 71240),
    ("GGC", 72652),
    ("GGG", 74248),
    ("GGT", 75511),
    ("GGN", 75511),
    ("GT$", 75549),
    ("GTA", 76882),
    ("GTC", 77956),
    ("GTG", 78807),
    ("GTT", 80112),
    ("GTN", 80112),
    ("GN$", 80112),
    ("GNA", 80113),
    ("GNC", 80113),
    ("GNG", 80113),
    ("GNT", 80113),
    ("GNN", 80113),
    ("T$$", 80113),
    ("T$A", 80184),
    ("T$C", 80228),
    ("T$G", 80272),
    ("T$T", 80323),
    ("T$N", 80323),
    ("TA$", 80382),
    ("TAA", 82619),
    ("TAC", 84004),
    ("TAG", 85452),
    ("TAT", 86954),
    ("TAN", 86956),
    ("TC$", 87010),
    ("TCA", 88322),
    ("TCC", 89508),
    ("TCG", 91064),
    ("TCT", 92350),
    ("TCN", 92350),
    ("TG$", 92421),
    ("TGA", 93796),
    ("TGC", 94785),
    ("TGG", 95861),
    ("TGT", 96942),
    ("TGN", 96942),
    ("TT$", 96995),
    ("TTA", 98375),
    ("TTC", 99814),
    ("TTG", 100758),
    ("TTT", 101897),
    ("TTN", 101897),
    ("TN$", 101897),
    ("TNA", 101897),
    ("TNC", 101897),
    ("TNG", 101897),
    ("TNT", 101897),
    ("TNN", 101897),
    ("N$$", 101897),
    ("N$A", 101897),
    ("N$C", 101897),
    ("N$G", 101898),
    ("N$T", 101898),
    ("N$N", 101898),
    ("NA$", 101898),
    ("NAA", 101901),
    ("NAC", 101902),
    ("NAG", 101902),
    ("NAT", 101902),
    ("NAN", 101902),
    ("NC$", 101902),
    ("NCA", 101902),
    ("NCC", 101902),
    ("NCG", 101902),
    ("NCT", 101902),
    ("NCN", 101902),
    ("NG$", 101902),
    ("NGA", 101902),
    ("NGC", 101902),
    ("NGG", 101902),
    ("NGT", 101902),
    ("NGN", 101902),
    ("NT$", 101902),
    ("NTA", 101903),
    ("NTC", 101903),
    ("NTG", 101904),
    ("NTT", 101904),
    ("NTN", 101904),
    ("NN$", 101905),
    ("NNA", 101907),
    ("NNC", 101907),
    ("NNG", 101907),
    ("NNT", 101908),
    ("NNN", 102000)
  )

  testLF(
    "tens",
    "GGCCCTAAACA" -> (0, 0),
    "GCCCTAAACAG" -> (0, 0),
    "CCCTAAACAGG" -> (0, 0),
    "CCTAAACAGGT" -> (0, 0),
    "CTAAACAGGTG" -> (0, 0),
    "TAAACAGGTGG" -> (0, 0),
    "AAACAGGTGGT" -> (0, 0),
    "AACAGGTGGTA" -> (0, 0),
    "ACAGGTGGTAA" -> (0, 0),
    "CAGGTGGTAAG" -> (0, 0)
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
//        withClue(s"$str") {
//          counts.size should be(expected.size)
//          for {
//            ((r, c), expectedCount) <- expected
//            actualCount = counts(r, c)
//          } {
//            withClue(s"($r,$c)") {
//              actualCount should be(expectedCount)
//            }
//          }
//        }
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
