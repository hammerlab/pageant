package org.hammerlab.pageant.rdd

import org.hammerlab.pageant.utils.SparkSuite
import GridCDFRDD._


class GridCDFRDDTest extends SparkSuite {

  test("4x4") {
    val input =
      for {
        r ← 0 until 4
        c ← 0 until 4
      } yield {
        (r, c) → (4 * (3 - r) + (3 - c))  // aka: 15 - 4r - c
      }

    /*

      Input / PDF:

         3   2 |  1   0
         7   6 |  5   4
        -------+-------
        11  10 |  9   8
        15  14 | 13  12

      After summing within each partition/block:

         5   2 |  1   0
        18   8 | 10   4
        -------+-------
        21  10 | 17   8
        50  24 | 42  20

      Output / CDF:

         6   3 |  1   0
        28  18 | 10   4
        -------+-------
        66  45 | 27  12
       120  84 | 52  24

     */

    val rdd = sc.parallelize(input)
    val gridRDD = GridCDFRDD.rddToGridCDFRDD(rdd)
    val partitioner = gridRDD.partitioner
    partitioner.numPartitions should be(4)
    partitioner.partitionRows should be(2)
    partitioner.partitionCols should be(2)

    gridRDD.rdd.count should be(16)

    val cdf = gridRDD.cdf(_ + _, 0).sortByKey().collect

    val rawExpected =
      List(
        List(  6,  3,  1,  0),
        List( 28, 18, 10,  4),
        List( 66, 45, 27, 12),
        List(120, 84, 52, 24)
      )

    val expected =
      (for {
        (row, r) ← rawExpected.map(_.zipWithIndex).zipWithIndex
        (t, c) ← row
      } yield {
        (3 - r, c) → t
      }).sortBy(_._1)

    cdf should be(expected)
  }

  test("10x10") {
    val input =
      for {
        r ← 0 until 10
        c ← 0 until 10
      } yield {
        (r, c) → (10 * (9 - r) + (9 - c))
      }

    val rdd = sc.parallelize(input)
    val cdf = rdd.cdf(_ + _, 0).sortByKey().values.collect

    /*

      Input / PDF:

           9    8    7    6    5    4    3    2    1    0
          19   18   17   16   15   14   13   12   11   10
          29   28   27   26   25   24   23   22   21   20
          39   38   37   36   35   34   33   32   31   30
          49   48   47   46   45   44   43   42   41   40
          59   58   57   56   55   54   53   52   51   50
          69   68   67   66   65   64   63   62   61   60
          79   78   77   76   75   74   73   72   71   70
          89   88   87   86   85   84   83   82   81   80
          99   98   97   96   95   94   93   92   91   90

      Output / CDF:

          45   36   28   21   15   10    6    3    1    0
         190  162  136  112   90   70   52   36   22   10
         435  378  324  273  225  180  138   99   63   30
         780  684  592  504  420  340  264  192  124   60
        1225 1080  940  805  675  550  430  315  205  100
        1770 1566 1368 1176  990  810  636  468  306  150
        2415 2142 1876 1617 1365 1120  882  651  427  210
        3160 2808 2464 2128 1800 1480 1168  864  568  280
        4005 3564 3132 2709 2295 1890 1494 1107  729  360
        4950 4410 3880 3360 2850 2350 1860 1380  910  450

     */

    val expectedStr =
      """
        |   45   36   28   21   15   10    6    3    1    0
        |  190  162  136  112   90   70   52   36   22   10
        |  435  378  324  273  225  180  138   99   63   30
        |  780  684  592  504  420  340  264  192  124   60
        | 1225 1080  940  805  675  550  430  315  205  100
        | 1770 1566 1368 1176  990  810  636  468  306  150
        | 2415 2142 1876 1617 1365 1120  882  651  427  210
        | 3160 2808 2464 2128 1800 1480 1168  864  568  280
        | 4005 3564 3132 2709 2295 1890 1494 1107  729  360
        | 4950 4410 3880 3360 2850 2350 1860 1380  910  450
      """.trim.stripMargin

    val expected = expectedStr.split("\n").reverse.map(_.trim.split("\\s+").map(_.toInt))

    val actual = cdf.grouped(10).toArray

    actual should be(expected)
  }
}
