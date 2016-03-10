package org.hammerlab.pageant.fm.index

/*
 * Building the BWT of the strings:

 *   ACAG$
 *   TCAG$
 *   CCGA$
 *   AGTC$
 *
 * using a bunch of different partition/block-size configurations.
 */
class SmallDirectFMTest extends DirectFMTest {

  val strings = List(
    "ACAG$",
    "TCAG$",
    "CCGA$",
    "AGTC$"
  )

  val blocks100 = List(
    ("0 0 0 0 0 0", "GGAC") :: Nil,
    ("0 0 0 0 0 0", "GGACGTAA") :: Nil,
    ("0 0 0 0 0 0", "GGACGCCTAACG") :: Nil,
    ("0 0 0 0 0 0", "GGACGCCTATCAACAG") :: Nil,
    ("0 0 0 0 0 0", "GGACG$CC$TAT$CAACAG$") :: Nil
  )

  val blocks10 = List(
    ("0 0 0 0 0 0", "GGAC") :: Nil,

    ("0 0 0 0 0 0", "GGACGTAA") :: Nil,

    ("0 0 0 0 0 0", "GGACGCCTAA") ::
    ("0 3 3 3 1 0", "CG") :: Nil,

    ("0 0 0 0 0 0", "GGACGCCTAT") ::
    ("0 2 3 3 2 0", "CAACAG") :: Nil,

    ("0 0 0 0 0 0", "GGACG$CC$T") ::
    ("2 1 3 3 1 0", "AT$CAACAG$") :: Nil
  )

  val blocks5 = List(
    ("0 0 0 0 0 0", "GGAC") :: Nil,

    ("0 0 0 0 0 0", "GGACG") ::
    ("0 1 1 3 0 0", "TAA") :: Nil,

    ("0 0 0 0 0 0", "GGACG") ::
    ("0 1 1 3 0 0", "CCTAA") ::
    ("0 3 3 3 1 0", "CG") :: Nil,

    ("0 0 0 0 0 0", "GGACG") ::
    ("0 1 1 3 0 0", "CCTAT") ::
    ("0 2 3 3 2 0", "CAACA") ::
    ("0 5 5 3 2 0", "G") :: Nil,

    ("0 0 0 0 0 0", "GGACG") ::
    ("0 1 1 3 0 0", "$CC$T") ::
    ("2 1 3 3 1 0", "AT$CA") ::
    ("3 3 4 3 2 0", "ACAG$") :: Nil
  )

  val blocks4 = List(
    ("0 0 0 0 0 0", "GGAC") :: Nil,

    ("0 0 0 0 0 0", "GGAC") ::
    ("0 1 1 2 0 0", "GTAA") :: Nil,

    ("0 0 0 0 0 0", "GGAC") ::
    ("0 1 1 2 0 0", "GCCT") ::
    ("0 1 3 3 1 0", "AACG") :: Nil,

    ("0 0 0 0 0 0", "GGAC") ::
    ("0 1 1 2 0 0", "GCCT") ::
    ("0 1 3 3 1 0", "ATCA") ::
    ("0 3 4 3 2 0", "ACAG") :: Nil,

    ("0 0 0 0 0 0", "GGAC") ::
    ("0 1 1 2 0 0", "G$CC") ::
    ("1 1 3 3 0 0", "$TAT") ::
    ("2 2 3 3 2 0", "$CAA") ::
    ("3 4 4 3 2 0", "CAG$") :: Nil
  )

  val blocks3 = List(
    ("0 0 0 0 0 0", "GGA") ::
    ("0 1 0 2 0 0", "C") :: Nil,

    ("0 0 0 0 0 0", "GGA") ::
    ("0 1 0 2 0 0", "CGT") ::
    ("0 1 1 3 1 0", "AA") :: Nil,

    ("0 0 0 0 0 0", "GGA") ::
    ("0 1 0 2 0 0", "CGC") ::
    ("0 1 2 3 0 0", "CTA") ::
    ("0 2 3 3 1 0", "ACG") :: Nil,

    ("0 0 0 0 0 0", "GGA") ::
    ("0 1 0 2 0 0", "CGC") ::
    ("0 1 2 3 0 0", "CTA") ::
    ("0 2 3 3 1 0", "TCA") ::
    ("0 3 4 3 2 0", "ACA") ::
    ("0 5 5 3 2 0", "G") :: Nil,

    ("0 0 0 0 0 0", "GGA") ::
    ("0 1 0 2 0 0", "CG$") ::
    ("1 1 1 3 0 0", "CC$") ::
    ("2 1 3 3 0 0", "TAT") ::
    ("2 2 3 3 2 0", "$CA") ::
    ("3 3 4 3 2 0", "ACA") ::
    ("3 5 5 3 2 0", "G$") :: Nil
  )

  val bounds100 = List(
    100 :: Nil,
    100 :: Nil,
    100 :: Nil,
    100 :: Nil,
    100 :: Nil
  )

  val bounds15 = List(
    15 :: Nil,
    15 :: Nil,
    15 :: Nil,
    15 :: 30 :: Nil,
    15 :: 30 :: Nil
  )

  val bounds12 = List(
    12 :: Nil,
    12 :: Nil,
    12 :: Nil,
    12 :: 24 :: Nil,
    12 :: 24 :: Nil
  )

  val bounds10 = List(
    10 :: Nil,
    10 :: Nil,
    10 :: 20 :: Nil,
    10 :: 20 :: Nil,
    10 :: 20 :: Nil
  )

  val bounds9 = List(
    9 :: Nil,
    9 :: Nil,
    9 :: 18 :: Nil,
    9 :: 18 :: Nil,
    9 :: 18 :: 27 :: Nil
  )

  val bounds8 = List(
    8 :: Nil,
    8 :: Nil,
    8 :: 16 :: Nil,
    8 :: 16 :: Nil,
    8 :: 16 :: 24 :: Nil
  )

  val bounds6 = List(
    6 :: Nil,
    6 :: 12 :: Nil,
    6 :: 12 :: Nil,
    6 :: 12 :: 18 :: Nil,
    6 :: 12 :: 18 :: 24 :: Nil
  )

  val bounds5 = List(
    5 :: Nil,
    5 :: 10 :: Nil,
    5 :: 10 :: 15 :: Nil,
    5 :: 10 :: 15 :: 20 :: Nil,
    5 :: 10 :: 15 :: 20 :: Nil
  )

  val bounds4 = List(
    4 :: Nil,
    4 :: 8 :: Nil,
    4 :: 8 :: 12 :: Nil,
    4 :: 8 :: 12 :: 16 :: Nil,
    4 :: 8 :: 12 :: 16 :: 20 :: Nil
  )

  val bounds3 = List(
    3 :: 6 :: Nil,
    3 :: 6 :: 9 :: Nil,
    3 :: 6 :: 9 :: 12 :: Nil,
    3 :: 6 :: 9 :: 12 :: 15 :: 18 :: Nil,
    3 :: 6 :: 9 :: 12 :: 15 :: 18 :: 21 :: Nil
  )

  val boundsMap = Map(
      3 → bounds3,
      4 → bounds4,
      5 → bounds5,
      6 → bounds6,
      8 → bounds8,
      9 → bounds9,
     10 → bounds10,
     12 → bounds12,
     15 → bounds15,
    100 → bounds100
  )

  val blocksMap = Map(
      3 → blocks3,
      4 → blocks4,
      5 → blocks5,
     10 → blocks10,
    100 → blocks100
  )

  val stepCounts = List(
    "0  4  4  4  4  4",
    "0  4  5  6  8  8",
    "0  4  7  8 11 12",
    "0  4  7 11 15 16",
    "0  4  9 14 18 20"
  )

  override val spsMap = Map(
    1 → List(
      sp("ACAG", 0, 4),
      sp("TCAG", 1, 4),
      sp("CCGA", 2, 4),
      sp("AGTC", 3, 4)
    ),
    2 → List(
      sp("ACA", 6, 5),
      sp("TCA", 7, 5),
      sp("CCG", 4, 8),
      sp("AGT", 5, 8)
    ),
    3 → List(
      sp("AC",  5,  8),
      sp("TC",  6,  8),
      sp("CC", 10,  8),
      sp("AG", 11, 11)
    ),
    4 → List(
      sp("A",  8,  5),
      sp("T",  9, 16),
      sp("C", 10, 10),
      sp("A", 14,  7)
    ),
    5 → List(
      sp("",  5),
      sp("", 19),
      sp("", 12),
      sp("",  8)
    )
  )

  override val nspsMap = Map(
    1 → List(
      nsp("ACAG", 0, 0, 4),
      nsp("TCAG", 0, 1, 4),
      nsp("CCGA", 0, 2, 4),
      nsp("AGTC", 0, 3, 4)
    ),
    2 → List(
      nsp("ACA", 4, 6, 5),
      nsp("TCA", 4, 7, 5),
      nsp("CCG", 4, 4, 8),
      nsp("AGT", 4, 5, 8)
    ),
    3 → List(
      nsp("AC", 5,  5,  8),
      nsp("TC", 5,  6,  8),
      nsp("CC", 8, 10,  8),
      nsp("AG", 8, 11, 11)
    ),
    4 → List(
      nsp("A",  8,  8,  5),
      nsp("T",  8,  9, 16),
      nsp("C",  8, 10, 10),
      nsp("A", 11, 14,  7)
    ),
    5 → List(
      nsp("",  5,  5),
      nsp("", 16, 19),
      nsp("", 10, 12),
      nsp("",  7,  8)
    )
  )

  // Tests
  testFn(100)
  testFn(10)
  testFn(5)
  testFn(5, 2)
  testFn(5, 3)
  testFn(4)
  testFn(4, 2)
  testFn(4, 3)
  testFn(3)
  testFn(3, 2)
  testFn(3, 3)

}

/*

Scratch Work

String: ACAG$TCAG$CCGA$ACTC$

""

$  A  C  G  T  N
0  0  0  0  0  0

 0  0 ACAG $       * 0        0   0   4
 1  0 TCAG $       * 1        0   1   4
 2  0 CCGA $       * 2        0   2   4
 3  0 AGTC $       * 3        0   3   4

$  A  C  G  T  N
0  4  4  4  4  4

 0  0  ACA G $     * 0        4   6   5
 1  0  TCA G $     * 1        4   7   5
 2  0  CCG A $     * 2        4   4   8
 3  0  AGT C $     * 3        4   5   8

2G 1A 1C
GGAC

$  A  C  G  T  N
0  4  5  6  8  8

 0  0  ACA G $          - 0
 1  1  TCA G $          - 1
 2  2  CCG A $          - 2
 3  3  AGT C $          - 3
 4      CC G A$    * 2        8  10   8
 5      AG T C$    * 3        8  11  11
 6      AC A G$    * 0        5   5   8
 7      TC A G$    * 1        5   6   8

2G 1A 1C 1G 1T 2A
GGACGTAA

$  A  C  G  T  N
0  4  7  8 11 12

 0  0  ACA G $
 1  1  TCA G $
 2  2  CCG A $
 3  3  AGT C $
 4  4   CC G A$         - 2
 5       A C AG$   * 0        8   8   5
 6       T C AG$   * 1        8   9  16
 7  5   AG T C$         - 3
 8  6   AC A G$         - 0
 9  7   TC A G$         - 1
10       C C GA$   * 2        8  10  10
11       A G TC$   * 3       11  14   7

2G 1A 1C 1G 2C 1T 2A 1C 1G
GGACGCCTAACG

$  A  C  G  T  N
0  4  7 11 15 16

 0  0  ACA G $
 1  1  TCA G $
 2  2  CCG A $
 3  3  AGT C $
 4  4   CC G A$
 5  5    A C AG$        - 0
 6  6    T C AG$        - 1
 7  7   AG T C$
 8       $ A CAG$  * 0        5   5
 9       $ T CAG$  * 1       16  19
10       $ C CGA$  * 2       10  12
11  8   AC A G$
12  9   TC A G$
13 10    C C GA$        - 2
14       $ A GTC$  * 3        7   8
15 11    A G TC$        - 3

2G 1A 1C 1G 2C 1T 1A 1T 1C 2A 1C 1A 1G
GGACGCCTATCAACAG

$  A  C  G  T  N
0  4  9 14 18 20

 0  0  ACA G $
 1  1  TCA G $
 2  2  CCG A $
 3  3  AGT C $
 4  4   CC G A$
 5         $ ACAG$ * 0
 6  5    C C AG$
 7  6    A C AG$
 8         $ AGTC$ * 3
 9  7   AG T C$
10  8    $ A CAG$      - 0
11  9    $ T CAG$      - 1
12         $ CCGA$ * 2
13 10    $ C CGA$      - 2   C
14 11   AC A G$              A
15 12   TC A G$              A
16 13    C C GA$             C
17 14    $ A GTC$      - 3   A
18 15    A G TC$             G
19         $ TCAG$ * 1       $

2G 1A 1C 1G 1$ 2C 1$ 1T 1A 1T 1$ 1C 2A 1C 1A 1G 1$
GGACG$CC$TAT$CAACAG$

 */
