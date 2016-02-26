package org.hammerlab.pageant.fm.blocks

import org.scalatest.{FunSuite, Matchers}
import org.hammerlab.pageant.fm.utils.Utils.{AT, T, VT, N, toI}

class MergeInsertsIteratorTest extends FunSuite with Matchers {
  val emptyCounts = Array.fill(N)(0L)

  def testFn(name: String, blockRunStrs: String*)(insertsStr: String, expected: String): Unit = {
    test(name) {
      val blockRuns = blockRunStrs.map(Utils.runs)
      val blocks = blockRuns.foldLeft((100L, Array(1L, 2L, 3L, 4L, 5L, 6L), Array[RunLengthBWTBlock]()))((t, runs) => {
        var (idx, counts, blocks) = t
        val nextIdx = idx
        val nextCounts = counts.clone()

        runs.foreach(r ⇒ {
          counts(r.t) += r.n
          idx += r.n
        })

        val nextBlock = RunLengthBWTBlock(nextIdx, nextCounts, runs)

        (
          idx,
          counts,
          blocks :+ nextBlock
        )
      })._3

      val inserts =
        if (insertsStr.isEmpty)
          Array[(Long, T)]()
        else
          insertsStr.split(" ").map(s => {
            val t = toI(s.last)
            val p = s.dropRight(1).toLong
            (p, t)
          })

      val it = new MergeInsertsIterator(blocks.toIterator, inserts.toIterator)

      it.toList should be (Utils.runs(expected))
    }
  }

  testFn(
    "no inserts",
    "10A"
  )(
    "",
    "10A"
  )

  testFn(
    "prepend",
    "10A"
  )(
    "100C",
    "1C 10A"
  )

  testFn(
    "two prepends",
    "10A"
  )(
    "100C 100G",
    "1C 1G 10A"
  )

  testFn(
    "one second-insert",
    "10A"
  )(
    "101C",
    "1A 1C 9A"
  )

  testFn(
    "two second-inserts",
    "10A"
  )(
    "101C 101G",
    "1A 1C 1G 9A"
  )

  testFn(
    "one same prepend",
    "10A"
  )(
    "100A",
    "11A"
  )

  testFn(
    "two same prepends",
    "10A"
  )(
    "100A 100A",
    "12A"
  )

  testFn(
    "three same prepends",
    "10A"
  )(
    "100A 100A 100A",
    "13A"
  )

  testFn(
    "two same prepends, one same second-insert",
    "10A"
  )(
    "100A 100A 101A",
    "13A"
  )

  testFn(
    "mixed prepends 1",
    "10A"
  )(
    "100A 100A 100C",
    "2A 1C 10A"
  )

  testFn(
    "mixed prepends 2",
    "10A"
  )(
    "100A 100C 100A",
    "1A 1C 11A"
  )

  testFn(
    "mixed prepends 3",
    "10A"
  )(
    "100C 100A 100A",
    "1C 12A"
  )

  testFn(
    "prepends and inserts 1",
    "10A"
  )(
    "100A 100C 100A 101A 105A",
    "1A 1C 13A"
  )

  testFn(
    "prepends and inserts 2",
    "10A"
  )(
    "100A 100C 100A 101A 101C 105A",
    "1A 1C 3A 1C 10A"
  )

  testFn(
    "prepends and inserts 3",
    "10A"
  )(
    "100A 100C 100A 101A 105A 105C",
    "1A 1C 8A 1C 5A"
  )

  testFn(
    "prepends and inserts 4",
    "10A"
  )(
    "100A 100C 100A 100G 105A 105C",
    "1A 1C 1A 1G 6A 1C 5A"
  )

  testFn(
    "append same",
    "10A"
  )(
    "110A",
    "11A"
  )

  testFn(
    "append different",
    "10A"
  )(
    "110C",
    "10A 1C"
  )

  testFn(
    "prepend and append same",
    "10A"
  )(
    "100A 110A",
    "12A"
  )

  testFn(
    "multi-run no inserts",
    "10A 5C"
  )(
    "",
    "10A 5C"
  )

  testFn(
    "multi-run same insert between 1",
    "10A 5C"
  )(
    "110A",
    "11A 5C"
  )

  testFn(
    "multi-run same insert between 2",
    "10A 5C"
  )(
    "110C",
    "10A 6C"
  )

  testFn(
    "multi-run same inserts between 1",
    "10A 5C"
  )(
    "110A 110C",
    "11A 6C"
  )

  testFn(
    "multi-run same inserts between 2",
    "10A 5C"
  )(
    "110A 110A 110G 110C 110C",
    "12A 1G 7C"
  )

  testFn(
    "multi-run same reversed inserts between",
    "10A 5C"
  )(
    "110C 110A",
    "10A 1C 1A 5C"
  )

  testFn(
    "multi-run same mixed inserts between",
    "10A 5C"
  )(
    "110C 110A 110C 110G",
    "10A 1C 1A 1C 1G 5C"
  )


  testFn(
    "multi-run appends and prepends 1",
    "10A 5C"
  )(
    "100A 110A 110C 115C",
    "12A 7C"
  )

  testFn(
    "multi-run appends and prepends 2",
    "10A 5C"
  )(
    "100A 110C 115C",
    "11A 7C"
  )

  testFn(
    "multi-run appends and prepends 3",
    "10A 5C"
  )(
    "100A 110A 115C",
    "12A 6C"
  )

  testFn(
    "multi-block 1",
    "10A 5C",
    "5C 10G"
  )(
    "",
    "10A 10C 10G"
  )

  testFn(
    "multi-block 2",
    "10A 5C",
    "5C 10G"
  )(
    "100A 110A 110C 115C 120C 125G 130G",
    "12A 13C 12G"
  )

}

/*

Level 1:

C:  32  0  0  0  0

 I  1    B    O

 0  $    A     0  0  0  0  0
 1  $    A     0  1  0  0  0
 2  $    A     0  2  0  0  0
 3  $    A     0  3  0  0  0
 4  $    A     0  4  0  0  0
 5  $    A     0  5  0  0  0
 6  $    A     0  6  0  0  0
 7  $    A     0  7  0  0  0
 8  $    C     0  8  0  0  0
 9  $    C     0  8  1  0  0
10  $    C     0  8  2  0  0
11  $    C     0  8  3  0  0
12  $    C     0  8  4  0  0
13  $    C     0  8  5  0  0
14  $    C     0  8  6  0  0
15  $    C     0  8  7  0  0
16  $    G     0  8  8  0  0
17  $    G     0  8  8  1  0
18  $    G     0  8  8  2  0
19  $    G     0  8  8  3  0
20  $    G     0  8  8  4  0
21  $    G     0  8  8  5  0
22  $    G     0  8  8  6  0
23  $    G     0  8  8  7  0
24  $    T     0  8  8  8  0
25  $    T     0  8  8  8  1
26  $    T     0  8  8  8  2
27  $    T     0  8  8  8  3
28  $    T     0  8  8  8  4
29  $    T     0  8  8  8  5
30  $    T     0  8  8  8  6
31  $    T     0  8  8  8  7
32             0  8  8  8  8


Level 2:

C:  32  8  8  8  8

 I  1    B    O
 0  $    A     0  0  0  0  0
 1  $    A     0  1  0  0  0
 2  $    A     0  2  0  0  0
 3  $    A     0  3  0  0  0
 4  $    A     0  4  0  0  0
 5  $    A     0  5  0  0  0
 6  $    A     0  6  0  0  0
 7  $    A     0  7  0  0  0
 8  $    C     0  8  0  0  0
 9  $    C     0  8  1  0  0
10  $    C     0  8  2  0  0
11  $    C     0  8  3  0  0
12  $    C     0  8  4  0  0
13  $    C     0  8  5  0  0
14  $    C     0  8  6  0  0
15  $    C     0  8  7  0  0
16  $    G     0  8  8  0  0
17  $    G     0  8  8  1  0
18  $    G     0  8  8  2  0
19  $    G     0  8  8  3  0
20  $    G     0  8  8  4  0
21  $    G     0  8  8  5  0
22  $    G     0  8  8  6  0
23  $    G     0  8  8  7  0
24  $    T     0  8  8  8  0
25  $    T     0  8  8  8  1
26  $    T     0  8  8  8  2
27  $    T     0  8  8  8  3
28  $    T     0  8  8  8  4
29  $    T     0  8  8  8  5
30  $    T     0  8  8  8  6
31  $    T     0  8  8  8  7
32  A$   AA    0  8  8  8  8
33  A$   AC    0  9  8  8  8
34  A$   CG    0 10  8  8  8
35  A$   CT    0 10  9  8  8
36  A$   GA    0 10 10  8  8
37  A$   GC    0 10 10  9  8
38  A$   TG    0 10 10 10  8
39  A$   TT    0 10 10 10  9
40  C$   AT    0 10 10 10 10
41  C$   AG    0 11 10 10 10
42  C$   CC    0 12 10 10 10
43  C$   CA    0 12 11 10 10
44  C$   GT    0 12 12 10 10
45  C$   GG    0 12 12 11 10
46  C$   TC    0 12 12 12 10
47  C$   TA    0 12 12 12 11
48  G$   AA    0 12 12 12 12
49  G$   AC    0 13 12 12 12
50  G$   CG    0 14 12 12 12
51  G$   CT    0 14 13 12 12
52  G$   GA    0 14 14 12 12
53  G$   GC    0 14 14 13 12
54  G$   TG    0 14 14 14 12
55  G$   TT    0 14 14 14 13
56  T$   AT    0 14 14 14 14
57  T$   AG    0 15 14 14 14
58  T$   CC    0 16 14 14 14
59  T$   CA    0 16 15 14 14
60  T$   GT    0 16 16 14 14
61  T$   GG    0 16 16 15 14
62  T$   TC    0 16 16 16 14
63  T$   TA    0 16 16 16 15
64             0 16 16 16 16


Level 3:

C:  32 16 16 16 16



 */
