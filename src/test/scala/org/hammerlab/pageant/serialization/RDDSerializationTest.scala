package org.hammerlab.pageant.serialization

import org.hammerlab.pageant.utils.{KryoSuite, JavaSerializerSuite}

trait SerdeRDDTest extends Utils {

  def testSmallInts(ps: Int*): Unit = {
    test("rdd small ints") {
      verifyFileSizeListAndSerde(
        "small-ints",
        1 to 400,
        ps
      )
    }
  }

  def testMediumInts(ps: Int*): Unit = {
    test("rdd medium ints") {
      verifyFileSizeListAndSerde(
        "medium-ints",
        (1 to 400).map(_ + 500),
        ps
      )
    }
  }

  def testLongs(ps: Int*): Unit = {
    test("rdd longs") {
      verifyFileSizeListAndSerde(
        "longs",
        (1 to 400).map(_ + 12345678L),
        ps
      )
    }
  }

  def testSomeFoos(n: Int, ps: Int*): Unit = {
    test(s"some foos $n") {
      verifyFileSizeListAndSerde(
        "foos",
        Foos((if (ps.size == 1) 4 else ps.size) * n, 20),
        ps
      )
    }
  }
}

class JavaSequenceFileRDDTest extends SequenceFileRDDTest with SerdeRDDTest with JavaSerializerSuite {
  testSmallInts(9475)
  testMediumInts(9475)
  testLongs(9575)

  testSomeFoos(1, 223)
  testSomeFoos(10, 1375)
  testSomeFoos(100, 13015)
  testSomeFoos(1000, 129335)
}

class JavaBZippedSequenceFileRDDTest extends BZippedSequenceFileRDDTest with SerdeRDDTest with JavaSerializerSuite {
  testSmallInts(648, 655, 666, 651)
  testMediumInts(652, 655, 668, 646)
  testLongs(662, 676, 664, 675)

  testSomeFoos(1, 399, 397, 394, 394)
  testSomeFoos(10, 528, 525, 531, 526)
  testSomeFoos(100, 863, 853, 881, 873)
  testSomeFoos(1000, 2128, 2163, 2133, 2158)
}

class KryoSequenceFileRDDTest extends SequenceFileRDDTest with SerdeRDDTest with KryoSuite {
  testSmallInts(1532, 1595, 1595, 1595)
  testMediumInts(1595)
  testLongs(1795)

  testSomeFoos(1, 171)
  testSomeFoos(10, 855)
  testSomeFoos(100, 7792, 7855)
  testSomeFoos(1000, 77792, 77855)
}

class KryoSequenceFileFooRDDTest extends SequenceFileRDDTest with SerdeRDDTest with FooRegistrarTest {
  testSmallInts(1532, 1595)
  testMediumInts(1595)
  testLongs(1795)

  testSomeFoos(1, 131)
  testSomeFoos(10, 455)
  testSomeFoos(100, 3752, 3815)
  testSomeFoos(1000, 37392, 37455)
}

class KryoBzippedSequenceFileFooRDDTest extends BZippedSequenceFileRDDTest with SerdeRDDTest with FooRegistrarTest {
  testSmallInts(463, 456, 447, 445)
  testMediumInts(443, 446, 447, 444)
  testLongs(461, 462, 460, 461)

  testSomeFoos(1, 301)
  testSomeFoos(10, 353, 361, 360, 359)
  testSomeFoos(100, 716, 729, 706, 722)
  testSomeFoos(1000, 1985, 1714, 1674, 1665)
}

class JavaDirectFileRDDTest extends DirectFileRDDTest with SerdeRDDTest with JavaSerializerSuite {
  testSmallInts(1072)
  testMediumInts(1072)
  testLongs(1469)

  testSomeFoos(1, 116)
  testSomeFoos(10, 413)
  testSomeFoos(100, 3384)
  testSomeFoos(1000, 33804)
}

class KryoDirectFileRDDTest extends DirectFileRDDTest with SerdeRDDTest with KryoSuite {
  testSmallInts(137, 200)
  testMediumInts(200)
  testLongs(400)

  testSomeFoos(1, 23)
  testSomeFoos(10, 230)
  testSomeFoos(100, 2337, 2400)
  testSomeFoos(1000, 23937, 24000)
}

class KryoGzippedDirectFileRDDTest extends GzippedDirectFileRDDTest with SerdeRDDTest with KryoSuite {
  testSmallInts(160, 175, 173, 179)
  testMediumInts(176, 174, 176, 176)
  testLongs(177, 183, 179, 181)

  testSomeFoos(1, 26)
  testSomeFoos(10, 76, 77, 80, 77)
  testSomeFoos(100, 417, 421, 416, 434)
  testSomeFoos(1000, 3134, 3109, 3094, 3100)
}

class KryoDirectFileWithClassesRDDTest extends DirectFileRDDTest(true) with SerdeRDDTest with KryoSuite {
  testSmallInts(237, 300, 300, 300)
  testMediumInts(300)
  testLongs(500)

  testSomeFoos(1, 64)
  testSomeFoos(10, 640)
  testSomeFoos(100, 6437, 6500)
  testSomeFoos(1000, 64937, 65000)
}

class KryoDirectFileWithClassesAndFooRDDTest extends DirectFileRDDTest(true) with SerdeRDDTest with FooRegistrarTest {
  testSmallInts(237, 300, 300, 300)
  testMediumInts(300)
  testLongs(500)

  testSomeFoos(1, 24)
  testSomeFoos(10, 240)
  testSomeFoos(100, 2437, 2500)
  testSomeFoos(1000, 24937, 25000)
}

class KryoGzippedDirectFileWithClassesAndFooRDDTest extends GzippedDirectFileRDDTest(true) with SerdeRDDTest with FooRegistrarTest {
  testSmallInts(197, 191, 209, 213)
  testMediumInts(214, 210, 211, 213)
  testLongs(195, 197, 193, 197)

  testSomeFoos(1, 27)
  testSomeFoos(10, 83, 83, 87, 84)
  testSomeFoos(100, 425, 435, 432, 444)
  testSomeFoos(1000, 3049, 3029, 3013, 3031)
}
