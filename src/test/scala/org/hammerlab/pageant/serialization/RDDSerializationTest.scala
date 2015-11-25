package org.hammerlab.pageant.serialization

trait SerdeRDDTest extends Utils {

  def testSmallInts(ps: Int*): Unit = {
    sparkTest("rdd small ints") {
      verifyFileSizeListAndSerde(
        "small-ints",
        1 to 400,
        ps
      )
    }
  }

  def testMediumInts(ps: Int*): Unit = {
    sparkTest("rdd medium ints") {
      verifyFileSizeListAndSerde(
        "medium-ints",
        (1 to 400).map(_ + 500),
        ps
      )
    }
  }

  def testLongs(ps: Int*): Unit = {
    sparkTest("rdd longs") {
      verifyFileSizeListAndSerde(
        "longs",
        (1 to 400).map(_ + 12345678L),
        ps
      )
    }
  }

  def testSomeFoos(n: Int, ps: Int*): Unit = {
    sparkTest(s"some foos $n") {
      verifyFileSizeListAndSerde(
        "foos",
        Foos(ps.size*n, 20),
        ps
      )
    }
  }
}

class JavaSequenceFileRDDTest extends SequenceFileRDDTest with SerdeRDDTest {
    testSmallInts(9475, 9475, 9475, 9475)
  testMediumInts(9475, 9475, 9475, 9475)
  testLongs(9575, 9575, 9575, 9575)

  testSomeFoos(1, 223, 223, 223, 223)
  testSomeFoos(10, 1375, 1375, 1375, 1375)
  testSomeFoos(100, 13015, 13015, 13015, 13015)
}

class KryoSequenceFileRDDTest extends SequenceFileRDDTest with SerdeRDDTest with KryoSerializerTest {
  testSmallInts(1532, 1595, 1595, 1595)
  testMediumInts(1595, 1595, 1595, 1595)
  testLongs(1795, 1795, 1795, 1795)

  testSomeFoos(1, 171, 171, 171, 171)
  testSomeFoos(10, 855, 855, 855, 855)
  testSomeFoos(100, 7792, 7855, 7855, 7855)
}

class KryoSequenceFileFooRDDTest extends SequenceFileRDDTest with SerdeRDDTest with KryoFooRegistrarTest {
  testSmallInts(1532, 1595, 1595, 1595)
  testMediumInts(1595, 1595, 1595, 1595)
  testLongs(1795, 1795, 1795, 1795)

  testSomeFoos(1, 131, 131, 131, 131)
  testSomeFoos(10, 455, 455, 455, 455)
  testSomeFoos(100, 3752, 3815, 3815, 3815)
}

class JavaDirectFileRDDTest extends DirectFileRDDTest with SerdeRDDTest {
  testSmallInts(1072, 1072, 1072, 1072)
  testMediumInts(1072, 1072, 1072, 1072)
  testLongs(1469, 1469, 1469, 1469)

  testSomeFoos(1, 116, 116, 116, 116)
  testSomeFoos(10, 413, 413, 413, 413)
  testSomeFoos(100, 3384, 3384, 3384, 3384)
}

class KryoDirectFileRDDTest extends DirectFileRDDTest with SerdeRDDTest with KryoSerializerTest {
  testSmallInts(137, 200, 200, 200)
  testMediumInts(200, 200, 200, 200)
  testLongs(400, 400, 400, 400)

  testSomeFoos(1, 23, 23, 23, 23)
  testSomeFoos(10, 230, 230, 230, 230)
  testSomeFoos(100, 2337, 2400, 2400, 2400)
}

class KryoDirectFileWithClassesRDDTest extends DirectFileRDDTest(true) with SerdeRDDTest with KryoSerializerTest {
  testSmallInts(237, 300, 300, 300)
  testMediumInts(300, 300, 300, 300)
  testLongs(500, 500, 500, 500)

  testSomeFoos(1, 64, 64, 64, 64)
  testSomeFoos(10, 640, 640, 640, 640)
  testSomeFoos(100, 6437, 6500, 6500, 6500)
}

class KryoDirectFileWithClassesAndFooRDDTest extends DirectFileRDDTest(true) with SerdeRDDTest with KryoFooRegistrarTest {
  testSmallInts(237, 300, 300, 300)
  testMediumInts(300, 300, 300, 300)
  testLongs(500, 500, 500, 500)

  testSomeFoos(1, 24, 24, 24, 24)
  testSomeFoos(10, 240, 240, 240, 240)
  testSomeFoos(100, 2437, 2500, 2500, 2500)
}
