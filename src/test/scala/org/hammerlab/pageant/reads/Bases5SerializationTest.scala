package org.hammerlab.pageant.reads

import org.hammerlab.pageant.serialization.{DirectFileRDDTest, Utils ⇒ TestUtils}
import org.hammerlab.pageant.utils.{KryoNoReferenceTracking, KryoSerdePageantRegistrar, KryoSerdePageantRegistrarNoReferences, KryoSuite, PageantRegistrar}

import scala.util.Random

class Bases5SerializationTest(withClasses: Boolean = false)
  extends DirectFileRDDTest(withClasses)
    with TestUtils {

  val bases = Bases5.cToI.map(_.swap)

  Random.setSeed(1234L)

  def randomBases(n: Int) = {
    (0 until n).map(_ => Random.nextInt(5)).map(bases.apply).mkString("")
  }

  def makeBases(n: Int, k: Int) = {
    (0 until n).map(_ => Bases5(randomBases(k)))
  }

  def testBases(n: Int, k: Int, size: Int): Unit = {
    test(s"bases ${n}x$k") {
      val c = sc.getConf
      verifyFileSizesAndSerde(
        s"bases-${n}x$k",
        makeBases(4*n, k),
        size
      )
    }
  }
}

class Bases5SerdeTest
  extends Bases5SerializationTest
    with KryoSuite {

  testBases(10,  0,  40)
  testBases(10,  1,  50)
  testBases(10,  2,  50)
  testBases(10,  3,  50)
  testBases(10,  4,  60)

  testBases( 1,  8,   7)
  testBases(10,  9,  70)
  testBases(10, 10,  80)
  testBases(10, 11,  80)
  testBases(10, 12,  80)
  testBases(10, 16, 100)
}

class Bases5SerdeWithRegistrarAndClassesTest
  extends Bases5SerializationTest(true)
    with KryoSuite
    with PageantRegistrar {

  testBases( 1,  8,   6)
  testBases(10,  9,  60)
  testBases(10, 10,  70)
  testBases(10, 11,  70)
  testBases(10, 12,  70)
  testBases(10, 16,  90)
}

class Bases5SerdeWithRegistrarTest
  extends Bases5SerializationTest
    with KryoSuite
    with PageantRegistrar  {

  testBases( 1,  8,   5)
  testBases(10,  9,  50)
  testBases(10, 10,  60)
  testBases(10, 11,  60)
  testBases(10, 12,  60)
  testBases(10, 16,  80)
}

class Bases5SerdeWithRegistrarNoReferencesTest
  extends Bases5SerializationTest
    with KryoSuite
    with PageantRegistrar
    with KryoNoReferenceTracking {

  testBases( 1,  8,   4)
  testBases(10,  9,  40)
  testBases(10, 10,  50)
  testBases(10, 11,  50)
  testBases(10, 12,  50)
  testBases(10, 16,  70)
}
