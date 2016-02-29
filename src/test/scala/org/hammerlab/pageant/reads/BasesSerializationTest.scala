package org.hammerlab.pageant.reads

import org.hammerlab.pageant.serialization.{DirectFileRDDTest, Utils => TestUtils}
import org.hammerlab.pageant.utils.{KryoSuite, KryoSerdePageantRegistrar, KryoSerdePageantRegistrarNoReferences}

import scala.util.Random

class BasesSerializationTest(withClasses: Boolean = false)
  extends DirectFileRDDTest(withClasses)
    with TestUtils {

  val bases = Bases.cToI.map(_.swap)

  Random.setSeed(1234L)

  def randomBases(n: Int) = {
    (0 until n).map(_ => Random.nextInt(4)).map(bases.apply).mkString("")
  }

  def makeBases(n: Int, k: Int) = {
    (0 until n).map(_ => Bases(randomBases(k)))
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

class BasesSerdeTest extends BasesSerializationTest with KryoSuite {
  testBases(1, 8, 6)
  testBases(10, 10, 70)
  testBases(10, 16, 80)
}

class BasesSerdeWithRegistrarAndClassesTest extends BasesSerializationTest(true) with KryoSerdePageantRegistrar {
  testBases(1, 8, 5)
  testBases(10, 10, 60)
  testBases(10, 16, 70)
}

class BasesSerdeWithRegistrarTest extends BasesSerializationTest with KryoSerdePageantRegistrar  {
  testBases(1, 8, 4)
  testBases(10, 10, 50)
  testBases(10, 16, 60)
}

class BasesSerdeWithRegistrarNoReferencesTest extends BasesSerializationTest with KryoSerdePageantRegistrarNoReferences {
  testBases(1, 8, 3)
  testBases(10, 10, 40)
  testBases(10, 16, 50)
}
