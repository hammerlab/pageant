package org.hammerlab.pageant

import org.bdgenomics.utils.misc.SparkFunSuite
import org.hammerlab.pageant.serialization.{Utils => TestUtils, KryoSerializerTest, DirectFileRDDTest}

import scala.util.Random

trait KryoBasesRegistrarTest {
  self: SparkFunSuite =>
  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.hammerlab.pageant.PageantKryoRegistrar"
  )
}

trait KryoBasesRegistrarNoReferencesTest {
  self: SparkFunSuite =>
  override val properties = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.hammerlab.pageant.PageantKryoRegistrar",
    "spark.kryo.referenceTracking" -> "false"
  )
}

class BasesSerializationTest(withClasses: Boolean = false)
  extends DirectFileRDDTest(withClasses)
    with TestUtils{

  val bases = "ACGT".zipWithIndex.map(p => (p._2, p._1)).toMap

  Random.setSeed(1234L)

  def randomBases(n: Int) = {
    (0 until n).map(_ => Random.nextInt(4)).map(bases.apply).mkString("")
  }

  def makeBases(n: Int, k: Int) = {
    (0 until n).map(_ => Bases(randomBases(k)))
  }

  def testBases(n: Int, k: Int, size: Int): Unit = {
    sparkTest(s"bases ${n}x${k}") {
      verifyFileSizesAndSerde(
        s"bases-${n}x${k}",
        makeBases(4*n, k),
        size
      )
    }
  }

}

class BasesKryoTest extends BasesSerializationTest with KryoSerializerTest  {
  testBases(1, 8, 6)
  testBases(10, 10, 70)
  testBases(10, 16, 80)
}

class BasesWithRegistrarAndClassesTest extends BasesSerializationTest(true) with KryoBasesRegistrarTest {
  testBases(1, 8, 5)
  testBases(10, 10, 60)
  testBases(10, 16, 70)
}

class BasesWithRegistrarTest extends BasesSerializationTest with KryoBasesRegistrarTest  {
  testBases(1, 8, 4)
  testBases(10, 10, 50)
  testBases(10, 16, 60)
}

class BasesWithRegistrarNoReferencesTest extends BasesSerializationTest with KryoBasesRegistrarNoReferencesTest {
  testBases(1, 8, 3)
  testBases(10, 10, 40)
  testBases(10, 16, 50)
}
