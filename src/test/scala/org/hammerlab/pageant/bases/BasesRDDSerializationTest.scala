package org.hammerlab.pageant.bases

import org.hammerlab.pageant.utils.SequenceFileRDDTest

import scala.util.Random

class BasesRDDSerializationTest
  extends SequenceFileRDDTest {

  val bases = Bases.cToI.map(_.swap)

  Random.setSeed(1234L)

  def randomBases(n: Int) =
    (0 until n).map(_ => Random.nextInt(bases.size).toByte).map(bases.apply).mkString("")

  def makeBases(n: Int, k: Int) =
    (0 until n).map(_ => Bases5(randomBases(k)))

  test("4 partitions of 10") {
    verifyRDDSerde(
      makeBases(40, 4)
    )
  }
}
