package org.hammerlab.pageant.bases

class Bases5RDDSerializationTest
  extends BasesRDDSerializationTest {

  override val bases = Bases5.cToT.map(_.swap)
}
