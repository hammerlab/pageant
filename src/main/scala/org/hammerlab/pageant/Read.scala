package org.hammerlab.pageant

import org.bdgenomics.formats.avro.AlignmentRecord

case class Read(name: String, first: Boolean, seq: Bases) {
  def rc: Read = this.copy(seq = seq.rc)
}

object Read {
  def apply(a: AlignmentRecord): Read = new Read(a.getReadName, a.getFirstOfPair, Bases(a.getSequence))
}

case class Pair(name: String, b1O: Option[Bases], b2O: Option[Bases])
case class ProperPair(name: String, b1: Bases, b2: Bases)

case class Pairs(name: String, p1: Pair, p2: Pair) {

}

//object Pairs {
  //def apply(p: Pair): Pairs = Pairs(p.complement(false), p.complement(true))
//  def apply(r1O: Option[Read], r2O: Option[Read]): Pairs = {
//
//  }
//}
