package org.hammerlab.pageant.suffixes.pdc3

import org.hammerlab.pageant.suffixes.pdc3.PDC3.{T, OT}

case class Joined(t0O: OT = None, t1O: OT = None, n0O: OT = None, n1O: OT = None) {
  override def toString: String = {
    val s =
      List(
        t0O.getOrElse(" "),
        t1O.getOrElse(" "),
        n0O.getOrElse(" "),
        n1O.getOrElse(" ")
      ).mkString(",")
    s"J($s)"
  }
}

object Joined {
  def merge(j1: Joined, j2: Joined): Joined = {
    def get(fn: Joined => OT): OT = {
      (fn(j1), fn(j2)) match {
        case (Some(f1), Some(f2)) => throw new Exception(s"Merge error: $j1 $j2")
        case (f1O, f2O) => f1O.orElse(f2O)
      }
    }
    Joined(
      get(_.t0O),
      get(_.t1O),
      get(_.n0O),
      get(_.n1O)
    )
  }
  def mergeT(t: (Joined, Joined)): Joined = merge(t._1, t._2)

}

