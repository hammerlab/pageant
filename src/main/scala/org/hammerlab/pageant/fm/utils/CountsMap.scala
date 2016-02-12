package org.hammerlab.pageant.fm.utils

import org.hammerlab.pageant.fm.utils.Utils.TPos

case class CountsMap(m: Map[TPos, Map[TPos, Long]])

object CountsMap {
  def apply(bm: BoundsMap): CountsMap = CountsMap(
    for {
      (s, m) <- bm.m
    } yield {
      s -> ( for { (e, b) <- m } yield e -> b.count )
    }
  )
}

