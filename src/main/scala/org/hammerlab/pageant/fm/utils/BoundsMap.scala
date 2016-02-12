package org.hammerlab.pageant.fm.utils

import org.hammerlab.pageant.fm.utils.Utils.TPos

case class BoundsMap(m: Map[TPos, Map[TPos, Bounds]]) {
  def filter(l: TPos, r: TPos): BoundsMap = {
    BoundsMap(
      for {
        (s, em) <- m
        if s <= l
      } yield {
        s -> (for {(e, b) <- em if e >= r} yield e -> b)
      }
    )
  }
}

