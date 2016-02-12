package org.hammerlab.pageant.fm.finder

import org.hammerlab.pageant.fm.utils.FMSuite

trait FMFinderTest {
  self: FMSuite =>
  var fmf: FMFinder[_] = _
  def initFinder(): FMFinder[_]
  fmInits.append((sc, fm) => {
    fmf = initFinder()
  })
}

trait BroadcastFinderTest extends FMFinderTest {
  self: FMSuite =>
  def initFinder(): FMFinder[_] = {
    BroadcastTFinder(fm)
  }
}

trait AllTFinderTest extends FMFinderTest {
  self: FMSuite =>
  def initFinder(): FMFinder[_] = {
    AllTFinder(fm)
  }
}
