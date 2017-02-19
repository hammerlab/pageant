package org.hammerlab.pageant.coverage

import org.bdgenomics.utils.cli.Args4j
import org.hammerlab.pageant.Suite
import org.hammerlab.test.matchers.files.DirMatcher.dirMatch
import org.hammerlab.test.resources.File

class CoverageDepthTest extends Suite {

  test("one sample with intervals") {
    check(
      "coverage.intervals.golden",
      "--intervals-file", File("intervals.bed"),
      File("r1.sam"))
  }

  test("one sample without intervals") {
    check(
      "coverage.golden",
      File("r1.sam")
    )
  }

  test("two samples with intervals") {
    check(
      "coverage.intervals.golden2",
      "--intervals-file", File("intervals.bed"),
      File("r1.sam"),
      File("r2.sam")
    )
  }

  test("two samples without intervals") {
    check(
      "coverage.golden2",
      File("r1.sam"),
      File("r2.sam")
    )
  }

  def check(expectedDir: String, extraArgs: String*): Unit = {
    val outDir = tmpDir()
    val args =
      Args4j[Arguments](
        Array(
          "-v",
          "--out", outDir
        ) ++
          extraArgs
      )

    CoverageDepth.run(args, sc)

    outDir should dirMatch(expectedDir)
  }
}
