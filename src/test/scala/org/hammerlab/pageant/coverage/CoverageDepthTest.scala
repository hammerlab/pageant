package org.hammerlab.pageant.coverage

import org.bdgenomics.utils.cli.Args4j
import org.hammerlab.pageant.utils.PageantSuite
import org.hammerlab.test.matchers.files.DirMatcher.dirMatch
import org.hammerlab.test.resources.File

class CoverageDepthTest extends PageantSuite {
  test("one sample with intervals") {
    val outDir = tmpDir()
    val args =
      Args4j[Arguments](
        Array(
          "--intervals-file", File("intervals.bed").path,
          "-v",
          "--out", outDir,
          File("r1.sam").path
        )
      )

    CoverageDepth.run(args, sc)

    outDir should dirMatch("coverage.intervals.golden")
  }

  test("one sample without intervals") {
    val outDir = tmpDir()
    val args =
      Args4j[Arguments](
        Array(
          "-v",
          "--out", outDir,
          File("r1.sam").path
        )
      )

    CoverageDepth.run(args, sc)

    outDir should dirMatch("coverage.golden")
  }

  test("two samples with intervals") {
    val outDir = tmpDir()
    val args =
      Args4j[Arguments](
        Array(
          "--intervals-file", File("intervals.bed").path,
          "-v",
          "--out", outDir,
          File("r1.sam").path, File("r2.sam").path
        )
      )

    CoverageDepth.run(args, sc)

    outDir should dirMatch("coverage.intervals.golden2")
  }
}
