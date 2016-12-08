package org.hammerlab.pageant.coverage

import org.bdgenomics.utils.cli.Args4j
import org.hammerlab.pageant.utils.PageantSuite
import org.hammerlab.test.resources.File
import org.hammerlab.test.files.TmpFiles
import org.hammerlab.test.matchers.files.DirMatcher.dirMatch

class CoverageDepthTest extends PageantSuite with TmpFiles {
  test("one sample") {
    val outDir = tmpDir()
    val args =
      Args4j[Arguments](
        Array(
          "--reads", File("r1.sam").path,
          "--loci-file", File("intervals.bed").path,
          "-v",
          outDir
        )
      )

    CoverageDepth.run(args, sc)

    outDir should dirMatch("coverage.golden")
  }

  test("two samples") {
    val outDir = tmpDir()
    val args =
      Args4j[Arguments](
        Array(
          "--reads", File("r1.sam").path, File("r2.sam").path,
          "--loci-file", File("intervals.bed").path,
          "-v",
          outDir
        )
      )

    CoverageDepth.run(args, sc)

    outDir should dirMatch("coverage.golden2")
  }
}
