package org.hammerlab.pageant.coverage

import org.apache.spark.SparkContext
import org.hammerlab.args4s.StringOptionHandler
import org.hammerlab.commands.{ Args, SparkCommand }
import org.hammerlab.genomics.loci.args.LociArgs
import org.hammerlab.pageant.histogram.JointHistogram
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Argument, Option => Args4JOption }

class Arguments
  extends Args
    with LociArgs {

  @Argument(
    index = 0,
    required = true,
    usage = "Path to write results to",
    metaVar = "OUT"
  )
  var outPath: String = _

  @Args4JOption(
    name = "--force",
    aliases = Array("-f"),
    usage = "Write results file even if it already exists"
  )
  var force: Boolean = false

  @Args4JOption(
    name = "--force",
    aliases = Array("-f"),
    usage = "Force recomputation of joint-histogram even if one already exists on disk",
    handler = classOf[StringOptionHandler]
  )
  var jointHistogramPathOpt: Option[String] = None

  @Args4JOption(
    name = "--reads",
    handler = classOf[StringArrayOptionHandler],
    usage = "Paths to BAM files"
  )
  var readsPaths: Array[String] = Array()

  @Args4JOption(
    name = "--interval-partition-bytes",
    aliases = Array("-b"),
    usage = "Number of bytes per chunk of input interval-file"
  )
  var intervalPartitionBytes: Int = 1 << 20

  @Args4JOption(
    name = "--verbose",
    aliases = Array("-v"),
    usage = "When set, output full PDF and CDF of coverage-depth histogram"
  )
  var verbose: Boolean = false
}

object CoverageDepth extends SparkCommand[Arguments] {

  override def name: String = "coverage-depth"
  override def description: String = ""

  override def run(args: Arguments, sc: SparkContext): Unit = {
    val outPath = args.outPath

    val force = args.force
    val forceStr = if (force) " (forcing)" else ""

    val intervalPathOpt = args.lociFileOpt

    val intervalPathStr =
      intervalPathOpt
      .map(intervalPath => s"against $intervalPath ")
      .getOrElse("")

    val jh =
      args.jointHistogramPathOpt match {
        case Some(jointHistogramPath) =>
          println(s"Loading JointHistogram: $jointHistogramPath")
          JointHistogram.load(sc, jointHistogramPath)
        case None =>
          println(
            s"Analyzing ${args.readsPaths.mkString("(", ", ", ")")} ${intervalPathStr}and writing to $outPath$forceStr"
          )
          JointHistogram.fromFiles(
            sc,
            args.readsPaths,
            intervalPathOpt.toList,
            bytesPerIntervalPartition = args.intervalPartitionBytes
          )
      }

    args.readsPaths match {
      case Array(readsPath) ⇒
        one.Result(jh, args.verbose).save(outPath, force)
      case Array(reads1Path, reads2Path) ⇒
        two.Result(jh, args.verbose).save(outPath, force)
    }
  }
}
