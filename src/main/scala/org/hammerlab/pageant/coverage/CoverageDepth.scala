package org.hammerlab.pageant.coverage

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.hammerlab.commands.{ Args, SparkCommand }
import org.hammerlab.genomics.loci.args.LociArgs
import org.hammerlab.pageant.histogram.JointHistogram
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4JOption }

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
    name = "--persist-joint-histogram",
    aliases = Array("-jh"),
    usage = "When set, save the computed joint-histogram; if one already exists, skip reading it, recompute it, and overwrite it"
  )
  var writeJointHistogram: Boolean = false

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
    name = "--persist-distributions",
    aliases = Array("-v"),
    usage = "When set, persist full PDF and CDF of coverage-depth histogram"
  )
  var writeFullDistributions: Boolean = false
}

object CoverageDepth extends SparkCommand[Arguments] {

  override def defaultRegistrar: String = "org.hammerlab.pageant.kryo.Registrar"

  override def name: String = "coverage-depth"
  override def description: String = "Given one or two sets of reads, and an optional set of intervals, compute a joint histogram over the reads' coverage of the genome, on and off the provided intervals."

  override def run(args: Arguments, sc: SparkContext): Unit = {
    val outPath = args.outPath

    val force = args.force
    val forceStr = if (force) " (forcing)" else ""

    val intervalPathOpt = args.lociFileOpt

    val intervalPathStr =
      intervalPathOpt
        .map(intervalPath => s"against $intervalPath ")
        .getOrElse("")

    val jointHistogramPath = getJointHistogramPath(args.outPath)

    val jointHistogramPathExists =
      jointHistogramPath
        .getFileSystem(sc.hadoopConfiguration)
        .exists(jointHistogramPath)

    val writeJointHistogram = args.writeJointHistogram

    val jh =
      if (!writeJointHistogram && jointHistogramPathExists) {
        println(s"Loading JointHistogram: $jointHistogramPath")
        JointHistogram.load(sc, jointHistogramPath)
      } else {
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
        one.Result(jh).save(
          outPath,
          force = force,
          writeFullDistributions = args.writeFullDistributions,
          writeJointHistogram = writeJointHistogram
        )
      case Array(reads1Path, reads2Path) ⇒
        two.Result(jh).save(
          outPath,
          force = force,
          writeFullDistributions = args.writeFullDistributions,
          writeJointHistogram = writeJointHistogram
        )
    }
  }

  def getJointHistogramPath(dir: String): Path = new Path(dir, "jh")
}
