name := "pageant"
version := "1.0.0-SNAPSHOT"

sparkVersion := "2.1.0"

scala211Only
addSparkDeps

deps ++= Seq(
  libs.value('adam_core),
  libs.value('args4j),
  libs.value('args4s),
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  libs.value('htsjdk),
  libs.value('loci),
  libs.value('magic_rdds),
  libs.value('readsets),
  libs.value('spark_commands),
  libs.value('spire),
  libs.value('string_utils)
)

compileAndTestDeps += libs.value('reference)

takeFirstLog4JProperties

excludeFilter in Test := NothingFilter
