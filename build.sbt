name := "pageant"
version := "1.0.0-SNAPSHOT"

sparkVersion := "2.0.2"

addSparkDeps

libraryDependencies ++= Seq(
  libraries.value('args4j),
  libraries.value('args4s),
  libraries.value('bdg_utils_cli),
  libraries.value('bdg_formats),
  libraries.value('spire),
  libraries.value('spark_commands),
  libraries.value('string_utils),
  libraries.value('adam_core),
  libraries.value('htsjdk),
  libraries.value('magic_rdds),
  libraries.value('loci)
)

assemblyMergeStrategy in assembly := {
  // Two org.bdgenomics deps include the same log4j.properties.
  case PathList("log4j.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

excludeFilter in Test := NothingFilter
