name := "pageant"
version := "1.0.0-SNAPSHOT"

sparkVersion := "2.0.2"

providedDeps ++= Seq(
  libraries.value('spark),
  libraries.value('hadoop)
)

libraryDependencies ++= Seq(
  libraries.value('args4j),
  libraries.value('args4s),
  libraries.value('bdg_utils_cli),
  libraries.value('bdg_formats),
  libraries.value('kryo),
  libraries.value('spire),
  libraries.value('spark_commands),
  libraries.value('string_utils),
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  "com.github.samtools" % "htsjdk" % "2.6.1" exclude("org.xerial.snappy", "snappy-java"),
  "org.hammerlab" %% "magic-rdds" % "1.3.1",
  "org.hammerlab" %% "genomic-loci" % "1.4.4"
)

testDeps ++= Seq(
  libraries.value('spark_tests),
  libraries.value('test_utils)
)

assemblyMergeStrategy in assembly := {
  // Two org.bdgenomics deps include the same log4j.properties.
  case PathList("log4j.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}
