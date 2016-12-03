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
  libraries.value('bdg_formats),
  libraries.value('kryo),
  libraries.value('spire),
  libraries.value('spark_commands),
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  "com.github.samtools" % "htsjdk" % "2.6.1" exclude("org.xerial.snappy", "snappy-java"),
  "org.hammerlab" %% "magic-rdds" % "1.3.1",
  "org.hammerlab" %% "genomic-loci" % "1.4.3"
)

testDeps ++= Seq(
  libraries.value('spark_tests)
)

assemblyMergeStrategy in assembly := {
  // Two org.bdgenomics deps include the same log4j.properties.
  case PathList("log4j.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}
