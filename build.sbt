name := "pageant"
version := "1.0.0-SNAPSHOT"

providedDeps ++= Seq(
  libraries.value('spark),
  libraries.value('mllib),
  libraries.value('hadoop)
)

libraryDependencies ++= Seq(
  "com.esotericsoftware.kryo" % "kryo" % "2.21",
  "args4j" % "args4j" % "2.33",
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.10.0",
  "org.bdgenomics.quinine" %% "quinine-core" % "0.0.2" exclude("org.bdgenomics.adam", "adam-core"),
  "org.spire-math" %% "spire" % "0.11.0",
  "org.scalanlp" %% "breeze" % "0.12",
  "com.github.samtools" % "htsjdk" % "2.6.1" exclude("org.xerial.snappy", "snappy-java"),
  libraries.value('hadoop_bam),
  "org.apache.commons" % "commons-math3" % "3.0",
  "org.hammerlab" %% "args4s" % "1.0.0",
  "org.hammerlab" %% "magic-rdds" % "1.3.1",
  "org.hammerlab" %% "spark-commands" % "1.0.0",
  "org.clapper" %% "grizzled-slf4j" % "1.0.3",
  "org.hammerlab" %% "spark-util" % "1.1.1",
  "org.hammerlab" %% "genomic-loci" % "1.4.2"
)

testDeps ++= Seq(
  libraries.value('spark_testing_base),
  "org.bdgenomics.utils" %% "utils-misc" % "0.2.10",
  "org.hammerlab" %% "spark-tests" % "1.1.3"
)
