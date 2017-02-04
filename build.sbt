
//lazy val root = (project in file("."))

//lazy val parentBuild = (project in file("sbt-parent/project")).settings(scalaVersion := "2.10.6")

//lazy val parent = (project in file("sbt-parent")).settings(scalaVersion := "2.10.6")

lazy val tests = (project in file("test-utils"))

val tt = "test->test"
val ct = "compile->compile;test->test"

lazy val t = tests % tt

lazy val sparkTests = (project in file("spark-tests")).dependsOn(tests)

lazy val st = sparkTests % tt

lazy val args4s = project

lazy val iterator = project.dependsOn(t)

lazy val utils = (project in file("utils")).dependsOn(t, st)

lazy val reference = project.dependsOn(t, st, utils, iterator)

lazy val r = reference % ct

lazy val magicRDDs = (project in file("magic-rdds")).dependsOn(t, st, iterator)

lazy val loci = project.dependsOn(t, st, r, iterator, args4s)

lazy val adamCore = (project in file("adam-core")).dependsOn(t, st, loci, r)

lazy val reads = project.dependsOn(t, st, adamCore, r, utils % ct)

lazy val readsets = project.dependsOn(t, st, adamCore, r, reads % ct, iterator, loci, magicRDDs, utils % "test->compile;test->test")

lazy val pageant = project.dependsOn(t, st, adamCore, args4s, readsets, loci, magicRDDs, r)
