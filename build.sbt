
// Shorthands for common configurations
val tt = "test->test"
val ct = "compile->compile;test->test"
val tctt = "test->compile;test->test"

// Project-declaration helpers
def proj(dir: String, deps: ClasspathDep[ProjectReference]*): Project =
  Project(dir, new File(dir)).dependsOn(deps: _*)

//def sparkProj(dir: String, deps: ClasspathDep[ProjectReference]*): Project =
//  proj(dir, (tests :: sparkTestsDep :: deps.toList): _*)

// Sub-projects
/*
lazy val testUtils = (project in file("test-utils"))
lazy val tests = testUtils % tt

lazy val sparkTests = proj("spark-tests", testUtils)

lazy val sparkTestsDep = sparkTests % tt

lazy val args4s = project

lazy val iterator = project.dependsOn(tests)

lazy val utils = sparkProj("utils")

lazy val reference = sparkProj("reference", iterator)
lazy val referenceCompileTest = reference % ct

lazy val magicRDDs = sparkProj("magic-rdds", iterator)

lazy val loci =
  sparkProj(
    "loci",
    args4s,
    iterator,
    referenceCompileTest
  )

lazy val adamCore =
  sparkProj(
    "adam-core",
    referenceCompileTest,
    loci
  )

lazy val reads =
  sparkProj(
    "reads",
    utils % ct,
    referenceCompileTest,
    adamCore
  )

lazy val readsets =
  sparkProj(
    "readsets",
    iterator,
    magicRDDs,
    utils % tctt,
    referenceCompileTest,
    loci,
    adamCore,
    reads % ct
  )

lazy val pageant =
  sparkProj(
    "pageant",
    args4s,
    magicRDDs,
    referenceCompileTest,
    loci,
    adamCore,
    readsets
  )
*/
