name := "TriestSimpleV2"

scalaVersion := "2.11.11"

val log4jVersion = "11.0"

val sparkVersion = "2.4.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.logging.log4j" %% "log4j-api-scala" % log4jVersion,
  //"org.apache.spark" %% "spark-hive" % sparkVersion
)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.22"

// Ammonite
//libraryDependencies += {
//  val version = scalaBinaryVersion.value match {
//    case "2.10" => "1.0.3"
//    case _ ⇒ "1.6.0"
//  }
//  "com.lihaoyi" % "ammonite" % version % "test" cross CrossVersion.full
//}

//sourceGenerators in Test += Def.task {
//  val file = (sourceManaged in Test).value / "amm.scala"
//  IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
//  Seq(file)
//}.taskValue

// Optional, required for the `source` command to work
//(fullClasspath in Test) ++= {
//  (updateClassifiers in Test).value
//    .configurations
//    .find(_.configuration == Test.name)
//    .get
//    .modules
//    .flatMap(_.artifacts)
//    .collect{case (a, f) if a.classifier == Some("sources") => f}
//}