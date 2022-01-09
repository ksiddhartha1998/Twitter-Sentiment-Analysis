name := "twitter-sentiment_soc-networks"

version := "0.1"

scalaVersion := "2.11.12"


val sparkVersion = "2.4.0"


lazy val root = (project in file(".")).
  settings(
    name := "kafka",
    version := "1.0",
    scalaVersion := "2.12.8",
    mainClass in Compile := Some("kafka")
  )

resolvers += Resolver.url("Typesafe Ivy releases", url("https://repo.typesafe.com/typesafe/ivy-releases"))(Resolver.ivyStylePatterns)
resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.11.12" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "com.typesafe" % "config" % "1.3.4",
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" % "spark-graphx_2.11" % sparkVersion % "provided",
  "graphframes" % "graphframes" % "0.8.0-spark3.0-s_2.12",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
)

libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}