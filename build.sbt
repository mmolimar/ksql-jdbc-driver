val projectVersion = "1.2.0-SNAPSHOT"
val projectScalaVersion = "2.12.10"
val ksqlVersion = "5.4.0"
val kafkaVersion = "2.4.0"
val scalaTestVersion = "3.1.0"
val scalaMockVersion = "3.6.0"
val wsApiVersion = "2.1.1"

val repos = Seq(
  "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
  "Confluent Snapshots Maven Repo" at "https://s3-us-west-2.amazonaws.com/confluent-snapshots/",
  Resolver.mavenLocal
)

val dependencies = Seq(
  "io.confluent.ksql" % "ksql-rest-app" % ksqlVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion % "test",
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % scalaMockVersion % "test",
  "javax.ws.rs" % "javax.ws.rs-api" % wsApiVersion artifacts Artifact("javax.ws.rs-api", "jar", "jar")
)

val common = Seq(
  organization := "com.github.mmolimar",
  name := "ksql-jdbc-driver",
  version := projectVersion,
  scalaVersion := projectScalaVersion,
  crossScalaVersions := Seq("2.11.12", projectScalaVersion),
  resolvers ++= repos,
  libraryDependencies ++= dependencies
)

lazy val root = project.in(file("."))
  .configs(Configs.all: _*)
  .settings(
    common,
    Tests.settings
  )
  .enablePlugins(ScoverageSbtPlugin, CoverallsPlugin, AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", _*) => MergeStrategy.first
  case PathList("javax", "annotation", _*) => MergeStrategy.first
  case "module-info.class" => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
