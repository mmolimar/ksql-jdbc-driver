name := "ksql-jdbc-driver"

version := "1.2-SNAPSHOT"

initialize := {
  assert(Integer.parseInt(sys.props("java.specification.version").split("\\.")(1)) >= 8, "Java 8 or above required")
}

scalaVersion := "2.11.11"

resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
resolvers += "Confluent Snapshots Maven Repo" at "https://s3-us-west-2.amazonaws.com/confluent-snapshots/"
resolvers += Resolver.mavenLocal

val ksqlVersion = "5.4.0-SNAPSHOT"
val kafkaVersion = "2.3.0"
val scalaTestVersion = "3.0.8"
val scalaMockVersion = "3.6.0"
val wsApiVersion = "2.1.1"

libraryDependencies += "io.confluent.ksql" % "ksql-rest-app" % ksqlVersion
libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % scalaMockVersion % "test"
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % wsApiVersion artifacts Artifact("javax.ws.rs-api", "jar", "jar")

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", _*) => MergeStrategy.first
  case "module-info.class" => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
