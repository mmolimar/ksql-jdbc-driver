name := "ksql-jdbc-driver"

version := "0.1-SNAPSHOT"

initialize := {
  assert(Integer.parseInt(sys.props("java.specification.version").split("\\.")(1)) >= 7, "Java 7 or above required")
}

scalaVersion := "2.11.11"

resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
resolvers += Resolver.mavenLocal

libraryDependencies += "io.confluent.ksql" % "ksql-rest-app" % "0.1-SNAPSHOT"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.11.0.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test"

