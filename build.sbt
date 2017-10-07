name := "ksql-jdbc-driver"

version := "0.1-SNAPSHOT"

initialize := {
  assert(Integer.parseInt(sys.props("java.specification.version").split("\\.")(1)) >= 7, "Java 7 or above required")
}

scalaVersion := "2.11.11"

resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
resolvers += Resolver.mavenLocal

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "io.confluent.ksql" % "ksql-cli" % "0.1-SNAPSHOT"
