name := "ksql-jdbc-driver"

version := "0.2-SNAPSHOT"

initialize := {
  assert(Integer.parseInt(sys.props("java.specification.version").split("\\.")(1)) >= 7, "Java 7 or above required")
}

scalaVersion := "2.11.11"

resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
resolvers += "Confluent Snapshots Maven Repo" at "https://s3-us-west-2.amazonaws.com/confluent-snapshots/"
resolvers += Resolver.mavenLocal

libraryDependencies += "io.confluent.ksql" % "ksql-rest-app" % "4.1.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test"

