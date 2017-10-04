name := "ksql-jdbc-driver"

version := "1.0"

scalaVersion := "2.11.11"

initialize := {
  assert(
    Integer.parseInt(sys.props("java.specification.version").split("\\.")(1))
      >= 7,
    "Java 7 or above required")
}