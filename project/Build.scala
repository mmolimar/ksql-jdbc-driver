import sbt._

object KsqlJdbcBuild extends Build {
  lazy val root = Project("root", file("."))
    .configs(Configs.all: _*)
    .settings(Testing.settings: _*)
}
