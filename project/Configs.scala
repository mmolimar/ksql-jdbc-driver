import sbt._

object Configs {
  val IntegrationTest = config("it") extend (Test)
  val all = Seq(IntegrationTest)
}
