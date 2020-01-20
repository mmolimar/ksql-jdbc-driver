import sbt.Keys._
import sbt.{Def, _}

object Tests {

  import Configs._

  private lazy val testSettings = Seq(
    fork in Test := false,
    parallelExecution in Test := false
  )
  private lazy val itSettings = inConfig(IntegrationTest)(Defaults.testSettings) ++ Seq(
    fork in IntegrationTest := false,
    parallelExecution in IntegrationTest := false,
    scalaSource in IntegrationTest := baseDirectory.value / "src/it/scala"
  )
  private lazy val testAll = TaskKey[Unit]("testAll", "Executes unit and integration tests.")

  lazy val settings: Seq[Def.Setting[_]] = testSettings ++ itSettings ++ Seq(
    testAll := (test in Test).dependsOn(test in IntegrationTest).value
  )
}
