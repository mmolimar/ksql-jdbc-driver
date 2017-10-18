package com.github.mmolimar.ksql.jdbc

import java.sql.SQLFeatureNotSupportedException

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import org.scalatest.{Matchers, WordSpec}


class KsqlStatementSpec extends WordSpec with Matchers {

  val implementedMethods = Seq("")

  "A KsqlStatement" when {

    val statement = new KsqlStatement

    "validating specs" should {

      "throw not supported exception if not supported" in {
        reflectMethods[KsqlStatement](implementedMethods, false, statement)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              try {
                method()
              } catch {
                case t: Throwable => throw t.getCause
              }
            }
          })
      }
      "work if implemented" in {
        reflectMethods[KsqlStatement](implementedMethods, true, statement)
          .foreach(method => {
            method()
          })
      }
    }
  }
}
