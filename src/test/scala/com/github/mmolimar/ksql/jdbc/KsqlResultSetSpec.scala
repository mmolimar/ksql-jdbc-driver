package com.github.mmolimar.ksql.jdbc

import java.sql.SQLFeatureNotSupportedException

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import org.scalatest.{Matchers, WordSpec}


class KsqlResultSetSpec extends WordSpec with Matchers {

  val implementedMethods = Seq("")

  "A KsqlResultSet" when {

    val resultSet = new KsqlResultSet(null)

    "validating specs" should {

      "throw not supported exception if not supported" in {
        reflectMethods[KsqlResultSet](implementedMethods, false, resultSet)
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
        reflectMethods[KsqlResultSet](implementedMethods, true, resultSet)
          .foreach(method => {
            method()
          })
      }
    }
  }

}
