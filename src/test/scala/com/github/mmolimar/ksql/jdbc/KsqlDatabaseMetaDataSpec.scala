package com.github.mmolimar.ksql.jdbc

import java.sql.SQLFeatureNotSupportedException

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.runtime.universe._


class KsqlDatabaseMetaDataSpec extends WordSpec with Matchers {

  val implementedMethods = Seq("getDriverName", "getDriverVersion", "getDriverMajorVersion", "getDriverMinorVersion",
    "getJDBCMajorVersion", "getJDBCMinorVersion", "getCatalogs")

  "A KsqlDatabaseMetaData" when {

    val metadata = new KsqlDatabaseMetaData(null)

    "validating specs" should {

      "throw not supported exception if not supported" in {
        reflectMethods[KsqlDatabaseMetaData](implementedMethods, false, metadata)
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
        reflectMethods[KsqlDatabaseMetaData](implementedMethods, true, metadata)
          .foreach(method => {
            method()
          })
      }
    }
  }

}
