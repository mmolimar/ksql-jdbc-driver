package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.{SQLException, SQLFeatureNotSupportedException}

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import com.github.mmolimar.ksql.jdbc.{Headers, TableTypes}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}


class StaticResultSetSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  val implementedMethods = Seq("next", "getString", "getBytes", "getByte", "getBytes", "getBoolean", "getShort",
    "getInt", "getLong", "getFloat", "getDouble")

  "A StaticResultSet" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new StaticResultSet[String](Map.empty, Iterator(Seq("")))
        reflectMethods[StaticResultSet[String]](implementedMethods, false, resultSet)
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

        val resultSet = new StaticResultSet[String](Headers.tableTypes, Iterator(Seq(TableTypes.TABLE.name),
          Seq(TableTypes.STREAM.name)))
        resultSet.next should be(true)
        resultSet.getString(1) should be(TableTypes.TABLE.name)
        resultSet.getString("TABLE_TYPE") should be(TableTypes.TABLE.name)
        resultSet.getString("table_type") should be(TableTypes.TABLE.name)
        resultSet.next should be(true)
        resultSet.getString(1) should be(TableTypes.STREAM.name)
        resultSet.getString("TABLE_TYPE") should be(TableTypes.STREAM.name)
        resultSet.getString("table_type") should be(TableTypes.STREAM.name)
        assertThrows[SQLException] {
          resultSet.getString("UNKNOWN")
        }
        resultSet.next should be(false)
      }
    }
  }

}

