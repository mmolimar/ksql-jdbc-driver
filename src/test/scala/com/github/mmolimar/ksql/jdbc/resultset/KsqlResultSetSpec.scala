package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.{ResultSet, SQLException, SQLFeatureNotSupportedException}

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.entity.StreamedRow
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._


class KsqlResultSetSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  val implementedMethods = Seq("isLast", "isAfterLast", "isBeforeFirst", "isFirst", "next",
    "getConcurrency", "close", "getString", "getBytes", "getByte", "getBytes", "getBoolean", "getShort",
    "getInt", "getLong", "getFloat", "getDouble")

  "A KsqlResultSet" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new KsqlResultSet(mock[JdbcQueryStream], 0)
        reflectMethods[KsqlResultSet](implementedMethods, false, resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work if implemented" in {

        val mockedQueryStream = mock[JdbcQueryStream]
        inSequence {
          (mockedQueryStream.hasNext _).expects.returns(true)
          val columnValues = Seq[AnyRef]("string", "bytes".getBytes, Boolean.box(true), Byte.box('0'),
            Short.box(1), Int.box(2), Long.box(3L), Float.box(4.4f), Double.box(5.5d))
          (mockedQueryStream.next _).expects.returns(StreamedRow.row(new GenericRow(columnValues.asJava)))
          (mockedQueryStream.hasNext _).expects.returns(false)
          (mockedQueryStream.close _).expects.returns()
          (mockedQueryStream.close _).expects.throws(new IllegalStateException("Cannot call close() when already closed."))
          (mockedQueryStream.hasNext _).expects.throws(new IllegalStateException("Cannot call hasNext() once closed."))
        }

        val resultSet = new KsqlResultSet(mockedQueryStream)
        resultSet.isLast should be(false)
        resultSet.isAfterLast should be(false)
        resultSet.isBeforeFirst should be(false)
        resultSet.getConcurrency should be(ResultSet.CONCUR_READ_ONLY)

        resultSet.isFirst should be(true)
        resultSet.next should be(true)

        resultSet.getString(1) should be("string")
        resultSet.getBytes(2) should be("bytes".getBytes)
        resultSet.getBoolean(3) should be(Boolean.box(true))
        resultSet.getByte(4) should be(Byte.box('0'))
        resultSet.getShort(5) should be(Short.box(1))
        resultSet.getInt(6) should be(Int.box(2))
        resultSet.getLong(7) should be(Long.box(3L))
        resultSet.getFloat(8) should be(Float.box(4.4f))
        resultSet.getDouble(9) should be(Double.box(5.5d))

        assertThrows[SQLException] {
          resultSet.getString(1000)
        }

        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getString("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getBytes("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getBoolean("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getByte("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getShort("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getInt("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getLong("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getFloat("UNKNOWN")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          resultSet.getDouble("UNKNOWN")
        }

        resultSet.next should be(false)
        resultSet.isFirst should be(false)
        resultSet.close
        assertThrows[IllegalStateException] {
          resultSet.close
        }
        assertThrows[IllegalStateException] {
          resultSet.next
        }
      }
    }
  }

  "A ResultSetNotSupported" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new ResultSetNotSupported
        reflectMethods[ResultSetNotSupported](Seq.empty, false, resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

    }
  }

}

