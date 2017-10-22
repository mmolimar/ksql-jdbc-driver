package com.github.mmolimar.ksql.jdbc

import java.io.InputStream
import java.sql.{ResultSet, SQLException, SQLFeatureNotSupportedException}
import javax.ws.rs.core.Response

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.client.KsqlRestClient
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

        val resultSet = new KsqlResultSet(null)
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

        val mockResponse = mock[Response]
        (mockResponse.getEntity _).expects.returns(mock[InputStream]).anyNumberOfTimes

        val mockQueryStream = mock[MockQueryStream]
        inSequence {
          (mockQueryStream.hasNext _).expects.returns(true)
          val columnValues = Seq[AnyRef]("string", "bytes".getBytes, Boolean.box(true), Byte.box('0'),
            Short.box(1), Int.box(2), Long.box(3L), Float.box(4.4f), Double.box(5.5d))
          (mockQueryStream.next _).expects.returns(new StreamedRow(new GenericRow(columnValues.asJava), null))
          (mockQueryStream.hasNext _).expects.returns(false)
          (mockQueryStream.close _).expects.returns()
          (mockQueryStream.close _).expects.throws(new IllegalStateException("Cannot call close() when already closed"))
          (mockQueryStream.hasNext _).expects.throws(new IllegalStateException("Cannot call hasNext() once closed"))
        }

        val resultSet = new KsqlResultSet(mockQueryStream)

        resultSet.isLast should be(false)
        resultSet.isAfterLast should be(false)
        resultSet.isBeforeFirst should be(false)
        resultSet.getConcurrency should be(ResultSet.CONCUR_READ_ONLY)

        resultSet.isFirst should be(true)
        resultSet.next should be(true)

        resultSet.getString(0) should be("string")
        resultSet.getBytes(1) should be("bytes".getBytes)
        resultSet.getBoolean(2) should be(Boolean.box(true))
        resultSet.getByte(3) should be(Byte.box('0'))
        resultSet.getShort(4) should be(Short.box(1))
        resultSet.getInt(5) should be(Int.box(2))
        resultSet.getLong(6) should be(Long.box(3L))
        resultSet.getFloat(7) should be(Float.box(4.4f))
        resultSet.getDouble(8) should be(Double.box(5.5d))

        assertThrows[SQLException] {
          resultSet.getString(1000)
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


        class MockQueryStream extends KsqlRestClient.QueryStream(mockResponse)
      }
    }
  }


}

