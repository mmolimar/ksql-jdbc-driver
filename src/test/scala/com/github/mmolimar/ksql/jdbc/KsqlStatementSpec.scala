package com.github.mmolimar.ksql.jdbc

import java.io.InputStream
import java.sql.SQLFeatureNotSupportedException
import javax.ws.rs.core.Response

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}


class KsqlStatementSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  val implementedMethods = Seq("execute", "executeQuery")

  "A KsqlStatement" when {

    val mockResponse = mock[Response]
    (mockResponse.getEntity _).expects.returns(mock[InputStream])

    val mockKsqlRestClient = mock[KsqlRestClient]
    (mockKsqlRestClient.makeQueryRequest _).expects(*)
      .returns(RestResponse.successful[KsqlRestClient.QueryStream](new KsqlRestClient.QueryStream(mockResponse)))
      .anyNumberOfTimes

    val statement = new KsqlStatement(mockKsqlRestClient)

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
