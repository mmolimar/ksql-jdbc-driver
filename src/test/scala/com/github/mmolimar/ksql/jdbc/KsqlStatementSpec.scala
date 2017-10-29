package com.github.mmolimar.ksql.jdbc

import java.io.InputStream
import java.sql.{SQLException, SQLFeatureNotSupportedException}
import javax.ws.rs.core.Response

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity.{ErrorMessage, KsqlEntityList}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}


class KsqlStatementSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  val implementedMethods = Seq("execute", "executeQuery")

  "A KsqlStatement" when {

    val mockResponse = mock[Response]
    (mockResponse.getEntity _).expects.returns(mock[InputStream]).anyNumberOfTimes

    val mockKsqlRestClient = mock[KsqlRestClient]

    val statement = new KsqlStatement(mockKsqlRestClient)

    "validating specs" should {

      "throw not supported exception if not supported" in {
        (mockKsqlRestClient.makeQueryRequest _).expects(*)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](new KsqlRestClient.QueryStream(mockResponse)))
          .noMoreThanOnce

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
        (mockKsqlRestClient.makeKsqlRequest _).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](new KsqlEntityList))
          .once
        statement.execute("") should be(true)

        (mockKsqlRestClient.makeQueryRequest _).expects(*)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](new KsqlRestClient.QueryStream(mockResponse)))
          .once
        statement.executeQuery("") should not be (null)

        assertThrows[SQLException] {
          (mockKsqlRestClient.makeQueryRequest _).expects(*)
            .returns(RestResponse.erroneous(new ErrorMessage(null, null)))
            .once
          statement.executeQuery("")
        }
      }
    }
  }
}
