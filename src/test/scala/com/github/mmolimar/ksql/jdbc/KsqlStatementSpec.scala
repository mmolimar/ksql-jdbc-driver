package com.github.mmolimar.ksql.jdbc

import java.io.InputStream
import java.sql.{SQLException, SQLFeatureNotSupportedException}

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity.{KsqlEntityList, KsqlErrorMessage}
import javax.ws.rs.core.Response
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}


class KsqlStatementSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  val implementedMethods = Seq("execute", "executeQuery")

  "A KsqlStatement" when {

    val mockResponse = mock[Response]
    (mockResponse.getEntity _).expects.returns(mock[InputStream]).anyNumberOfTimes
    val mockedKsqlRestClient = mock[MockableKsqlRestClient]
    val statement = new KsqlStatement(mockedKsqlRestClient)

    "validating specs" should {

      "throw not supported exception if not supported" in {

        (mockedKsqlRestClient.makeQueryRequest _).expects(*)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
          .noMoreThanOnce

        reflectMethods[KsqlStatement](implementedMethods, false, statement)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work if implemented" in {
        (mockedKsqlRestClient.makeKsqlRequest _).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](new KsqlEntityList))
          .once
        statement.execute("") should be(true)

        (mockedKsqlRestClient.makeQueryRequest _).expects(*)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
          .once
        Option(statement.executeQuery("")) should not be (None)

        assertThrows[SQLException] {
          (mockedKsqlRestClient.makeQueryRequest _).expects(*)
            .returns(RestResponse.erroneous(new KsqlErrorMessage(-1, null, null)))
            .once
          statement.executeQuery("")
        }
      }
    }
  }
}
