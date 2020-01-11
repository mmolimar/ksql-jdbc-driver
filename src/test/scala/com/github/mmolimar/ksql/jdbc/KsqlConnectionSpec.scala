package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, SQLException, SQLFeatureNotSupportedException}
import java.util.{Collections, Properties}

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.rest.client.{KsqlRestClient, MockableKsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity._
import org.eclipse.jetty.http.HttpStatus.Code
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KsqlConnectionSpec extends AnyWordSpec with Matchers with MockFactory {

  "A KsqlConnection" when {

    "validating specs" should {
      val values = KsqlConnectionValues("localhost", 8080, None, None, Map.empty[String, String])
      val mockKsqlRestClient = mock[MockableKsqlRestClient]
      val ksqlConnection = new KsqlConnection(values, new Properties) {
        override def init: KsqlRestClient = mockKsqlRestClient
      }

      "throw not supported exception if not supported" in {
        val methods = implementedMethods[KsqlConnection]
        reflectMethods[KsqlConnection](methods = methods, implemented = false, obj = ksqlConnection)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work if implemented" in {
        assertThrows[SQLException] {
          ksqlConnection.isClosed
        }
        ksqlConnection.getTransactionIsolation should be(Connection.TRANSACTION_NONE)
        ksqlConnection.setClientInfo(new Properties)

        (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](Code.OK, new KsqlEntityList))
        ksqlConnection.setClientInfo("", "")
        assertThrows[SQLException] {
          (mockKsqlRestClient.makeKsqlRequest(_: String)).expects(*)
            .returns(RestResponse.erroneous(Code.INTERNAL_SERVER_ERROR, new KsqlErrorMessage(-1, "", Collections.emptyList[String])))
          ksqlConnection.setClientInfo("", "")
        }

        ksqlConnection.isReadOnly should be(false)

        (mockKsqlRestClient.makeStatusRequest _: () => RestResponse[CommandStatuses]).expects
          .returns(RestResponse.successful[CommandStatuses]
            (Code.OK, new CommandStatuses(Collections.emptyMap[CommandId, CommandStatus.Status])))
        ksqlConnection.isValid(0) should be(true)

        Option(ksqlConnection.getMetaData) should not be None

        Option(ksqlConnection.createStatement) should not be None
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createStatement(-1, -1)
        }
        ksqlConnection.setAutoCommit(true)
        ksqlConnection.setAutoCommit(false)
        ksqlConnection.getAutoCommit should be(false)
        ksqlConnection.getWarnings should be(None.orNull)
        ksqlConnection.getCatalog should be(None.orNull)
        ksqlConnection.setCatalog("test")
        ksqlConnection.getCatalog should be(None.orNull)

        (mockKsqlRestClient.close _).expects
        ksqlConnection.close()
        ksqlConnection.isClosed should be(true)
        ksqlConnection.commit()
      }
    }
  }

  "A ConnectionNotSupported" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new ConnectionNotSupported
        reflectMethods[ConnectionNotSupported](methods = Seq.empty, implemented = false, obj = resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }
    }
  }

}
