package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, SQLException, SQLFeatureNotSupportedException}
import java.util.{Collections, Properties}

import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity.{CommandStatus, CommandStatuses, ErrorMessage, KsqlEntityList}
import io.confluent.ksql.rest.server.computation.CommandId
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

class KsqlConnectionSpec extends WordSpec with Matchers with MockFactory {

  "A KsqlConnection" when {
    "validating specs" should {
      val values = KsqlConnectionValues("localhost", 8080, Map.empty[String, String])
      val ksqlRestClient = mock[KsqlRestClient]
      val ksqlConnection = new KsqlConnection(values, new Properties) {
        override def init: KsqlRestClient = ksqlRestClient
      }

      "throw not supported exception if not supported" in {
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setAutoCommit(true)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setHoldability(0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.clearWarnings
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getNetworkTimeout
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createBlob
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createSQLXML
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setSavepoint
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setSavepoint("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createNClob
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getClientInfo("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getClientInfo
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getSchema
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setNetworkTimeout(null, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getMetaData
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getTypeMap
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.rollback
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.rollback(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createStatement
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createStatement(0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createStatement(0, 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getHoldability
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setReadOnly(false)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setTypeMap(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getCatalog
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createClob
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.nativeSQL("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setTransactionIsolation(0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareCall("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareCall("", 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareCall("", 0, 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createArrayOf("", null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setCatalog("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getAutoCommit
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.abort(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareStatement("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareStatement("", 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareStatement("", 0, 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareStatement("", 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareStatement("", Array.empty[Int])
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.prepareStatement("", Array.empty[String])
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.releaseSavepoint(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.isClosed
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.createStruct("", Array.empty[AnyRef])
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.getWarnings
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.setSchema("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.commit
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.unwrap(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          ksqlConnection.isWrapperFor(null)
        }
      }
      "work if implemented" in {
        ksqlConnection.getTransactionIsolation should be(Connection.TRANSACTION_NONE)
        ksqlConnection.setClientInfo(new Properties)

        (ksqlRestClient.makeKsqlRequest _).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](new KsqlEntityList))
        ksqlConnection.setClientInfo("", "")
        assertThrows[SQLException] {
          (ksqlRestClient.makeKsqlRequest _).expects(*)
            .returns(RestResponse.erroneous(new ErrorMessage("", Collections.emptyList[String])))
          ksqlConnection.setClientInfo("", "")
        }

        ksqlConnection.isReadOnly should be(true)

        (ksqlRestClient.makeStatusRequest _: () => RestResponse[CommandStatuses]).expects
          .returns(RestResponse.successful[CommandStatuses]
            (new CommandStatuses(Collections.emptyMap[CommandId, CommandStatus.Status])))
        ksqlConnection.isValid(0) should be(true)

        (ksqlRestClient.close _).expects
        ksqlConnection.close
      }
    }
  }

}
