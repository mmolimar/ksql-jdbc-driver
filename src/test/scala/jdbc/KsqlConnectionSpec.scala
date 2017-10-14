package jdbc

import java.sql.{Connection, SQLFeatureNotSupportedException}
import java.util.Properties

import com.github.mmolimar.ksql.jdbc.{KsqlConnection, KsqlConnectionValues}
import io.confluent.ksql.rest.client.KsqlRestClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

class KsqlConnectionSpec extends WordSpec with Matchers with MockFactory {

  "A KsqlConnection" when {
    "validating specs" should {
      val values = KsqlConnectionValues("localhost", 8080, Map.empty[String, String])
      val mockedInitConnection = mockFunction[KsqlRestClient]
      mockedInitConnection.expects.returns(new KsqlRestClient("http://localhost:45678"))
      val connection = new KsqlConnection(values, new Properties) {
        override def initConnection(): KsqlRestClient = mockedInitConnection.apply

        override def setClientInfo(name: String, value: String): Unit = {}

        override def isValid(timeout: Int): Boolean = true
      }

      "throw not supported exception if not supported" in {
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setAutoCommit(true)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setHoldability(0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.clearWarnings
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getNetworkTimeout
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createBlob
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createSQLXML
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setSavepoint
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setSavepoint("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createNClob
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getClientInfo("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getClientInfo
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getSchema
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setNetworkTimeout(null, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getMetaData
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getTypeMap
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.rollback
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.rollback(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createStatement
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createStatement(0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createStatement(0, 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getHoldability
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setReadOnly(false)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setTypeMap(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getCatalog
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createClob
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.nativeSQL("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setTransactionIsolation(0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareCall("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareCall("", 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareCall("", 0, 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createArrayOf("", null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setCatalog("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getAutoCommit
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.abort(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareStatement("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareStatement("", 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareStatement("", 0, 0, 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareStatement("", 0)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareStatement("", Array.empty[Int])
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.prepareStatement("", Array.empty[String])
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.releaseSavepoint(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.isClosed
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.createStruct("", Array.empty[AnyRef])
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.getWarnings
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.setSchema("")
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.commit
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.unwrap(null)
        }
        assertThrows[SQLFeatureNotSupportedException] {
          connection.isWrapperFor(null)
        }
      }
      "work if implemented" in {
        connection.getTransactionIsolation should be(Connection.TRANSACTION_NONE)
        connection.setClientInfo(new Properties)
        connection.setClientInfo("", "")
        connection.isReadOnly should be(true)
        connection.isValid(0) should be(true)
        connection.close
      }
    }
  }

}
