package com.github.mmolimar.ksql.jdbc

import java.sql._
import java.util
import java.util.Properties
import java.util.concurrent.Executor

import com.github.mmolimar.ksql.jdbc.Exceptions._
import io.confluent.ksql.rest.client.KsqlRestClient

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class KsqlConnectionValues(ksqlServer: String, port: Int, config: Map[String, String]) {
  def getKsqlUrl: String = {
    val protocol = if (isSecured) "https://" else "http://"
    protocol + ksqlServer + ":" + port
  }

  def isSecured: Boolean = config.get("secured").getOrElse("false").toBoolean
}

class KsqlConnection(values: KsqlConnectionValues, properties: Properties) extends Connection with WrapperNotSupported {

  private val ksqlClient = init

  private[jdbc] def init: KsqlRestClient = {
    new KsqlRestClient(values.getKsqlUrl, properties.asScala.toMap[String, AnyRef].asJava)
  }

  private[jdbc] def validate: Unit = {
    Try(ksqlClient.makeRootRequest) match {
      case Success(response) if response.isErroneous =>
        throw CannotConnect(values.ksqlServer, response.getErrorMessage.getMessage)
      case Failure(e) => throw CannotConnect(values.ksqlServer, e.getMessage)
      case _ =>
    }
  }

  private[jdbc] def executeKsqlCommand(ksql: String) = ksqlClient.makeKsqlRequest(ksql)

  override def setAutoCommit(autoCommit: Boolean): Unit = throw NotSupported()

  override def setHoldability(holdability: Int): Unit = throw NotSupported()

  override def clearWarnings: Unit = throw NotSupported()

  override def getNetworkTimeout: Int = throw NotSupported()

  override def createBlob: Blob = throw NotSupported()

  override def createSQLXML: SQLXML = throw NotSupported()

  override def setSavepoint: Savepoint = throw NotSupported()

  override def setSavepoint(name: String): Savepoint = throw NotSupported()

  override def createNClob: NClob = throw NotSupported()

  override def getTransactionIsolation: Int = Connection.TRANSACTION_NONE

  override def getClientInfo(name: String): String = throw NotSupported()

  override def getClientInfo: Properties = throw NotSupported()

  override def getSchema: String = throw NotSupported()

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = throw NotSupported()

  override def getMetaData: DatabaseMetaData = new KsqlDatabaseMetaData(this)

  override def getTypeMap: util.Map[String, Class[_]] = throw NotSupported()

  override def rollback: Unit = throw NotSupported()

  override def rollback(savepoint: Savepoint): Unit = throw NotSupported()

  override def createStatement: Statement = createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = {
    createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY ||
      resultSetConcurrency != ResultSet.CONCUR_READ_ONLY ||
      resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
      throw NotSupported("ResultSetType, ResultSetConcurrency and ResultSetHoldability must be" +
        " TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT respectively ")
    }
    new KsqlStatement(ksqlClient)
  }

  override def getHoldability: Int = throw NotSupported()

  override def setReadOnly(readOnly: Boolean): Unit = throw NotSupported()

  override def setClientInfo(name: String, value: String): Unit = {
    val ksql = s"SET '${name.trim}'='${value.trim}';"
    if (ksqlClient.makeKsqlRequest(ksql).isErroneous) {
      throw InvalidProperty(name)
    }
  }

  override def setClientInfo(properties: Properties): Unit = {
    properties.asScala.foreach(entry => setClientInfo(entry._1, entry._2))
  }

  override def isReadOnly: Boolean = true

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = throw NotSupported()

  override def getCatalog: String = throw NotSupported()

  override def createClob: Clob = throw NotSupported()

  override def nativeSQL(sql: String): String = throw NotSupported()

  override def setTransactionIsolation(level: Int): Unit = throw NotSupported()

  override def prepareCall(sql: String): CallableStatement = throw NotSupported()

  override def prepareCall(sql: String, resultSetType: Int,
                           resultSetConcurrency: Int): CallableStatement = throw NotSupported()

  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int,
                           resultSetHoldability: Int): CallableStatement = throw NotSupported()

  override def createArrayOf(typeName: String, elements: scala.Array[AnyRef]): Array = throw NotSupported()

  override def setCatalog(catalog: String): Unit = throw NotSupported()

  override def close: Unit = ksqlClient.close

  override def getAutoCommit: Boolean = throw NotSupported()

  override def abort(executor: Executor): Unit = throw NotSupported()

  override def isValid(timeout: Int): Boolean = ksqlClient.makeStatusRequest.isSuccessful

  override def prepareStatement(sql: String): PreparedStatement = throw NotSupported()

  override def prepareStatement(sql: String, resultSetType: Int,
                                resultSetConcurrency: Int): PreparedStatement = throw NotSupported()

  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int,
                                resultSetHoldability: Int): PreparedStatement = throw NotSupported()

  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = throw NotSupported()

  override def prepareStatement(sql: String, columnIndexes: scala.Array[Int]): PreparedStatement = throw NotSupported()

  override def prepareStatement(sql: String, columnNames: scala.Array[String]): PreparedStatement = throw NotSupported()

  override def releaseSavepoint(savepoint: Savepoint): Unit = throw NotSupported()

  override def isClosed: Boolean = throw NotSupported()

  override def createStruct(typeName: String, attributes: scala.Array[AnyRef]): Struct = throw NotSupported()

  override def getWarnings: SQLWarning = throw NotSupported()

  override def setSchema(schema: String): Unit = throw NotSupported()

  override def commit: Unit = throw NotSupported()

}
