package com.github.mmolimar.ksql.jdbc

import java.sql._
import java.util
import java.util.concurrent.Executor
import java.util.{Collections, Properties}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity.KsqlEntityList

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class KsqlConnectionValues(ksqlServer: String, port: Int, config: Map[String, String]) {

  def ksqlUrl: String = {
    val protocol = if (isSecured) "https://" else "http://"
    protocol + ksqlServer + ":" + port
  }

  def jdbcUrl: String = {
    val suffix = if (config.isEmpty) "" else "?"
    s"${KsqlDriver.ksqlPrefix}$ksqlServer:$port$suffix${
      config.map(c => s"${c._1}=${c._2}").mkString("&")
    }"
  }

  def isSecured: Boolean = config.getOrElse("secured", "false").toBoolean

  def properties: Boolean = config.getOrElse("properties", "false").toBoolean

  def timeout: Long = config.getOrElse("timeout", "0").toLong

}

class KsqlConnection(private[jdbc] val values: KsqlConnectionValues, properties: Properties)
  extends Connection with WrapperNotSupported {

  private val ksqlClient = init

  private[jdbc] def init: KsqlRestClient = {
    val props = if (values.properties) {
      properties.asScala.toMap[String, AnyRef].asJava
    } else {
      Collections.emptyMap[String, AnyRef]
    }
    new KsqlRestClient(values.ksqlUrl, props)
  }

  private[jdbc] def validate: Unit = {
    Try(ksqlClient.makeRootRequest) match {
      case Success(response) if response.isErroneous =>
        throw CannotConnect(values.ksqlServer, response.getErrorMessage.getMessage)
      case Failure(e) => throw CannotConnect(values.ksqlServer, e.getMessage)
      case _ =>
    }
  }

  private[jdbc] def executeKsqlCommand(ksql: String): RestResponse[KsqlEntityList] = ksqlClient.makeKsqlRequest(ksql)

  override def setAutoCommit(autoCommit: Boolean): Unit = {}

  override def setHoldability(holdability: Int): Unit = throw NotSupported("setHoldability")

  override def clearWarnings: Unit = throw NotSupported("clearWarnings")

  override def getNetworkTimeout: Int = throw NotSupported("getNetworkTimeout")

  override def createBlob: Blob = throw NotSupported("createBlob")

  override def createSQLXML: SQLXML = throw NotSupported("createSQLXML")

  override def setSavepoint: Savepoint = throw NotSupported("setSavepoint")

  override def setSavepoint(name: String): Savepoint = throw NotSupported("setSavepoint")

  override def createNClob: NClob = throw NotSupported("createNClob")

  override def getTransactionIsolation: Int = Connection.TRANSACTION_NONE

  override def getClientInfo(name: String): String = throw NotSupported("getClientInfo")

  override def getClientInfo: Properties = throw NotSupported("getClientInfo")

  override def getSchema: String = throw NotSupported("getSchema")

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = throw NotSupported("setNetworkTimeout")

  override def getMetaData: DatabaseMetaData = new KsqlDatabaseMetaData(this)

  override def getTypeMap: util.Map[String, Class[_]] = throw NotSupported("getTypeMap")

  override def rollback: Unit = throw NotSupported("rollback")

  override def rollback(savepoint: Savepoint): Unit = throw NotSupported("rollback")

  override def createStatement: Statement = createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = {
    createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY ||
      resultSetConcurrency != ResultSet.CONCUR_READ_ONLY ||
      resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
      throw NotSupported("ResultSetType, ResultSetConcurrency and ResultSetHoldability must be" +
        " TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT respectively.")
    }
    new KsqlStatement(ksqlClient, values.timeout)
  }

  override def getHoldability: Int = throw NotSupported("getHoldability")

  override def setReadOnly(readOnly: Boolean): Unit = throw NotSupported("setReadOnly")

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

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = throw NotSupported("setTypeMap")

  override def getCatalog: String = None.orNull

  override def createClob: Clob = throw NotSupported("createClob")

  override def nativeSQL(sql: String): String = throw NotSupported("nativeSQL")

  override def setTransactionIsolation(level: Int): Unit = throw NotSupported("setTransactionIsolation")

  override def prepareCall(sql: String): CallableStatement = throw NotSupported("prepareCall")

  override def prepareCall(sql: String, resultSetType: Int,
                           resultSetConcurrency: Int): CallableStatement = throw NotSupported("prepareCall")

  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int,
                           resultSetHoldability: Int): CallableStatement = throw NotSupported("prepareCall")

  override def createArrayOf(typeName: String, elements: scala.Array[AnyRef]): Array = throw NotSupported("createArrayOf")

  override def setCatalog(catalog: String): Unit = {}

  override def close: Unit = ksqlClient.close

  override def getAutoCommit: Boolean = false

  override def abort(executor: Executor): Unit = throw NotSupported("abort")

  override def isValid(timeout: Int): Boolean = ksqlClient.makeStatusRequest.isSuccessful

  override def prepareStatement(sql: String): PreparedStatement = throw NotSupported("prepareStatement")

  override def prepareStatement(sql: String, resultSetType: Int,
                                resultSetConcurrency: Int): PreparedStatement = throw NotSupported("prepareStatement")

  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int,
                                resultSetHoldability: Int): PreparedStatement = throw NotSupported("prepareStatement")

  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = throw NotSupported("prepareStatement")

  override def prepareStatement(sql: String, columnIndexes: scala.Array[Int]): PreparedStatement =
    throw NotSupported("prepareStatement")

  override def prepareStatement(sql: String, columnNames: scala.Array[String]): PreparedStatement =
    throw NotSupported("prepareStatement")

  override def releaseSavepoint(savepoint: Savepoint): Unit = throw NotSupported("releaseSavepoint")

  override def isClosed: Boolean = throw NotSupported("isClosed")

  override def createStruct(typeName: String, attributes: scala.Array[AnyRef]): Struct =
    throw NotSupported("createStruct")

  override def getWarnings: SQLWarning = None.orNull

  override def setSchema(schema: String): Unit = throw NotSupported("setSchema")

  override def commit: Unit = throw NotSupported("commit")

}
