package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

import com.github.mmolimar.ksql.jdbc.helpers.Exceptions._
import com.github.mmolimar.ksql.jdbc.helpers.{InvalidUrl, NotSupported}

object KsqlDriver {

  val ksqlPrefix = "jdbc:ksql:"

  val urlRegex = """jdbc:ksql:(:?[\\/\\.\\-_A-Za-z0-9]+:?)(\\?([A-Za-z0-9]+=[A-Za-z0-9]+)((\\&([A-Za-z0-9]+=[A-Za-z0-9]+))*)?)?""".r

  def parseUrl(url: String) = {
    url match {
      case urlRegex(brokers, properties) => println(brokers)
      case _ => wrapExceptionAndThrow(InvalidUrl(url))
    }
  }
}

class KsqlDriver extends Driver {

  override def acceptsURL(url: String): Boolean = Option(url).exists(_.startsWith(KsqlDriver.ksqlPrefix))

  override def jdbcCompliant(): Boolean = false

  override def getPropertyInfo(url: String, info: Properties): scala.Array[DriverPropertyInfo] = scala.Array.empty

  override def getMinorVersion: Int = 0

  override def getMajorVersion: Int = 1

  override def getParentLogger: Logger = wrapExceptionAndThrow(NotSupported("getParentLogger not supported"))

  override def connect(url: String, properties: Properties): Connection = {
    if (!acceptsURL(url)) wrapExceptionAndThrow(InvalidUrl(url))
    null
  }
}
