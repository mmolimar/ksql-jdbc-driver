package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

import com.github.mmolimar.ksql.jdbc.Exceptions._

import scala.util.matching.Regex

object KsqlDriver {

  val ksqlName = "KSQL"
  val ksqlPrefix = "jdbc:ksql://"

  val driverName = "KSQL JDBC driver"
  val driverMajorVersion = 1
  val driverMinorVersion = 0
  val driverVersion = s"$driverMajorVersion.$driverMinorVersion"

  val jdbcMajorVersion = 4
  val jdbcMinorVersion = 1

  val ksqlMajorVersion = 5
  val ksqlMinorVersion = 1
  val ksqlMicroVersion = 2
  val ksqlVersion = s"$ksqlMajorVersion.$ksqlMinorVersion.$ksqlMicroVersion"

  private val ksqlUserPassRegex = "((.+):(.+)@){0,1}"
  private val ksqlServerRegex = "([A-Za-z0-9._%+-]+):([0-9]{1,5})"
  private val ksqlPropsRegex = "(\\?([A-Za-z0-9._-]+=[A-Za-z0-9._-]+(&[A-Za-z0-9._-]+=[A-Za-z0-9._-]+)*)){0,1}"

  val urlRegex: Regex = s"$ksqlPrefix$ksqlUserPassRegex$ksqlServerRegex$ksqlPropsRegex\\z".r

  def parseUrl(url: String): KsqlConnectionValues = url match {
    case urlRegex(_, username, password, ksqlServer, port, _, props, _) =>
      KsqlConnectionValues(
        ksqlServer,
        port.toInt,
        Option(username),
        Option(password),
        Option(props).map(_.split("&").map(_.split("=")).map(p => p(0) -> p(1)).toMap).getOrElse(Map.empty)
      )
    case _ => throw InvalidUrl(url)
  }
}

class KsqlDriver extends Driver {

  override def acceptsURL(url: String): Boolean = Option(url).exists(_.startsWith(KsqlDriver.ksqlPrefix))

  override def jdbcCompliant: Boolean = false

  override def getPropertyInfo(url: String, info: Properties): scala.Array[DriverPropertyInfo] = scala.Array.empty

  override def getMinorVersion: Int = KsqlDriver.driverMinorVersion

  override def getMajorVersion: Int = KsqlDriver.driverMajorVersion

  override def getParentLogger: Logger = throw NotSupported("getParentLogger")

  override def connect(url: String, properties: Properties): Connection = {
    if (!acceptsURL(url)) throw InvalidUrl(url)

    val connection = buildConnection(KsqlDriver.parseUrl(url), properties)
    connection.validate()
    connection
  }

  private[jdbc] def buildConnection(values: KsqlConnectionValues, properties: Properties): KsqlConnection = {
    new KsqlConnection(values, properties)
  }
}
