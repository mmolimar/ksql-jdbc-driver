package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

import com.github.mmolimar.ksql.jdbc.Exceptions._

import scala.util.Try

object KsqlDriver {

  val driverName = "KSQL JDBC"
  val version = "0.1-SNAPSHOT"
  val ksqlPrefix = "jdbc:ksql://"
  val majorVersion = 1
  val minorVersion = 0
  val jdbcMajorVersion = 4
  val jdbcMinorVersion = 1

  private val ksqlServerRegex = "([A-Za-z0-9._%+-]+):([0-9]+)"

  private val ksqlPropsRegex = "(\\?([A-Za-z0-9._-]+=[A-Za-z0-9._-]+(&[A-Za-z0-9._-]+=[A-Za-z0-9._-]+)*)){0,1}"

  val urlRegex = s"${ksqlPrefix}${ksqlServerRegex}${ksqlPropsRegex}\\z".r

  def parseUrl(url: String): KsqlConnectionValues = {
    url match {
      case urlRegex(ksqlServer, port, _, properties, _) =>
        KsqlConnectionValues(ksqlServer, port.toInt,
          Try(properties.split("&")).getOrElse(Array.empty[String])
            .map(_.split("=")).map(prop => prop(0) -> prop(1)).toMap)
      case _ => throw InvalidUrl(url)
    }
  }
}

class KsqlDriver extends Driver {

  override def acceptsURL(url: String): Boolean = Option(url).exists(_.startsWith(KsqlDriver.ksqlPrefix))

  override def jdbcCompliant(): Boolean = false

  override def getPropertyInfo(url: String, info: Properties): scala.Array[DriverPropertyInfo] = scala.Array.empty

  override def getMinorVersion: Int = KsqlDriver.minorVersion

  override def getMajorVersion: Int = KsqlDriver.majorVersion

  override def getParentLogger: Logger = throw NotSupported("getParentLogger method not supported")

  override def connect(url: String, properties: Properties): Connection = {
    if (!acceptsURL(url)) throw InvalidUrl(url)

    val connection = new KsqlConnection(KsqlDriver.parseUrl(url), properties)
    connection.validate
    connection
  }
}
