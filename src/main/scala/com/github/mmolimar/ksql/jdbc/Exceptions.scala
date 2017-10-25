package com.github.mmolimar.ksql.jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}

sealed trait KsqlException

case class InvalidUrl(url: String) extends KsqlException

case class CannotConnect(url: String, msg: String) extends KsqlException

case class InvalidProperty(name: String) extends KsqlException

case class NotSupported(msg: String = "Feature not supported") extends KsqlException

case class KsqlQueryError(msg: String = "Error executing query") extends KsqlException

case class KsqlCommandError(msg: String = "Error executing command") extends KsqlException

case class InvalidColumn(msg: String = "Invalid column") extends KsqlException

case class EmptyRow(msg: String = "Current row is empty") extends KsqlException

case class UnknownTableType(msg: String = "Table type does not exist") extends KsqlException

case class UnknownCatalog(msg: String = "Catalog does not exist") extends KsqlException

case class UnknownSchema(msg: String = "Schema does not exist") extends KsqlException

object Exceptions {

  implicit def wrapException(error: KsqlException): SQLException = {
    error match {
      case e: InvalidUrl => new SQLException(s"URL with value ${e.url} is not valid." +
        s"It must match de regex ${KsqlDriver.urlRegex}")
      case e: CannotConnect => new SQLException(s"Cannot connect to this URL ${e.url}. Error message: ${e.msg}")
      case e: InvalidProperty => new SQLException(e.name)
      case e: NotSupported => new SQLFeatureNotSupportedException(e.msg)
      case e: KsqlQueryError => new SQLException(e.msg)
      case e: KsqlCommandError => new SQLException(e.msg)
      case e: InvalidColumn => new SQLException(e.msg)
      case e: EmptyRow => new SQLException(e.msg)
      case e: UnknownTableType => new SQLException(e.msg)
      case e: UnknownCatalog => new SQLException(e.msg)
      case e: UnknownSchema => new SQLException(e.msg)
      case _ => new SQLException("Unknown KSQL Exception")
    }
  }

}
