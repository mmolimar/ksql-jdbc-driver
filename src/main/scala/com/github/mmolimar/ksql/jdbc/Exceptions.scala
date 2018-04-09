package com.github.mmolimar.ksql.jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}

sealed trait KsqlException {
  def message: String = ""
}

case class InvalidUrl(url: String) extends KsqlException {
  override def message = s"URL with value ${url} is not valid. It must match de regex ${KsqlDriver.urlRegex}"
}

case class CannotConnect(url: String, msg: String) extends KsqlException {
  override def message = s"Cannot connect to this URL ${url}. Error message: ${msg}"
}

case class InvalidProperty(name: String) extends KsqlException {
  override def message = s"Invalid property ${name}."
}

case class NotSupported(override val message: String = "Feature not supported") extends KsqlException

case class KsqlQueryError(override val message: String = "Error executing query") extends KsqlException

case class KsqlCommandError(override val message: String = "Error executing command") extends KsqlException

case class InvalidColumn(override val message: String = "Invalid column") extends KsqlException

case class EmptyRow(override val message: String = "Current row is empty") extends KsqlException

case class UnknownTableType(override val message: String = "Table type does not exist") extends KsqlException

case class UnknownCatalog(override val message: String = "Catalog does not exist") extends KsqlException

case class UnknownSchema(override val message: String = "Schema does not exist") extends KsqlException

object Exceptions {

  implicit def wrapException(error: KsqlException): SQLException = {
    error match {
      case ns: NotSupported => new SQLFeatureNotSupportedException(ns.message)
      case e => new SQLException(e.message)
    }
  }

}
