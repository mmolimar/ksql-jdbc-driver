package com.github.mmolimar.ksql.jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}

sealed trait KsqlException

case class InvalidUrl(url: String) extends KsqlException

case class CannotConnect(url: String, msg: String) extends KsqlException

case class InvalidProperty(name: String) extends KsqlException

case class NotSupported(msg: String = "Feature not supported") extends KsqlException

object Exceptions {

  implicit def wrapException(error: KsqlException): SQLException = {
    error match {
      case e: InvalidUrl => new SQLException(s"URL with value ${e.url} is not valid." +
        s"It must match de regex ${KsqlDriver.urlRegex}")
      case e: NotSupported => new SQLFeatureNotSupportedException(e.msg)
      case e: CannotConnect => new SQLException(s"Cannot connect to this URL ${e.url}. Error message: ${e.msg}")
      case e: InvalidProperty => new SQLException(e.name)
      case _ => new SQLException("Unknown KSQL Exception")
    }
  }

}
