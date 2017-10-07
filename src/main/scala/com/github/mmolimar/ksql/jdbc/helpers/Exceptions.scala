package com.github.mmolimar.ksql.jdbc.helpers

import java.sql.{SQLException, SQLFeatureNotSupportedException}

import com.github.mmolimar.ksql.jdbc.KsqlDriver

private[jdbc] class KsqlError

case class NotSupported(message: String = "Feature not supported") extends KsqlError
case class InvalidUrl(url: String) extends KsqlError

object Exceptions {

  def wrapException(error: KsqlError): SQLException = {
    error match {
      case e: InvalidUrl => new SQLException(s"URL with value ${e.url} is not valid." +
        s"It must match de regex ${KsqlDriver.urlRegex}")
      case e: NotSupported => new SQLFeatureNotSupportedException(e.message)
    }
  }

  def wrapExceptionAndThrow(error: KsqlError) = {
    throw wrapException(error)
  }

}
