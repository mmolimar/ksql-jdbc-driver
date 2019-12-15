package com.github.mmolimar.ksql.jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}

import io.confluent.ksql.rest.entity.KsqlErrorMessage

sealed trait KsqlException {

  def message: String

  def cause: Throwable

}

case class InvalidUrl(url: String, override val cause: Throwable = None.orNull) extends KsqlException {
  override def message = s"URL with value $url is not valid. It must match de regex ${KsqlDriver.urlRegex}."
}

case class CannotConnect(url: String, msg: String, override val cause: Throwable = None.orNull) extends KsqlException {
  override def message = s"Cannot connect to this URL $url. Error message: $msg."
}

case class NotConnected(url: String, override val cause: Throwable = None.orNull) extends KsqlException {
  override def message = s"Not connected to database: $url."
}

case class InvalidProperty(name: String, override val cause: Throwable = None.orNull) extends KsqlException {
  override def message = s"Invalid property $name."
}

case class NotSupported(feature: String, override val cause: Throwable = None.orNull) extends KsqlException {
  override val message = s"Feature not supported: $feature."
}

case class InvalidValue(prop: String, value: String, override val cause: Throwable = None.orNull) extends KsqlException {
  override val message = s"value '' is not valid for property: $prop."
}

case class AlreadyClosed(override val message: String = "Already closed.",
                         override val cause: Throwable = None.orNull) extends KsqlException

class KsqlError(val prefix: String, val ksqlMessage: Option[KsqlErrorMessage], val cause: Throwable) extends KsqlException {
  override def message: String =
    s"$prefix.${ksqlMessage.map(msg => s" Error code [${msg.getErrorCode}]. Message: ${msg.getMessage}").getOrElse("")}"
}

case class KsqlQueryError(override val prefix: String = "Error executing query.",
                          override val ksqlMessage: Option[KsqlErrorMessage] = None,
                          override val cause: Throwable = None.orNull) extends KsqlError(prefix, ksqlMessage, cause)

case class KsqlCommandError(override val prefix: String = "Error executing command.",
                            override val ksqlMessage: Option[KsqlErrorMessage] = None,
                            override val cause: Throwable = None.orNull) extends KsqlError(prefix, ksqlMessage, cause)

case class KsqlEntityListError(override val prefix: String = "Invalid KSQL entity list.",
                               override val ksqlMessage: Option[KsqlErrorMessage] = None,
                               override val cause: Throwable = None.orNull) extends KsqlError(prefix, ksqlMessage, cause)

case class InvalidColumn(override val message: String = "Invalid column.",
                         override val cause: Throwable = None.orNull) extends KsqlException

case class EmptyRow(override val message: String = "Current row is empty.",
                    override val cause: Throwable = None.orNull) extends KsqlException

case class ResultSetError(override val message: String = "Error accessing to the result set.",
                          override val cause: Throwable = None.orNull) extends KsqlException

case class UnknownTableType(override val message: String = "Table type does not exist.",
                            override val cause: Throwable = None.orNull) extends KsqlException

case class UnknownCatalog(override val message: String = "Catalog does not exist.",
                          override val cause: Throwable = None.orNull) extends KsqlException

case class UnknownSchema(override val message: String = "Schema does not exist.",
                         override val cause: Throwable = None.orNull) extends KsqlException

object Exceptions {

  implicit def wrapException(error: KsqlException): SQLException = {
    error match {
      case ns: NotSupported => new SQLFeatureNotSupportedException(ns.message)
      case e => new SQLException(e.message, e.cause)
    }
  }

}
