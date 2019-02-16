package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.{ResultSetMetaData, Types}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.implicits.toIndexedMap
import com.github.mmolimar.ksql.jdbc.{HeaderField, InvalidColumn}
import io.confluent.ksql.rest.entity.SchemaInfo.{Type => KsqlType}


class KsqlResultSetMetaData(private[jdbc] val columns: List[HeaderField]) extends ResultSetMetaDataNotSupported {

  private val fieldByIndex: Map[Int, HeaderField] = columns

  private def getField(index: Int): HeaderField = fieldByIndex.get(index)
    .getOrElse(throw InvalidColumn(s"Column with index '$index' does not exist."))

  override def getColumnClassName(column: Int): String = {
    getField(column).jdbcType match {
      case Types.INTEGER => classOf[java.lang.Integer]
      case Types.BIGINT => classOf[java.lang.Long]
      case Types.DOUBLE => classOf[java.lang.Double]
      case Types.BOOLEAN => classOf[java.lang.Boolean]
      case Types.VARCHAR => classOf[java.lang.String]
      case Types.JAVA_OBJECT => classOf[java.util.Map[AnyRef, AnyRef]]
      case Types.ARRAY => classOf[java.sql.Array]
      case Types.STRUCT => classOf[java.sql.Struct]
      case _ => classOf[java.lang.String]
    }
  }.getName

  override def getColumnCount: Int = columns.size

  override def getColumnDisplaySize(column: Int): Int = getField(column).length

  override def getColumnLabel(column: Int): String = getField(column).label

  override def getColumnName(column: Int): String = getField(column).name

  override def getColumnTypeName(column: Int): String = {
    getField(column).jdbcType match {
      case Types.INTEGER => KsqlType.INTEGER
      case Types.BIGINT => KsqlType.BIGINT
      case Types.DOUBLE => KsqlType.DOUBLE
      case Types.BOOLEAN => KsqlType.BOOLEAN
      case Types.VARCHAR => KsqlType.STRING
      case Types.JAVA_OBJECT => KsqlType.MAP
      case Types.ARRAY => KsqlType.ARRAY
      case Types.STRUCT => KsqlType.STRUCT
      case _ => KsqlType.STRING
    }
  }.name

  override def getColumnType(column: Int): Int = getField(column).jdbcType

  override def getPrecision(column: Int): Int = getField(column).jdbcType match {
    case Types.DOUBLE => -1
    case _ => 0
  }

  override def getScale(column: Int): Int = getField(column).jdbcType match {
    case Types.DOUBLE => -1
    case _ => 0
  }

  override def isCaseSensitive(column: Int): Boolean = getField(column).jdbcType match {
    case Types.VARCHAR => true
    case _ => false
  }

  override def isNullable(column: Int): Int = ResultSetMetaData.columnNullableUnknown

}
