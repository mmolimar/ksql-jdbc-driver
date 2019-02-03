package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.{ResultSetMetaData, Types}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.implicits.toIndexedMap
import com.github.mmolimar.ksql.jdbc.{HeaderField, InvalidColumn, NotSupported, WrapperNotSupported}
import io.confluent.ksql.rest.entity.SchemaInfo.{Type => KsqlType}

private[resultset] class ResultSetMetadataNotSupported extends ResultSetMetaData with WrapperNotSupported {

  override def getCatalogName(column: Int): String = throw NotSupported("getCatalogName")

  override def getColumnClassName(column: Int): String = throw NotSupported("getColumnClassName")

  override def getColumnCount: Int = throw NotSupported("getColumnCount")

  override def getColumnDisplaySize(column: Int): Int = throw NotSupported("getColumnDisplaySize")

  override def getColumnLabel(column: Int): String = throw NotSupported("getColumnLabel")

  override def getColumnName(column: Int): String = throw NotSupported("getColumnName")

  override def getColumnTypeName(column: Int): String = throw NotSupported("getColumnTypeName")

  override def getColumnType(column: Int): Int = throw NotSupported("getColumnType")

  override def getPrecision(column: Int): Int = throw NotSupported("getPrecision")

  override def getSchemaName(column: Int): String = throw NotSupported("getSchemaName")

  override def getScale(column: Int): Int = throw NotSupported("getScale")

  override def getTableName(column: Int): String = throw NotSupported("getTableName")

  override def isAutoIncrement(column: Int): Boolean = throw NotSupported("isAutoIncrement")

  override def isCaseSensitive(column: Int): Boolean = throw NotSupported("isCaseSensitive")

  override def isCurrency(column: Int): Boolean = throw NotSupported("isCurrency")

  override def isDefinitelyWritable(column: Int): Boolean = throw NotSupported("isDefinitelyWritable")

  override def isNullable(column: Int): Int = throw NotSupported("isNullable")

  override def isReadOnly(column: Int): Boolean = throw NotSupported("isReadOnly")

  override def isSearchable(column: Int): Boolean = throw NotSupported("isSearchable")

  override def isSigned(column: Int): Boolean = throw NotSupported("isSigned")

  override def isWritable(column: Int): Boolean = throw NotSupported("isWritable")

}

class KsqlResultSetMetadata(private[jdbc] val columns: List[HeaderField]) extends ResultSetMetadataNotSupported {

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

  override def getColumnDisplaySize(column: Int): Int = getField(column).jdbcType match {
    case Types.INTEGER | Types.BIGINT | Types.DOUBLE => 16
    case Types.BOOLEAN => 5
    case _ => 64
  }

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
