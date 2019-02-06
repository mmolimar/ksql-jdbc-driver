package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.ResultSetMetaData

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.{NotSupported, WrapperNotSupported}


private[resultset] class ResultSetMetaDataNotSupported extends ResultSetMetaData with WrapperNotSupported {

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
