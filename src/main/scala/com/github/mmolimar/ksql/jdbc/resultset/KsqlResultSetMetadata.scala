package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.{ResultSetMetaData, Types}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.{InvalidColumn, NotSupported, WrapperNotSupported}
import io.confluent.ksql.rest.entity.SchemaInfo.Type._
import io.confluent.ksql.rest.entity.{FieldInfo, QueryDescription}

import scala.collection.JavaConverters._

class KsqlResultSetMetadata(queryDesc: QueryDescription) extends ResultSetMetaData with WrapperNotSupported {

  private lazy val fields: Map[Int, FieldInfo] = queryDesc.getFields.asScala.zipWithIndex
    .map { case (field, index) => (index + 1) -> field }.toMap

  private def getField(index: Int): FieldInfo = fields.get(index)
    .getOrElse(throw InvalidColumn(s"Column with index $index does not exist"))

  override def getSchemaName(column: Int): String = getField(column).getSchema.getTypeName

  override def getCatalogName(column: Int): String = queryDesc.getSources.asScala.mkString(", ")

  override def getColumnLabel(column: Int): String = getField(column).getName

  override def getColumnName(column: Int): String = getField(column).getName

  override def getColumnTypeName(column: Int): String = getField(column).getSchema.getTypeName

  override def getColumnClassName(column: Int): String = {
    getField(column).getSchema.getType match {
      case INTEGER => classOf[java.lang.Integer]
      case BIGINT => classOf[java.lang.Long]
      case DOUBLE => classOf[java.lang.Double]
      case BOOLEAN => classOf[java.lang.Boolean]
      case STRING => classOf[java.lang.String]
      case MAP => classOf[java.util.Map[AnyRef, AnyRef]]
      case ARRAY => classOf[java.sql.Array]
      case STRUCT => classOf[java.sql.Struct]
    }
  }.getCanonicalName

  override def isCaseSensitive(column: Int): Boolean = getField(column).getSchema.getType match {
    case STRING => true
    case _ => false
  }

  override def getTableName(column: Int): String = queryDesc.getTopology

  override def getColumnType(column: Int): Int = getField(column).getSchema.getType match {
    case INTEGER => Types.INTEGER
    case BIGINT => Types.BIGINT
    case DOUBLE => Types.DOUBLE
    case BOOLEAN => Types.BOOLEAN
    case STRING => Types.VARCHAR
    case MAP => Types.JAVA_OBJECT
    case ARRAY => Types.ARRAY
    case STRUCT => Types.STRUCT
  }

  override def getColumnCount: Int = fields.size

  override def getPrecision(column: Int): Int = getField(column).getSchema.getType match {
    case DOUBLE => -1
    case _ => 0
  }

  override def getScale(column: Int): Int = getField(column).getSchema.getType match {
    case DOUBLE => -1
    case _ => 0
  }

  override def isSigned(column: Int): Boolean = throw NotSupported("isSigned")

  override def isWritable(column: Int): Boolean = throw NotSupported("isWritable")

  override def isAutoIncrement(column: Int): Boolean = throw NotSupported("isAutoIncrement")

  override def isReadOnly(column: Int): Boolean = throw NotSupported("isReadOnly")

  override def isCurrency(column: Int): Boolean = throw NotSupported("isCurrency")

  override def isSearchable(column: Int): Boolean = throw NotSupported("isSearchable")

  override def isDefinitelyWritable(column: Int): Boolean = throw NotSupported("isDefinitelyWritable")

  override def isNullable(column: Int): Int = throw NotSupported("isNullable")

  override def getColumnDisplaySize(column: Int): Int = throw NotSupported("getColumnDisplaySize")

}
