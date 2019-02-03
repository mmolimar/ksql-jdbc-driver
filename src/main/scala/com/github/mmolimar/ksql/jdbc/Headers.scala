package com.github.mmolimar.ksql.jdbc

import java.sql.Types


case class HeaderField(name: String, label: String, jdbcType: Int, length: Int, index: Int)

object HeaderField {

  def apply(name: String, jdbcType: Int, length: Int): HeaderField = {
    HeaderField(name, name.toUpperCase, jdbcType, length, -1)
  }

  def apply(name: String, label: String, jdbcType: Int, length: Int): HeaderField = {
    HeaderField(name, label, jdbcType, length, -1)
  }
}

private object implicits {

  implicit def toIndexedMap(headers: List[HeaderField]): Map[Int, HeaderField] = {
    headers.zipWithIndex.map { case (header, index) => {
      HeaderField(header.name, header.label, header.jdbcType, header.length, index + 1)
    }
    }.map(h => h.index -> h).toMap
  }
}

object DatabaseMetadataHeaders {

  val tableTypes = List(HeaderField("TABLE_TYPE", Types.VARCHAR, 0))

  val catalogs = List(HeaderField("TABLE_CAT", Types.VARCHAR, 0))

  val schemas = List(
    HeaderField("TABLE_SCHEM", Types.VARCHAR, 0),
    HeaderField("TABLE_CATALOG", Types.VARCHAR, 0)
  )

  val superTables = List(
    HeaderField("TABLE_CAT", Types.VARCHAR, 0),
    HeaderField("TABLE_SCHEM", Types.VARCHAR, 0),
    HeaderField("TABLE_NAME", Types.VARCHAR, 255),
    HeaderField("SUPERTABLE_NAME", Types.VARCHAR, 0)
  )

  val tables = List(
    HeaderField("TABLE_CAT", java.sql.Types.VARCHAR, 0),
    HeaderField("TABLE_SCHEM", java.sql.Types.VARCHAR, 0),
    HeaderField("TABLE_NAME", java.sql.Types.VARCHAR, 255),
    HeaderField("TABLE_TYPE", java.sql.Types.VARCHAR, 8),
    HeaderField("REMARKS", java.sql.Types.VARCHAR, 0),
    HeaderField("TYPE_CAT", java.sql.Types.VARCHAR, 0),
    HeaderField("TYPE_SCHEM", java.sql.Types.VARCHAR, 0),
    HeaderField("TYPE_NAME", java.sql.Types.VARCHAR, 0),
    HeaderField("SELF_REFERENCING_COL_NAME", java.sql.Types.VARCHAR, 0),
    HeaderField("REF_GENERATION", java.sql.Types.VARCHAR, 0)
  )

  val columns = List(
    HeaderField("TABLE_CAT", Types.VARCHAR, 0),
    HeaderField("TABLE_SCHEM", Types.VARCHAR, 0),
    HeaderField("TABLE_NAME", Types.VARCHAR, 255),
    HeaderField("COLUMN_NAME", Types.VARCHAR, 255),
    HeaderField("DATA_TYPE", Types.INTEGER, 5),
    HeaderField("TYPE_NAME", Types.VARCHAR, 16),
    HeaderField("COLUMN_SIZE", Types.INTEGER, Integer.toString(Integer.MAX_VALUE).length),
    HeaderField("BUFFER_LENGTH", Types.INTEGER, 10),
    HeaderField("DECIMAL_DIGITS", Types.INTEGER, 10),
    HeaderField("NUM_PREC_RADIX", Types.INTEGER, 10),
    HeaderField("NULLABLE", Types.INTEGER, 10),
    HeaderField("REMARKS", Types.VARCHAR, 0),
    HeaderField("COLUMN_DEF", Types.VARCHAR, 0),
    HeaderField("SQL_DATA_TYPE", Types.INTEGER, 10),
    HeaderField("SQL_DATETIME_SUB", Types.INTEGER, 10),
    HeaderField("CHAR_OCTET_LENGTH", Types.INTEGER, Integer.toString(Integer.MAX_VALUE).length),
    HeaderField("ORDINAL_POSITION", Types.INTEGER, 10),
    HeaderField("IS_NULLABLE", Types.VARCHAR, 3),
    HeaderField("SCOPE_CATALOG", Types.VARCHAR, 0),
    HeaderField("SCOPE_SCHEMA", Types.VARCHAR, 0),
    HeaderField("SCOPE_TABLE", Types.VARCHAR, 0),
    HeaderField("SOURCE_DATA_TYPE", Types.SMALLINT, 0),
    HeaderField("IS_AUTOINCREMENT", Types.VARCHAR, 3),
    HeaderField("IS_GENERATEDCOLUMN", Types.VARCHAR, 3)
  )

  def mapDataType(dataType: String): Int = dataType match {
    case "BOOL" | "BOOLEAN" => Types.BOOLEAN
    case "INT" | "INTEGER" => Types.INTEGER
    case "LONG" | "BIGINT" => Types.BIGINT
    case "DOUBLE" => Types.DOUBLE
    case "STRING" | "VARCHAR" => Types.VARCHAR
    case dt if dt.startsWith("ARRAY") => Types.ARRAY
    case dt if dt.startsWith("MAP") => Types.STRUCT
    case _ => Types.OTHER
  }

}

object KsqlEntityHeaders {

  val commandStatusEntity = List(
    HeaderField("COMMAND_STATUS_ID_TYPE", Types.VARCHAR, 16),
    HeaderField("COMMAND_STATUS_ID_ENTITY", Types.VARCHAR, 32),
    HeaderField("COMMAND_STATUS_ID_ACTION", Types.VARCHAR, 16),
    HeaderField("COMMAND_STATUS_STATUS", Types.VARCHAR, 16),
    HeaderField("COMMAND_STATUS_MESSAGE", Types.VARCHAR, 128)
  )

  val executionPlanEntity = List(
    HeaderField("EXECUTION_PLAN", Types.VARCHAR, 256)
  )

  val functionDescriptionListEntity = List(
    HeaderField("FUNCTION_DESCRIPTION_AUTHOR", Types.VARCHAR, 32),
    HeaderField("FUNCTION_DESCRIPTION_DESCRIPTION", Types.VARCHAR, 64),
    HeaderField("FUNCTION_DESCRIPTION_NAME", Types.VARCHAR, 16),
    HeaderField("FUNCTION_DESCRIPTION_PATH", Types.VARCHAR, 64),
    HeaderField("FUNCTION_DESCRIPTION_VERSION", Types.VARCHAR, 8),
    HeaderField("FUNCTION_DESCRIPTION_TYPE", Types.VARCHAR, 16),
    HeaderField("FUNCTION_DESCRIPTION_FN_DESC", Types.VARCHAR, 128),
    HeaderField("FUNCTION_DESCRIPTION_FN_RETURN_TYPE", Types.VARCHAR, 16),
    HeaderField("FUNCTION_DESCRIPTION_FN_ARGS", Types.VARCHAR, 128)
  )

  val functionNameListEntity = List(
    HeaderField("FUNCTION_NAME_FN_NAME", Types.VARCHAR, 16),
    HeaderField("FUNCTION_NAME_FN_TYPE", Types.VARCHAR, 16)
  )

  val kafkaTopicsListEntity = List(
    HeaderField("KAFKA_TOPIC_NAME", Types.VARCHAR, 16),
    HeaderField("KAFKA_TOPIC_CONSUMER_COUNT", Types.INTEGER, 16),
    HeaderField("KAFKA_TOPIC_CONSUMER_GROUP_COUNT", Types.INTEGER, 16),
    HeaderField("KAFKA_TOPIC_REGISTERED", Types.BOOLEAN, 5),
    HeaderField("KAFKA_TOPIC_REPLICA_INFO", Types.VARCHAR, 32)
  )

  val ksqlTopicsListEntity = List(
    HeaderField("KSQL_TOPIC_NAME", Types.VARCHAR, 16),
    HeaderField("KSQL_TOPIC_KAFKA_TOPIC", Types.VARCHAR, 16),
    HeaderField("KSQL_TOPIC_FORMAT", Types.VARCHAR, 16)
  )

  val propertiesListEntity = List(
    HeaderField("PROPERTY_NAME", Types.VARCHAR, 16),
    HeaderField("PROPERTY_VALUE", Types.VARCHAR, 32)
  )

  val queriesEntity = List(
    HeaderField("QUERY_ID", Types.VARCHAR, 16),
    HeaderField("QUERY_STRING", Types.VARCHAR, 64),
    HeaderField("QUERY_SINKS", Types.VARCHAR, 32)
  )

  val queryDescriptionEntity = List(
    HeaderField("QUERY_DESCRIPTION_ID", Types.VARCHAR, 16),
    HeaderField("QUERY_DESCRIPTION_FIELDS", Types.VARCHAR, 128),
    HeaderField("QUERY_DESCRIPTION_SOURCES", Types.VARCHAR, 32),
    HeaderField("QUERY_DESCRIPTION_SINKS", Types.VARCHAR, 32),
    HeaderField("QUERY_DESCRIPTION_TOPOLOGY", Types.VARCHAR, 256),
    HeaderField("QUERY_DESCRIPTION_EXECUTION_PLAN", Types.VARCHAR, 256)
  )

  val queryDescriptionEntityList = queryDescriptionEntity

  val sourceDescriptionEntity = List(
    HeaderField("SOURCE_DESCRIPTION_KEY", Types.VARCHAR, 16),
    HeaderField("SOURCE_DESCRIPTION_NAME", Types.VARCHAR, 16),
    HeaderField("SOURCE_DESCRIPTION_TOPIC", Types.VARCHAR, 16),
    HeaderField("SOURCE_DESCRIPTION_TYPE", Types.VARCHAR, 16),
    HeaderField("SOURCE_DESCRIPTION_FORMAT", Types.VARCHAR, 16),
    HeaderField("SOURCE_DESCRIPTION_FIELDS", Types.VARCHAR, 128),
    HeaderField("SOURCE_DESCRIPTION_PARTITIONS", Types.INTEGER, 16),
    HeaderField("SOURCE_DESCRIPTION_STATISTICS", Types.VARCHAR, 128),
    HeaderField("SOURCE_DESCRIPTION_ERROR_STATS", Types.VARCHAR, 128),
    HeaderField("SOURCE_DESCRIPTION_TIMESTAMP", Types.VARCHAR, 32)
  )

  val sourceDescriptionEntityList = sourceDescriptionEntity

  val streamsListEntity = List(
    HeaderField("STREAM_NAME", Types.VARCHAR, 16),
    HeaderField("STREAM_TOPIC", Types.VARCHAR, 16),
    HeaderField("STREAM_FORMAT", Types.VARCHAR, 16)
  )

  val tablesListEntity = List(
    HeaderField("TABLE_NAME", Types.VARCHAR, 16),
    HeaderField("TABLE_TOPIC", Types.VARCHAR, 16),
    HeaderField("TABLE_FORMAT", Types.VARCHAR, 16),
    HeaderField("TABLE_WINDOWS", Types.BOOLEAN, 5)
  )

  val topicDescriptionEntity = List(
    HeaderField("TOPIC_DESCRIPTION_NAME", Types.VARCHAR, 16),
    HeaderField("TOPIC_DESCRIPTION_KAFKA_TOPIC", Types.VARCHAR, 16),
    HeaderField("TOPIC_DESCRIPTION_FORMAT", Types.VARCHAR, 16),
    HeaderField("TOPIC_DESCRIPTION_SCHEMA_STRING", Types.BOOLEAN, 64)
  )

}
