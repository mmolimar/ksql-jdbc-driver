package com.github.mmolimar.ksql.jdbc

import java.sql.Types

import scala.collection.immutable


case class HeaderField(name: String, jdbcType: Int, length: Int, index: Int) {
  def this(name: String, jdbcType: Int, length: Int) = this(name, jdbcType, length, -1)
}

object HeaderField {
  def apply(name: String, jdbcType: Int, length: Int) = new HeaderField(name, jdbcType, length, -1)
}

object Headers {
  val tableTypes: Map[String, HeaderField] = Seq(new HeaderField("TABLE_TYPE", Types.VARCHAR, 0))

  val catalogs: Map[String, HeaderField] = Seq(HeaderField("TABLE_CAT", Types.VARCHAR, 0))

  val schemas: Map[String, HeaderField] = Seq(
    HeaderField("TABLE_SCHEM", Types.VARCHAR, 0),
    HeaderField("TABLE_CATALOG", Types.VARCHAR, 0)
  )

  val superTables: Map[String, HeaderField] = Seq(
    HeaderField("TABLE_CAT", Types.VARCHAR, 0),
    HeaderField("TABLE_SCHEM", Types.VARCHAR, 0),
    HeaderField("TABLE_NAME", Types.VARCHAR, 255),
    HeaderField("SUPERTABLE_NAME", Types.VARCHAR, 0)
  )

  val tables: Map[String, HeaderField] = Seq(
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

  val columns: Map[String, HeaderField] = Seq(
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
    case "BOOL" | "BOOLEAN" => Types.INTEGER
    case "INT" | "INTEGER" => Types.INTEGER
    case "LONG" | "BIGINT" => Types.BIGINT
    case "DOUBLE" => Types.DOUBLE
    case "STRING" | "VARCHAR" => Types.VARCHAR
    case dt if dt.startsWith("ARRAY") => Types.ARRAY
    case dt if dt.startsWith("MAP") => Types.STRUCT
    case _ => Types.OTHER
  }

  private implicit def toMap(headers: Seq[HeaderField]): Map[String, HeaderField] = {
    headers.zipWithIndex.map { case (header, index) => {
      (header.name, HeaderField(header.name, header.jdbcType, header.length, index + 1))
    }
    }.toCaseInsensitiveMap
  }

  private implicit class ToTreeMap[+A](tuples: TraversableOnce[A]) {

    def toCaseInsensitiveMap[U](implicit ev: A <:< (String, U)): immutable.Map[String, U] = {
      val b = immutable.TreeMap.newBuilder[String, U](new Ordering[String] {
        def compare(x: String, y: String): Int = String.CASE_INSENSITIVE_ORDER.compare(x.toString, y.toString)
      })
      for (x <- tuples)
        b += x

      b.result()
    }
  }

}
