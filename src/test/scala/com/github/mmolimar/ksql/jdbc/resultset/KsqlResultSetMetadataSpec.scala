package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.{SQLException, SQLFeatureNotSupportedException}
import java.util
import java.util.Collections

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.query.QueryId
import io.confluent.ksql.rest.entity.SchemaInfo.Type
import io.confluent.ksql.rest.entity.{EntityQueryId, FieldInfo, QueryDescription, SchemaInfo}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class KsqlResultSetMetadataSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  val implementedMethods = Seq("getSchemaName", "getCatalogName", "getColumnLabel", "getColumnName",
    "getColumnTypeName", "getColumnClassName", "isCaseSensitive", "getTableName", "getColumnType",
    "getColumnCount", "getPrecision", "getScale")

  "A KsqlResultSetMetadata" when {

    "validating specs" should {

      val fields = util.Arrays.asList(
        new FieldInfo("field1", new SchemaInfo(Type.INTEGER, None.orNull, None.orNull)),
        new FieldInfo("field2", new SchemaInfo(Type.BIGINT, None.orNull, None.orNull)),
        new FieldInfo("field3", new SchemaInfo(Type.DOUBLE, None.orNull, None.orNull)),
        new FieldInfo("field4", new SchemaInfo(Type.BOOLEAN, None.orNull, None.orNull)),
        new FieldInfo("field5", new SchemaInfo(Type.STRING, None.orNull, None.orNull)),
        new FieldInfo("field6", new SchemaInfo(Type.MAP, None.orNull, None.orNull)),
        new FieldInfo("field7", new SchemaInfo(Type.ARRAY, None.orNull, None.orNull)),
        new FieldInfo("field8", new SchemaInfo(Type.STRUCT, None.orNull, None.orNull))
      )
      val queryDesc = new QueryDescription(
        new EntityQueryId(new QueryId("testquery")),
        "select * from text;",
        fields,
        Collections.singleton("TEST"),
        Collections.emptySet[String],
        "test",
        "executionPlan",
        Collections.emptyMap[String, AnyRef]
      )
      val resultSet = new KsqlResultSetMetadata(queryDesc)

      "throw not supported exception if not supported" in {

        reflectMethods[KsqlResultSetMetadata](implementedMethods, false, resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work if implemented" in {

        resultSet.getCatalogName(1) should be("TEST")
        resultSet.getSchemaName(2) should be("BIGINT")
        resultSet.getColumnLabel(3) should be("field3")
        resultSet.getColumnName(3) should be("field3")
        resultSet.getColumnTypeName(3) should be("DOUBLE")

        resultSet.getColumnClassName(1) should be("java.lang.Integer")
        resultSet.getColumnType(1) should be(java.sql.Types.INTEGER)
        resultSet.getColumnClassName(2) should be("java.lang.Long")
        resultSet.getColumnType(2) should be(java.sql.Types.BIGINT)
        resultSet.getColumnClassName(3) should be("java.lang.Double")
        resultSet.getColumnType(3) should be(java.sql.Types.DOUBLE)
        resultSet.getColumnClassName(4) should be("java.lang.Boolean")
        resultSet.getColumnType(4) should be(java.sql.Types.BOOLEAN)
        resultSet.getColumnClassName(5) should be("java.lang.String")
        resultSet.getColumnType(5) should be(java.sql.Types.VARCHAR)
        resultSet.getColumnClassName(6) should be("java.util.Map")
        resultSet.getColumnType(6) should be(java.sql.Types.JAVA_OBJECT)
        resultSet.getColumnClassName(7) should be("java.sql.Array")
        resultSet.getColumnType(7) should be(java.sql.Types.ARRAY)
        resultSet.getColumnClassName(8) should be("java.sql.Struct")
        resultSet.getColumnType(8) should be(java.sql.Types.STRUCT)

        resultSet.isCaseSensitive(2) should be(false)
        resultSet.isCaseSensitive(5) should be(true)
        resultSet.getTableName(3) should be("test")
        resultSet.getColumnType(3) should be(java.sql.Types.DOUBLE)
        resultSet.getColumnCount should be(8)
        resultSet.getPrecision(3) should be(-1)
        resultSet.getPrecision(2) should be(0)
        resultSet.getScale(3) should be(-1)
        resultSet.getScale(4) should be(0)

        assertThrows[SQLException] {
          resultSet.getSchemaName(0)
        }
        assertThrows[SQLException] {
          resultSet.getSchemaName(9)
        }
      }
    }
  }
}
