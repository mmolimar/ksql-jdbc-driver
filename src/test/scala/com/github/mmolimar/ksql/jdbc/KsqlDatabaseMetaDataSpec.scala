package com.github.mmolimar.ksql.jdbc

import java.io.InputStream
import java.sql.{SQLException, SQLFeatureNotSupportedException}
import java.util.{Collections, Properties}

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity._
import javax.ws.rs.core.Response
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._


class KsqlDatabaseMetaDataSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  "A KsqlDatabaseMetaData" when {
    val implementedMethods = Seq("getDatabaseProductName", "getDatabaseMajorVersion", "getDatabaseMinorVersion",
      "getDatabaseProductVersion", "getDriverName", "getDriverVersion", "getDriverMajorVersion",
      "getDriverMinorVersion", "getJDBCMajorVersion", "getJDBCMinorVersion", "getConnection", "getCatalogs",
      "getCatalogTerm", "getMaxStatements", "getMaxStatementLength", "getTableTypes", "getTables", "getTypeInfo",
      "getSchemas", "getSuperTables", "getUDTs", "getColumns", "getURL", "isReadOnly", "getSQLKeywords", "getProcedures",
      "getNumericFunctions", "getSchemaTerm", "getStringFunctions", "getSystemFunctions", "getTimeDateFunctions",
      "getProcedureTerm", "supportsAlterTableWithAddColumn", "supportsAlterTableWithDropColumn",
      "supportsCatalogsInDataManipulation", "supportsCatalogsInTableDefinitions", "supportsCatalogsInProcedureCalls",
      "supportsMultipleResultSets", "supportsMultipleTransactions", "supportsSavepoints",
      "supportsSchemasInDataManipulation", "supportsSchemasInTableDefinitions",
      "supportsStoredFunctionsUsingCallSyntax", "supportsStoredProcedures"
    )

    val mockResponse = mock[Response]
    val mockedKsqlRestClient = mock[MockableKsqlRestClient]

    val values = KsqlConnectionValues("localhost", 8080, Map.empty[String, String])
    val ksqlConnection = new KsqlConnection(values, new Properties) {
      override def init: KsqlRestClient = mockedKsqlRestClient
    }
    val metadata = new KsqlDatabaseMetaData(ksqlConnection)

    "validating specs" should {

      "throw not supported exception if not supported" in {
        (mockResponse.getEntity _).expects.returns(mock[InputStream]).once
        (mockedKsqlRestClient.makeQueryRequest _).expects(*)
          .returns(RestResponse.successful[KsqlRestClient.QueryStream](mockQueryStream(mockResponse)))
          .anyNumberOfTimes

        reflectMethods[KsqlDatabaseMetaData](implementedMethods, false, metadata)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work if implemented" in {
        val specialMethods = Set("getTables", "getColumns", "getNumericFunctions", "getStringFunctions",
          "getSystemFunctions", "getTimeDateFunctions")
        val methods = implementedMethods
          .filterNot(specialMethods.contains(_))

        reflectMethods[KsqlDatabaseMetaData](methods, true, metadata)
          .foreach(method => {
            method()
          })

        assertThrows[SQLException] {
          metadata.getTables("", "", "", Array[String]("test"))
        }

        (mockedKsqlRestClient.makeKsqlRequest _).expects(*)
          .returns(RestResponse.erroneous(new KsqlErrorMessage(-1, "error message", Collections.emptyList[String])))
          .once
        assertThrows[SQLException] {
          metadata.getTables("", "", "", Array[String](TableTypes.TABLE.name))
        }

        (mockedKsqlRestClient.makeKsqlRequest _).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](new KsqlEntityList))
          .twice
        metadata.getTables("", "", "[a-z]*",
          Array[String](TableTypes.TABLE.name, TableTypes.STREAM.name)).next should be(false)

        (mockedKsqlRestClient.makeKsqlRequest _).expects(*)
          .returns(RestResponse.successful[KsqlEntityList](new KsqlEntityList))
          .twice
        metadata.getColumns("", "", "", "").next should be(false)

        assertThrows[SQLException] {
          metadata.getColumns("test", "", "test", "test")
        }
        assertThrows[SQLException] {
          metadata.getColumns("", "test", "test", "test")
        }

        val fnList = new FunctionNameList(
          "LIST FUNCTIONS;",
          List(
            new SimpleFunctionInfo("TESTFN", FunctionType.scalar),
            new SimpleFunctionInfo("TESTDATEFN", FunctionType.scalar)
          ).asJava
        )
        val entityListFn = new KsqlEntityList
        entityListFn.add(fnList)

        val descFn1 = new FunctionDescriptionList("DESCRIBE FUNCTION testfn;",
          "TESTFN", "Description", "Confluent", "version", "path",
          List(
            new FunctionInfo(List(new ArgumentInfo("arg1", "INT", "Description")).asJava, "BIGINT", "Description"),
            new FunctionInfo(List(new ArgumentInfo("arg1", "INT", "Description")).asJava, "STRING", "Description")
          ).asJava,
          FunctionType.scalar
        )
        val descFn2 = new FunctionDescriptionList("DESCRIBE FUNCTION testdatefn;",
          "TESTDATEFN", "Description", "Unknown", "version", "path",
          List(
            new FunctionInfo(List(new ArgumentInfo("arg1", "INT", "Description")).asJava, "BIGINT", "Description")
          ).asJava,
          FunctionType.scalar
        )
        val entityDescribeFn1 = new KsqlEntityList
        entityDescribeFn1.add(descFn1)
        val entityDescribeFn2 = new KsqlEntityList
        entityDescribeFn2.add(descFn2)

        (mockedKsqlRestClient.makeKsqlRequest _).expects("LIST FUNCTIONS;")
          .returns(RestResponse.successful[KsqlEntityList](entityListFn))
          .repeat(4)
        (mockedKsqlRestClient.makeKsqlRequest _).expects("DESCRIBE FUNCTION TESTFN;")
          .returns(RestResponse.successful[KsqlEntityList](entityDescribeFn1))
          .repeat(4)
        (mockedKsqlRestClient.makeKsqlRequest _).expects("DESCRIBE FUNCTION TESTDATEFN;")
          .returns(RestResponse.successful[KsqlEntityList](entityDescribeFn2))
          .repeat(4)

        metadata.getNumericFunctions should be("TESTDATEFN,TESTFN")
        metadata.getStringFunctions should be("TESTFN")
        metadata.getSystemFunctions should be("TESTFN")
        metadata.getTimeDateFunctions should be("TESTDATEFN")

        Option(metadata.getConnection) should not be (None)
        metadata.getCatalogs.next should be(false)
        metadata.getCatalogTerm should be("TOPIC")
        metadata.getSchemaTerm should be("")
        metadata.getProcedureTerm should be("")

        val tableTypes = metadata.getTableTypes
        tableTypes.next should be(true)
        tableTypes.getString(1) should be(TableTypes.TABLE.name)
        tableTypes.getString("TABLE_TYPE") should be(TableTypes.TABLE.name)
        tableTypes.getString("table_type") should be(TableTypes.TABLE.name)
        tableTypes.next should be(true)
        tableTypes.getString(1) should be(TableTypes.STREAM.name)
        tableTypes.getString("TABLE_TYPE") should be(TableTypes.STREAM.name)
        tableTypes.getString("table_type") should be(TableTypes.STREAM.name)
        tableTypes.next should be(false)

        metadata.getSchemas.next should be(false)
        metadata.getSchemas("", "").next should be(false)
        assertThrows[SQLException] {
          metadata.getSchemas("test", "")
        }
        assertThrows[SQLException] {
          metadata.getSchemas("", "test")
        }

        metadata.getSuperTables("", "", "test").next should be(false)
        assertThrows[SQLException] {
          metadata.getSuperTables("test", "", "test")
        }
        assertThrows[SQLException] {
          metadata.getSuperTables("", "test", "test")
        }

        metadata.getURL should be("jdbc:ksql://localhost:8080")
        metadata.getTypeInfo.getMetaData.getColumnCount should be(18)
        metadata.getSQLKeywords.split(",").length should be(17)
        metadata.getMaxStatements should be(0)
        metadata.getMaxStatementLength should be(0)
        metadata.getProcedures(None.orNull, None.orNull, None.orNull).next should be(false)
        metadata.supportsAlterTableWithAddColumn should be(false)
        metadata.supportsAlterTableWithDropColumn should be(false)
        metadata.supportsCatalogsInDataManipulation should be(false)
        metadata.supportsCatalogsInTableDefinitions should be(false)
        metadata.supportsCatalogsInProcedureCalls should be(false)
        metadata.supportsMultipleResultSets should be(false)
        metadata.supportsMultipleTransactions should be(false)
        metadata.supportsSchemasInDataManipulation should be(false)
        metadata.supportsSchemasInTableDefinitions should be(false)
        metadata.supportsStoredFunctionsUsingCallSyntax should be(true)
        metadata.supportsStoredProcedures should be(false)
        metadata.supportsSavepoints should be(false)

      }
    }
  }

  "A DatabaseMetaDataNotSupported" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val metadata = new DatabaseMetaDataNotSupported
        reflectMethods[DatabaseMetaDataNotSupported](Seq.empty, false, metadata)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }
    }
  }

}
