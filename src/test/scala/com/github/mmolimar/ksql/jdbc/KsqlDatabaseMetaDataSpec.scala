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
    val implementedMethods = Seq("allProceduresAreCallable", "allTablesAreSelectable",
      "getMaxConnections", "getIdentifierQuoteString", "getIdentifierQuoteString", "getSearchStringEscape",
      "getExtraNameCharacters", "getDatabaseProductName", "getDatabaseMajorVersion", "getDatabaseMinorVersion",
      "getDatabaseProductVersion", "getDriverName", "getDriverVersion", "getDriverMajorVersion",
      "getDriverMinorVersion", "getJDBCMajorVersion", "getJDBCMinorVersion", "getConnection", "getCatalogs",
      "getCatalogTerm", "getMaxStatements", "getMaxStatementLength", "getTableTypes", "getTables", "getTypeInfo",
      "getSchemas", "getSuperTables", "getUDTs", "getColumns", "getURL", "isReadOnly", "getSQLKeywords", "getProcedures",
      "getNumericFunctions", "getSchemaTerm", "getStringFunctions", "getSystemFunctions", "getTimeDateFunctions",
      "getProcedureTerm",
      "nullsAreSortedHigh", "nullsAreSortedLow", "nullsAreSortedAtStart", "nullsAreSortedAtEnd", "nullPlusNonNullIsNull",
      "usesLocalFiles", "usesLocalFilePerTable",
      "supportsAlterTableWithAddColumn", "supportsAlterTableWithDropColumn",
      "supportsCatalogsInDataManipulation", "supportsCatalogsInTableDefinitions", "supportsCatalogsInProcedureCalls",
      "supportsMultipleResultSets", "supportsMultipleTransactions", "supportsSavepoints",
      "supportsSchemasInDataManipulation", "supportsSchemasInTableDefinitions",
      "supportsStoredFunctionsUsingCallSyntax", "supportsStoredProcedures", "storesUpperCaseIdentifiers",
      "storesLowerCaseIdentifiers", "storesMixedCaseIdentifiers", "storesUpperCaseQuotedIdentifiers",
      "storesLowerCaseQuotedIdentifiers", "storesMixedCaseQuotedIdentifiers", "supportsMixedCaseQuotedIdentifiers",
      "supportsColumnAliasing", "supportsMixedCaseIdentifiers", "supportsConvert", "supportsTableCorrelationNames",
      "supportsDifferentTableCorrelationNames", "supportsExpressionsInOrderBy", "supportsExtendedSQLGrammar",
      "supportsGroupBy", "supportsOrderByUnrelated", "supportsGroupByUnrelated", "supportsGroupByBeyondSelect",
      "supportsLikeEscapeClause", "supportsNonNullableColumns", "supportsMinimumSQLGrammar", "supportsCoreSQLGrammar",
      "supportsExtendedSQLGrammar", "supportsOuterJoins", "supportsFullOuterJoins",
      "supportsLimitedOuterJoins", "supportsUnion", "supportsUnionAll", "supportsTransactions"
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
      }

      "check JDBC specs" in {

        metadata.getURL should be("jdbc:ksql://localhost:8080")
        metadata.getTypeInfo.getMetaData.getColumnCount should be(18)
        metadata.getSQLKeywords.split(",").length should be(17)
        metadata.getMaxStatements should be(0)
        metadata.getMaxStatementLength should be(0)
        metadata.getProcedures(None.orNull, None.orNull, None.orNull).next should be(false)
        metadata.getIdentifierQuoteString should be(" ")
        metadata.getSearchStringEscape should be("%")
        metadata.getExtraNameCharacters should be("#@")
        metadata.getMaxConnections should be(0)
        metadata.getIdentifierQuoteString should be(" ")

        metadata.allProceduresAreCallable should be(false)
        metadata.allTablesAreSelectable should be(false)

        metadata.nullsAreSortedHigh should be(false)
        metadata.nullsAreSortedLow should be(false)
        metadata.nullsAreSortedAtStart should be(false)
        metadata.nullsAreSortedAtEnd should be(false)

        metadata.usesLocalFiles should be(true)
        metadata.usesLocalFilePerTable should be(true)

        metadata.nullPlusNonNullIsNull should be(true)

        metadata.storesUpperCaseIdentifiers should be(false)
        metadata.storesLowerCaseIdentifiers should be(false)
        metadata.storesMixedCaseIdentifiers should be(true)
        metadata.storesUpperCaseQuotedIdentifiers should be(false)
        metadata.storesLowerCaseQuotedIdentifiers should be(false)
        metadata.storesMixedCaseQuotedIdentifiers should be(true)

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
        metadata.supportsMixedCaseIdentifiers should be(true)
        metadata.supportsMixedCaseQuotedIdentifiers should be(true)
        metadata.supportsColumnAliasing should be(true)
        metadata.supportsConvert should be(false)
        metadata.supportsConvert(12, 15) should be(false)
        metadata.supportsTableCorrelationNames should be(true)
        metadata.supportsDifferentTableCorrelationNames should be(true)
        metadata.supportsExpressionsInOrderBy should be(true)
        metadata.supportsExtendedSQLGrammar should be(false)
        metadata.supportsGroupBy should be(true)
        metadata.supportsOrderByUnrelated should be(false)
        metadata.supportsGroupByUnrelated should be(true)
        metadata.supportsGroupByBeyondSelect should be(true)
        metadata.supportsLikeEscapeClause should be(true)
        metadata.supportsNonNullableColumns should be(true)
        metadata.supportsMinimumSQLGrammar should be(true)
        metadata.supportsCoreSQLGrammar should be(false)
        metadata.supportsExtendedSQLGrammar should be(false)
        metadata.supportsOuterJoins should be(false)
        metadata.supportsFullOuterJoins should be(false)
        metadata.supportsLimitedOuterJoins should be(false)
        metadata.supportsTransactions should be(false)
        metadata.supportsUnion should be(false)
        metadata.supportsUnionAll should be(false)
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
