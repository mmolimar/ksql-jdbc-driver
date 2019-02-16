package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, DatabaseMetaData, ResultSet, RowIdLifetime, Types}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.implicits.ResultSetStream
import com.github.mmolimar.ksql.jdbc.resultset.IteratorResultSet
import io.confluent.ksql.rest.entity.{SourceDescriptionEntity, StreamsList, TablesList}

import scala.collection.JavaConverters._
import scala.collection.mutable


object TableTypes {

  sealed trait TableType {
    def name: String
  }

  case object TABLE extends TableType {
    val name: String = "TABLE"
  }

  case object STREAM extends TableType {
    val name: String = "STREAM"
  }

  val tableTypes = Seq(TABLE, STREAM)

}

class DatabaseMetaDataNotSupported extends DatabaseMetaData with WrapperNotSupported {

  override def supportsMultipleOpenResults: Boolean = throw NotSupported("supportsMultipleOpenResults")

  override def supportsSubqueriesInIns: Boolean = throw NotSupported("supportsSubqueriesInIns")

  override def getSuperTypes(catalog: String, schemaPattern: String,
                             typeNamePattern: String): ResultSet = throw NotSupported("getSuperTypes")

  override def getTablePrivileges(catalog: String, schemaPattern: String,
                                  tableNamePattern: String): ResultSet = throw NotSupported("getTablePrivileges")

  override def supportsFullOuterJoins: Boolean = throw NotSupported("supportsFullOuterJoins")

  override def insertsAreDetected(`type`: Int): Boolean = throw NotSupported("insertsAreDetected")

  override def getDriverMajorVersion: Int = throw NotSupported("getDriverMajorVersion")

  override def getDatabaseProductVersion: String = throw NotSupported("getDatabaseProductVersion")

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean,
                            approximate: Boolean): ResultSet = throw NotSupported("getIndexInfo")

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String,
                                  columnNamePattern: String): ResultSet = throw NotSupported("getFunctionColumns")

  override def supportsCatalogsInTableDefinitions: Boolean = throw NotSupported("supportsCatalogsInTableDefinitions")

  override def isCatalogAtStart: Boolean = throw NotSupported("isCatalogAtStart")

  override def getJDBCMinorVersion: Int = throw NotSupported("getJDBCMinorVersion")

  override def supportsMixedCaseQuotedIdentifiers: Boolean = throw NotSupported("supportsMixedCaseQuotedIdentifiers")

  override def storesUpperCaseQuotedIdentifiers: Boolean = throw NotSupported("storesUpperCaseQuotedIdentifiers")

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet =
    throw NotSupported("getUDTs")

  override def getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String,
                             attributeNamePattern: String): ResultSet = throw NotSupported("getAttributes")

  override def supportsStoredFunctionsUsingCallSyntax: Boolean = throw NotSupported("supportsStoredFunctionsUsingCallSyntax")

  override def nullsAreSortedAtStart: Boolean = throw NotSupported("nullsAreSortedAtStart")

  override def getMaxIndexLength: Int = throw NotSupported("getMaxIndexLength")

  override def getMaxTablesInSelect: Int = throw NotSupported("getMaxTablesInSelect")

  override def getClientInfoProperties: ResultSet = throw NotSupported("getClientInfoProperties")

  override def supportsSchemasInDataManipulation: Boolean = throw NotSupported("supportsSchemasInDataManipulation")

  override def getDatabaseMinorVersion: Int = throw NotSupported("getDatabaseMinorVersion")

  override def supportsSchemasInProcedureCalls: Boolean = throw NotSupported("supportsSchemasInProcedureCalls")

  override def supportsOuterJoins: Boolean = throw NotSupported("supportsOuterJoins")

  override def supportsGroupBy: Boolean = throw NotSupported("supportsGroupBy")

  override def doesMaxRowSizeIncludeBlobs: Boolean = throw NotSupported("doesMaxRowSizeIncludeBlobs")

  override def supportsCatalogsInDataManipulation: Boolean = throw NotSupported("supportsCatalogsInDataManipulation")

  override def getDatabaseProductName: String = throw NotSupported("getDatabaseProductName")

  override def supportsOpenCursorsAcrossCommit: Boolean = throw NotSupported("supportsOpenCursorsAcrossCommit")

  override def supportsTableCorrelationNames: Boolean = throw NotSupported("supportsTableCorrelationNames")

  override def supportsExtendedSQLGrammar: Boolean = throw NotSupported("supportsExtendedSQLGrammar")

  override def getJDBCMajorVersion: Int = throw NotSupported("getJDBCMajorVersion")

  override def getUserName: String = throw NotSupported("getUserName")

  override def getMaxProcedureNameLength: Int = throw NotSupported("getMaxProcedureNameLength")

  override def getDriverName: String = throw NotSupported("getDriverName")

  override def getMaxRowSize: Int = throw NotSupported("getMaxRowSize")

  override def dataDefinitionCausesTransactionCommit: Boolean = throw NotSupported("dataDefinitionCausesTransactionCommit")

  override def getMaxColumnNameLength: Int = throw NotSupported("getMaxColumnNameLength")

  override def getMaxSchemaNameLength: Int = throw NotSupported("getMaxSchemaNameLength")

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = throw NotSupported("getVersionColumns")

  override def getNumericFunctions: String = throw NotSupported("getNumericFunctions")

  override def supportsIntegrityEnhancementFacility: Boolean = throw NotSupported("supportsIntegrityEnhancementFacility")

  override def getIdentifierQuoteString: String = throw NotSupported("getIdentifierQuoteString")

  override def supportsNonNullableColumns: Boolean = throw NotSupported("supportsNonNullableColumns")

  override def getMaxConnections: Int = throw NotSupported("getMaxConnections")

  override def supportsResultSetHoldability(holdability: Int): Boolean = throw NotSupported("supportsResultSetHoldability")

  override def supportsGroupByBeyondSelect: Boolean = throw NotSupported("supportsGroupByBeyondSelect")

  override def getFunctions(catalog: String, schemaPattern: String,
                            functionNamePattern: String): ResultSet = throw NotSupported("getFunctions")

  override def supportsSchemasInPrivilegeDefinitions: Boolean = throw NotSupported("supportsSchemasInPrivilegeDefinitions")

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean =
    throw NotSupported("supportsResultSetConcurrency")

  override def getURL: String = throw NotSupported("getURL")

  override def supportsSubqueriesInQuantifieds: Boolean = throw NotSupported("supportsSubqueriesInQuantifieds")

  override def supportsBatchUpdates: Boolean = throw NotSupported("supportsBatchUpdates")

  override def supportsLikeEscapeClause: Boolean = throw NotSupported("supportsLikeEscapeClause")

  override def supportsExpressionsInOrderBy: Boolean = throw NotSupported("supportsExpressionsInOrderBy")

  override def allTablesAreSelectable: Boolean = throw NotSupported("allTablesAreSelectable")

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String,
                                 foreignCatalog: String, foreignSchema: String,
                                 foreignTable: String): ResultSet = throw NotSupported("getCrossReference")

  override def getDatabaseMajorVersion: Int = throw NotSupported("getDatabaseMajorVersion")

  override def supportsColumnAliasing: Boolean = throw NotSupported("supportsColumnAliasing")

  override def getMaxCursorNameLength: Int = throw NotSupported("getMaxCursorNameLength")

  override def getRowIdLifetime: RowIdLifetime = throw NotSupported("getRowIdLifetime")

  override def ownDeletesAreVisible(`type`: Int): Boolean = throw NotSupported("ownDeletesAreVisible")

  override def supportsDifferentTableCorrelationNames: Boolean = throw NotSupported("supportsDifferentTableCorrelationNames")

  override def getDefaultTransactionIsolation: Int = throw NotSupported("getDefaultTransactionIsolation")

  override def getSearchStringEscape: String = throw NotSupported("getSearchStringEscape")

  override def getMaxUserNameLength: Int = throw NotSupported("getMaxUserNameLength")

  override def supportsANSI92EntryLevelSQL: Boolean = throw NotSupported("supportsANSI92EntryLevelSQL")

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String,
                                   columnNamePattern: String): ResultSet = throw NotSupported("getProcedureColumns")

  override def storesMixedCaseQuotedIdentifiers: Boolean = throw NotSupported("storesMixedCaseQuotedIdentifiers")

  override def supportsANSI92FullSQL: Boolean = throw NotSupported("supportsANSI92FullSQL")

  override def getMaxStatementLength: Int = throw NotSupported("getMaxStatementLength")

  override def othersDeletesAreVisible(`type`: Int): Boolean = throw NotSupported("othersDeletesAreVisible")

  override def supportsTransactions: Boolean = throw NotSupported("supportsTransactions")

  override def deletesAreDetected(`type`: Int): Boolean = throw NotSupported("deletesAreDetected")

  override def locatorsUpdateCopy: Boolean = throw NotSupported("locatorsUpdateCopy")

  override def allProceduresAreCallable: Boolean = throw NotSupported("allProceduresAreCallable")

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet =
    throw NotSupported("getImportedKeys")

  override def usesLocalFiles: Boolean = throw NotSupported("usesLocalFiles")

  override def supportsLimitedOuterJoins: Boolean = throw NotSupported("supportsLimitedOuterJoins")

  override def storesMixedCaseIdentifiers: Boolean = throw NotSupported("storesMixedCaseIdentifiers")

  override def getCatalogTerm: String = throw NotSupported("getCatalogTerm")

  override def getMaxColumnsInGroupBy: Int = throw NotSupported("getMaxColumnsInGroupBy")

  override def supportsSubqueriesInExists: Boolean = throw NotSupported("supportsSubqueriesInExists")

  override def supportsPositionedUpdate: Boolean = throw NotSupported("supportsPositionedUpdate")

  override def supportsGetGeneratedKeys: Boolean = throw NotSupported("supportsGetGeneratedKeys")

  override def supportsUnion: Boolean = throw NotSupported("supportsUnion")

  override def nullsAreSortedLow: Boolean = throw NotSupported("nullsAreSortedLow")

  override def getSQLKeywords: String = throw NotSupported("getSQLKeywords")

  override def supportsCorrelatedSubqueries: Boolean = throw NotSupported("supportsCorrelatedSubqueries")

  override def isReadOnly: Boolean = throw NotSupported("isReadOnly")

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet =
    throw NotSupported("getProcedures")

  override def supportsUnionAll: Boolean = throw NotSupported("supportsUnionAll")

  override def supportsCoreSQLGrammar: Boolean = throw NotSupported("supportsCoreSQLGrammar")

  override def getPseudoColumns(catalog: String, schemaPattern: String,
                                tableNamePattern: String, columnNamePattern: String): ResultSet =
    throw NotSupported("getPseudoColumns")

  override def getCatalogs: ResultSet = throw NotSupported("getCatalogs")

  override def getSuperTables(catalog: String, schemaPattern: String,
                              tableNamePattern: String): ResultSet = throw NotSupported("getSuperTables")

  override def getMaxColumnsInOrderBy: Int = throw NotSupported("getMaxColumnsInOrderBy")

  override def supportsAlterTableWithAddColumn: Boolean = throw NotSupported("supportsAlterTableWithAddColumn")

  override def getProcedureTerm: String = throw NotSupported("getProcedureTerm")

  override def getMaxCharLiteralLength: Int = throw NotSupported("getMaxCharLiteralLength")

  override def supportsMixedCaseIdentifiers: Boolean = throw NotSupported("supportsMixedCaseIdentifiers")

  override def supportsDataDefinitionAndDataManipulationTransactions: Boolean =
    throw NotSupported("supportsDataDefinitionAndDataManipulationTransactions")

  override def supportsCatalogsInProcedureCalls: Boolean = throw NotSupported("supportsCatalogsInProcedureCalls")

  override def supportsGroupByUnrelated: Boolean = throw NotSupported("supportsGroupByUnrelated")

  override def getResultSetHoldability: Int = throw NotSupported("getResultSetHoldability")

  override def ownUpdatesAreVisible(`type`: Int): Boolean = throw NotSupported("ownUpdatesAreVisible")

  override def nullsAreSortedHigh: Boolean = throw NotSupported("nullsAreSortedHigh")

  override def getTables(catalog: String, schemaPattern: String,
                         tableNamePattern: String, types: Array[String]): ResultSet = throw NotSupported("getTables")

  override def supportsMultipleTransactions: Boolean = throw NotSupported("supportsMultipleTransactions")

  override def supportsNamedParameters: Boolean = throw NotSupported("supportsNamedParameters")

  override def getTypeInfo: ResultSet = throw NotSupported("getTypeInfo")

  override def supportsAlterTableWithDropColumn: Boolean = throw NotSupported("supportsAlterTableWithDropColumn")

  override def getSchemaTerm: String = throw NotSupported("getSchemaTerm")

  override def nullPlusNonNullIsNull: Boolean = throw NotSupported("nullPlusNonNullIsNull")

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = throw NotSupported("getPrimaryKeys")

  override def supportsOpenCursorsAcrossRollback: Boolean = throw NotSupported("supportsOpenCursorsAcrossRollback")

  override def getMaxBinaryLiteralLength: Int = throw NotSupported("getMaxBinaryLiteralLength")

  override def getExtraNameCharacters: String = throw NotSupported("getExtraNameCharacters")

  override def getSchemas: ResultSet = throw NotSupported("getSchemas")

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = throw NotSupported("getSchemas")

  override def supportsMultipleResultSets: Boolean = throw NotSupported("supportsMultipleResultSets")

  override def ownInsertsAreVisible(`type`: Int): Boolean = throw NotSupported("ownInsertsAreVisible")

  override def nullsAreSortedAtEnd: Boolean = throw NotSupported("nullsAreSortedAtEnd")

  override def supportsSavepoints: Boolean = throw NotSupported("supportsSavepoints")

  override def getMaxStatements: Int = throw NotSupported("getMaxStatements")

  override def getBestRowIdentifier(catalog: String, schema: String,
                                    table: String, scope: Int, nullable: Boolean): ResultSet =
    throw NotSupported("getBestRowIdentifier")

  override def getDriverVersion: String = throw NotSupported("getDriverVersion")

  override def storesUpperCaseIdentifiers: Boolean = throw NotSupported("storesUpperCaseIdentifiers")

  override def storesLowerCaseIdentifiers: Boolean = throw NotSupported("storesLowerCaseIdentifiers")

  override def getMaxCatalogNameLength: Int = throw NotSupported("getMaxCatalogNameLength")

  override def supportsDataManipulationTransactionsOnly: Boolean =
    throw NotSupported("supportsDataManipulationTransactionsOnly")

  override def getSystemFunctions: String = throw NotSupported("getSystemFunctions")

  override def getColumnPrivileges(catalog: String, schema: String,
                                   table: String, columnNamePattern: String): ResultSet =
    throw NotSupported("getColumnPrivileges")

  override def getDriverMinorVersion: Int = throw NotSupported("getDriverMinorVersion")

  override def getMaxTableNameLength: Int = throw NotSupported("getMaxTableNameLength")

  override def dataDefinitionIgnoredInTransactions: Boolean = throw NotSupported("dataDefinitionIgnoredInTransactions")

  override def getStringFunctions: String = throw NotSupported("getStringFunctions")

  override def getMaxColumnsInSelect: Int = throw NotSupported("getMaxColumnsInSelect")

  override def usesLocalFilePerTable: Boolean = throw NotSupported("usesLocalFilePerTable")

  override def autoCommitFailureClosesAllResultSets: Boolean =
    throw NotSupported("autoCommitFailureClosesAllResultSets")

  override def supportsCatalogsInIndexDefinitions: Boolean = throw NotSupported("supportsCatalogsInIndexDefinitions")

  override def storesLowerCaseQuotedIdentifiers: Boolean = throw NotSupported("storesLowerCaseQuotedIdentifiers")

  override def othersUpdatesAreVisible(`type`: Int): Boolean = throw NotSupported("othersUpdatesAreVisible")

  override def supportsStatementPooling: Boolean = throw NotSupported("supportsStatementPooling")

  override def supportsCatalogsInPrivilegeDefinitions: Boolean =
    throw NotSupported("supportsCatalogsInPrivilegeDefinitions")

  override def supportsStoredProcedures: Boolean = throw NotSupported("supportsStoredProcedures")

  override def supportsSelectForUpdate: Boolean = throw NotSupported("supportsSelectForUpdate")

  override def supportsOpenStatementsAcrossCommit: Boolean = throw NotSupported("supportsOpenStatementsAcrossCommit")

  override def supportsSubqueriesInComparisons: Boolean = throw NotSupported("supportsSubqueriesInComparisons")

  override def supportsTransactionIsolationLevel(level: Int): Boolean =
    throw NotSupported("supportsTransactionIsolationLevel")

  override def getTableTypes: ResultSet = throw NotSupported("getTableTypes")

  override def getMaxColumnsInTable: Int = throw NotSupported("getMaxColumnsInTable")

  override def getConnection: Connection = throw NotSupported("getConnection")

  override def updatesAreDetected(`type`: Int): Boolean = throw NotSupported("updatesAreDetected")

  override def supportsPositionedDelete: Boolean = throw NotSupported("supportsPositionedDelete")

  override def getColumns(catalog: String, schemaPattern: String,
                          tableNamePattern: String, columnNamePattern: String): ResultSet =
    throw NotSupported("getColumns")

  override def supportsResultSetType(`type`: Int): Boolean = throw NotSupported("supportsResultSetType")

  override def supportsMinimumSQLGrammar: Boolean = throw NotSupported("supportsMinimumSQLGrammar")

  override def generatedKeyAlwaysReturned: Boolean = throw NotSupported("generatedKeyAlwaysReturned")

  override def supportsConvert: Boolean = throw NotSupported("supportsConvert")

  override def supportsConvert(fromType: Int, toType: Int): Boolean = throw NotSupported("supportsConvert")

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet =
    throw NotSupported("getExportedKeys")

  override def supportsOrderByUnrelated: Boolean = throw NotSupported("supportsOrderByUnrelated")

  override def getSQLStateType: Int = throw NotSupported("getSQLStateType")

  override def supportsOpenStatementsAcrossRollback: Boolean = throw NotSupported("supportsOpenStatementsAcrossRollback")

  override def getMaxColumnsInIndex: Int = throw NotSupported("getMaxColumnsInIndex")

  override def getTimeDateFunctions: String = throw NotSupported("getTimeDateFunctions")

  override def supportsSchemasInIndexDefinitions: Boolean = throw NotSupported("supportsSchemasInIndexDefinitions")

  override def supportsANSI92IntermediateSQL: Boolean = throw NotSupported("supportsANSI92IntermediateSQL")

  override def getCatalogSeparator: String = throw NotSupported("getCatalogSeparator")

  override def othersInsertsAreVisible(`type`: Int): Boolean = throw NotSupported("othersInsertsAreVisible")

  override def supportsSchemasInTableDefinitions: Boolean = throw NotSupported("supportsSchemasInTableDefinitions")

}

class KsqlDatabaseMetaData(private val ksqlConnection: KsqlConnection) extends DatabaseMetaDataNotSupported {

  override def getDriverMajorVersion: Int = KsqlDriver.driverMajorVersion

  override def getDriverMinorVersion: Int = KsqlDriver.driverMinorVersion

  override def getJDBCMajorVersion: Int = KsqlDriver.jdbcMajorVersion

  override def getJDBCMinorVersion: Int = KsqlDriver.jdbcMinorVersion

  override def getDriverVersion: String = KsqlDriver.driverVersion

  override def getDriverName: String = KsqlDriver.driverName

  override def getDatabaseProductName: String = KsqlDriver.ksqlName

  override def getDatabaseProductVersion: String = KsqlDriver.ksqlVersion

  override def getDatabaseMajorVersion: Int = KsqlDriver.driverMajorVersion

  override def getDatabaseMinorVersion: Int = KsqlDriver.driverMinorVersion

  override def getCatalogTerm: String = "TOPIC"

  override def getSchemaTerm: String = ""

  override def getProcedureTerm: String = ""

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet =
    new IteratorResultSet(List.empty[HeaderField], 0, Iterator.empty)

  override def getSQLKeywords: String = Seq("CREATE STREAM", "DESCRIBE", "DESCRIBE FUNCTION", "EXPLAIN",
    "DROP STREAM", "DROP TABLE", "PRINT", "CAST", "SHOW FUNCTIONS", "LIST FUNCTIONS",
    "SHOW TOPICS", "SHOW FUNCTIONS", "SHOW STREAMS", "SHOW TABLES", "SHOW QUERIES", "SHOW PROPERTIES",
    "TERMINATE").mkString(",")

  override def getMaxStatements: Int = 0

  override def getMaxStatementLength: Int = 0

  override def getURL: String = ksqlConnection.values.jdbcUrl

  override def isReadOnly: Boolean = true

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet = {
    new IteratorResultSet(DatabaseMetadataHeaders.procedures, 0, Iterator.empty)
  }

  override def getCatalogs: ResultSet = new IteratorResultSet(DatabaseMetadataHeaders.catalogs, 0, Iterator.empty)

  override def getConnection: Connection = ksqlConnection

  override def getSuperTables(catalog: String, schemaPattern: String,
                              tableNamePattern: String): ResultSet = {
    validateCatalogAndSchema(catalog, schemaPattern)

    new IteratorResultSet(DatabaseMetadataHeaders.superTables, 0, Iterator.empty)
  }

  override def getTables(catalog: String, schemaPattern: String,
                         tableNamePattern: String, types: Array[String]): ResultSet = {

    validateCatalogAndSchema(catalog, schemaPattern)

    types.foreach(t => if (!TableTypes.tableTypes.map(_.name).contains(t)) throw UnknownTableType(s"Unknown table type $t"))
    val tablePattern = {
      if (Option(tableNamePattern).getOrElse("").equals("")) ".*" else tableNamePattern
    }.toUpperCase.r.pattern

    val itTables = if (types.contains(TableTypes.TABLE.name)) {
      val tables = ksqlConnection.executeKsqlCommand("SHOW TABLES;")
      if (tables.isErroneous) throw KsqlCommandError(s"Error showing tables: ${tables.getErrorMessage.getMessage}")

      tables.getResponse.asScala.flatMap(_.asInstanceOf[TablesList].getTables.asScala)
        .filter(tb => tablePattern.matcher(tb.getName.toUpperCase).matches)
        .map(tb => {
          Seq("", "", tb.getName, TableTypes.TABLE.name, "Topic: " + tb.getTopic + ". Windowed: " + tb.getIsWindowed,
            "", tb.getFormat, "", "", "")
        }).toIterator
    } else Iterator.empty

    val itStreams = if (types.contains(TableTypes.STREAM.name)) {
      val streams = ksqlConnection.executeKsqlCommand("SHOW STREAMS;")
      if (streams.isErroneous) throw KsqlCommandError(s"Error showing streams: ${streams.getErrorMessage.getMessage}")

      streams.getResponse.asScala.flatMap(_.asInstanceOf[StreamsList].getStreams.asScala)
        .filter(tb => tablePattern.matcher(tb.getName.toUpperCase).matches)
        .map(tb => {
          Seq("", "", tb.getName, TableTypes.STREAM.name, "Topic: " + tb.getTopic, "", tb.getFormat, "", "", "")
        }).toIterator
    } else Iterator.empty

    new IteratorResultSet(DatabaseMetadataHeaders.tables, 0, itTables ++ itStreams)
  }

  override def getSchemas: ResultSet = new IteratorResultSet(DatabaseMetadataHeaders.schemas, 0, Iterator.empty)

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = {
    validateCatalogAndSchema(catalog, schemaPattern)
    getSchemas
  }

  override def getTableTypes: ResultSet = new IteratorResultSet(DatabaseMetadataHeaders.tableTypes, 0,
    Iterator(Seq(TableTypes.TABLE.name), Seq(TableTypes.STREAM.name)))

  override def getTypeInfo: ResultSet = {
    val booleanType = Seq(
      "BOOLEAN",
      Types.BOOLEAN,
      3L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(false),
      Boolean.box(false),
      Boolean.box(false),
      "BOOLEAN",
      0,
      0,
      Types.BOOLEAN,
      0,
      10
    )
    val integerType = Seq(
      "INTEGER",
      Types.INTEGER,
      10L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(true),
      Boolean.box(false),
      Boolean.box(false),
      "INTEGER",
      0,
      0,
      Types.INTEGER,
      0,
      10
    )
    val bitIntType = Seq(
      "BIGINT",
      Types.BIGINT,
      19L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(true),
      Boolean.box(false),
      Boolean.box(false),
      "BIGINT",
      0,
      0,
      Types.BIGINT,
      0,
      10
    )
    val doubleType = Seq(
      "DOUBLE",
      Types.DOUBLE,
      22L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(true),
      Boolean.box(false),
      Boolean.box(false),
      "DOUBLE",
      -308,
      308,
      Types.DOUBLE,
      0,
      10
    )
    val varcharType = Seq(
      "VARCHAR",
      Types.VARCHAR,
      65535L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(false),
      Boolean.box(false),
      Boolean.box(false),
      "STRING",
      0,
      0,
      Types.VARCHAR,
      0,
      10
    )
    val arrayType = Seq(
      "ARRAY",
      Types.ARRAY,
      65535L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(false),
      Boolean.box(false),
      Boolean.box(false),
      "ARRAY",
      0,
      0,
      Types.ARRAY,
      0,
      10
    )
    val mapType = Seq(
      "JAVA_OBJECT",
      Types.JAVA_OBJECT,
      65535L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(false),
      Boolean.box(false),
      Boolean.box(false),
      "ARRAY",
      0,
      0,
      Types.JAVA_OBJECT,
      0,
      10
    )
    val structType = Seq(
      "STRUCT",
      Types.STRUCT,
      65535L,
      "",
      "",
      "",
      DatabaseMetaData.typeNullable,
      Boolean.box(false),
      DatabaseMetaData.typeSearchable,
      Boolean.box(false),
      Boolean.box(false),
      Boolean.box(false),
      "STRUCT",
      0,
      0,
      Types.STRUCT,
      0,
      10
    )
    val typeIterator: Iterator[Seq[Any]] = Iterator(booleanType, integerType, bitIntType, doubleType, varcharType,
      arrayType, mapType, structType)
    new IteratorResultSet(DatabaseMetadataHeaders.typeInfo, 0, typeIterator)
  }

  override def getColumns(catalog: String, schemaPattern: String,
                          tableNamePattern: String, columnNamePattern: String): ResultSet = {
    validateCatalogAndSchema(catalog, schemaPattern)

    val tables = getTables(catalog, schemaPattern, tableNamePattern, TableTypes.tableTypes.map(_.name).toArray)
    val columnPattern = {
      if (Option(columnNamePattern).getOrElse("").equals("")) ".*" else columnNamePattern
    }.toUpperCase.r.pattern

    var columns: Iterator[Seq[AnyRef]] = Iterator.empty
    tables.toStream.foreach { table =>
      val tableName = table.getString(3)
      val describe = ksqlConnection.executeKsqlCommand(s"DESCRIBE $tableName;")
      if (describe.isErroneous) throw KsqlCommandError(s"Error describing table $tableName: " +
        describe.getErrorMessage.getMessage)

      //generated fields from KSQL engine
      var defaultFields: Iterator[Seq[AnyRef]] = Iterator.empty
      if (columnPattern.matcher("_ID").matches) {
        defaultFields ++= Iterator(Seq[AnyRef]("", "", tableName, "_ID", Int.box(Types.BIGINT), "BIGINT",
          Int.box(Int.MaxValue), Int.box(0), "null", Int.box(10), Int.box(DatabaseMetaData.columnNullableUnknown),
          "", "", Int.box(-1), Int.box(-1), Int.box(32), Int.box(17), "", "", "", "",
          Int.box(Types.BIGINT), "YES", "YES"))
      }
      if (columnPattern.matcher("_NAME").matches) {
        defaultFields ++= Iterator(Seq[AnyRef]("", "", tableName, "_NAME", Int.box(Types.VARCHAR), "VARCHAR",
          Int.box(Int.MaxValue), Int.box(0), "null", Int.box(10), Int.box(DatabaseMetaData.columnNullableUnknown),
          "", "", Int.box(-1), Int.box(-1), Int.box(32), Int.box(17), "", "", "", "",
          Int.box(Types.VARCHAR), "YES", "YES"))
      }
      columns ++= defaultFields

      columns ++= describe.getResponse.asScala.map(_.asInstanceOf[SourceDescriptionEntity])
        .map(_.getSourceDescription)
        .filter(sd => columnPattern.matcher(sd.getName.toUpperCase).matches)
        .map(sd => {
          Seq[AnyRef]("", "", tableName, sd.getName, Int.box(DatabaseMetadataHeaders.mapDataType(sd.getType)), sd.getType,
            Int.box(Int.MaxValue), Int.box(0), "null", Int.box(10), Int.box(DatabaseMetaData.columnNullableUnknown),
            "", "", Int.box(-1), Int.box(-1), Int.box(32), Int.box(17), "", "", "", "",
            Int.box(DatabaseMetadataHeaders.mapDataType(sd.getType)), "NO", "NO")

        }).toIterator
    }
    new IteratorResultSet(DatabaseMetadataHeaders.columns, 0, columns)
  }

  override def getNumericFunctions: String = availableFunctions(
    author = None,
    types = Set("INT", "BIGINT", "DOUBLE")
  ).mkString(",")

  override def getStringFunctions: String = availableFunctions(
    author = None,
    types = Set("VARCHAR", "STRING")
  ).mkString(",")

  override def getSystemFunctions: String = availableFunctions(author = Some("Confluent")).mkString(",")

  override def getTimeDateFunctions: String = availableFunctions(
    author = None,
    names = Set(".*TIME.*", ".*DATE.*"),
    types = Set.empty
  ).mkString(",")

  override def supportsAlterTableWithAddColumn: Boolean = false

  override def supportsAlterTableWithDropColumn: Boolean = false

  override def supportsCatalogsInDataManipulation: Boolean = false

  override def supportsCatalogsInTableDefinitions: Boolean = false

  override def supportsCatalogsInProcedureCalls: Boolean = false

  override def supportsMultipleResultSets: Boolean = false

  override def supportsMultipleTransactions: Boolean = false

  override def supportsSavepoints: Boolean = false

  override def supportsSchemasInDataManipulation: Boolean = false

  override def supportsSchemasInTableDefinitions: Boolean = false

  override def supportsStoredFunctionsUsingCallSyntax: Boolean = true

  override def supportsStoredProcedures: Boolean = false

  private def availableFunctions(author: Option[String] = None, names: Set[String] = Set.empty,
                                 types: Set[String] = Set(".*")): Set[String] = {
    var functions = mutable.Set.empty[String]

    (ksqlConnection.createStatement.executeQuery("LIST FUNCTIONS")).toStream.foreach { fn =>
      val fnName = fn.getString("FUNCTION_NAME_FN_NAME").toUpperCase

      ksqlConnection.createStatement.executeQuery(s"DESCRIBE FUNCTION $fnName").toStream.foreach { fnDesc =>
        val fnAuthor = fnDesc.getString("FUNCTION_DESCRIPTION_AUTHOR").trim.toUpperCase
        val fnReturnType = fnDesc.getString("FUNCTION_DESCRIPTION_FN_RETURN_TYPE")
        if ((types.filter(fnReturnType.matches(_)).nonEmpty || names.filter(fnName.matches(_)).nonEmpty) &&
          (author.isEmpty || author.get.toUpperCase == fnAuthor.toUpperCase)) {
          functions += fnName
        }
      }
    }
    functions.toSet
  }

  private def validateCatalogAndSchema(catalog: String, schema: String) = {
    if (catalog != null && catalog != "") throw UnknownCatalog(s"Unknown catalog $catalog")
    if (schema != null && schema != "") throw UnknownSchema(s"Unknown schema $schema")
  }

}
