package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, DatabaseMetaData, ResultSet, RowIdLifetime, Types}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.resultset.StaticResultSet
import io.confluent.ksql.rest.entity.{SourceDescription, StreamsList, TablesList}

import scala.collection.JavaConverters._

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

class KsqlDatabaseMetaData(private val ksqlConnection: KsqlConnection) extends DatabaseMetaData with WrapperNotSupported {

  override def supportsMultipleOpenResults: Boolean = throw NotSupported()

  override def supportsSubqueriesInIns: Boolean = throw NotSupported()

  override def getSuperTypes(catalog: String, schemaPattern: String,
                             typeNamePattern: String): ResultSet = throw NotSupported()

  override def getTablePrivileges(catalog: String, schemaPattern: String,
                                  tableNamePattern: String): ResultSet = throw NotSupported()

  override def supportsFullOuterJoins: Boolean = throw NotSupported()

  override def insertsAreDetected(`type`: Int): Boolean = throw NotSupported()

  override def getDriverMajorVersion: Int = KsqlDriver.majorVersion

  override def getDatabaseProductVersion: String = throw NotSupported()

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean,
                            approximate: Boolean): ResultSet = throw NotSupported()

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String,
                                  columnNamePattern: String): ResultSet = throw NotSupported()

  override def supportsCatalogsInTableDefinitions: Boolean = throw NotSupported()

  override def isCatalogAtStart: Boolean = throw NotSupported()

  override def getJDBCMinorVersion: Int = KsqlDriver.jdbcMinorVersion

  override def supportsMixedCaseQuotedIdentifiers: Boolean = throw NotSupported()

  override def storesUpperCaseQuotedIdentifiers: Boolean = throw NotSupported()

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String,
                       types: Array[Int]): ResultSet = throw NotSupported()

  override def getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String,
                             attributeNamePattern: String): ResultSet = throw NotSupported()

  override def supportsStoredFunctionsUsingCallSyntax: Boolean = throw NotSupported()

  override def nullsAreSortedAtStart: Boolean = throw NotSupported()

  override def getMaxIndexLength: Int = throw NotSupported()

  override def getMaxTablesInSelect: Int = throw NotSupported()

  override def getClientInfoProperties: ResultSet = throw NotSupported()

  override def supportsSchemasInDataManipulation: Boolean = throw NotSupported()

  override def getDatabaseMinorVersion: Int = throw NotSupported()

  override def supportsSchemasInProcedureCalls: Boolean = throw NotSupported()

  override def supportsOuterJoins: Boolean = throw NotSupported()

  override def supportsGroupBy: Boolean = throw NotSupported()

  override def doesMaxRowSizeIncludeBlobs: Boolean = throw NotSupported()

  override def supportsCatalogsInDataManipulation: Boolean = throw NotSupported()

  override def getDatabaseProductName: String = throw NotSupported()

  override def supportsOpenCursorsAcrossCommit: Boolean = throw NotSupported()

  override def supportsTableCorrelationNames: Boolean = throw NotSupported()

  override def supportsExtendedSQLGrammar: Boolean = throw NotSupported()

  override def getJDBCMajorVersion: Int = KsqlDriver.jdbcMajorVersion

  override def getUserName: String = throw NotSupported()

  override def getMaxProcedureNameLength: Int = throw NotSupported()

  override def getDriverName: String = KsqlDriver.driverName

  override def getMaxRowSize: Int = throw NotSupported()

  override def dataDefinitionCausesTransactionCommit: Boolean = throw NotSupported()

  override def getMaxColumnNameLength: Int = throw NotSupported()

  override def getMaxSchemaNameLength: Int = throw NotSupported()

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = throw NotSupported()

  override def getNumericFunctions: String = throw NotSupported()

  override def supportsIntegrityEnhancementFacility: Boolean = throw NotSupported()

  override def getIdentifierQuoteString: String = throw NotSupported()

  override def supportsNonNullableColumns: Boolean = throw NotSupported()

  override def getMaxConnections: Int = throw NotSupported()

  override def supportsResultSetHoldability(holdability: Int): Boolean = throw NotSupported()

  override def supportsGroupByBeyondSelect: Boolean = throw NotSupported()

  override def getFunctions(catalog: String, schemaPattern: String,
                            functionNamePattern: String): ResultSet = throw NotSupported()

  override def supportsSchemasInPrivilegeDefinitions: Boolean = throw NotSupported()

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean = throw NotSupported()

  override def getURL: String = throw NotSupported()

  override def supportsSubqueriesInQuantifieds: Boolean = throw NotSupported()

  override def supportsBatchUpdates: Boolean = throw NotSupported()

  override def supportsLikeEscapeClause: Boolean = throw NotSupported()

  override def supportsExpressionsInOrderBy: Boolean = throw NotSupported()

  override def allTablesAreSelectable: Boolean = throw NotSupported()

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String,
                                 foreignCatalog: String, foreignSchema: String,
                                 foreignTable: String): ResultSet = throw NotSupported()

  override def getDatabaseMajorVersion: Int = throw NotSupported()

  override def supportsColumnAliasing: Boolean = throw NotSupported()

  override def getMaxCursorNameLength: Int = throw NotSupported()

  override def getRowIdLifetime: RowIdLifetime = throw NotSupported()

  override def ownDeletesAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def supportsDifferentTableCorrelationNames: Boolean = throw NotSupported()

  override def getDefaultTransactionIsolation: Int = throw NotSupported()

  override def getSearchStringEscape: String = throw NotSupported()

  override def getMaxUserNameLength: Int = throw NotSupported()

  override def supportsANSI92EntryLevelSQL: Boolean = throw NotSupported()

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String,
                                   columnNamePattern: String): ResultSet = throw NotSupported()

  override def storesMixedCaseQuotedIdentifiers: Boolean = throw NotSupported()

  override def supportsANSI92FullSQL: Boolean = throw NotSupported()

  override def getMaxStatementLength: Int = throw NotSupported()

  override def othersDeletesAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def supportsTransactions: Boolean = throw NotSupported()

  override def deletesAreDetected(`type`: Int): Boolean = throw NotSupported()

  override def locatorsUpdateCopy: Boolean = throw NotSupported()

  override def allProceduresAreCallable: Boolean = throw NotSupported()

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet = throw NotSupported()

  override def usesLocalFiles: Boolean = throw NotSupported()

  override def supportsLimitedOuterJoins: Boolean = throw NotSupported()

  override def storesMixedCaseIdentifiers: Boolean = throw NotSupported()

  override def getCatalogTerm: String = throw NotSupported()

  override def getMaxColumnsInGroupBy: Int = throw NotSupported()

  override def supportsSubqueriesInExists: Boolean = throw NotSupported()

  override def supportsPositionedUpdate: Boolean = throw NotSupported()

  override def supportsGetGeneratedKeys: Boolean = throw NotSupported()

  override def supportsUnion: Boolean = throw NotSupported()

  override def nullsAreSortedLow: Boolean = throw NotSupported()

  override def getSQLKeywords: String = throw NotSupported()

  override def supportsCorrelatedSubqueries: Boolean = throw NotSupported()

  override def isReadOnly: Boolean = throw NotSupported()

  override def getProcedures(catalog: String, schemaPattern: String,
                             procedureNamePattern: String): ResultSet = throw NotSupported()

  override def supportsUnionAll: Boolean = throw NotSupported()

  override def supportsCoreSQLGrammar: Boolean = throw NotSupported()

  override def getPseudoColumns(catalog: String, schemaPattern: String,
                                tableNamePattern: String, columnNamePattern: String): ResultSet = throw NotSupported()

  override def getCatalogs: ResultSet = new StaticResultSet[String](Headers.catalogs, Iterator.empty)

  override def getSuperTables(catalog: String, schemaPattern: String,
                              tableNamePattern: String): ResultSet = {
    validateCatalogAndSchema(catalog, schemaPattern)

    new StaticResultSet[String](Headers.superTables, Iterator.empty)
  }

  override def getMaxColumnsInOrderBy: Int = throw NotSupported()

  override def supportsAlterTableWithAddColumn: Boolean = throw NotSupported()

  override def getProcedureTerm: String = throw NotSupported()

  override def getMaxCharLiteralLength: Int = throw NotSupported()

  override def supportsMixedCaseIdentifiers: Boolean = throw NotSupported()

  override def supportsDataDefinitionAndDataManipulationTransactions: Boolean = throw NotSupported()

  override def supportsCatalogsInProcedureCalls: Boolean = throw NotSupported()

  override def supportsGroupByUnrelated: Boolean = throw NotSupported()

  override def getResultSetHoldability: Int = throw NotSupported()

  override def ownUpdatesAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def nullsAreSortedHigh: Boolean = throw NotSupported()

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

    new StaticResultSet[String](Headers.tables, itTables ++ itStreams)
  }

  override def supportsMultipleTransactions: Boolean = throw NotSupported()

  override def supportsNamedParameters: Boolean = throw NotSupported()

  override def getTypeInfo: ResultSet = throw NotSupported()

  override def supportsAlterTableWithDropColumn: Boolean = throw NotSupported()

  override def getSchemaTerm: String = throw NotSupported()

  override def nullPlusNonNullIsNull: Boolean = throw NotSupported()

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = throw NotSupported()

  override def supportsOpenCursorsAcrossRollback: Boolean = throw NotSupported()

  override def getMaxBinaryLiteralLength: Int = throw NotSupported()

  override def getExtraNameCharacters: String = throw NotSupported()

  override def getSchemas: ResultSet = new StaticResultSet[String](Headers.schemas, Iterator.empty)

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = {
    validateCatalogAndSchema(catalog, schemaPattern)
    getSchemas
  }

  override def supportsMultipleResultSets: Boolean = throw NotSupported()

  override def ownInsertsAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def nullsAreSortedAtEnd: Boolean = throw NotSupported()

  override def supportsSavepoints: Boolean = throw NotSupported()

  override def getMaxStatements: Int = throw NotSupported()

  override def getBestRowIdentifier(catalog: String, schema: String,
                                    table: String, scope: Int, nullable: Boolean): ResultSet = throw NotSupported()

  override def getDriverVersion: String = KsqlDriver.version

  override def storesUpperCaseIdentifiers: Boolean = throw NotSupported()

  override def storesLowerCaseIdentifiers: Boolean = throw NotSupported()

  override def getMaxCatalogNameLength: Int = throw NotSupported()

  override def supportsDataManipulationTransactionsOnly: Boolean = throw NotSupported()

  override def getSystemFunctions: String = throw NotSupported()

  override def getColumnPrivileges(catalog: String, schema: String,
                                   table: String, columnNamePattern: String): ResultSet = throw NotSupported()

  override def getDriverMinorVersion: Int = KsqlDriver.minorVersion

  override def getMaxTableNameLength: Int = throw NotSupported()

  override def dataDefinitionIgnoredInTransactions: Boolean = throw NotSupported()

  override def getStringFunctions: String = throw NotSupported()

  override def getMaxColumnsInSelect: Int = throw NotSupported()

  override def usesLocalFilePerTable: Boolean = throw NotSupported()

  override def autoCommitFailureClosesAllResultSets: Boolean = throw NotSupported()

  override def supportsCatalogsInIndexDefinitions: Boolean = throw NotSupported()

  override def storesLowerCaseQuotedIdentifiers: Boolean = throw NotSupported()

  override def othersUpdatesAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def supportsStatementPooling: Boolean = throw NotSupported()

  override def supportsCatalogsInPrivilegeDefinitions: Boolean = throw NotSupported()

  override def supportsStoredProcedures: Boolean = throw NotSupported()

  override def supportsSelectForUpdate: Boolean = throw NotSupported()

  override def supportsOpenStatementsAcrossCommit: Boolean = throw NotSupported()

  override def supportsSubqueriesInComparisons: Boolean = throw NotSupported()

  override def supportsTransactionIsolationLevel(level: Int): Boolean = throw NotSupported()

  override def getTableTypes: ResultSet = new StaticResultSet[String](Headers.tableTypes,
    Iterator(Seq(TableTypes.TABLE.name), Seq(TableTypes.STREAM.name)))

  override def getMaxColumnsInTable: Int = throw NotSupported()

  override def getConnection: Connection = ksqlConnection

  override def updatesAreDetected(`type`: Int): Boolean = throw NotSupported()

  override def supportsPositionedDelete: Boolean = throw NotSupported()

  override def getColumns(catalog: String, schemaPattern: String,
                          tableNamePattern: String, columnNamePattern: String): ResultSet = {
    validateCatalogAndSchema(catalog, schemaPattern)

    val tables = getTables(catalog, schemaPattern, tableNamePattern, TableTypes.tableTypes.map(_.name).toArray)
    val columnPattern = {
      if (Option(columnNamePattern).getOrElse("").equals("")) ".*" else columnNamePattern
    }.toUpperCase.r.pattern

    var tableSchemas: Iterator[Seq[AnyRef]] = Iterator.empty
    while (tables.next) {
      val tableName = tables.getString(3)
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
      tableSchemas ++= defaultFields

      tableSchemas ++= describe.getResponse.asScala.map(_.asInstanceOf[SourceDescription])
        .flatMap(_.getSchema.asScala)
        .filter(sch => columnPattern.matcher(sch.getName.toUpperCase).matches)
        .map(sch => {
          Seq[AnyRef]("", "", tableName, sch.getName, Int.box(Headers.mapDataType(sch.getType)), sch.getType,
            Int.box(Int.MaxValue), Int.box(0), "null", Int.box(10), Int.box(DatabaseMetaData.columnNullableUnknown),
            "", "", Int.box(-1), Int.box(-1), Int.box(32), Int.box(17), "", "", "", "",
            Int.box(Headers.mapDataType(sch.getType)), "NO", "NO")

        }).toIterator
    }
    new StaticResultSet[AnyRef](Headers.columns, tableSchemas)
  }

  override def supportsResultSetType(`type`: Int): Boolean = throw NotSupported()

  override def supportsMinimumSQLGrammar: Boolean = throw NotSupported()

  override def generatedKeyAlwaysReturned: Boolean = throw NotSupported()

  override def supportsConvert: Boolean = throw NotSupported()

  override def supportsConvert(fromType: Int, toType: Int): Boolean = throw NotSupported()

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet = throw NotSupported()

  override def supportsOrderByUnrelated: Boolean = throw NotSupported()

  override def getSQLStateType: Int = throw NotSupported()

  override def supportsOpenStatementsAcrossRollback: Boolean = throw NotSupported()

  override def getMaxColumnsInIndex: Int = throw NotSupported()

  override def getTimeDateFunctions: String = throw NotSupported()

  override def supportsSchemasInIndexDefinitions: Boolean = throw NotSupported()

  override def supportsANSI92IntermediateSQL: Boolean = throw NotSupported()

  override def getCatalogSeparator: String = throw NotSupported()

  override def othersInsertsAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def supportsSchemasInTableDefinitions: Boolean = throw NotSupported()

  private def validateCatalogAndSchema(catalog: String, schema: String) = {
    if (catalog != null && catalog != "") throw UnknownCatalog(s"Unknown catalog $catalog")
    if (schema != null && schema != "") throw UnknownSchema(s"Unknown schema $schema")
  }

}
