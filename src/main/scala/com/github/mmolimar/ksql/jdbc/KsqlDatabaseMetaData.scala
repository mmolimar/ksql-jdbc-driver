package com.github.mmolimar.ksql.jdbc

import java.sql.{Connection, DatabaseMetaData, ResultSet, RowIdLifetime}

import com.github.mmolimar.ksql.jdbc.Exceptions._


class KsqlDatabaseMetaData extends DatabaseMetaData {

  override def supportsMultipleOpenResults: Boolean = throw NotSupported()

  override def supportsSubqueriesInIns: Boolean = throw NotSupported()

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet = throw NotSupported()

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = throw NotSupported()

  override def supportsFullOuterJoins: Boolean = throw NotSupported()

  override def insertsAreDetected(`type`: Int): Boolean = throw NotSupported()

  override def getDriverMajorVersion: Int = KsqlDriver.majorVersion

  override def getDatabaseProductVersion: String = throw NotSupported()

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean, approximate: Boolean): ResultSet = throw NotSupported()

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet = throw NotSupported()

  override def supportsCatalogsInTableDefinitions: Boolean = throw NotSupported()

  override def isCatalogAtStart: Boolean = throw NotSupported()

  override def getJDBCMinorVersion: Int = KsqlDriver.jdbcMinorVersion

  override def supportsMixedCaseQuotedIdentifiers: Boolean = throw NotSupported()

  override def storesUpperCaseQuotedIdentifiers: Boolean = throw NotSupported()

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet = throw NotSupported()

  override def getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String, attributeNamePattern: String): ResultSet = throw NotSupported()

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

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet = throw NotSupported()

  override def supportsSchemasInPrivilegeDefinitions: Boolean = throw NotSupported()

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean = throw NotSupported()

  override def getURL: String = throw NotSupported()

  override def supportsSubqueriesInQuantifieds: Boolean = throw NotSupported()

  override def supportsBatchUpdates: Boolean = throw NotSupported()

  override def supportsLikeEscapeClause: Boolean = throw NotSupported()

  override def supportsExpressionsInOrderBy: Boolean = throw NotSupported()

  override def allTablesAreSelectable: Boolean = throw NotSupported()

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String): ResultSet = throw NotSupported()

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

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String, columnNamePattern: String): ResultSet = throw NotSupported()

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

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet = throw NotSupported()

  override def supportsUnionAll: Boolean = throw NotSupported()

  override def supportsCoreSQLGrammar: Boolean = throw NotSupported()

  override def getPseudoColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = throw NotSupported()

  override def getCatalogs: ResultSet = throw NotSupported()

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = throw NotSupported()

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

  override def getTables(catalog: String, schemaPattern: String, tableNamePattern: String, types: Array[String]): ResultSet = throw NotSupported()

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

  override def getSchemas: ResultSet = throw NotSupported()

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = throw NotSupported()

  override def supportsMultipleResultSets: Boolean = throw NotSupported()

  override def ownInsertsAreVisible(`type`: Int): Boolean = throw NotSupported()

  override def nullsAreSortedAtEnd: Boolean = throw NotSupported()

  override def supportsSavepoints: Boolean = throw NotSupported()

  override def getMaxStatements: Int = throw NotSupported()

  override def getBestRowIdentifier(catalog: String, schema: String, table: String, scope: Int, nullable: Boolean): ResultSet = throw NotSupported()

  override def getDriverVersion: String = KsqlDriver.version

  override def storesUpperCaseIdentifiers: Boolean = throw NotSupported()

  override def storesLowerCaseIdentifiers: Boolean = throw NotSupported()

  override def getMaxCatalogNameLength: Int = throw NotSupported()

  override def supportsDataManipulationTransactionsOnly: Boolean = throw NotSupported()

  override def getSystemFunctions: String = throw NotSupported()

  override def getColumnPrivileges(catalog: String, schema: String, table: String, columnNamePattern: String): ResultSet = throw NotSupported()

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

  override def getTableTypes: ResultSet = throw NotSupported()

  override def getMaxColumnsInTable: Int = throw NotSupported()

  override def getConnection: Connection = throw NotSupported()

  override def updatesAreDetected(`type`: Int): Boolean = throw NotSupported()

  override def supportsPositionedDelete: Boolean = throw NotSupported()

  override def getColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = throw NotSupported()

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

  override def unwrap[T](iface: Class[T]): T = throw NotSupported()

  override def isWrapperFor(iface: Class[_]): Boolean = throw NotSupported()

}
