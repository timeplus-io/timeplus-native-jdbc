/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.timeplus.jdbc;

import com.timeplus.data.DataTypeFactory;
import com.timeplus.data.IDataType;
import com.timeplus.log.Logger;
import com.timeplus.log.LoggerFactory;
import com.timeplus.settings.TimeplusDefines;
import com.timeplus.jdbc.wrapper.SQLDatabaseMetadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public final class TimeplusDatabaseMetadata implements SQLDatabaseMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(TimeplusDatabaseMetadata.class);

    private final String url;
    private final TimeplusConnection connection;

    // we will not close connection
    public TimeplusDatabaseMetadata(String url, TimeplusConnection connection) {
        this.url = url;
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.cfg().user();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Timeplus";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.serverContext().version();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "com.timeplus.timeplus.native.jdbc";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return String.valueOf(TimeplusDefines.CLIENT_REVISION);
    }

    @Override
    public int getDriverMajorVersion() {
        return TimeplusDefines.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return TimeplusDefines.MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "GLOBAL,ARRAY";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog,
                                   String schemaPattern,
                                   String procedureNamePattern) throws SQLException {

        return TimeplusResultSetBuilder
                .builder(9, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "PROCEDURE_CAT",
                        "PROCEDURE_SCHEM",
                        "PROCEDURE_NAME",
                        "RES_1",
                        "RES_2",
                        "RES_3",
                        "REMARKS",
                        "PROCEDURE_TYPE",
                        "SPECIFIC_NAME")
                .columnTypes(
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "uint8",
                        "string")
                .build();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog,
                                         String schemaPattern,
                                         String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return TimeplusResultSetBuilder
                .builder(20, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "1", "2", "3", "4", "5",
                        "6", "7", "8", "9", "10",
                        "11", "12", "13", "14", "15",
                        "16", "17", "18", "19", "20")
                .columnTypes(
                        "uint32", "uint32", "uint32", "uint32", "uint32",
                        "uint32", "uint32", "uint32", "uint32", "uint32",
                        "uint32", "uint32", "uint32", "uint32", "uint32",
                        "uint32", "uint32", "uint32", "uint32", "uint32")
                .build();
    }

    @Override
    public ResultSet getTables(String catalog,
                               String schemaPattern,
                               String tableNamePattern,
                               String[] types) throws SQLException {
        /*
         TABLE_CAT                 String => table catalog (may be null)
         TABLE_SCHEM               String => table schema (may be null)
         TABLE_NAME                String => table name
         TABLE_TYPE                String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
                                             "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
         REMARKS                   String => explanatory comment on the table
         TYPE_CAT                  String => the types catalog (may be null)
         TYPE_SCHEM                String => the types schema (may be null)
         TYPE_NAME                 String => type name (may be null)
         SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
         REF_GENERATION            String => specifies how values in SELF_REFERENCING_COL_NAME are created.
                                             Values are "SYSTEM", "USER", "DERIVED". (may be null)
         */
        String sql = "select database, name, engine from system.tables where 1=1";
        if (schemaPattern != null) {
            sql += " and database like '" + schemaPattern + "'";
        }
        if (tableNamePattern != null) {
            sql += " and name like '" + tableNamePattern + "'";
        }
        sql += " order by database, name";
        ResultSet result = request(sql);

        TimeplusResultSetBuilder builder = TimeplusResultSetBuilder
                .builder(10, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TABLE_CAT",
                        "TABLE_SCHEM",
                        "TABLE_NAME",
                        "TABLE_TYPE",
                        "REMARKS",
                        "TYPE_CAT",
                        "TYPE_SCHEM",
                        "TYPE_NAME",
                        "SELF_REFERENCING_COL_NAME",
                        "REF_GENERATION")
                .columnTypes(
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                        "string");

        List<String> typeList = types != null ? Arrays.asList(types) : null;
        if (typeList != null) {
            typeList.replaceAll(type -> type.equals("STREAM") ? "TABLE" : type);
        }
        while (result.next()) {
            List<String> row = new ArrayList<>();
            row.add(TimeplusDefines.DEFAULT_CATALOG);
            row.add(result.getString(1));
            row.add(result.getString(2));
            String type, e = result.getString(3).intern();
            switch (e) {
                case "View":
                case "MaterializedView":
                case "Merge":
                case "Distributed":
                case "Null":
                    type = "VIEW"; // some kind of view
                    break;
                case "Set":
                case "Join":
                case "Buffer":
                    type = "OTHER"; // not a real table
                    break;
                default:
                    type = "TABLE";
                    break;
            }
            row.add(type);
            for (int i = 3; i < 9; i++) {
                row.add(null);
            }
            if (typeList == null || typeList.contains(type)) {
                builder.addRow(row);
            }
        }
        result.close();
        return builder.build();
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String sql = "select name as TABLE_SCHEM, '" + TimeplusDefines.DEFAULT_CATALOG + "' as TABLE_CATALOG from system.databases";
        if (catalog != null) {
            sql += " where TABLE_CATALOG = '" + catalog + '\'';
        }
        if (schemaPattern != null) {
            if (catalog != null) {
                sql += " and ";
            } else {
                sql += " where ";
            }
            sql += "name LIKE '" + schemaPattern + '\'';
        }
        return request(sql);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return TimeplusResultSetBuilder
                .builder(1, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames("TABLE_CAT")
                .columnTypes("string")
                .addRow(TimeplusDefines.DEFAULT_CATALOG).build();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return TimeplusResultSetBuilder
                .builder(1, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames("TABLE_TYPE")
                .columnTypes("string")
                .addRow("TABLE")
                .addRow("VIEW")
                .addRow("OTHER").build();
    }

    @Override
    public ResultSet getColumns(String catalog,
                                String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        StringBuilder query;
        if (connection.serverContext().version().compareTo("1.1.54237") > 0) {
            query = new StringBuilder(
                    "SELECT database, table, name, type, default_kind as default_type, default_expression ");
        } else {
            query = new StringBuilder(
                    "SELECT database, table, name, type, default_type, default_expression ");
        }
        query.append("FROM system.columns ");
        List<String> predicates = new ArrayList<>();
        if (schemaPattern != null) {
            predicates.add("database LIKE '" + schemaPattern + "' ");
        }
        if (tableNamePattern != null) {
            predicates.add("table LIKE '" + tableNamePattern + "' ");
        }
        if (columnNamePattern != null) {
            predicates.add("name LIKE '" + columnNamePattern + "' ");
        }
        if (!predicates.isEmpty()) {
            query.append(" WHERE ");
            buildAndCondition(query, predicates);
        }
        TimeplusResultSetBuilder builder = TimeplusResultSetBuilder
                .builder(24, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TABLE_CAT",
                        "TABLE_SCHEM",
                        "TABLE_NAME",
                        "COLUMN_NAME",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "COLUMN_SIZE",
                        "BUFFER_LENGTH",
                        "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "COLUMN_DEF",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SCOPE_CATALOG",
                        "SCOPE_SCHEMA",
                        "SCOPE_TABLE",
                        "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT",
                        "IS_GENERATEDCOLUMN")
                .columnTypes(
                        "string",
                        "string",
                        "string",
                        "string",
                        "int32",
                        "string",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "string",
                        "string",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "string",
                        "string",
                        "string",
                        "string",
                        "int32",
                        "string",
                        "string");
        ResultSet descTable = request(query.toString());
        int colNum = 1;
        while (descTable.next()) {
            List<Object> row = new ArrayList<>();
            //catalog name
            row.add(TimeplusDefines.DEFAULT_CATALOG);
            //database name
            row.add(descTable.getString("database"));
            //table name
            row.add(descTable.getString("table"));
            //column name
            IDataType dataType = DataTypeFactory.get(descTable.getString("type"), connection.serverContext());
            row.add(descTable.getString("name"));
            //data type
            row.add(dataType.sqlTypeId());
            //type name
            row.add(dataType.name());
            // column size / precision
            row.add(dataType.getPrecision());
            //buffer length
            row.add(0);
            // decimal digits
            row.add(dataType.getScale());
            // radix
            row.add(10);
            // nullable
            row.add(dataType.nullable() ? columnNullable : columnNoNulls);
            //remarks
            row.add(null);

            // COLUMN_DEF
            if ("DEFAULT".equals(descTable.getString("default_type"))) {
                row.add(descTable.getString("default_expression"));
            } else {
                row.add(null);
            }

            //"SQL_DATA_TYPE", unused per JavaDoc
            row.add(null);
            //"SQL_DATETIME_SUB", unused per JavaDoc
            row.add(null);

            // char octet length
            row.add(0);
            // ordinal
            row.add(colNum);
            colNum += 1;

            //IS_NULLABLE
            row.add(dataType.nullable() ? "YES" : "NO");
            //"SCOPE_CATALOG",
            row.add(null);
            //"SCOPE_SCHEMA",
            row.add(null);
            //"SCOPE_TABLE",
            row.add(null);
            //"SOURCE_DATA_TYPE",
            row.add(null);
            //"IS_AUTOINCREMENT"
            row.add("NO");
            //"IS_GENERATEDCOLUMN"
            row.add("NO");

            builder.addRow(row);
        }
        descTable.close();
        return builder.build();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog,
                                         String schema,
                                         String table,
                                         String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(String catalog,
                                        String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog,
                                          String schema,
                                          String table,
                                          int scope,
                                          boolean nullable) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog,
                                       String parentSchema,
                                       String parentTable,
                                       String foreignCatalog,
                                       String foreignSchema,
                                       String foreignTable) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        TimeplusResultSetBuilder builder = TimeplusResultSetBuilder
                .builder(18, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TYPE_NAME",
                        "DATA_TYPE",
                        "PRECISION",
                        "LITERAL_PREFIX",
                        "LITERAL_SUFFIX",
                        "CREATE_PARAMS",
                        "NULLABLE",
                        "CASE_SENSITIVE",
                        "SEARCHABLE",
                        "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE",
                        "AUTO_INCREMENT",
                        "LOCAL_TYPE_NAME",
                        "MINIMUM_SCALE",
                        "MAXIMUM_SCALE",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "NUM_PREC_RADIX")
                .columnTypes(
                        "string",
                        "int32",
                        "int32",
                        "string",
                        "string",
                        "string",
                        "int32",
                        "int8",
                        "int32",
                        "int8",
                        "int8",
                        "int8",
                        "string",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "int32")
                .addRow(
                        "string", Types.VARCHAR,
                        null,       // precision - todo
                        '\'', '\'', null,
                        typeNoNulls, true, typeSearchable,
                        true,       // unsigned
                        true,       // fixed precision (money)
                        false,      //auto-incr
                        null,
                        null, null, // scale - should be fixed
                        null, null,
                        10
                );
        int[] sizes = {8, 16, 32, 64};
        boolean[] signed = {true, false};
        for (int size : sizes) {
            for (boolean b : signed) {
                String name = (b ? "" : "u") + "int" + size;
                builder.addRow(
                        name, (size <= 16 ? Types.INTEGER : Types.BIGINT),
                        null,       // precision - todo
                        null, null, null,
                        typeNoNulls, true, typePredBasic,
                        !b,         // unsigned
                        true,       // fixed precision (money)
                        false,      //auto-incr
                        null,
                        null, null, // scale - should be fixed
                        null, null,
                        10
                );
            }
        }
        int[] floatSizes = {32, 64};
        for (int floatSize : floatSizes) {
            String name = "float" + floatSize;
            builder.addRow(
                    name, Types.FLOAT,
                    null,       // precision - todo
                    null, null, null,
                    typeNoNulls, true, typePredBasic,
                    false,      // unsigned
                    true,       // fixed precision (money)
                    false,      //auto-incr
                    null,
                    null, null, // scale - should be fixed
                    null, null,
                    10);
        }
        builder.addRow(
                "date", Types.DATE,
                null, // precision - todo
                null, null, null,
                typeNoNulls, true, typePredBasic,
                false, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10);
        builder.addRow(
                "datetime", Types.TIMESTAMP,
                null, // precision - todo
                null, null, null,
                typeNoNulls, true, typePredBasic,
                false, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10);
        return builder.build();
    }

    @Override
    public ResultSet getIndexInfo(String catalog,
                                  String schema,
                                  String table,
                                  boolean unique,
                                  boolean approximate) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog,
                             String schemaPattern,
                             String typeNamePattern,
                             int[] types) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getAttributes(String catalog,
                                   String schemaPattern,
                                   String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return (int) connection.serverContext().majorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return (int) connection.serverContext().minorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return TimeplusDefines.MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return TimeplusDefines.MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctions(String catalog,
                                  String schemaPattern,
                                  String functionNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog,
                                        String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getPseudoColumns(String catalog,
                                      String schemaPattern,
                                      String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public Logger logger() {
        return TimeplusDatabaseMetadata.LOG;
    }


    private ResultSet request(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    private ResultSet getEmptyResultSet() throws SQLException {
        return TimeplusResultSetBuilder
                .builder(1, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames("some")
                .columnTypes("string")
                .build();
    }

    private void buildAndCondition(StringBuilder dest, List<String> conditions) {
        Iterator<String> iter = conditions.iterator();
        if (iter.hasNext()) {
            String entry = iter.next();
            dest.append(entry);
        }
        while (iter.hasNext()) {
            String entry = iter.next();
            dest.append(" AND ").append(entry);
        }
    }
}
