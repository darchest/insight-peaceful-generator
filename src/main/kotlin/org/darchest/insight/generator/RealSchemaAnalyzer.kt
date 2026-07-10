/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet

class RealSchemaAnalyzer {

    fun analyze(connection: Connection, schemaName: String): SchemaDescriptor {
        val meta = connection.metaData
        requireSchemaExists(meta, schemaName)
        return SchemaDescriptor(
            name = schemaName,
            tables = readTables(meta, schemaName),
        )
    }

    private fun requireSchemaExists(meta: DatabaseMetaData, schemaName: String) {
        meta.schemas.use { rs ->
            while (rs.next()) {
                if (schemaName == rs.getString("TABLE_SCHEM"))
                    return
            }
        }
        throw IllegalArgumentException("Schema $schemaName does not exist")
    }

    private fun readTables(meta: DatabaseMetaData, schemaName: String): List<TableDescriptor> {
        val tables = mutableListOf<TableDescriptor>()
        meta.getTables(null, schemaName, null, arrayOf("TABLE")).use { rs ->
            while (rs.next()) {
                val tableName = rs.getString("TABLE_NAME")
                tables.add(
                    TableDescriptor(
                        name = tableName,
                        columns = readColumns(meta, schemaName, tableName),
                        indexes = readIndexes(meta, schemaName, tableName),
                        primaryKey = readPrimaryKey(meta, schemaName, tableName),
                        foreignKeys = readForeignKeys(meta, schemaName, tableName),
                    )
                )
            }
        }
        return tables.sortedBy { it.name }
    }

    private fun readColumns(meta: DatabaseMetaData, schemaName: String, tableName: String): List<ColumnDescriptor> {
        val columns = mutableListOf<ColumnDescriptor>()
        meta.getColumns(null, schemaName, tableName, null).use { rs ->
            while (rs.next()) {
                columns.add(
                    ColumnDescriptor(
                        name = rs.getString("COLUMN_NAME"),
                        ordinalPosition = rs.getInt("ORDINAL_POSITION"),
                        jdbcType = rs.getInt("DATA_TYPE"),
                        typeName = rs.getString("TYPE_NAME"),
                        columnSize = rs.getIntOrNull("COLUMN_SIZE"),
                        decimalDigits = rs.getIntOrNull("DECIMAL_DIGITS"),
                        nullable = rs.getString("IS_NULLABLE") == "YES",
                        defaultValue = rs.getString("COLUMN_DEF"),
                        autoIncrement = rs.getString("IS_AUTOINCREMENT") == "YES",
                    )
                )
            }
        }
        return columns.sortedBy { it.ordinalPosition }
    }

    private fun readIndexes(meta: DatabaseMetaData, schemaName: String, tableName: String): List<IndexDescriptor> {
        val indexes = linkedMapOf<String, IndexBuilder>()
        meta.getIndexInfo(null, schemaName, tableName, false, false).use { rs ->
            while (rs.next()) {
                val indexName = rs.getString("INDEX_NAME") ?: continue
                val builder = indexes.getOrPut(indexName) {
                    IndexBuilder(
                        name = indexName,
                        unique = !rs.getBoolean("NON_UNIQUE"),
                    )
                }
                builder.columns.add(
                    IndexColumnDescriptor(
                        name = rs.getString("COLUMN_NAME"),
                        ordinalPosition = rs.getShort("ORDINAL_POSITION").toInt(),
                        ascending = rs.getString("ASC_OR_DESC") != "D",
                    )
                )
            }
        }
        return indexes.values
            .map { builder ->
                IndexDescriptor(
                    name = builder.name,
                    unique = builder.unique,
                    columns = builder.columns.sortedBy { it.ordinalPosition },
                )
            }
            .sortedBy { it.name }
    }

    private fun readPrimaryKey(meta: DatabaseMetaData, schemaName: String, tableName: String): PrimaryKeyDescriptor? {
        var pkName: String? = null
        val columns = mutableListOf<KeyColumnDescriptor>()
        meta.getPrimaryKeys(null, schemaName, tableName).use { rs ->
            while (rs.next()) {
                if (pkName == null)
                    pkName = rs.getString("PK_NAME")
                columns.add(
                    KeyColumnDescriptor(
                        name = rs.getString("COLUMN_NAME"),
                        sequence = rs.getShort("KEY_SEQ").toInt(),
                    )
                )
            }
        }
        if (columns.isEmpty())
            return null
        return PrimaryKeyDescriptor(
            name = pkName,
            columns = columns.sortedBy { it.sequence },
        )
    }

    private fun readForeignKeys(meta: DatabaseMetaData, schemaName: String, tableName: String): List<ForeignKeyDescriptor> {
        val foreignKeys = linkedMapOf<String, ForeignKeyBuilder>()
        meta.getImportedKeys(null, schemaName, tableName).use { rs ->
            while (rs.next()) {
                val fkName = rs.getString("FK_NAME") ?: continue
                val builder = foreignKeys.getOrPut(fkName) {
                    ForeignKeyBuilder(
                        name = fkName,
                        referencedSchema = rs.getString("PKTABLE_SCHEM"),
                        referencedTable = rs.getString("PKTABLE_NAME"),
                        onUpdateRule = rs.getShort("UPDATE_RULE").toInt(),
                        onDeleteRule = rs.getShort("DELETE_RULE").toInt(),
                    )
                }
                builder.columns.add(
                    ForeignKeyColumnDescriptor(
                        column = rs.getString("FKCOLUMN_NAME"),
                        referencedColumn = rs.getString("PKCOLUMN_NAME"),
                        sequence = rs.getShort("KEY_SEQ").toInt(),
                    )
                )
            }
        }
        return foreignKeys.values
            .map { builder ->
                ForeignKeyDescriptor(
                    name = builder.name,
                    columns = builder.columns.sortedBy { it.sequence },
                    referencedSchema = builder.referencedSchema,
                    referencedTable = builder.referencedTable,
                    onUpdateRule = builder.onUpdateRule,
                    onDeleteRule = builder.onDeleteRule,
                )
            }
            .sortedBy { it.name ?: "" }
    }

    private fun ResultSet.getIntOrNull(columnLabel: String): Int? {
        val value = getInt(columnLabel)
        return if (wasNull()) null else value
    }

    private class IndexBuilder(
        val name: String,
        val unique: Boolean,
        val columns: MutableList<IndexColumnDescriptor> = mutableListOf(),
    )

    private class ForeignKeyBuilder(
        val name: String,
        val referencedSchema: String?,
        val referencedTable: String,
        val onUpdateRule: Int,
        val onDeleteRule: Int,
        val columns: MutableList<ForeignKeyColumnDescriptor> = mutableListOf(),
    )
}
