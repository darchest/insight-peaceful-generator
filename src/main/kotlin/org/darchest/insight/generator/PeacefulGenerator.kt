/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

import mu.KotlinLogging
import org.darchest.insight.Index
import org.darchest.insight.SqlTypeConverter
import org.darchest.insight.Table
import org.darchest.insight.TableColumn
import org.darchest.insight.ddl.CreateTable
import java.sql.Connection
import java.sql.DatabaseMetaData

class Schema(val name: String, val tables: List<Table>)

class PeacefulGenerator {

    private val logger = KotlinLogging.logger {}

    suspend fun generate(conn: Connection, targetSchema: Schema) {
        val meta = conn.metaData

        if (!isSchemaExists(meta, targetSchema))
            throw RuntimeException("Schema ${targetSchema.name} isn't exists. Create it by itself")

        val existsTables = existsTables(meta, targetSchema)

        val missingTables = missingTables(existsTables, targetSchema)

        generateNewTables(conn, missingTables)

        val actualTables = actualTables(existsTables, targetSchema)

        actualyzeTables(conn, meta, targetSchema, actualTables)

        // val unwantedTables = unwantedTables(existsTables, targetSchema)
    }

    private fun isSchemaExists(meta: DatabaseMetaData, targetSchema: Schema): Boolean {
        val schemasRs = meta.schemas
        schemasRs.use {
            while (schemasRs.next()) {
                if (targetSchema.name == schemasRs.getString("TABLE_SCHEM"))
                    return true
            }
            return false
        }
    }

    private fun existsTables(meta: DatabaseMetaData, targetSchema: Schema): List<String> {
        val existsTables = mutableListOf<String>()
        val tablesRs = meta.getTables(null, targetSchema.name, null, arrayOf("TABLE"))
        tablesRs.use {
            while (tablesRs.next()) {
                existsTables.add(tablesRs.getString("TABLE_NAME"))
            }
        }
        return existsTables
    }

    private fun missingTables(existingTables: List<String>, targetSchema: Schema): List<Table> {
        val missingTables = mutableListOf<Table>()
        targetSchema.tables.forEach { t ->
            val tableName = t.sqlName
            if (!existingTables.contains(tableName))
                missingTables.add(t)
        }
        return missingTables
    }

    private fun actualTables(existingTables: List<String>, targetSchema: Schema): List<Table> {
        val actualTables = mutableListOf<Table>()
        targetSchema.tables.forEach { t ->
            val tableName = t.sqlName
            if (existingTables.contains(tableName))
                actualTables.add(t)
        }
        return actualTables
    }

    private fun unwantedTables(existingTables: List<String>, targetSchema: Schema): List<String> {
        val unwantedTables = mutableListOf<String>()
        existingTables.forEach { tblName ->
            if (targetSchema.tables.indexOfFirst { t -> t.sqlName == tblName } == -1)
                unwantedTables.add(tblName)
        }
        return unwantedTables
    }

    private suspend fun generateNewTables(conn: Connection, tables: List<Table>) {
        tables.forEach { t -> generateNewTable(conn, t) }
    }

    private suspend fun generateNewTable(conn: Connection, table: Table) {
        CreateTable(table).execute()
    }

    private suspend fun actualyzeTables(conn: Connection, meta: DatabaseMetaData, targetSchema: Schema, tables: List<Table>) {
        tables.forEach { t -> actualyzeTable(conn, meta, targetSchema, t) }
    }

    private suspend fun actualyzeTable(conn: Connection, meta: DatabaseMetaData, targetSchema: Schema, table: Table) {
        processTableColumns(conn, meta, targetSchema, table)
        processTableIndexes(conn, meta, targetSchema, table)
    }

    private suspend fun processTableColumns(conn: Connection, meta: DatabaseMetaData, targetSchema: Schema, table: Table) {
        val existsColumns = existsColumns(conn, meta, targetSchema, table)

        val missingColumns = missingColumns(existsColumns, table)

        generateNewColumns(conn, targetSchema, table, missingColumns)
    }

    private suspend fun processTableIndexes(conn: Connection, meta: DatabaseMetaData, targetSchema: Schema, table: Table) {
        val existsIndexes = existsIndexes(conn, meta, targetSchema, table)

        val missingIndexes = missingIndexes(existsIndexes, table)

        generateNewIndexes(conn, targetSchema, table, missingIndexes)
    }

    private fun existsColumns(conn: Connection, meta: DatabaseMetaData, targetSchema: Schema, table: Table): List<String> {
        val existsColumns = mutableListOf<String>()
        val columnsRs = meta.getColumns(null, targetSchema.name, table.sqlName, null)
        columnsRs.use {
            while (columnsRs.next())
                existsColumns.add(columnsRs.getString("COLUMN_NAME"))
        }
        return existsColumns
    }

    private fun missingColumns(existing: List<String>, table: Table): List<TableColumn<*, *>> {
        val missing = mutableListOf<TableColumn<*, *>>()
        table.columns().forEach { c ->
            val columnName = c.name
            if (!existing.contains(columnName))
                missing.add(c)
        }
        return missing
    }

    private suspend fun generateNewColumns(conn: Connection, targetSchema: Schema, table: Table, columns: List<TableColumn<*, *>>) {
        columns.forEach { column -> generateNewColumn(conn, targetSchema, table, column) }
    }

    private suspend fun generateNewColumn(conn: Connection, targetSchema: Schema, table: Table, column: TableColumn<*, *>) {
        val builder = StringBuilder()
        builder.append("ALTER TABLE ", table.sqlName, " ADD COLUMN ", column.name, ' ', column.getSqlStringType())
        column.length?.apply { builder.append("(", this, ")") }
        builder.append(" NOT NULL")
        val def = column.default()
        if (def != null) {
            builder.append(" DEFAULT ")
            builder.append(SqlTypeConverter.javaToSql(def.javaClass, def.sqlClass, def.getValue()))
        }
        logger.debug { builder }
        conn.prepareStatement(builder.toString()).use { statement ->
            statement.execute()
        }
    }

    private fun existsIndexes(conn: Connection, meta: DatabaseMetaData, targetSchema: Schema, table: Table): List<String> {
        val exists = mutableListOf<String>()
        val rs = meta.getIndexInfo(null, targetSchema.name, table.sqlName, true, false)
        rs.use {
            while (rs.next())
                exists.add(rs.getString("INDEX_NAME"))
        }
        return exists
    }

    private fun missingIndexes(existing: List<String>, table: Table): List<Index> {
        val missing = mutableListOf<Index>()
        table.indexes().forEach { index ->
            val name = index.name
            if (!existing.contains(name))
                missing.add(index)
        }
        return missing
    }

    private suspend fun generateNewIndexes(conn: Connection, targetSchema: Schema, table: Table, indexes: List<Index>) {
        indexes.forEach { index -> generateNewIndex(conn, targetSchema, table, index) }
    }

    private suspend fun generateNewIndex(conn: Connection, targetSchema: Schema, table: Table, index: Index) {
        val builder = StringBuilder()
        builder.append("CREATE INDEX IF NOT EXISTS ", index.name, " ON ", table.sqlName, " (")
        val iter = index.columns.iterator()
        var col = iter.next()
        builder.append(col.getSql(table.vendor()).first)
        while (iter.hasNext()) {
            builder.append(", ")
            col = iter.next()
            builder.append(col.getSql(table.vendor()).first)
        }
        builder.append(");")
        logger.debug { builder }
        conn.createStatement().use { statement ->
            statement.execute(builder.toString())
        }
    }
}