/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

object SchemaDdl {

    fun createTableSql(table: TableDescriptor): String {
        if (table.columns.isEmpty())
            throw IllegalArgumentException("No columns in table ${table.name}")

        val builder = StringBuilder()
        builder.append("CREATE TABLE IF NOT EXISTS ", table.name, "\n(\n\t")
        val columnIter = table.columns.iterator()
        appendColumnDefinition(builder, columnIter.next())
        while (columnIter.hasNext()) {
            builder.append(",\n\t")
            appendColumnDefinition(builder, columnIter.next())
        }
        table.primaryKey?.let { primaryKey ->
            builder.append(",\n\t")
            appendPrimaryKey(builder, primaryKey)
        }
        builder.append("\n);")
        return builder.toString()
    }

    fun addColumnSql(tableName: String, column: ColumnDescriptor): String {
        val builder = StringBuilder()
        builder.append("ALTER TABLE ", tableName, " ADD COLUMN ")
        appendColumnDefinition(builder, column)
        return builder.toString()
    }

    fun createIndexSql(tableName: String, index: IndexDescriptor): String {
        val builder = StringBuilder()
        if (index.unique)
            builder.append("CREATE UNIQUE INDEX IF NOT EXISTS ")
        else
            builder.append("CREATE INDEX IF NOT EXISTS ")
        builder.append(index.name, " ON ", tableName, " (")
        val columns = index.columns.sortedBy { it.ordinalPosition }
        builder.append(columns.joinToString(", ") { it.name })
        builder.append(");")
        return builder.toString()
    }

    fun alterColumnSizeSql(tableName: String, columnName: String, typeName: String, newSize: Int): String {
        return "ALTER TABLE $tableName ALTER COLUMN $columnName TYPE $typeName($newSize)"
    }

    fun addForeignKeySql(tableName: String, foreignKey: ForeignKeyDescriptor): String {
        val builder = StringBuilder()
        builder.append("ALTER TABLE ", tableName, " ADD ")
        foreignKey.name?.let { name ->
            builder.append("CONSTRAINT ", name, " ")
        }
        appendForeignKey(builder, foreignKey)
        return builder.toString()
    }

    private fun appendColumnDefinition(builder: StringBuilder, column: ColumnDescriptor) {
        builder.append(column.name, ' ', column.typeName)
        column.columnSize?.apply { builder.append("(", this, ")") }
        if (!column.nullable)
            builder.append(" NOT NULL")
        column.defaultValue?.let { defaultValue ->
            builder.append(" DEFAULT ", defaultValue)
        }
    }

    private fun appendPrimaryKey(builder: StringBuilder, primaryKey: PrimaryKeyDescriptor) {
        builder.append("PRIMARY KEY (")
        builder.append(
            primaryKey.columns
                .sortedBy { it.sequence }
                .joinToString(", ") { it.name }
        )
        builder.append(")")
    }

    private fun appendForeignKey(builder: StringBuilder, foreignKey: ForeignKeyDescriptor) {
        builder.append("FOREIGN KEY (")
        builder.append(
            foreignKey.columns
                .sortedBy { it.sequence }
                .joinToString(", ") { it.column }
        )
        builder.append(") REFERENCES ")
        foreignKey.referencedSchema?.apply { builder.append(this, ".") }
        builder.append(foreignKey.referencedTable)
        builder.append(" (")
        builder.append(
            foreignKey.columns
                .sortedBy { it.sequence }
                .joinToString(", ") { it.referencedColumn }
        )
        builder.append(")")
    }
}
