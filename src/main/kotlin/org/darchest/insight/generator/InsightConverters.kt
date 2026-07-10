/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

import org.darchest.insight.*
import java.sql.DatabaseMetaData
import java.sql.Types

suspend fun TableColumn<*, *>.toDescriptor(ordinalPosition: Int, vendor: Vendor): ColumnDescriptor {
    val def = default()
    return ColumnDescriptor(
        name = name,
        ordinalPosition = ordinalPosition,
        jdbcType = Types.OTHER,
        typeName = getSqlStringType(),
        columnSize = length,
        decimalDigits = null,
        nullable = false,
        defaultValue = def?.let { SqlTypeConvertersRegistry.javaToSql(it.javaClass, it.sqlClass, it.getValue()) },
        autoIncrement = false,
    )
}

suspend fun Index.toDescriptor(vendor: Vendor): IndexDescriptor {
    return IndexDescriptor(
        name = name,
        unique = unique,
        columns = columns.mapIndexed { index, column ->
            IndexColumnDescriptor(
                name = column.sqlName(vendor),
                ordinalPosition = index + 1,
                ascending = true,
            )
        },
    )
}

fun PrimaryKey.toDescriptor(): PrimaryKeyDescriptor {
    return PrimaryKeyDescriptor(
        name = null,
        columns = columns.mapIndexed { index, column ->
            KeyColumnDescriptor(
                name = column.name,
                sequence = index + 1,
            )
        },
    )
}

fun ForeignKey<*>.toDescriptor(): ForeignKeyDescriptor {
    return ForeignKeyDescriptor(
        name = null,
        columns = columns.zip(foreignColumns).mapIndexed { index, (column, foreignColumn) ->
            ForeignKeyColumnDescriptor(
                column = column.name,
                referencedColumn = foreignColumn.name,
                sequence = index + 1,
            )
        },
        referencedSchema = foreignTable.schemaName,
        referencedTable = foreignTable.sqlName,
        onUpdateRule = DatabaseMetaData.importedKeyNoAction,
        onDeleteRule = DatabaseMetaData.importedKeyNoAction,
    )
}

private fun Unique.toIndexDescriptor(): IndexDescriptor {
    val columnNames = columns.map { it.name }
    return IndexDescriptor(
        name = columnNames.joinToString("_") + "_unique",
        unique = true,
        columns = columnNames.mapIndexed { index, columnName ->
            IndexColumnDescriptor(
                name = columnName,
                ordinalPosition = index + 1,
                ascending = true,
            )
        },
    )
}

suspend fun Table.toDescriptor(vendor: Vendor = this.vendor()): TableDescriptor {
    val constraints = constraints()
    val uniqueIndexes = constraints.filterIsInstance<Unique>().map { it.toIndexDescriptor() }
    return TableDescriptor(
        name = sqlName,
        columns = columns().mapIndexed { index, column ->
            column.toDescriptor(index + 1, vendor)
        },
        indexes = indexes().map { it.toDescriptor(vendor) } + uniqueIndexes,
        primaryKey = constraints.filterIsInstance<PrimaryKey>().firstOrNull()?.toDescriptor(),
        foreignKeys = constraints.filterIsInstance<ForeignKey<*>>().map { it.toDescriptor() },
    )
}

suspend fun Collection<Table>.toSchemaDescriptor(schemaName: String, vendor: Vendor? = null): SchemaDescriptor {
    if (isEmpty())
        throw IllegalArgumentException("Tables collection is empty")
    val resolvedVendor = vendor ?: first().vendor()
    return SchemaDescriptor(
        name = schemaName,
        tables = map { it.toDescriptor(resolvedVendor) },
    )
}

private suspend fun SqlValue<*, *>.sqlName(vendor: Vendor): String {
    if (this is TableColumn<*, *>)
        return name
    return getSql(vendor).first
}
