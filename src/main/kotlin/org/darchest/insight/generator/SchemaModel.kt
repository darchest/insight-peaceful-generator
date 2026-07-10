/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

data class SchemaDescriptor(
    val name: String,
    val tables: List<TableDescriptor>,
)

data class TableDescriptor(
    val name: String,
    val columns: List<ColumnDescriptor>,
    val indexes: List<IndexDescriptor>,
    val primaryKey: PrimaryKeyDescriptor?,
    val foreignKeys: List<ForeignKeyDescriptor>,
)

data class ColumnDescriptor(
    val name: String,
    val ordinalPosition: Int,
    val jdbcType: Int,
    val typeName: String,
    val columnSize: Int?,
    val decimalDigits: Int?,
    val nullable: Boolean,
    val defaultValue: String?,
    val autoIncrement: Boolean,
)

data class IndexDescriptor(
    val name: String,
    val unique: Boolean,
    val columns: List<IndexColumnDescriptor>,
)

data class IndexColumnDescriptor(
    val name: String,
    val ordinalPosition: Int,
    val ascending: Boolean,
)

data class PrimaryKeyDescriptor(
    val name: String?,
    val columns: List<KeyColumnDescriptor>,
)

data class KeyColumnDescriptor(
    val name: String,
    val sequence: Int,
)

data class ForeignKeyDescriptor(
    val name: String?,
    val columns: List<ForeignKeyColumnDescriptor>,
    val referencedSchema: String?,
    val referencedTable: String,
    val onUpdateRule: Int,
    val onDeleteRule: Int,
)

data class ForeignKeyColumnDescriptor(
    val column: String,
    val referencedColumn: String,
    val sequence: Int,
)
