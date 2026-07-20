/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

enum class ModificationKind {
    CREATE_TABLE,
    ADD_COLUMN,
    ADD_INDEX,
    ALTER_COLUMN_SIZE,
    ADD_FOREIGN_KEY,
}

sealed interface SchemaModification {
    val kind: ModificationKind
    val description: String
    val sql: String
}

data class CreateTableModification(
    val table: TableDescriptor,
    override val description: String,
    override val sql: String,
) : SchemaModification {
    override val kind = ModificationKind.CREATE_TABLE
}

data class AddColumnModification(
    val tableName: String,
    val column: ColumnDescriptor,
    override val description: String,
    override val sql: String,
) : SchemaModification {
    override val kind = ModificationKind.ADD_COLUMN
}

data class AddIndexModification(
    val tableName: String,
    val index: IndexDescriptor,
    override val description: String,
    override val sql: String,
) : SchemaModification {
    override val kind = ModificationKind.ADD_INDEX
}

data class AlterColumnSizeModification(
    val tableName: String,
    val columnName: String,
    val typeName: String,
    val oldSize: Int?,
    val newSize: Int,
    override val description: String,
    override val sql: String,
) : SchemaModification {
    override val kind = ModificationKind.ALTER_COLUMN_SIZE
}

data class AddForeignKeyModification(
    val tableName: String,
    val foreignKey: ForeignKeyDescriptor,
    override val description: String,
    override val sql: String,
) : SchemaModification {
    override val kind = ModificationKind.ADD_FOREIGN_KEY
}

data class ModificationPlan(
    val modifications: List<SchemaModification>,
)
