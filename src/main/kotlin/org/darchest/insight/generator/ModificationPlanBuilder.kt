/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

class ModificationPlanBuilder {

    fun build(current: SchemaDescriptor, target: SchemaDescriptor): ModificationPlan {
        val modifications = mutableListOf<SchemaModification>()
        val currentTables = current.tables.associateBy { it.name }

        appendCreateTables(modifications, currentTables, target)
        appendAddColumns(modifications, currentTables, target)
        appendAlterColumnSizes(modifications, currentTables, target)
        appendAddIndexes(modifications, currentTables, target)

        return ModificationPlan(modifications)
    }

    private fun appendCreateTables(
        modifications: MutableList<SchemaModification>,
        currentTables: Map<String, TableDescriptor>,
        target: SchemaDescriptor,
    ) {
        target.tables
            .sortedBy { it.name }
            .filter { it.name !in currentTables }
            .forEach { table ->
                modifications.add(
                    CreateTableModification(
                        table = table,
                        description = "Создать таблицу ${table.name}",
                        sql = SchemaDdl.createTableSql(table),
                    )
                )
            }
    }

    private fun appendAddColumns(
        modifications: MutableList<SchemaModification>,
        currentTables: Map<String, TableDescriptor>,
        target: SchemaDescriptor,
    ) {
        target.tables
            .sortedBy { it.name }
            .forEach { targetTable ->
                val currentTable = currentTables[targetTable.name] ?: return@forEach
                val currentColumns = currentTable.columns.associateBy { it.name }
                targetTable.columns
                    .sortedBy { it.ordinalPosition }
                    .filter { it.name !in currentColumns }
                    .forEach { column ->
                        modifications.add(
                            AddColumnModification(
                                tableName = targetTable.name,
                                column = column,
                                description = "Добавить столбец ${targetTable.name}.${column.name}",
                                sql = SchemaDdl.addColumnSql(targetTable.name, column),
                            )
                        )
                    }
            }
    }

    private fun appendAlterColumnSizes(
        modifications: MutableList<SchemaModification>,
        currentTables: Map<String, TableDescriptor>,
        target: SchemaDescriptor,
    ) {
        target.tables
            .sortedBy { it.name }
            .forEach { targetTable ->
                val currentTable = currentTables[targetTable.name] ?: return@forEach
                val currentColumns = currentTable.columns.associateBy { it.name }
                targetTable.columns.forEach { targetColumn ->
                    val currentColumn = currentColumns[targetColumn.name] ?: return@forEach
                    val newSize = targetColumn.columnSize ?: return@forEach
                    if (targetColumn.typeName != currentColumn.typeName)
                        return@forEach
                    if (newSize == currentColumn.columnSize)
                        return@forEach
                    modifications.add(
                        AlterColumnSizeModification(
                            tableName = targetTable.name,
                            columnName = targetColumn.name,
                            typeName = targetColumn.typeName,
                            oldSize = currentColumn.columnSize,
                            newSize = newSize,
                            description = "Изменить размер столбца ${targetTable.name}.${targetColumn.name}: ${currentColumn.columnSize} → $newSize",
                            sql = SchemaDdl.alterColumnSizeSql(
                                targetTable.name,
                                targetColumn.name,
                                targetColumn.typeName,
                                newSize,
                            ),
                        )
                    )
                }
            }
    }

    private fun appendAddIndexes(
        modifications: MutableList<SchemaModification>,
        currentTables: Map<String, TableDescriptor>,
        target: SchemaDescriptor,
    ) {
        target.tables
            .sortedBy { it.name }
            .forEach { targetTable ->
                val currentIndexNames = currentTables[targetTable.name]
                    ?.indexes
                    ?.map { it.name }
                    ?.toSet()
                    ?: emptySet()
                targetTable.indexes
                    .sortedBy { it.name }
                    .filter { it.name !in currentIndexNames }
                    .forEach { index ->
                        modifications.add(
                            AddIndexModification(
                                tableName = targetTable.name,
                                index = index,
                                description = "Добавить индекс ${index.name} на ${targetTable.name}",
                                sql = SchemaDdl.createIndexSql(targetTable.name, index),
                            )
                        )
                    }
            }
    }
}
