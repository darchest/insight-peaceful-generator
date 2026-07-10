/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

import org.darchest.insight.Vendor

object SchemaIdentifierValidator {

    fun validate(targetSchema: SchemaDescriptor, vendor: Vendor) {
        val maxLength = vendor.getMaxIdentifierLength()
        val violations = mutableListOf<String>()

        checkIdentifier("схема", targetSchema.name, maxLength, violations)

        targetSchema.tables.forEach { table ->
            checkIdentifier("таблица", table.name, maxLength, violations)

            table.columns.forEach { column ->
                checkIdentifier("столбец '${table.name}'.'${column.name}'", column.name, maxLength, violations)
            }

            table.indexes.forEach { index ->
                checkIdentifier("индекс '${index.name}' на '${table.name}'", index.name, maxLength, violations)
            }

            table.primaryKey?.name?.let { name ->
                checkIdentifier("первичный ключ '${name}' на '${table.name}'", name, maxLength, violations)
            }

            table.foreignKeys.forEach { foreignKey ->
                foreignKey.name?.let { name ->
                    checkIdentifier("внешний ключ '${name}' на '${table.name}'", name, maxLength, violations)
                }
                foreignKey.referencedSchema?.let { schema ->
                    checkIdentifier(
                        "ссылка на схему '${schema}' из '${table.name}'",
                        schema,
                        maxLength,
                        violations,
                    )
                }
                checkIdentifier(
                    "ссылка на таблицу '${foreignKey.referencedTable}' из '${table.name}'",
                    foreignKey.referencedTable,
                    maxLength,
                    violations,
                )
                foreignKey.columns.forEach { column ->
                    checkIdentifier(
                        "столбец внешнего ключа '${table.name}'.'${column.column}'",
                        column.column,
                        maxLength,
                        violations,
                    )
                    checkIdentifier(
                        "ссылочный столбец '${foreignKey.referencedTable}'.'${column.referencedColumn}'",
                        column.referencedColumn,
                        maxLength,
                        violations,
                    )
                }
            }
        }

        if (violations.isNotEmpty()) {
            throw IllegalArgumentException(
                "Идентификаторы превышают максимальную длину $maxLength: ${violations.joinToString(", ")}"
            )
        }
    }

    private fun checkIdentifier(
        context: String,
        name: String,
        maxLength: Int,
        violations: MutableList<String>,
    ) {
        if (name.length > maxLength)
            violations.add("$context '$name' (${name.length})")
    }
}
