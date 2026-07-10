/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

import mu.KotlinLogging
import java.sql.Connection

class PeacefulGenerator {

    private val logger = KotlinLogging.logger {}
    private val schemaAnalyzer = RealSchemaAnalyzer()
    private val planBuilder = ModificationPlanBuilder()

    suspend fun generate(conn: Connection, targetSchema: SchemaDescriptor) {
        val current = schemaAnalyzer.analyze(conn, targetSchema.name)
        val plan = planBuilder.build(current, targetSchema)
        logPlan(targetSchema.name, plan)

        if (plan.modifications.isNotEmpty())
            executePlan(conn, plan)

        val remaining = planBuilder.build(schemaAnalyzer.analyze(conn, targetSchema.name), targetSchema)
        logResult(targetSchema.name, remaining)
    }

    private fun logPlan(schemaName: String, plan: ModificationPlan) {
        if (plan.modifications.isEmpty()) {
            logger.info { "Изменений для схемы $schemaName не требуется" }
            return
        }
        logger.info { "План изменений схемы $schemaName (${plan.modifications.size} шагов):" }
        plan.modifications.forEach { modification ->
            logger.info { "- ${modification.description}" }
            logger.debug { modification.sql }
        }
    }

    private suspend fun executePlan(conn: Connection, plan: ModificationPlan) {
        plan.modifications.forEach { modification ->
            logger.debug { modification.sql }
            conn.createStatement().use { statement ->
                statement.execute(modification.sql)
            }
        }
    }

    private fun logResult(schemaName: String, remainingPlan: ModificationPlan) {
        if (remainingPlan.modifications.isEmpty()) {
            logger.info { "Схема $schemaName приведена к ожидаемому состоянию" }
            return
        }
        logger.warn {
            "Схема $schemaName не полностью соответствует ожидаемому состоянию, осталось изменений: ${remainingPlan.modifications.size}"
        }
        remainingPlan.modifications.forEach { modification ->
            logger.warn { "- ${modification.description}" }
        }
    }
}
