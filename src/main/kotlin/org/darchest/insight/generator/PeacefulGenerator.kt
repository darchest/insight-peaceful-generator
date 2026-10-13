/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.generator

import mu.KotlinLogging
import org.darchest.insight.ConnectionManager
import org.darchest.insight.Table
import org.darchest.insight.Vendor
import java.sql.Connection

class PeacefulGenerator(val genPack: String = "default") {

    private val logger = KotlinLogging.logger {}
    private val schemaAnalyzer = RealSchemaAnalyzer()
    private val planBuilder = ModificationPlanBuilder()

    private val discovery by lazy {
        GeneratableDiscovery(Thread.currentThread().contextClassLoader ?: javaClass.classLoader)
    }
    private val discoveredTables by lazy { discovery.tables(genPack).distinctBy { it.schemaName to it.sqlName } }
    private val discoveredSources by lazy { discovery.recordsSources(discoveredTables) }

    fun tables(): List<Table> = discoveredTables

    fun recordsSources(): List<RecordsSource<*>> = discoveredSources

    companion object {
        /** Catalog discovery includes every generation pack without changing any schema. */
        fun allTables(): List<Table> = GeneratableDiscovery(
            Thread.currentThread().contextClassLoader ?: PeacefulGenerator::class.java.classLoader).tables(null)
            .distinctBy { it.schemaName to it.sqlName }
    }

    suspend fun generate() {
        val sources = recordsSources()
        val tables = tables()
        require(tables.isNotEmpty()) { "No @Generatable tables found for genPack '$genPack'" }
        val connections = tables.map { it.connectionName }.distinct()
        require(connections.size == 1) { "A generation pack must use one connection" }
        val conn = ConnectionManager.getConnection(connections.single())
        val vendor = ConnectionManager.getVendor(connections.single())
        // Create all schemas' tables before inserting any cross-schema seed records.
        val targets = tables.groupBy { it.schemaName ?: conn.schema ?: "public" }
            .map { (name, items) -> items.toSchemaDescriptor(name, vendor) }
        val plans = targets.map { target ->
            SchemaIdentifierValidator.validate(target, vendor)
            planBuilder.build(schemaAnalyzer.analyze(conn, target.name), target).also { logPlan(target.name, it) }
        }
        plans.forEachIndexed { i, plan ->
            inSchema(conn, targets[i].name) {
                executePlan(conn, ModificationPlan(plan.modifications.filter { m -> m.kind != ModificationKind.ADD_FOREIGN_KEY }))
            }
        }
        RecordsGenerator.generate(conn, sources, vendor)
        plans.forEachIndexed { i, plan ->
            inSchema(conn, targets[i].name) {
                executePlan(conn, ModificationPlan(plan.modifications.filter { m -> m.kind == ModificationKind.ADD_FOREIGN_KEY }))
            }
        }
        targets.forEach { target ->
            logResult(target.name, planBuilder.build(schemaAnalyzer.analyze(conn, target.name), target))
        }
    }

    suspend fun generate(conn: Connection, targetSchema: SchemaDescriptor, vendor: Vendor) {
        generate(conn, targetSchema, vendor, emptyList())
    }

    suspend fun generate(conn: Connection, targetSchema: SchemaDescriptor, vendor: Vendor, recordsSources: List<RecordsSource<*>>) {
        SchemaIdentifierValidator.validate(targetSchema, vendor)

        val current = schemaAnalyzer.analyze(conn, targetSchema.name)
        val plan = planBuilder.build(current, targetSchema)
        logPlan(targetSchema.name, plan)

        inSchema(conn, targetSchema.name) {
            executePlan(conn, ModificationPlan(plan.modifications.filter { it.kind != ModificationKind.ADD_FOREIGN_KEY }))
            RecordsGenerator.generate(conn, recordsSources, vendor)
            executePlan(conn, ModificationPlan(plan.modifications.filter { it.kind == ModificationKind.ADD_FOREIGN_KEY }))
        }

        val remaining = planBuilder.build(schemaAnalyzer.analyze(conn, targetSchema.name), targetSchema)
        logResult(targetSchema.name, remaining)
    }

    private suspend fun inSchema(conn: Connection, name: String, block: suspend () -> Unit) {
        val previous = conn.schema
        try {
            conn.schema = name
            block()
        } finally {
            conn.schema = previous
        }
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
