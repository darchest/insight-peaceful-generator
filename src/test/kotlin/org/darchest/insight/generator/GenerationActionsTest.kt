package org.darchest.insight.generator

import kotlinx.coroutines.runBlocking
import org.darchest.insight.Table
import org.darchest.insight.Vendor
import java.sql.Types
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Generatable(actions = 0)
private class CatalogOnlyTable : Table("catalog_only") {
    override fun vendor(): Vendor = error("A catalog-only table must not resolve its vendor")
}

class GenerationActionsTest {
    private val column = ColumnDescriptor("label", 1, Types.VARCHAR, "varchar", 80, null, false, null, false)
    private val table = TableDescriptor(
        name = "existing",
        columns = listOf(column, column.copy(name = "new_column", ordinalPosition = 2)),
        indexes = listOf(IndexDescriptor("label_idx", false, listOf(IndexColumnDescriptor("label", 1, true)))),
        primaryKey = null,
        foreignKeys = listOf(ForeignKeyDescriptor(
            "parent_fk", listOf(ForeignKeyColumnDescriptor("label", "label", 1)), "public", "parent", 3, 3,
        )),
    )
    private val current = SchemaDescriptor("public", listOf(table.copy(
        columns = listOf(column.copy(columnSize = 40)), indexes = emptyList(), foreignKeys = emptyList(),
    )))

    private fun plan(actions: Int) = ModificationPlanBuilder().build(current, SchemaDescriptor("public", listOf(
        table.copy(actions = actions), table.copy(name = "new_table", actions = actions),
    )))

    @Test
    fun `default actions preserve every schema modification kind`() {
        assertEquals(Generatable.ALL, table.actions)
        assertEquals(ModificationKind.entries.toSet(), plan(Generatable.ALL).modifications.map { it.kind }.toSet())
    }

    @Test
    fun `each schema flag permits only its modification kind`() {
        val flags = mapOf(
            Generatable.CREATE_TABLE to ModificationKind.CREATE_TABLE,
            Generatable.ADD_COLUMN to ModificationKind.ADD_COLUMN,
            Generatable.ALTER_COLUMN_SIZE to ModificationKind.ALTER_COLUMN_SIZE,
            Generatable.ADD_INDEX to ModificationKind.ADD_INDEX,
            Generatable.ADD_FOREIGN_KEY to ModificationKind.ADD_FOREIGN_KEY,
        )
        for ((flag, kind) in flags) {
            assertEquals(setOf(kind), plan(flag).modifications.map { it.kind }.toSet())
        }
        assertEquals(setOf(ModificationKind.ADD_COLUMN, ModificationKind.ADD_INDEX),
            plan(Generatable.ADD_COLUMN or Generatable.ADD_INDEX).modifications.map { it.kind }.toSet())
    }

    @Test
    fun `zero and records-only flags produce no schema changes`() {
        assertTrue(plan(0).modifications.isEmpty())
        assertTrue(plan(Generatable.RECORDS).modifications.isEmpty())
    }

    @Test
    fun `foreign key SQL quotes mixed case, spaces, and embedded quotes in identifiers`() {
        val foreignKey = ForeignKeyDescriptor(
            name = "fk\"name",
            columns = listOf(
                ForeignKeyColumnDescriptor("Other Column", "OtherId", 2),
                ForeignKeyColumnDescriptor("LocalId", "RecordId", 1),
            ),
            referencedSchema = "Schema Name",
            referencedTable = "Пикеты",
            onUpdateRule = 3,
            onDeleteRule = 3,
        )
        assertEquals(
            "ALTER TABLE \"Ground Cards\" ADD CONSTRAINT \"fk\"\"name\" FOREIGN KEY (\"LocalId\", \"Other Column\") REFERENCES \"Schema Name\".\"Пикеты\" (\"RecordId\", \"OtherId\")",
            SchemaDdl.addForeignKeySql("Ground Cards", foreignKey),
        )
        assertEquals(
            "ALTER TABLE \"Ground Cards\" ADD FOREIGN KEY (\"LocalId\", \"Other Column\") REFERENCES \"Пикеты\" (\"RecordId\", \"OtherId\")",
            SchemaDdl.addForeignKeySql("Ground Cards", foreignKey.copy(name = null, referencedSchema = null)),
        )
    }

    @Test
    fun `create table SQL quotes primary key columns`() {
        val pk = PrimaryKeyDescriptor(null, listOf(KeyColumnDescriptor("RecordId", 1)))
        val ddl = SchemaDdl.createTableSql(table.copy(columns = listOf(column.copy(name = "RecordId")), primaryKey = pk))
        assertTrue(ddl.contains("PRIMARY KEY (\"RecordId\")"), ddl)
    }

    @Test
    fun `catalog-only pack retains tables and skips connection and record-source initialization`() = runBlocking {
        val generator = PeacefulGenerator()
        generator.tables += ::CatalogOnlyTable
        generator.recordsSources += { error("A disabled pack must not instantiate its record sources") }
        generator.generate()
        assertEquals("catalog_only", generator.tables.single()().sqlName)
    }
}
