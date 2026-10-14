package org.darchest.insight.generator

import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.withContext
import org.darchest.insight.*
import org.darchest.insight.dml.insert
import org.darchest.insight.dml.update
import org.darchest.insight.impl.select
import java.sql.Connection

internal object RecordsGenerator {
    suspend fun generate(conn: Connection, sources: List<RecordsSource<*>>, vendor: Vendor) =
        withContext(ConnectionManager.localConnection.asContextElement(conn)) {
            for (source in sources) {
                for (record in source.records()) upsert(conn, source.tableFactory(), record, vendor)
            }
        }

    private suspend fun upsert(conn: Connection, lookup: Table, record: Table, vendor: Vendor) {
        val keys = record.constraints().filterIsInstance<PrimaryKey>().single().columns.toList()
        require(keys.isNotEmpty() && keys.all { it.state == SqlValue.State.VALUE_SET && it.getValue() != null }) {
            "Seed record must set the primary key of ${record.sqlName}"
        }
        require(record.columns().none { it.state == SqlValue.State.EXPR_SET }) { "Seed records must contain literal values" }
        val keyValues = keys.map { it.name to keyValue(it) }
        fun predicate(table: Table): SqlValue<*, *> = vendor.createLogicalOperation(
            LogicalOperation.Operator.AND,
            keyValues.map { (name, value) ->
                vendor.createComparisonOperation(table.column(name), ComparisonOperation.Operator.EQ, value)
            },
        )

        // Reading a separate table preserves the seed's VALUE_SET column states.
        val finder = select(lookup) {
            connection = conn
            fields(keyValues.map { lookup.column(it.first) })
            where(predicate(lookup))
            limit(1)
        }
        val exists = try {
            finder.read()
            finder.next()
        } finally {
            finder.close()
        }
        if (exists)
            update(record) { predicate(record) }.execute()
        else
            insert(record).execute()
    }

    private suspend fun <J : Any, S : SqlType> keyValue(column: TableColumn<J, S>): SqlConst<J, S> =
        SqlConst(column.getValue(), column.javaClass, column.sqlType)
}
