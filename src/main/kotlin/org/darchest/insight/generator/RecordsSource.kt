package org.darchest.insight.generator

import org.darchest.insight.Table

interface RecordsSource<T : Table> {
    fun records(): MutableList<T> = mutableListOf()

    fun rec(builder: T.() -> Unit): T = RecordsTable.create(this).apply(builder)
}
