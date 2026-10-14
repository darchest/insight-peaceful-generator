package org.darchest.insight.generator

import org.darchest.insight.Table

interface RecordsSource<T : Table> {
    /** Creates a fresh table; ORM entities already implement this through their constructor factory. */
    fun tableFactory(): T

    fun records(): MutableList<T> = mutableListOf()

    fun rec(builder: T.() -> Unit): T = tableFactory().apply(builder)
}
