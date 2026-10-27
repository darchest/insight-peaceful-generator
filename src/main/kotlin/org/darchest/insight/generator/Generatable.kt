package org.darchest.insight.generator

import org.darchest.insight.Table

/** Marks a concrete table declaration for a KSP-generated init<GenPack>() extension. */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Generatable(val genPack: String = "default", val actions: Int = ALL) {
    companion object {
        const val CREATE_TABLE = 1
        const val ADD_COLUMN = 1 shl 1
        const val ALTER_COLUMN_SIZE = 1 shl 2
        const val ADD_INDEX = 1 shl 3
        const val ADD_FOREIGN_KEY = 1 shl 4
        const val RECORDS = 1 shl 5
        const val SCHEMA = CREATE_TABLE or ADD_COLUMN or ALTER_COLUMN_SIZE or ADD_INDEX or ADD_FOREIGN_KEY
        const val ALL = SCHEMA or RECORDS
    }
}

/** Unannotated, manually registered tables keep the default generation behavior. */
internal fun Table.generationActions(): Int =
    javaClass.getDeclaredAnnotation(Generatable::class.java)?.actions ?: Generatable.ALL
