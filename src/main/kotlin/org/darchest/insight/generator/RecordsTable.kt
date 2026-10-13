package org.darchest.insight.generator

import org.darchest.insight.Table
import java.lang.reflect.Constructor
import java.lang.reflect.Modifier
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.TypeVariable

/** Resolves RecordsSource<T> through generic interfaces and superclasses. */
internal object RecordsTable {
    private class TableType(val value: Class<out Table>?)

    private val types = object : ClassValue<TableType>() {
        override fun computeValue(type: Class<*>): TableType = TableType(resolve(type))
    }

    private val constructors = object : ClassValue<Constructor<out Table>>() {
        override fun computeValue(type: Class<*>): Constructor<out Table> {
            require(!type.isInterface && !Modifier.isAbstract(type.modifiers)) {
                "RecordsSource table must be concrete: ${type.name}"
            }
            require(Modifier.isPublic(type.modifiers)) {
                "RecordsSource table must be public: ${type.name}"
            }
            return try {
                type.asSubclass(Table::class.java).getConstructor()
            } catch (exception: NoSuchMethodException) {
                throw IllegalArgumentException(
                    "RecordsSource table must have a public no-argument constructor: ${type.name}", exception,
                )
            }
        }
    }

    fun type(sourceType: Class<*>): Class<out Table>? = types.get(sourceType).value

    fun constructor(sourceType: Class<*>): Constructor<out Table> {
        val tableType = requireNotNull(type(sourceType)) {
            "Cannot resolve RecordsSource<T> table type for ${sourceType.name}; use a concrete table type"
        }
        return constructors.get(tableType)
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Table> create(source: RecordsSource<T>): T = constructor(source.javaClass).newInstance() as T

    private fun resolve(sourceType: Class<*>): Class<out Table>? {
        fun resolveVariable(value: Type, bindings: Map<TypeVariable<*>, Type>): Type {
            if (value !is TypeVariable<*>) return value
            val bound = bindings[value] ?: return value
            return if (bound == value) value else resolveVariable(bound, bindings)
        }
        fun walk(value: Type, bindings: Map<TypeVariable<*>, Type>): Class<out Table>? {
            val raw: Class<*>
            val resolved: Map<TypeVariable<*>, Type>
            when (value) {
                is ParameterizedType -> {
                    raw = value.rawType as? Class<*> ?: return null
                    resolved = bindings + raw.typeParameters.zip(value.actualTypeArguments.map { resolveVariable(it, bindings) })
                }
                is Class<*> -> { raw = value; resolved = bindings }
                else -> return null
            }
            if (raw == RecordsSource::class.java) {
                val tableType = resolveVariable(raw.typeParameters.single(), resolved) as? Class<*> ?: return null
                return tableType.takeIf { Table::class.java.isAssignableFrom(it) }?.asSubclass(Table::class.java)
            }
            for (parent in raw.genericInterfaces) walk(parent, resolved)?.let { return it }
            return raw.genericSuperclass?.let { walk(it, resolved) }
        }
        return walk(sourceType, emptyMap())
    }
}
