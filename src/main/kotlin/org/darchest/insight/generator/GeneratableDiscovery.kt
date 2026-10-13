package org.darchest.insight.generator

import org.darchest.insight.Table
import java.lang.reflect.Modifier
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.TypeVariable

internal class GeneratableDiscovery(private val loader: ClassLoader) {
    private val classes by lazy { GenerationClasspath.scan(loader) }

    fun tables(genPack: String?): List<Table> = classes.values.filter { it.generatable }.sortedBy { it.name }
        .mapNotNull { definition ->
            val name = definition.name
            val type = Class.forName(name, false, loader)
            val annotation = type.getDeclaredAnnotation(Generatable::class.java) ?: return@mapNotNull null
            if (genPack != null && annotation.genPack != genPack) return@mapNotNull null
            require(!type.isInterface && !Modifier.isAbstract(type.modifiers)) { "@Generatable class must be concrete: $name" }
            require(Table::class.java.isAssignableFrom(type) && !RecordsSource::class.java.isAssignableFrom(type)) {
                "@Generatable must annotate a table declaration, not an entity or RecordsSource: $name"
            }
            val table = requireNotNull(instance(type)) { "@Generatable table must have a public no-argument constructor: $name" } as Table
            require(table.sqlName.isNotBlank()) { "@Generatable table must define a concrete SQL table name: $name" }
            table
        }

    fun recordsSources(tables: List<Table>): List<RecordsSource<*>> {
        val targets = tables.associateBy { it.javaClass }
        if (targets.isEmpty()) return emptyList()
        val sourceTypes = mutableMapOf<String, Boolean>()
        fun isSource(name: String): Boolean {
            if (name == RecordsSource::class.java.name) return true
            sourceTypes[name]?.let { return it }
            sourceTypes[name] = false
            val result = classes[name]?.parents?.any(::isSource) ?: false
            sourceTypes[name] = result
            return result
        }
        val candidates = classes.keys.sorted().filter(::isSource).mapNotNull { name ->
            val type = Class.forName(name, false, loader)
            if (type.isInterface || Modifier.isAbstract(type.modifiers) || !Modifier.isPublic(type.modifiers) ||
                type.isAnonymousClass || type.isLocalClass || type.isSynthetic) return@mapNotNull null
            val tableType = recordsTableType(type) ?: type.getMethod("tableFactory").returnType
            if (tableType !in targets) return@mapNotNull null
            val source = instance(type) as? RecordsSource<*> ?: return@mapNotNull null
            val table = source.tableFactory()
            val target = targets[table.javaClass] ?: return@mapNotNull null
            if (table.schemaName != target.schemaName || table.sqlName != target.sqlName) return@mapNotNull null
            table.javaClass to source
        }.groupBy({ it.first }, { it.second })

        return tables.mapNotNull { table ->
            val sources = candidates[table.javaClass] ?: return@mapNotNull null
            // An override replaces inherited seeds, including an override returning no records.
            val byOwner = sources.groupBy { it.javaClass.getMethod("records").declaringClass }
            val owners = byOwner.keys.filter { owner ->
                byOwner.keys.none { other -> other != owner && owner.isAssignableFrom(other) }
            }
            require(owners.size == 1) {
                "Ambiguous RecordsSource for ${table.javaClass.name}: ${owners.joinToString { it.name }}"
            }
            val owner = owners.single()
            byOwner.getValue(owner).minWith(compareBy<RecordsSource<*>>(
                { distance(it.javaClass, owner) }, { it.javaClass.name },
            ))
        }
    }

    private fun recordsTableType(type: Class<*>): Class<*>? {
        fun resolve(value: Type, bindings: Map<TypeVariable<*>, Type>): Type {
            if (value !is TypeVariable<*>) return value
            val bound = bindings[value] ?: return value
            return if (bound == value) value else resolve(bound, bindings)
        }
        fun walk(value: Type, bindings: Map<TypeVariable<*>, Type>): Class<*>? {
            val raw: Class<*>
            val resolved: Map<TypeVariable<*>, Type>
            when (value) {
                is ParameterizedType -> {
                    raw = value.rawType as? Class<*> ?: return null
                    resolved = bindings + raw.typeParameters.zip(value.actualTypeArguments.map { resolve(it, bindings) })
                }
                is Class<*> -> { raw = value; resolved = bindings }
                else -> return null
            }
            if (raw == RecordsSource::class.java)
                return resolve(raw.typeParameters.single(), resolved) as? Class<*>
            for (parent in raw.genericInterfaces) walk(parent, resolved)?.let { return it }
            return raw.genericSuperclass?.let { walk(it, resolved) }
        }
        return walk(type, emptyMap())
    }

    private fun instance(type: Class<*>): Any? {
        val singleton = type.fields.firstOrNull { it.name == "INSTANCE" && Modifier.isStatic(it.modifiers) && it.type == type }
        if (singleton != null) return singleton.get(null)
        val constructor = try { type.getConstructor() } catch (_: NoSuchMethodException) { return null }
        return constructor.newInstance()
    }

    private fun distance(type: Class<*>, owner: Class<*>): Int {
        var current: Class<*>? = type
        var distance = 0
        while (current != null) {
            if (current == owner) return distance
            current = current.superclass
            distance++
        }
        return Int.MAX_VALUE
    }
}
