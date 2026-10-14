package fixture

import org.darchest.insight.generator.Generatable
import org.darchest.insight.generator.PeacefulGenerator
import org.darchest.insight.generator.RecordsSource
import org.darchest.insight.generator.generated.initTemplates
import org.darchest.insight.generator.generated.initTemplateOtherPack
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotSame

open class TemplateTable(name: String) : FixtureTable(name)

@Generatable("templates")
class BareTemplateTable : TemplateTable("bare")

@Generatable("templates")
class SpecificTemplateTable : TemplateTable("specific")

open class RestrictedTemplateTable(name: String) : TemplateTable(name)

@Generatable("templates")
class NarrowTemplateTable : RestrictedTemplateTable("narrow")

@Generatable("templates")
class OptionalTemplateTable(val flag: Boolean = true) : TemplateTable("optional")

@Generatable("template-other-pack")
class OtherTemplateTable : TemplateTable("other_template")

open class TemplateSource<T : TemplateTable>(private val constructor: () -> T) : RecordsSource<T> {
    override fun tableFactory() = constructor()
    override fun records() = mutableListOf(rec {})
}

class SpecificTemplateSource : TemplateSource<SpecificTemplateTable>(::SpecificTemplateTable) {
    override fun records() = super.records().apply { add(rec {}) }
}

class IndependentTemplateSource : RecordsSource<SpecificTemplateTable> {
    override fun tableFactory() = SpecificTemplateTable()
    override fun records() = mutableListOf(rec {})
}

abstract class IntermediateTemplateSource<T : RestrictedTemplateTable>(private val constructor: () -> T) : RecordsSource<T> {
    override fun tableFactory() = constructor()
    override fun records() = mutableListOf(rec {})
}

class NarrowTemplateSource<T : RestrictedTemplateTable>(constructor: () -> T) : IntermediateTemplateSource<T>(constructor)

class FixedBoundTemplateSource(private val constructor: () -> RestrictedTemplateTable) : RecordsSource<RestrictedTemplateTable> {
    override fun tableFactory() = constructor()
    override fun records() = mutableListOf(rec {})
}

class TemplatesTest {
    @Test
    fun `generic template supplies records to a table without a specific source`() {
        val sources = PeacefulGenerator().initTemplates().recordsSources.map { it() }
        val source = sources.single { it.tableFactory() is BareTemplateTable }
        assertIs<TemplateSource<*>>(source)
        assertIs<BareTemplateTable>(source.records().single())
        assertNotSame(source.rec {}, source.rec {})
    }

    @Test
    fun `independent sources are collected and inherited records are not registered twice`() {
        val sources = PeacefulGenerator().initTemplates().recordsSources.map { it() }
            .filter { it.tableFactory() is SpecificTemplateTable }
        assertEquals(setOf(SpecificTemplateSource::class, IndependentTemplateSource::class), sources.map { it::class }.toSet())
        assertEquals(3, sources.flatMap { it.records() }.size)
    }

    @Test
    fun `template bounds and inherited generic substitutions restrict matching tables`() {
        val sources = PeacefulGenerator().initTemplates().recordsSources.map { it() }
        val restricted = sources.filterIsInstance<NarrowTemplateSource<*>>()
        assertEquals(1, restricted.size)
        assertIs<NarrowTemplateTable>(restricted.single().records().single())
        assertEquals(3, sources.count { it.tableFactory() is NarrowTemplateTable })
        assertIs<NarrowTemplateTable>(sources.filterIsInstance<FixedBoundTemplateSource>().single().records().single())
        val optional = sources.single { it.tableFactory() is OptionalTemplateTable }
        assertEquals(true, assertIs<OptionalTemplateTable>(optional.records().single()).flag)
    }

    @Test
    fun `templates are instantiated only for the selected generation pack`() {
        val generator = PeacefulGenerator().initTemplateOtherPack()
        assertEquals(listOf("other_template"), generator.tables.map { it().sqlName })
        assertIs<OtherTemplateTable>(generator.recordsSources.single()().records().single())
    }
}
