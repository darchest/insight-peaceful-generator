package fixture

import org.darchest.insight.Table
import org.darchest.insight.Vendor
import org.darchest.insight.generator.Generatable
import org.darchest.insight.generator.PeacefulGenerator
import org.darchest.insight.generator.RecordsSource
import org.darchest.insight.generator.generated.initDefault
import org.darchest.insight.generator.generated.initOtherPack
import org.darchest.insight.generator.generated.initAliases
import org.darchest.insight.generator.generated.initEscaped
import org.darchest.insight.generator.generated.initDefaultArguments
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue

abstract class FixtureTable(name: String) : Table(name) {
    override fun vendor(): Vendor = error("The initializer must not access the database")
}

object CreationCounts {
    var tables = 0
    var sources = 0
}

@Generatable
class DefaultTable : FixtureTable("default_table") {
    init { CreationCounts.tables++ }
}

open class BaseSource : RecordsSource<DefaultTable> {
    init { CreationCounts.sources++ }
    override fun tableFactory() = DefaultTable()
    override fun records() = mutableListOf(DefaultTable())
}

open class OverrideSource : BaseSource() {
    override fun records() = mutableListOf<DefaultTable>()
}

class InheritedSource : OverrideSource()

@Generatable("other-pack")
class OtherTable : FixtureTable("other_table")

abstract class GenericSource<A, T : Table>(private val constructor: () -> T) : RecordsSource<T> {
    override fun tableFactory() = constructor()
}
abstract class IntermediateSource<T : Table>(constructor: () -> T) : GenericSource<String, T>(constructor)
class OtherSource : IntermediateSource<OtherTable>(::OtherTable)

class UnannotatedTable : FixtureTable("unannotated_table")
class UnannotatedSource : RecordsSource<UnannotatedTable> {
    override fun tableFactory() = UnannotatedTable()
    init { error("Sources for unannotated tables must not be initialized") }
}

class InitializersTest {
    @Test
    fun `initializers select packs and the most specific records override`() {
        val generator = PeacefulGenerator().initDefault()
        assertEquals(listOf("default_table"), generator.tables.map { it().sqlName })
        assertEquals(OverrideSource::class.java.name, generator.recordsSources.single()().javaClass.name)
        assertTrue(generator.recordsSources.single()().records().isEmpty())

        generator.initOtherPack()
        assertEquals(listOf("other_table"), generator.tables.map { it().sqlName })
        val source = assertIs<OtherSource>(generator.recordsSources.single()())
        assertIs<OtherTable>(source.rec {})
    }

    @Test
    fun `reinitialization replaces both lists with factories that create fresh instances`() {
        val generator = PeacefulGenerator().initDefault()
        val previousTable = generator.tables.single()()
        val previousSource = generator.recordsSources.single()()
        generator.tables += ::UnannotatedTable
        assertSame(generator, generator.initDefault())
        assertEquals(1, generator.tables.size)
        assertEquals(1, generator.recordsSources.size)
        assertNotSame(previousTable, generator.tables.single()())
        assertNotSame(previousSource, generator.recordsSources.single()())
    }

    @Test
    fun `manual registration needs no generated initializer`() {
        val generator = PeacefulGenerator()
        assertTrue(generator.tables.isEmpty())
        assertTrue(generator.recordsSources.isEmpty())
        val constructor = ::UnannotatedTable
        generator.tables += constructor
        assertSame(constructor, generator.tables.single())
        assertIs<UnannotatedTable>(generator.tables.single()())
    }

    @Test
    fun `initialization stores constructors without constructing tables or sources`() {
        CreationCounts.tables = 0
        CreationCounts.sources = 0
        val generator = PeacefulGenerator().initDefault()
        assertEquals(0, CreationCounts.tables)
        assertEquals(0, CreationCounts.sources)
        generator.tables.single()()
        generator.recordsSources.single()().rec {}
        assertEquals(2, CreationCounts.tables)
        assertEquals(1, CreationCounts.sources)
    }

    @Test
    fun `constructor imports disambiguate duplicate and reserved simple names`() {
        val generator = PeacefulGenerator().initAliases()
        assertEquals(setOf("left", "right", "reserved", "already_suffixed"), generator.tables.map { it().sqlName }.toSet())
    }

    @Test
    fun `only keyword names are escaped and singleton factories remain usable`() {
        val generator = PeacefulGenerator().initEscaped()
        assertEquals(setOf("keyword", "singleton"), generator.tables.map { it().sqlName }.toSet())
    }

    @Test
    fun `constructor references retain default arguments without reflection`() {
        val generator = PeacefulGenerator().initDefaultArguments()
        assertTrue(assertIs<DefaultArgumentsTable>(generator.tables.single()()).flag)
        val source = assertIs<DefaultArgumentsSource>(generator.recordsSources.single()())
        assertTrue(source.flag)
        assertTrue(source.rec {}.flag)
    }
}
