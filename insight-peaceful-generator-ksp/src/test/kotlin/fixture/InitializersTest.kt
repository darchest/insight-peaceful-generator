package fixture

import org.darchest.insight.Table
import org.darchest.insight.Vendor
import org.darchest.insight.generator.Generatable
import org.darchest.insight.generator.PeacefulGenerator
import org.darchest.insight.generator.RecordsSource
import org.darchest.insight.generator.generated.initDefault
import org.darchest.insight.generator.generated.initOtherPack
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue

abstract class FixtureTable(name: String) : Table(name) {
    override fun vendor(): Vendor = error("The initializer must not access the database")
}

@Generatable
class DefaultTable : FixtureTable("default_table")

open class BaseSource : RecordsSource<DefaultTable> {
    override fun records() = mutableListOf(DefaultTable())
}

open class OverrideSource : BaseSource() {
    override fun records() = mutableListOf<DefaultTable>()
}

class InheritedSource : OverrideSource()

@Generatable("other-pack")
class OtherTable : FixtureTable("other_table")

abstract class GenericSource<A, T : Table> : RecordsSource<T>
abstract class IntermediateSource<T : Table> : GenericSource<String, T>()
class OtherSource : IntermediateSource<OtherTable>()

class UnannotatedTable : FixtureTable("unannotated_table")
class UnannotatedSource : RecordsSource<UnannotatedTable> {
    init { error("Sources for unannotated tables must not be initialized") }
}

class InitializersTest {
    @Test
    fun `initializers select packs and the most specific records override`() {
        val generator = PeacefulGenerator().initDefault()
        assertEquals(listOf("default_table"), generator.tables.map { it.sqlName })
        assertEquals(OverrideSource::class.java.name, generator.recordsSources.single().javaClass.name)
        assertTrue(generator.recordsSources.single().records().isEmpty())

        generator.initOtherPack()
        assertEquals(listOf("other_table"), generator.tables.map { it.sqlName })
        val source = assertIs<OtherSource>(generator.recordsSources.single())
        assertIs<OtherTable>(source.rec {})
    }

    @Test
    fun `reinitialization replaces both lists and creates fresh class instances`() {
        val generator = PeacefulGenerator().initDefault()
        val previousTable = generator.tables.single()
        val previousSource = generator.recordsSources.single()
        generator.tables += UnannotatedTable()
        assertSame(generator, generator.initDefault())
        assertEquals(1, generator.tables.size)
        assertEquals(1, generator.recordsSources.size)
        assertNotSame(previousTable, generator.tables.single())
        assertNotSame(previousSource, generator.recordsSources.single())
    }

    @Test
    fun `manual registration needs no generated initializer`() {
        val generator = PeacefulGenerator()
        assertTrue(generator.tables.isEmpty())
        assertTrue(generator.recordsSources.isEmpty())
        val table = UnannotatedTable()
        generator.tables += table
        assertSame(table, generator.tables.single())
    }
}
