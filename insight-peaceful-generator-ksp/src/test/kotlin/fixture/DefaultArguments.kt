package fixture

import org.darchest.insight.generator.Generatable
import org.darchest.insight.generator.RecordsSource

@Generatable("default-arguments")
class DefaultArgumentsTable(val flag: Boolean = true) : FixtureTable("default_arguments")

class DefaultArgumentsSource(val flag: Boolean = true) : RecordsSource<DefaultArgumentsTable> {
    override fun tableFactory() = DefaultArgumentsTable(flag)
}

val defaultTableConstructor: () -> DefaultArgumentsTable = ::DefaultArgumentsTable
val defaultSourceConstructor: () -> DefaultArgumentsSource = ::DefaultArgumentsSource
