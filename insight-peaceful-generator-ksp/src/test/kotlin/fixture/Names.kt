package fixture

import org.darchest.insight.generator.Generatable

@Generatable("aliases")
class Table : FixtureTable("reserved")

@Generatable("aliases")
class SameTable_1 : FixtureTable("already_suffixed")

@Generatable("escaped")
class `when` : FixtureTable("keyword")

@Generatable("escaped")
object `object` : FixtureTable("singleton")
