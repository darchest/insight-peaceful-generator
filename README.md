# Insight Peaceful Generator

Register tables and seed sources explicitly before generation. There is no runtime classpath scan.

```kotlin
val generator = PeacefulGenerator()
generator.initDefault()
generator.generate()
```

Add the processor to the application that declares the tables:

```kotlin
plugins {
    id("com.google.devtools.ksp") version "2.0.10-1.0.24"
}

dependencies {
    implementation("org.darchest:insight-peaceful-generator:1.0-SNAPSHOT")
    ksp("org.darchest:insight-peaceful-generator-ksp:1.0-SNAPSHOT")
}

ksp {
    arg("org.darchest.insight.generator.package", "my.application.generated")
}
```

Import `my.application.generated.initDefault`. The default generated package is
`org.darchest.insight.generator.generated`; use a distinct package for each application/module.

Each directly annotated `@Generatable` table belongs to its `genPack` (default: `"default"`).
Its `actions` argument is a bit mask, defaulting to `Generatable.ALL`. Available flags are
`CREATE_TABLE`, `ADD_COLUMN`, `ALTER_COLUMN_SIZE`, `ADD_INDEX`, `ADD_FOREIGN_KEY`, and `RECORDS`;
`SCHEMA` combines all schema flags. Combine flags with `or`, for example:

```kotlin
@Generatable(actions = Generatable.CREATE_TABLE or Generatable.ADD_COLUMN)
class MyTable : MyBaseTable("my_table")

@Generatable(actions = 0)
class LegacyTable : MyBaseTable("legacy_table")
```

Tables with `actions = 0` remain in `generator.tables` for consumers such as access
synchronization. Generation skips their schema descriptions, DDL, and seed records.
The initializer omits record-source constructors for tables without the `RECORDS` flag.
A pack containing only disabled tables needs no database connection. `RECORDS` alone seeds
existing tables; `SCHEMA` alone manages their structure without seeding them. The same masks
apply to explicit `TableDescriptor.actions` in the descriptor-based generation overload.

The processor generates one extension per pack: `"default"` → `initDefault()`,
`"generation-test"` → `initGenerationTest()`. Names producing the same function are rejected.
Tables must be accessible concrete classes with a callable no-argument constructor, or objects.
Record sources are found through their `RecordsSource<T>` inheritance, including ORM-generated
parents. All independent sources for a table are collected. Within an inheritance chain,
the most specific override of `records()` wins, so inherited seeds are not registered twice.
Sources with one table type parameter and a constructor accepting `() -> T` are instantiated
for each compatible table, using its constructor. All upper bounds of `T` must match.
Generation runs after all KSP rounds, so types created by other processors are included.

An initializer replaces both lists with constructor references (`::MyTable`, `::MyRecordsSource`)
without creating instances. Calling it twice does not duplicate entries. Instances are created
when `generate()` invokes the constructors. Kotlin objects use singleton factories instead.
Constructors with optional arguments use a typed adapter to keep their default values without
reflective calls. Generated imports only escape Kotlin keywords and unusual identifiers;
duplicate simple class names receive import aliases.
To combine packs, keep their lists and register the combined contents manually.
Each module indexes its own source declarations; initialize dependency modules explicitly.

Manual registration needs no annotation or processor:

```kotlin
val generator = PeacefulGenerator()
generator.tables += ::MyTable
generator.recordsSources += ::MyRecordsSource
generator.generate()
```

`RecordsSource<T>` supplies a `tableFactory(): T` that creates a fresh table. `rec()` and seed
lookups use it directly, without reflective constructor calls or generic type resolution.
ORM entities already implement this factory through their table constructor. A standalone
source can keep an explicit constructor:

```kotlin
class MyRecordsSource : RecordsSource<MyTable> {
    private val constructor = ::MyTable
    override fun tableFactory() = constructor()
}
```

A common source can seed tables that have no dedicated source:

```kotlin
open class CommonRecords<T : MyBaseTable>(private val constructor: () -> T) : RecordsSource<T> {
    override fun tableFactory() = constructor()
    override fun records() = mutableListOf(rec { /* common values */ })
}

class MyRecords : CommonRecords<MyTable>(::MyTable) {
    override fun records() = super.records().apply {
        add(rec { /* table-specific values */ })
    }
}
```

The generated initializer registers `{ CommonRecords<OtherTable>(::OtherTable) }` for a
compatible table without a dedicated source, and `::MyRecords` for `MyTable`. Independent
sources for `MyTable` are also registered. Templates can have additional optional constructor
arguments. A non-generic source accepting a table constructor can also be bound to a derived
table; a source with only a no-argument constructor applies to its exact table type.

Consumers such as table-access synchronization should reuse `generator.tables` instead of
performing a separate discovery pass.
