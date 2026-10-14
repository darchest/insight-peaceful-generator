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
The processor generates one extension per pack: `"default"` → `initDefault()`,
`"generation-test"` → `initGenerationTest()`. Names producing the same function are rejected.
Tables must be accessible concrete classes with a callable no-argument constructor, or objects.
Record sources are found through their `RecordsSource<T>` inheritance, including ORM-generated
parents. The most specific override of `records()` wins; unrelated overrides are rejected.
Generation runs after all KSP rounds, so types created by other processors are included.

An initializer replaces both lists with fresh instances. Calling it twice does not duplicate
entries. To combine packs, keep their lists and register the combined contents manually.
Each module indexes its own source declarations; initialize dependency modules explicitly.

Manual registration needs no annotation or processor:

```kotlin
val generator = PeacefulGenerator()
generator.tables += MyTable()
generator.recordsSources += MyRecordsSource()
generator.generate()
```

Consumers such as table-access synchronization should reuse `generator.tables` instead of
performing a separate discovery pass.
