package org.darchest.insight.generator

/** Marks a concrete table declaration for generation; entities and seed sources are discovered separately. */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Generatable(val genPack: String = "default")
