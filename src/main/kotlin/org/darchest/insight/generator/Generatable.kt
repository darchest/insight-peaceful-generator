package org.darchest.insight.generator

/** Marks a concrete table declaration for a KSP-generated init<GenPack>() extension. */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Generatable(val genPack: String = "default")
