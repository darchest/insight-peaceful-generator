package org.darchest.insight.generator

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.File
import java.net.URLClassLoader
import java.util.jar.JarFile

internal data class GenerationClass(val name: String, val parents: List<String>, val generatable: Boolean)

/** Indexes class headers so unrelated classes do not need to be loaded or initialized. */
internal object GenerationClasspath {
    fun scan(loader: ClassLoader): Map<String, GenerationClass> {
        val roots = linkedSetOf<File>()
        System.getProperty("java.class.path", "").split(File.pathSeparator)
            .filter { it.isNotEmpty() }.mapTo(roots, ::File)
        generateSequence(loader) { it.parent }.filterIsInstance<URLClassLoader>().forEach {
            it.urLs.filter { url -> url.protocol == "file" }.mapTo(roots) { url -> File(url.toURI()) }
        }
        loader.getResources("").toList().filter { it.protocol == "file" }
            .mapTo(roots) { File(it.toURI()) }
        val classes = linkedMapOf<String, GenerationClass>()
        fun register(bytes: ByteArray) {
            val definition = readHeader(bytes)
            classes.putIfAbsent(definition.name, definition)
        }
        for (root in roots) {
            if (root.isDirectory) {
                root.walkTopDown().filter { it.isFile && it.extension == "class" }.forEach { register(it.readBytes()) }
            } else if (root.isFile && root.extension.equals("jar", ignoreCase = true)) {
                JarFile(root).use { jar ->
                    jar.entries().asSequence()
                        .filter { !it.isDirectory && it.name.endsWith(".class") && !it.name.startsWith("META-INF/") }
                        .forEach { entry -> jar.getInputStream(entry).use { register(it.readBytes()) } }
                }
            }
        }
        return classes
    }

    private fun readHeader(bytes: ByteArray): GenerationClass = DataInputStream(ByteArrayInputStream(bytes)).use { input ->
        require(input.readInt() == 0xCAFEBABEL.toInt()) { "Invalid JVM class file" }
        input.readUnsignedShort() // minor version
        input.readUnsignedShort() // major version
        val count = input.readUnsignedShort()
        val strings = arrayOfNulls<String>(count)
        val classNames = IntArray(count)
        var index = 1
        while (index < count) {
            when (val tag = input.readUnsignedByte()) {
                1 -> strings[index] = input.readUTF()
                7 -> classNames[index] = input.readUnsignedShort()
                3, 4, 9, 10, 11, 12, 17, 18 -> input.readInt()
                5, 6 -> { input.readLong(); index++ }
                8, 16, 19, 20 -> input.readUnsignedShort()
                15 -> { input.readUnsignedByte(); input.readUnsignedShort() }
                else -> error("Unsupported JVM constant-pool tag: $tag")
            }
            index++
        }
        fun className(index: Int): String = checkNotNull(strings[classNames[index]]).replace('/', '.')
        input.readUnsignedShort() // access flags
        val name = className(input.readUnsignedShort())
        val superClass = input.readUnsignedShort()
        val parents = mutableListOf<String>()
        if (superClass != 0) parents += className(superClass)
        repeat(input.readUnsignedShort()) { parents += className(input.readUnsignedShort()) }
        val marker = "L${Generatable::class.java.name.replace('.', '/')};"
        GenerationClass(name, parents, strings.any { it?.contains(marker) == true })
    }
}
