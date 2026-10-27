plugins {
    kotlin("jvm")
    id("com.google.devtools.ksp")
    `maven-publish`
}

group = "org.darchest"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("com.google.devtools.ksp:symbol-processing-api:2.0.10-1.0.24")
    compileOnly(project(":"))

    testImplementation(project(":"))
    testImplementation("org.darchest:insight:1.0-SNAPSHOT")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    add("kspTest", files(tasks.jar))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
    repositories {
        mavenLocal()
        maven {
            name = "darchest"
            url = uri("https://mvn.darchest.org/repository/snapshots/")
            credentials {
                username = findProperty("mvn.darchest.user") as String? ?: ""
                password = findProperty("mvn.darchest.password") as String? ?: ""
            }
        }
    }
}
