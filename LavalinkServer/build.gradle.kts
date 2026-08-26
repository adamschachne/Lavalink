import org.apache.tools.ant.filters.ReplaceTokens
import org.springframework.boot.gradle.tasks.bundling.BootJar
import org.springframework.boot.gradle.tasks.run.BootRun

plugins {
    application
    `maven-publish`
}

apply(plugin = "org.springframework.boot")
apply(plugin = "com.gorylenko.gradle-git-properties")
apply(plugin = "org.ajoberstar.grgit")
apply(plugin = "com.adarshr.test-logger")
apply(plugin = "kotlin")
apply(plugin = "kotlin-spring")

val archivesBaseName = "Lavalink"
group = "dev.arbjerg.lavalink"

description = "Play audio to discord voice channels"

application {
    mainClass.set("lavalink.server.Launcher")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

configurations {
    compileOnly {
        extendsFrom(annotationProcessor.get())
    }
    all {
        resolutionStrategy {
            force("org.slf4j:slf4j-api:1.7.36")
        }
    }
}

dependencies {
    implementation(projects.protocol)
    implementation(projects.pluginApi) {
        exclude(group = "org.springframework.boot", module = "spring-boot-starter-tomcat")
    }

    implementation(libs.bundles.metrics)
    implementation(libs.bundles.spring) {
        exclude(group = "org.springframework.boot", module = "spring-boot-starter-tomcat")
    }

    implementation(libs.koe) {
        // This version of SLF4J does not recognise Logback 1.2.3
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.koe.udpqueue) {
        exclude(module = "lava-common")
        exclude(module = "udp-queue")
    }
	implementation(libs.bundles.libdave.natives)
    implementation(libs.bundles.udpqueue.natives) {
        exclude(group = "com.sedmelluq", module = "lava-common")
    }

    implementation(libs.lavaplayer)
    implementation(libs.lavaplayer.ip.rotator)

    implementation(libs.lavadsp)
    implementation(libs.kotlin.reflect)
    implementation(libs.logback)
    implementation(libs.sentry.logback)
    implementation(libs.oshi) {
        // This version of SLF4J does not recognise Logback 1.2.3
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.json)

    compileOnly(libs.spotbugs)

    testImplementation(libs.spring.boot.test)
}

tasks {
    build {
        doLast {
            println("Version: $version")
        }
    }

    processResources {
        val tokens = mapOf(
            "project.version"    to project.version,
            "project.groupId"    to project.group,
            "project.artifactId" to "Lavalink-Server",
            "env.BUILD_TIME"     to "1772758801000"
        )

        filter(ReplaceTokens::class, mapOf("tokens" to tokens))
        copy {
            from("application.yml.example")
            into("$buildDir/resources/main")
        }
        doLast {
            // The upstream git-properties task embeds the build clock, host,
            // and builder identity. Replace it with the exact pinned source
            // identity so equal inputs produce equal artifacts.
            file("$buildDir/resources/main/git.properties").writeText(
                """git.branch=3.7.13+red.5
git.commit.id=c5cb5a0d85d699b039f655614af3e2de0a5d8c61
git.commit.id.abbrev=c5cb5a0
git.commit.user.name=Jakub Kuczys
git.commit.user.email=me@jacken.men
git.commit.message.short=Revert \"Use tagged version\"
git.commit.message.full=Revert \"Use tagged version\"
git.commit.time=2026-03-06T01\:00\:01+0000
"""
            )
        }
    }

    matching { it.name == "generateGitProperties" }.configureEach {
        enabled = false
    }

    // https://stackoverflow.com/questions/41444916/multiple-artifacts-issue-with-deploying-zip-to-nexus
    named<AbstractArchiveTask>("bootDistTar") {
        archiveClassifier.set("bootTar")
    }

    named<AbstractArchiveTask>("bootDistZip") {
        archiveClassifier.set("bootZip")
    }

    named<Test>("test") {
        useJUnitPlatform()
    }

    withType<BootJar> {
        archiveFileName.set("Lavalink.jar")
    }

    withType<BootRun> {
        dependsOn("compileTestJava")

        //pass in custom jvm args
        // source: https://stackoverflow.com/a/25079415
        // example: ./gradlew bootRun -PjvmArgs="--illegal-access=debug -Dwhatever=value"
        if (project.hasProperty("jvmArgs")) {
            val args = project.property("jvmArgs")
                .toString()
                .split("\\s".toPattern())

            jvmArgs?.addAll(args)
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("LavalinkServer") {
            from(project.components["java"])

            pom {
                name.set("Lavalink Server")
                description.set("Lavalink Server")
                url.set("https://github.com/lavalink-devs/lavalink")

                licenses {
                    license {
                        name.set("The MIT License")
                        url.set("https://github.com/lavalink-devs/Lavalink/blob/master/LICENSE")
                    }
                }

                developers {
                    developer {
                        id.set("freyacodes")
                        name.set("Freya Arbjerg")
                        url.set("https://www.arbjerg.dev")
                    }
                }

                scm {
                    connection.set("scm:git:ssh://github.com/lavalink-devs/lavalink.git")
                    developerConnection.set("scm:git:ssh://github.com/lavalink-devs/lavalink.git")
                    url.set("https://github.com/lavalink-devs/lavalink")
                }
            }
        }
    }
}
