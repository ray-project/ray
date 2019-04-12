load("@rules_jvm_external//:defs.bzl", "maven_install")

def gen_java_deps():
    maven_install(
        artifacts = [
            "com.beust:jcommander:1.72",
            "com.github.davidmoten:flatbuffers-java:1.9.0.1",
            "com.google.guava:guava:27.0.1-jre",
            "com.puppycrawl.tools:checkstyle:8.15",
            "com.sun.xml.bind:jaxb-core:2.3.0",
            "com.sun.xml.bind:jaxb-impl:2.3.0",
            "com.typesafe:config:1.3.2",
            "commons-io:commons-io:2.5",
            "de.ruedigermoeller:fst:2.47",
            "javax.xml.bind:jaxb-api:2.3.0",
            "org.apache.commons:commons-lang3:3.4",
            "org.ow2.asm:asm:6.0",
            "org.slf4j:slf4j-log4j12:1.7.25",
            "org.testng:testng:6.9.9",
            "redis.clients:jedis:2.8.0",
        ],
        repositories = [
            "https://repo1.maven.org/maven2",
        ],
        # Fetch srcjars. Defaults to False.
        fetch_sources = True,
    )
