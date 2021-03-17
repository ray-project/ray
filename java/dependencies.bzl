load("@rules_jvm_external//:defs.bzl", "maven_install")

def gen_java_deps():
    maven_install(
        artifacts = [
            "com.google.code.gson:gson:2.8.5",
            "com.google.guava:guava:27.0.1-jre",
            "com.google.protobuf:protobuf-java:3.10.0",
            "com.puppycrawl.tools:checkstyle:8.15",
            "com.sun.xml.bind:jaxb-core:2.3.0",
            "com.sun.xml.bind:jaxb-impl:2.3.0",
            "com.typesafe:config:1.3.2",
            "commons-io:commons-io:2.5",
            "de.ruedigermoeller:fst:2.57",
            "javax.xml.bind:jaxb-api:2.3.0",
            "org.apache.commons:commons-lang3:3.4",
            "org.msgpack:msgpack-core:0.8.20",
            "org.ow2.asm:asm:6.0",
            "org.slf4j:slf4j-log4j12:1.7.25",
            "org.testng:testng:7.3.0",
            "redis.clients:jedis:2.8.0",
            "net.java.dev.jna:jna:5.5.0",
        ],
        repositories = [
            "https://repo1.maven.org/maven2/",
        ],
    )
