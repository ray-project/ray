load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

def gen_java_deps():
    maven_install(
        artifacts = [
            "com.fasterxml.jackson.core:jackson-databind:2.13.3",
            "com.github.java-json-tools:json-schema-validator:2.2.14",
            "com.google.code.gson:gson:2.9.1",
            "com.google.guava:guava:30.0-jre",
            "com.google.protobuf:protobuf-java:3.19.4",
            "com.google.protobuf:protobuf-java-util:3.19.4",
            "com.puppycrawl.tools:checkstyle:8.15",
            "com.sun.xml.bind:jaxb-core:2.3.0",
            "com.sun.xml.bind:jaxb-impl:2.3.0",
            "com.typesafe:config:1.3.2",
            "commons-io:commons-io:2.7",
            "de.ruedigermoeller:fst:2.57",
            "javax.xml.bind:jaxb-api:2.3.0",
            "javax.activation:activation:1.1.1",
            "org.apache.commons:commons-lang3:3.4",
            "org.msgpack:msgpack-core:0.8.20",
            "org.ow2.asm:asm:6.0",
            "org.apache.logging.log4j:log4j-api:2.17.1",
            "org.apache.logging.log4j:log4j-core:2.17.1",
            "org.apache.logging.log4j:log4j-slf4j-impl:2.17.1",
            "org.slf4j:slf4j-api:1.7.25",
            "com.lmax:disruptor:3.3.4",
            "org.yaml:snakeyaml:1.33",
            "net.java.dev.jna:jna:5.8.0",
            "org.apache.httpcomponents.client5:httpclient5:5.0.3",
            "org.apache.httpcomponents.core5:httpcore5:5.0.2",
            "org.apache.httpcomponents.client5:httpclient5-fluent:5.0.3",
            maven.artifact(
                group = "org.testng",
                artifact = "testng",
                version = "7.3.0",
                exclusions = [
                    "org.yaml:snakeyaml",
                    "com.google.guava:guava",
                ]
            ),
        ],
        repositories = [
            "https://repo1.maven.org/maven2/",
        ],
    )
