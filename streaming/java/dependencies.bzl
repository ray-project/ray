load("@rules_jvm_external//:defs.bzl", "maven_install")

def gen_streaming_java_deps():
    maven_install(
        name = "ray_streaming_maven",
        artifacts = [
            "com.beust:jcommander:1.72",
            "com.google.guava:guava:27.0.1-jre",
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.8.5",
            "com.github.davidmoten:flatbuffers-java:1.9.0.1",
            "com.google.protobuf:protobuf-java:3.8.0",
            "org.apache.commons:commons-lang3:3.4",
            "de.ruedigermoeller:fst:2.57",
            "org.aeonbits.owner:owner:1.0.10",
            "org.slf4j:slf4j-api:1.7.12",
            "org.slf4j:slf4j-log4j12:1.7.25",
            "org.apache.logging.log4j:log4j-core:2.8.2",
            "org.testng:testng:6.9.10",
            "log4j:log4j:1.2.17",
            "org.mockito:mockito-all:1.10.19",
            "org.apache.commons:commons-lang3:3.3.2",
            "org.msgpack:msgpack-core:0.8.20",
            "org.testng:testng:6.9.10",
	        "org.mockito:mockito-all:1.10.19",
	        "org.powermock:powermock-module-testng:1.6.6",
	        "org.powermock:powermock-api-mockito:1.6.6",
        ],
        repositories = [
            "https://repo1.maven.org/maven2/",
        ],
    )
