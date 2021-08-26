load("@rules_jvm_external//:defs.bzl", "maven_install")

def gen_streaming_java_deps():
    maven_install(
        name = "ray_streaming_maven",
        artifacts = [
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.8.5",
            "com.github.davidmoten:flatbuffers-java:1.9.0.1",
            "org.apache.commons:commons-lang3:3.4",
            "org.aeonbits.owner:owner:1.0.10",
            "org.mockito:mockito-all:1.10.19",
            "org.apache.commons:commons-lang3:3.3.2",
            "org.mockito:mockito-all:1.10.19",
	        "org.powermock:powermock-module-testng:1.6.6",
	        "org.powermock:powermock-api-mockito:1.6.6",
            "commons-collections:commons-collections:3.2.2",
        ],
        repositories = [
            "https://repo1.maven.org/maven2/",
        ],
    )
