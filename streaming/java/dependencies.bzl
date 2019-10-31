load("@rules_jvm_external//:defs.bzl", "maven_install")

def gen_java_deps():
    maven_install(
        artifacts = [
            "com.google.guava:guava:27.0.1-jre",
            "de.ruedigermoeller:fst:2.57",
            "org.apache.logging.log4j:log4j-slf4j-impl:2.8.2",
            "org.apache.logging.log4j:log4j-api:2.8.2",
            "org.apache.logging.log4j:log4j-core:2.8.2",
            "org.testng:testng:6.9.10",
        ],
        repositories = [
            "https://repo1.maven.org/maven2",
        ],
        # Fetch srcjars. Defaults to False.
        fetch_sources = False,
    )
