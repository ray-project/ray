# The following dependencies were calculated from:
#
# generate_workspace --maven_project=/home/admin/ruifang.crf/code/X/arrow/java --repositories=http://mvn.dev.alipay.net:8080/artifactory/repo
# TODO: now this file is not automatically generated


def generated_maven_jars():
  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-classic:jar:0.9.44 wanted version 1.1.2
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:jar:0.9.44 wanted version 1.1.2
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "ch_qos_logback_logback_classic",
      artifact = "ch.qos.logback:logback-classic:1.2.3",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "7c4f3c474fb2c041d8028740440937705ebb473a",
  )


  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_google_code_findbugs_jsr305",
      artifact = "com.google.code.findbugs:jsr305:3.0.2",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "25ea2e8b0c338a877313bd4672d3fe056ea78f0d",
  )


  # io.grpc:grpc-protobuf:jar:1.14.0
  native.maven_jar(
      name = "io_grpc_grpc_protobuf_lite",
      artifact = "io.grpc:grpc-protobuf-lite:1.14.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "2ac8c28ca927f954eaa228a931d9c163cf3d860f",
  )


  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT wanted version 2.10
  native.maven_jar(
      name = "joda_time_joda_time",
      artifact = "joda-time:joda-time:2.9.9",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "f7b520c458572890807d143670c9b24f4de90897",
  )


  # org.mockito:mockito-core:jar:2.7.22
  native.maven_jar(
      name = "net_bytebuddy_byte_buddy_agent",
      artifact = "net.bytebuddy:byte-buddy-agent:1.6.11",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "0200d9c012befccd211ff91082a151257b1dc084",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-classic:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_converter_classic",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "6f08f9bfd65869a51edc922432f46024d94f3ccd",
  )


  # io.netty:netty-handler-proxy:jar:4.1.22.Final got requested version
  # io.netty:netty-handler:jar:4.1.22.Final got requested version
  # io.netty:netty-codec:jar:4.1.22.Final
  native.maven_jar(
      name = "io_netty_netty_transport",
      artifact = "io.netty:netty-transport:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "3bd455cd9e5e5fb2e08fd9cd0acfa54c079ca989",
  )


  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_carrotsearch_hppc",
      artifact = "com.carrotsearch:hppc:0.7.2",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "710398361f2ae8fd594a133e3619045c16b24137",
  )


  # io.netty:netty-codec-http2:jar:4.1.22.Final
  native.maven_jar(
      name = "io_netty_netty_handler",
      artifact = "io.netty:netty-handler:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "a3a16b17d5a5ed6f784b0daba95e28d940356109",
  )


  # io.grpc:grpc-netty:jar:1.14.0
  native.maven_jar(
      name = "io_netty_netty_handler_proxy",
      artifact = "io.netty:netty-handler-proxy:4.1.27.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "1a822ce7760bc6eb4937b7e448c9e081fedcc807",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0HOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "org_mockito_mockito_core",
      artifact = "org.mockito:mockito-core:2.7.22",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "fcf63bc8010ca77991e3cadd8d33ad1a40603404",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-core:jar:0.9.44 got requested version
  # de.huxhorn.lilith:de.huxhorn.lilith.data.logging.protobuf:jar:0.9.44 got requested version
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-classic:jar:0.9.44
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:jar:0.9.44 got requested version
  native.maven_jar(
      name = "de_huxhorn_sulky_de_huxhorn_sulky_codec",
      artifact = "de.huxhorn.sulky:de.huxhorn.sulky.codec:0.9.17",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "4a2a917f1cc4b75d71aadeef4f4c178f229febc2",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.data.logging:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.data.eventsource:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "618f853d75d59855c955b3a1bf0f447476f8027f",
  )


  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_google_flatbuffers_flatbuffers_java",
      artifact = "com.google.flatbuffers:flatbuffers-java:1.9.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "460bc8f6411768659c1ffb529592e251a808b9f2",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  # de.huxhorn.lilith:de.huxhorn.lilith.data.logging.protobuf:jar:0.9.44 wanted version 2.5.0
  # io.grpc:grpc-protobuf:jar:1.14.0
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT wanted version 2.5.0
  native.maven_jar(
      name = "com_google_protobuf_protobuf_java",
      artifact = "com.google.protobuf:protobuf-java:3.5.1",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "8c3492f7662fa1cbf8ca76a0f5eb1146f7725acd",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "io_netty_netty_tcnative_boringssl_static",
      artifact = "io.netty:netty-tcnative-boringssl-static:2.0.12.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "b884be1450a7fd0854b98743836b8ccb0dfd75a4",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_h2database_h2",
      artifact = "com.h2database:h2:1.4.196",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "dd0034398d593aa3588c6773faac429bbd9aea0e",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_jul_to_slf4j",
      artifact = "org.slf4j:jul-to-slf4j:1.7.25",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "0af5364cd6679bfffb114f0dec8a157aaa283b76",
  )


  native.maven_jar(
      name = "net_java_dev_jna_jna",
      artifact = "net.java.dev.jna:jna:4.1.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "1c12d070e602efd8021891cdd7fd18bc129372d4",
  )


  native.maven_jar(
      name = "org_mockito_mockito_all",
      artifact = "org.mockito:mockito-all:1.10.19",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "539df70269cc254a58cccc5d8e43286b4a73bf30",
  )


  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "commons_codec_commons_codec",
      artifact = "commons-codec:commons-codec:1.10",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "4b95f4897fa13f2cd904aee711aeafc0c5295cd8",
  )


  native.maven_jar(
      name = "com_beust_jcommander",
      artifact = "com.beust:jcommander:1.72",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "6375e521c1e11d6563d4f25a07ce124ccf8cd171",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_log4j_over_slf4j",
      artifact = "org.slf4j:log4j-over-slf4j:1.7.25",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "a87bb47468f47ee7aabbd54f93e133d4215769c3",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "commons_cli_commons_cli",
      artifact = "commons-cli:commons-cli:1.4",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "c51c00206bb913cd8612b24abd9fa98ae89719b1",
  )


  # io.grpc:grpc-core:jar:1.14.0
  native.maven_jar(
      name = "com_google_errorprone_error_prone_annotations",
      artifact = "com.google.errorprone:error_prone_annotations:2.1.2",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "6dcc08f90f678ac33e5ef78c3c752b6f59e63e0c",
  )


  # io.netty:netty-transport:jar:4.1.22.Final
  native.maven_jar(
      name = "io_netty_netty_resolver",
      artifact = "io.netty:netty-resolver:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "b5484d17a97cb57b07d2a1ac092c249e47234c17",
  )


  # io.grpc:grpc-core:jar:1.14.0
  native.maven_jar(
      name = "io_opencensus_opencensus_contrib_grpc_metrics",
      artifact = "io.opencensus:opencensus-contrib-grpc-metrics:0.12.3",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "a4c7ff238a91b901c8b459889b6d0d7a9d889b4d",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "io_grpc_grpc_stub",
      artifact = "io.grpc:grpc-stub:1.14.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "74bfe83c0dc69bf903fff8df3568cbeb8b387d35",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "io_grpc_grpc_protobuf",
      artifact = "io.grpc:grpc-protobuf:1.14.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "d429fdc2e0d288b34ea7588bb762eb458f385bd5",
  )


  # io.netty:netty-handler-proxy:jar:4.1.22.Final
  native.maven_jar(
      name = "io_netty_netty_codec_socks",
      artifact = "io.netty:netty-codec-socks:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "d077b39da2dedc5dc5db50a44e5f4c30353e86f3",
  )


  # io.netty:netty-codec-http:jar:4.1.22.Final
  # io.netty:netty-handler:jar:4.1.22.Final got requested version
  # io.netty:netty-codec-socks:jar:4.1.22.Final got requested version
  native.maven_jar(
      name = "io_netty_netty_codec",
      artifact = "io.netty:netty-codec:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "239c0af275952e70bb4adf7cf8c03d88ddc394c9",
  )


  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  # io.netty:netty-transport:jar:4.1.22.Final got requested version
  # io.netty:netty-handler:jar:4.1.22.Final got requested version
  native.maven_jar(
      name = "io_netty_netty_buffer",
      artifact = "io.netty:netty-buffer:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "15e964a2095031364f534a6e21977f5ee9ca32a9",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-classic:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_core",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-core:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "636cc11e0e09d363af87766550c1deef8ab0c5bf",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "io_grpc_grpc_netty",
      artifact = "io.grpc:grpc-netty:1.14.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "21c6edadd45b6869384f8aa0df1663d62c503617",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-core:jar:0.9.44 wanted version 1.1.2
  # ch.qos.logback:logback-classic:jar:1.2.3
  native.maven_jar(
      name = "ch_qos_logback_logback_core",
      artifact = "ch.qos.logback:logback-core:1.2.3",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "864344400c3d4d92dfeb0a305dc87d953677c03c",
  )


  # org.mockito:mockito-core:jar:2.7.22
  native.maven_jar(
      name = "net_bytebuddy_byte_buddy",
      artifact = "net.bytebuddy:byte-buddy:1.6.11",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "8a8f9409e27f1d62c909c7eef2aa7b3a580b4901",
  )


  # io.opencensus:opencensus-contrib-grpc-metrics:jar:0.12.3 got requested version
  # io.grpc:grpc-core:jar:1.14.0
  native.maven_jar(
      name = "io_opencensus_opencensus_api",
      artifact = "io.opencensus:opencensus-api:0.12.3",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "743f074095f29aa985517299545e72cc99c87de0",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-core:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_sender",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.sender:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "fdeb2f1aaba42cad76c460ac4fd7072b8642520f",
  )


  native.maven_jar(
      name = "org_javassist_javassist",
      artifact = "org.javassist:javassist:3.19.0-GA",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "50120f69224dd8684b445a6f3a5b08fe9b5c60f6",
  )


  # org.mockito:mockito-core:jar:2.7.22
  native.maven_jar(
      name = "org_objenesis_objenesis",
      artifact = "org.objenesis:objenesis:2.5",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "612ecb799912ccf77cba9b3ed8c813da086076e9",
  )


  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT got requested version
  # io.netty:netty-resolver:jar:4.1.22.Final got requested version
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT got requested version
  # io.netty:netty-buffer:jar:4.1.22.Final
  native.maven_jar(
      name = "io_netty_netty_common",
      artifact = "io.netty:netty-common:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "56ff4deca53fc791ed59ac2b72eb6718714a4de9",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT wanted version 4.12
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "junit_junit",
      artifact = "junit:junit:4.11",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "4e031bb61df09069aeb2bffb4019e7a5034a4ee0",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-classic:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_logging_protobuf",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.data.logging.protobuf:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "56a037b365c7ff9b1c348e6d663fd3d778e2b0be",
  )


  # de.huxhorn.sulky:de.huxhorn.sulky.codec:jar:0.9.17
  # de.huxhorn.lilith:de.huxhorn.lilith.sender:jar:0.9.44 got requested version
  native.maven_jar(
      name = "de_huxhorn_sulky_de_huxhorn_sulky_io",
      artifact = "de.huxhorn.sulky:de.huxhorn.sulky.io:0.9.17",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "6baf1e17f80ba40f9a4b4b97235e72488047db02",
  )


  native.maven_jar(
      name = "log4j_log4j",
      artifact = "log4j:log4j:1.2.17",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "5af35056b4d257e4b64b9e8069c0746e8b08629f",
  )


  native.maven_jar(
      name = "org_slf4j_slf4j_log4j12",
      artifact = "org.slf4j:slf4j-log4j12:1.7.25",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "110cefe2df103412849d72ef7a67e4e91e4266b4",
  )


  # org.slf4j:jcl-over-slf4j:jar:1.7.25 got requested version
  # org.slf4j:jul-to-slf4j:jar:1.7.25
  # de.huxhorn.lilith:de.huxhorn.lilith.sender:jar:0.9.44 wanted version 1.7.7
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:jar:0.9.44 wanted version 1.7.7
  # org.slf4j:log4j-over-slf4j:jar:1.7.25 got requested version
  # ch.qos.logback:logback-classic:jar:1.2.3 got requested version
  # de.huxhorn.sulky:de.huxhorn.sulky.codec:jar:0.9.17 wanted version 1.7.7
  # de.huxhorn.sulky:de.huxhorn.sulky.formatting:jar:0.9.17 wanted version 1.7.7
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_slf4j_api",
      artifact = "org.slf4j:slf4j-api:1.7.25",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "da76ca59f6a57ee3102f8f9bd9cee742973efa8a",
  )


  native.maven_jar(
      name = "com_typesafe_config",
      artifact = "com.typesafe:config:1.3.2",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "d6ac0ce079f114adce620f2360c92a70b2cb36dc",
  )


  native.maven_jar(
      name = "org_apache_commons_commons_lang3",
      artifact = "org.apache.commons:commons-lang3:3.4",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "5fe28b9518e58819180a43a850fbc0dd24b7c050",
  )


  # io.netty:netty-handler-proxy:jar:4.1.22.Final got requested version
  # io.netty:netty-codec-http2:jar:4.1.22.Final
  native.maven_jar(
      name = "io_netty_netty_codec_http",
      artifact = "io.netty:netty-codec-http:4.1.22.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "3805f3ca0d57630200defc7f9bb6ed3382dcb10b",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOTi
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT  
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_classic",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.logback.appender.multiplex-classic:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "1ac6ffe5fc63f7900f8af54b0dfebf2508b41db7",
  )


  native.maven_jar(
      name = "commons_collections_commons_collections",
      artifact = "commons-collections:commons-collections:3.2.2",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "8ad72fe39fa8c91eaaf12aadb21e0c3661fe26d5",
  )


  # junit:junit:jar:4.11
  native.maven_jar(
      name = "org_hamcrest_hamcrest_core",
      artifact = "org.hamcrest:hamcrest-core:1.3",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "42a25dc3219429f0e5d060061f71acb49bf010a0",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "org_jmockit_jmockit",
      artifact = "org.jmockit:jmockit:1.33",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "8a2746af90706d8d68f3919e1a87d5e47b8c94d7",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.data.logging:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_sulky_de_huxhorn_sulky_formatting",
      artifact = "de.huxhorn.sulky:de.huxhorn.sulky.formatting:0.9.17",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "0501b143ee18ca9e7807438cc1b0acfa144f657f",
  )


  # io.grpc:grpc-netty:jar:1.14.0
  native.maven_jar(
      name = "io_netty_netty_codec_http2",
      artifact = "io.netty:netty-codec-http2:4.1.27.Final",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "3769790a2033667d663f9a526d5b63cfecdbdf4e",
  )


  native.maven_jar(
      name = "net_lingala_zip4j_zip4j",
      artifact = "net.lingala.zip4j:zip4j:1.3.2",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "4ba84e98ee017b74cb52f45962f929a221f3074c",
  )


  native.maven_jar(
      name = "org_testng_testng",
      artifact = "org.testng:testng:6.9.9",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "1bf509349476d6a48978cc2b04af9caa907781ab",
  )

  native.maven_jar(
      name = "org_ini4j_ini4j",
      artifact = "org.ini4j:ini4j:0.5.4",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "4a3ee4146a90c619b20977d65951825f5675b560",
  )


  native.maven_jar(
      name = "org_ow2_asm_asm",
      artifact = "org.ow2.asm:asm:6.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "bc6fa6b19424bb9592fe43bbc20178f92d403105",
  )


  # io.grpc:grpc-core:jar:1.14.0
  native.maven_jar(
      name = "com_google_code_gson_gson",
      artifact = "com.google.code.gson:gson:2.7",#2.8.5
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "751f548c85fa49f330cecbb1875893f971b33c4e",
  )


  # org.apache.arrow:arrow-java-root:pom:0.12.0-SNAPSHOT
  # pom.xml got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-format:jar:0.12.0-SNAPSHOT
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-memory:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-plasma:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "org_slf4j_jcl_over_slf4j",
      artifact = "org.slf4j:jcl-over-slf4j:1.7.25",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "f8c32b13ff142a513eeb5b6330b1588dcb2c0461",
  )


  # pom.xml got requested version
  # com.fasterxml.jackson.core:jackson-databind:bundle:2.7.9 wanted version 2.7.0
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  native.maven_jar(
      name = "com_fasterxml_jackson_core_jackson_annotations",
      artifact = "com.fasterxml.jackson.core:jackson-annotations:2.7.9",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "eb356e825cb73da42f7c902a3fe0276fe32b26c8",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.data.logging.protobuf:jar:0.9.44 got requested version
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.classic:jar:0.9.44
  # de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:jar:0.9.44 got requested version
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_logging",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.data.logging:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "3574a72628f59d78ff17210f10de057188043a4c",
  )


  # io.grpc:grpc-core:jar:1.14.0
  native.maven_jar(
      name = "io_grpc_grpc_context",
      artifact = "io.grpc:grpc-context:1.14.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "77252b5f926875891aaae5629e6ab2ef968cd6c6",
  )


  # io.grpc:grpc-protobuf:jar:1.14.0
  native.maven_jar(
      name = "com_google_api_grpc_proto_google_common_protos",
      artifact = "com.google.api.grpc:proto-google-common-protos:1.0.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "86f070507e28b930e50d218ee5b6788ef0dd05e6",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_classic",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.logback.classic:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "f84da242a1988240222f6a06b511a18ff9c64344",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_fasterxml_jackson_core_jackson_databind",
      artifact = "com.fasterxml.jackson.core:jackson-databind:2.7.9",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "a4c0b14c7dd85bdf4d25da074e90a10fa4b9b88b",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
      artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.7.9",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "163a1bd63ace592d2a449023a1c07056b885ff12",
  )


  native.maven_jar(
      name = "com_github_davidmoten_flatbuffers_java",
      artifact = "com.github.davidmoten:flatbuffers-java:1.9.0.1",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "0eaa1b27095bb5127879458bc2ee1b78e00d4a20",
  )


  native.maven_jar(
      name = "redis_clients_jedis",
      artifact = "redis.clients:jedis:2.8.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "a9486e200b2195f31ce9b8779a70a5e2450a73ba",
  )


  native.maven_jar(
      name = "org_apache_commons_commons_pool2",
      artifact = "org.apache.commons:commons-pool2:2.3",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "62a559a025fd890c30364296ece14643ba9c8c5b",
  )


  native.maven_jar(
      name = "commons_io_commons_io",
      artifact = "commons-io:commons-io:2.5",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "2852e6e05fbb95076fc091f6d1780f1f8fe35e0f",
  )


  # pom.xml got requested version
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT
  # io.grpc:grpc-protobuf:jar:1.14.0 got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  # io.grpc:grpc-protobuf-lite:jar:1.14.0 got requested version
  # io.grpc:grpc-core:jar:1.14.0
  # org.apache.arrow.gandiva:arrow-gandiva:jar:0.12.0-SNAPSHOT wanted version 23.0
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_google_guava_guava",
      artifact = "com.google.guava:guava:20.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "89507701249388e1ed5ddcf8c41f4ce1be7831ef",
  )


  # de.huxhorn.lilith:de.huxhorn.lilith.logback.converter-classic:jar:0.9.44
  native.maven_jar(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_converter",
      artifact = "de.huxhorn.lilith:de.huxhorn.lilith.data.converter:0.9.44",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "31e4994f9bc768c43491c3c0d12237ae478d21e3",
  )


  native.maven_jar(
      name = "de_ruedigermoeller_fst",
      artifact = "de.ruedigermoeller:fst:2.47",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "281d390ebed24a3621d3053825affc13d0eead8b",
  )


  # pom.xml got requested version
  # com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:bundle:2.7.9 got requested version
  # org.apache.arrow:arrow-vector:jar:0.12.0-SNAPSHOT
  # com.fasterxml.jackson.core:jackson-databind:bundle:2.7.9 got requested version
  # org.apache.arrow:arrow-jdbc:jar:0.12.0-SNAPSHOT got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version  
  # org.apache.arrow:arrow-tools:jar:0.12.0-SNAPSHOT
  native.maven_jar(
      name = "com_fasterxml_jackson_core_jackson_core",
      artifact = "com.fasterxml.jackson.core:jackson-core:2.7.9",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "09b530cec4fd2eb841ab8e79f19fc7cf0ec487b2",
  )


  # pom.xml got requested version
  # io.grpc:grpc-protobuf:jar:1.14.0 got requested version
  # org.apache.arrow:arrow-flight:jar:0.12.0-SNAPSHOT got requested version
  # io.grpc:grpc-protobuf-lite:jar:1.14.0 got requested version
  # io.grpc:grpc-stub:jar:1.14.0 got requested version
  # io.grpc:grpc-netty:jar:1.14.0
  native.maven_jar(
      name = "io_grpc_grpc_core",
      artifact = "io.grpc:grpc-core:1.14.0",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "526e5be291c96e248789d769c108a084febda07f",
  )


  # com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:bundle:2.7.9
  native.maven_jar(
      name = "org_yaml_snakeyaml",
      artifact = "org.yaml:snakeyaml:1.15",
      repository = "http://mvn.dev.alipay.net:8080/artifactory/repo/",
      sha1 = "3b132bea69e8ee099f416044970997bde80f4ea6",
  )




def generated_java_libraries():
  native.java_library(
      name = "log4j_log4j",
      visibility = ["//visibility:public"],
      exports = ["@log4j_log4j//jar"],
  )


  native.java_library(
      name = "org_slf4j_slf4j_log4j12",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_slf4j_log4j12//jar"],
      runtime_deps = [
          ":log4j_log4j",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "com_typesafe_config",
      visibility = ["//visibility:public"],
      exports = ["@com_typesafe_config//jar"],
  )


  native.java_library(
      name = "org_apache_commons_commons_lang3",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_commons_commons_lang3//jar"],
  )


  native.java_library(
      name = "com_google_code_findbugs_jsr305",
      visibility = ["//visibility:public"],
      exports = ["@com_google_code_findbugs_jsr305//jar"],
  )


  native.java_library(
      name = "io_grpc_grpc_core",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_core//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
          ":com_google_code_gson_gson",
          ":com_google_errorprone_error_prone_annotations",
          ":com_google_guava_guava",
          ":io_grpc_grpc_context",
          ":io_opencensus_opencensus_api",
          ":io_opencensus_opencensus_contrib_grpc_metrics",
      ],
  )


  native.java_library(
      name = "ch_qos_logback_logback_classic",
      visibility = ["//visibility:public"],
      exports = ["@ch_qos_logback_logback_classic//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_core",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "joda_time_joda_time",
      visibility = ["//visibility:public"],
      exports = ["@joda_time_joda_time//jar"],
  )


  native.java_library(
      name = "net_bytebuddy_byte_buddy_agent",
      visibility = ["//visibility:public"],
      exports = ["@net_bytebuddy_byte_buddy_agent//jar"],
  )


  native.java_library(
      name = "io_grpc_grpc_protobuf_lite",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_protobuf_lite//jar"],
      runtime_deps = [
          ":com_google_guava_guava",
          ":io_grpc_grpc_core",
      ],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_converter_classic",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_logback_converter_classic//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_classic",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_converter",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_logging",
          ":de_huxhorn_lilith_de_huxhorn_lilith_logback_classic",
          ":de_huxhorn_sulky_de_huxhorn_sulky_codec",
          ":de_huxhorn_sulky_de_huxhorn_sulky_formatting",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "io_netty_netty_transport",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_transport//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_common",
          ":io_netty_netty_resolver",
      ],
  )


  native.java_library(
      name = "com_carrotsearch_hppc",
      visibility = ["//visibility:public"],
      exports = ["@com_carrotsearch_hppc//jar"],
  )


  native.java_library(
      name = "org_mockito_mockito_core",
      visibility = ["//visibility:public"],
      exports = ["@org_mockito_mockito_core//jar"],
      runtime_deps = [
          ":net_bytebuddy_byte_buddy",
          ":net_bytebuddy_byte_buddy_agent",
          ":org_objenesis_objenesis",
      ],
  )


  native.java_library(
      name = "io_netty_netty_handler_proxy",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_handler_proxy//jar"],
      runtime_deps = [
          ":io_netty_netty_codec",
          ":io_netty_netty_codec_http",
          ":io_netty_netty_codec_socks",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "de_huxhorn_sulky_de_huxhorn_sulky_codec",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_sulky_de_huxhorn_sulky_codec//jar"],
      runtime_deps = [
          ":de_huxhorn_sulky_de_huxhorn_sulky_io",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource//jar"],
  )


  native.java_library(
      name = "com_google_flatbuffers_flatbuffers_java",
      visibility = ["//visibility:public"],
      exports = ["@com_google_flatbuffers_flatbuffers_java//jar"],
  )


  native.java_library(
      name = "com_google_protobuf_protobuf_java",
      visibility = ["//visibility:public"],
      exports = ["@com_google_protobuf_protobuf_java//jar"],
  )


  native.java_library(
      name = "com_h2database_h2",
      visibility = ["//visibility:public"],
      exports = ["@com_h2database_h2//jar"],
  )


  native.java_library(
      name = "org_slf4j_jul_to_slf4j",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_jul_to_slf4j//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "commons_codec_commons_codec",
      visibility = ["//visibility:public"],
      exports = ["@commons_codec_commons_codec//jar"],
  )


  native.java_library(
      name = "org_slf4j_log4j_over_slf4j",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_log4j_over_slf4j//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "commons_cli_commons_cli",
      visibility = ["//visibility:public"],
      exports = ["@commons_cli_commons_cli//jar"],
  )


  native.java_library(
      name = "io_netty_netty_tcnative_boringssl_static",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_tcnative_boringssl_static//jar"],
  )


  native.java_library(
      name = "io_grpc_grpc_stub",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_stub//jar"],
      runtime_deps = [
          ":io_grpc_grpc_core",
      ],
  )


  native.java_library(
      name = "io_grpc_grpc_protobuf",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_protobuf//jar"],
      runtime_deps = [
          ":com_google_api_grpc_proto_google_common_protos",
          ":com_google_guava_guava",
          ":com_google_protobuf_protobuf_java",
          ":io_grpc_grpc_core",
          ":io_grpc_grpc_protobuf_lite",
      ],
  )


  native.java_library(
      name = "io_netty_netty_codec_socks",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec_socks//jar"],
      runtime_deps = [
          ":io_netty_netty_codec",
      ],
  )


  native.java_library(
      name = "io_netty_netty_codec",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_common",
          ":io_netty_netty_resolver",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "io_netty_netty_buffer",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_buffer//jar"],
      runtime_deps = [
          ":io_netty_netty_common",
      ],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_core",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_core//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_core",
          ":de_huxhorn_lilith_de_huxhorn_lilith_sender",
          ":de_huxhorn_sulky_de_huxhorn_sulky_codec",
          ":de_huxhorn_sulky_de_huxhorn_sulky_io",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "io_grpc_grpc_netty",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_netty//jar"],
      runtime_deps = [
          ":com_google_code_findbugs_jsr305",
          ":com_google_code_gson_gson",
          ":com_google_errorprone_error_prone_annotations",
          ":com_google_guava_guava",
          ":io_grpc_grpc_context",
          ":io_grpc_grpc_core",
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_codec_http",
          ":io_netty_netty_codec_http2",
          ":io_netty_netty_codec_socks",
          ":io_netty_netty_common",
          ":io_netty_netty_handler",
          ":io_netty_netty_handler_proxy",
          ":io_netty_netty_resolver",
          ":io_netty_netty_transport",
          ":io_opencensus_opencensus_api",
          ":io_opencensus_opencensus_contrib_grpc_metrics",
      ],
  )


  native.java_library(
      name = "ch_qos_logback_logback_core",
      visibility = ["//visibility:public"],
      exports = ["@ch_qos_logback_logback_core//jar"],
  )


  native.java_library(
      name = "net_bytebuddy_byte_buddy",
      visibility = ["//visibility:public"],
      exports = ["@net_bytebuddy_byte_buddy//jar"],
  )


  native.java_library(
      name = "io_opencensus_opencensus_api",
      visibility = ["//visibility:public"],
      exports = ["@io_opencensus_opencensus_api//jar"],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_sender",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_sender//jar"],
      runtime_deps = [
          ":de_huxhorn_sulky_de_huxhorn_sulky_io",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "com_google_errorprone_error_prone_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_google_errorprone_error_prone_annotations//jar"],
  )


  native.java_library(
      name = "io_netty_netty_resolver",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_resolver//jar"],
      runtime_deps = [
          ":io_netty_netty_common",
      ],
  )


  native.java_library(
      name = "io_opencensus_opencensus_contrib_grpc_metrics",
      visibility = ["//visibility:public"],
      exports = ["@io_opencensus_opencensus_contrib_grpc_metrics//jar"],
      runtime_deps = [
          ":io_opencensus_opencensus_api",
      ],
  )


  native.java_library(
      name = "org_javassist_javassist",
      visibility = ["//visibility:public"],
      exports = ["@org_javassist_javassist//jar"],
  )


  native.java_library(
      name = "org_objenesis_objenesis",
      visibility = ["//visibility:public"],
      exports = ["@org_objenesis_objenesis//jar"],
  )


  native.java_library(
      name = "io_netty_netty_common",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_common//jar"],
  )


  native.java_library(
      name = "junit_junit",
      visibility = ["//visibility:public"],
      exports = ["@junit_junit//jar"],
      runtime_deps = [
          ":org_hamcrest_hamcrest_core",
      ],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_logging_protobuf",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_data_logging_protobuf//jar"],
      runtime_deps = [
          ":com_google_protobuf_protobuf_java",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_logging",
          ":de_huxhorn_sulky_de_huxhorn_sulky_codec",
      ],
  )


  native.java_library(
      name = "de_huxhorn_sulky_de_huxhorn_sulky_io",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_sulky_de_huxhorn_sulky_io//jar"],
  )


  native.java_library(
      name = "org_slf4j_slf4j_api",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_slf4j_api//jar"],
  )


  native.java_library(
      name = "io_netty_netty_codec_http",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec_http//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_common",
          ":io_netty_netty_resolver",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_classic",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_classic//jar"],
      runtime_deps = [
          ":ch_qos_logback_logback_classic",
          ":ch_qos_logback_logback_core",
          ":com_google_protobuf_protobuf_java",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_converter",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_logging",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_logging_protobuf",
          ":de_huxhorn_lilith_de_huxhorn_lilith_logback_appender_multiplex_core",
          ":de_huxhorn_lilith_de_huxhorn_lilith_logback_classic",
          ":de_huxhorn_lilith_de_huxhorn_lilith_logback_converter_classic",
          ":de_huxhorn_lilith_de_huxhorn_lilith_sender",
          ":de_huxhorn_sulky_de_huxhorn_sulky_codec",
          ":de_huxhorn_sulky_de_huxhorn_sulky_formatting",
          ":de_huxhorn_sulky_de_huxhorn_sulky_io",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "org_hamcrest_hamcrest_core",
      visibility = ["//visibility:public"],
      exports = ["@org_hamcrest_hamcrest_core//jar"],
  )


  native.java_library(
      name = "io_netty_netty_handler",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_handler//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "org_jmockit_jmockit",
      visibility = ["//visibility:public"],
      exports = ["@org_jmockit_jmockit//jar"],
  )


  native.java_library(
      name = "de_huxhorn_sulky_de_huxhorn_sulky_formatting",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_sulky_de_huxhorn_sulky_formatting//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "io_netty_netty_codec_http2",
      visibility = ["//visibility:public"],
      exports = ["@io_netty_netty_codec_http2//jar"],
      runtime_deps = [
          ":io_netty_netty_buffer",
          ":io_netty_netty_codec",
          ":io_netty_netty_codec_http",
          ":io_netty_netty_common",
          ":io_netty_netty_handler",
          ":io_netty_netty_resolver",
          ":io_netty_netty_transport",
      ],
  )


  native.java_library(
      name = "net_lingala_zip4j_zip4j",
      visibility = ["//visibility:public"],
      exports = ["@net_lingala_zip4j_zip4j//jar"],
  )


  native.java_library(
      name = "org_ini4j_ini4j",
      visibility = ["//visibility:public"],
      exports = ["@org_ini4j_ini4j//jar"],
  )


  native.java_library(
      name = "org_ow2_asm_asm",
      visibility = ["//visibility:public"],
      exports = ["@org_ow2_asm_asm//jar"],
  )


  native.java_library(
      name = "com_google_code_gson_gson",
      visibility = ["//visibility:public"],
      exports = ["@com_google_code_gson_gson//jar"],
  )


  native.java_library(
      name = "org_slf4j_jcl_over_slf4j",
      visibility = ["//visibility:public"],
      exports = ["@org_slf4j_jcl_over_slf4j//jar"],
      runtime_deps = [
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_annotations",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_annotations//jar"],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_logging",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_data_logging//jar"],
      runtime_deps = [
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource",
          ":de_huxhorn_sulky_de_huxhorn_sulky_formatting",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_logback_classic",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_logback_classic//jar"],
      runtime_deps = [
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_eventsource",
          ":de_huxhorn_lilith_de_huxhorn_lilith_data_logging",
          ":de_huxhorn_sulky_de_huxhorn_sulky_formatting",
          ":org_slf4j_slf4j_api",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_databind",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_databind//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_annotations",
          ":com_fasterxml_jackson_core_jackson_core",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_dataformat_jackson_dataformat_yaml//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_core",
          ":org_yaml_snakeyaml",
      ],
  )


  native.java_library(
      name = "io_grpc_grpc_context",
      visibility = ["//visibility:public"],
      exports = ["@io_grpc_grpc_context//jar"],
  )


  native.java_library(
      name = "com_google_api_grpc_proto_google_common_protos",
      visibility = ["//visibility:public"],
      exports = ["@com_google_api_grpc_proto_google_common_protos//jar"],
  )


  native.java_library(
      name = "com_github_davidmoten_flatbuffers_java",
      visibility = ["//visibility:public"],
      exports = ["@com_github_davidmoten_flatbuffers_java//jar"],
  )


  native.java_library(
      name = "redis_clients_jedis",
      visibility = ["//visibility:public"],
      exports = ["@redis_clients_jedis//jar"],
      runtime_deps = [
          ":org_apache_commons_commons_pool2",
      ],
  )


  native.java_library(
      name = "org_apache_commons_commons_pool2",
      visibility = ["//visibility:public"],
      exports = ["@org_apache_commons_commons_pool2//jar"],
  )


  native.java_library(
      name = "com_google_guava_guava",
      visibility = ["//visibility:public"],
      exports = ["@com_google_guava_guava//jar"],
  )


  native.java_library(
      name = "de_huxhorn_lilith_de_huxhorn_lilith_data_converter",
      visibility = ["//visibility:public"],
      exports = ["@de_huxhorn_lilith_de_huxhorn_lilith_data_converter//jar"],
  )


  native.java_library(
      name = "de_ruedigermoeller_fst",
      visibility = ["//visibility:public"],
      exports = ["@de_ruedigermoeller_fst//jar"],
      runtime_deps = [
          ":com_fasterxml_jackson_core_jackson_core",
          ":org_javassist_javassist",
          ":org_objenesis_objenesis",
      ],
  )


  native.java_library(
      name = "com_fasterxml_jackson_core_jackson_core",
      visibility = ["//visibility:public"],
      exports = ["@com_fasterxml_jackson_core_jackson_core//jar"],
  )


  native.java_library(
      name = "org_yaml_snakeyaml",
      visibility = ["//visibility:public"],
      exports = ["@org_yaml_snakeyaml//jar"],
  )


  native.java_library(
      name = "net_java_dev_jna_jna",
      visibility = ["//visibility:public"],
      exports = ["@net_java_dev_jna_jna//jar"],
  )


  native.java_library(
      name = "org_mockito_mockito_all",
      visibility = ["//visibility:public"],
      exports = ["@org_mockito_mockito_all//jar"],
  )


  native.java_library(
      name = "com_beust_jcommander",
      visibility = ["//visibility:public"],
      exports = ["@com_beust_jcommander//jar"],
  )

  native.java_library(
      name = "org_testng_testng",
      visibility = ["//visibility:public"],
      exports = ["@org_testng_testng//jar"],
  ) 
