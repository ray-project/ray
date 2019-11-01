### Build Ray Streaming

1. required tools
* jdk8 ([download](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html))
* maven ([download](https://maven.apache.org/download.cgi))
* bazel 0.21.0 ([download](https://docs.bazel.build/versions/master/install-os-x.html#install-with-installer-mac-os-x))


2. build ray
* go to folder ray
* run `sh build.sh -l java`
* go to ray/java
* run `mvn clean install -Dmaven.test.skip=true`

3. build ray streaming java
* go to folder ray/streaming/java
* run `bazel build all_modules`
* run `mvn clean install -Dmaven.test.skip=true`

4. build ray streaming python
`pip install -e . --verbose`