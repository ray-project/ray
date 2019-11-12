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

* `bazel build //streaming/java:gen_maven_deps` will generate flatbuffer java files in
 `streaming/java/streaming-runtime/src/main/java/org/ray/streaming/runtime/generated`.
 Or you can generate by `rm -rf /tmp/fbs && flatc -o /tmp/fbs -j streaming/src/format/streaming.fbs`, 
 then modify package name and copy to
  `streaming/java/streaming-runtime/src/main/java/org/ray/streaming/runtime/generated`

4. build ray streaming python
* cython
```bash
export PYTHON_BIN_PATH=`which python` 
bazel build //streaming/python:_streaming
```
* flatbuffer
    * `rm -rf /tmp/fbs && flatc -o /tmp/fbs -p streaming/src/format/streaming.fbs`, then copy files
    * Or use bazel rule
        * `rm -rf bazel-bin/streaming/python/cp_streaming_py_fbs_generated.out`
        * `bazel build //streaming/python:cp_streaming_py_fbs_generated`