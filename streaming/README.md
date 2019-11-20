### Build Ray Streaming

1. Required tools
    * jdk8 ([download](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html))
    * maven ([download](https://maven.apache.org/download.cgi))
    * bazel 1.0 ([download](https://docs.bazel.build/versions/master/install-os-x.html#install-with-installer-mac-os-x))

2. Build ray streaming java
    * build ray
        * go to ray
        * run `sh build.sh -l java`
        * go to ray/java
        * run `mvn clean install -Dmaven.test.skip=true`
    * build streaming
        * go to folder ray/streaming/java
        * run `bazel build all_modules`
        * run `mvn clean install -Dmaven.test.skip=true`

3. Build ray streaming python.

Ray streaming python is packaged with ray, building ray will build python streaming too.
```bash
cd python
pip install -e .
```
When only c++/cython files change:
```bash
export PYTHON_BIN_PATH=`which python` 
bazel build //:ray_pkg
```