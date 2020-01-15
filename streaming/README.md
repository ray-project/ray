# Ray Streaming

1. Build streaming java
    * build ray
        * `bazel build //java:gen_maven_deps`
        * `cd java && mvn clean install -Dmaven.test.skip=true && cd ..`
    * build streaming
        * `bazel build //streaming/java:gen_maven_deps`
        * `mvn clean install -Dmaven.test.skip=true`

2. Build ray python will build ray streaming python.

3. Run examples
    ```bash
    # c++ test
    cd streaming/ && bazel test ...
    sh src/test/run_streaming_queue_test.sh
    cd ..

    # python test
    pushd python/ray/streaming/
    pushd examples
    python simple.py --input-file toy.txt
    popd
    pushd tests
    pytest .
    popd
    popd

    # java test
    cd streaming/java/streaming-runtime
    mvn test
    ```