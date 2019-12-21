# Ray Streaming

1. Build streaming java
    * build ray
        * `sh build.sh -l java`
        * `cd java && mvn clean install -Dmaven.test.skip=true`
    * build streaming
        * `cd ray/streaming/java && bazel build all_modules`
        * `mvn clean install -Dmaven.test.skip=true`

2. Build ray will build ray streaming python.

3. Run examples
```bash
# c++ test
cd streaming/ && bazel test ...
sh src/test/run_streaming_queue_test.sh
cd ..

# python test
cd python/ray/streaming/
pushd examples
python simple.py --input-file toy.txt
popd
pushd tests
pytest .
popd
```