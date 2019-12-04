# Ray Streaming

1. Build streaming java
    * build ray
        * `sh build.sh -l java`
        * `cd java && mvn clean install -Dmaven.test.skip=true`
    * build streaming
        * `cd ray/streaming/java && bazel build all_modules`
        * `mvn clean install -Dmaven.test.skip=true`

2. Build ray streaming python.
Ray streaming python is packaged with ray, building ray will build python streaming too.
```bash
cd python
pip install -e .
```
When only c++/cython files change:
```bash
bazel build //:ray_pkg
```

3. Run examples
```bash
cd python/ray/streaming/
pushd examples
python simple.py --input-file toy.txt
popd
pushd tests
pytest .
popd
```